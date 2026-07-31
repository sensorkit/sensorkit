# SPDX-License-Identifier: Apache-2.0
"""UDL data publishers: SkyImagery upload and EOObservation posting.

Each publisher delivers one UDL record type and is enabled by the presence of
its block under ``UDLConfig.publish``. SkyImagery is frame-driven (fed from the
program's DataGraph sink); EOObservation is keyword-driven (fed from the senpai
module's published SenpaiResults) and never touches the frame stream.
"""

from __future__ import annotations

import asyncio
import io
import json
import math
import re
import time
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from sensorkit.backend.base import SpecialProperty, Subject
from sensorkit.common.keyword import get_keyword_info, validate_keyword_json
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.models import Detection, SenpaiResult
from sensorkit.udl.models import (
    EOObservationPublishConfig,
    UDLAPIConfig,
    UDLEndpointConfig,
)

if TYPE_CHECKING:
    from unifieddatalibrary.types import CollectRequestFull

    from sensorkit.astro.common import SitePosition
    from sensorkit.udl.program import UDLProgram


_LIGHT_AU_PER_DAY = 173.1446  # speed of light in AU/day


def _udl_ts(dt: datetime) -> str:
    """Format a datetime as UDL expects: ISO 8601 UTC with trailing 'Z' (no offset)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Publisher(ABC):
    """Base class for UDL data publishers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Identifies this publisher in logs and error messages."""

    async def start(self) -> None:
        """Start any background machinery this publisher needs."""

    @abstractmethod
    async def publish(
        self, context: dict, data: bytes, request: CollectRequestFull | None = None
    ) -> None:
        """Deliver one frame to the destination.

        Args:
            context: DataGraph sink metadata (task_id, frame_num, FileInfo, …).
            data: Raw FITS bytes from the sink's async stream.
            request: The CollectRequest the frame was collected for.
        """

    async def close(self) -> None:
        """Release connections and resources."""


# ── SkyImagery ──


class SkyImageryPublisher(Publisher):
    """Uploads FITS frames to the UDL SkyImagery filedrop.

    Each frame is zipped together with a SkyImagery metadata JSON and POSTed as
    raw ``application/zip`` — the payload proven against the live UDL filedrop.
    Raises on upload failure so the program can track delivered counts.
    """

    def __init__(self, program: UDLProgram):
        self._program = program
        self._config = program.config.publish.sky_imagery

    @property
    def name(self) -> str:
        return "sky_imagery"

    async def publish(
        self, context: dict, data: bytes, request: CollectRequestFull | None = None
    ) -> None:
        """Build and upload SkyImagery for a collected frame."""
        program = self._program

        # Extract context values
        info = context.get(FileInfo)
        filename = info.path.name if info else f"{request.id}_{context.get('frame_num', 0)}.fits"

        date_obs = context.get("date_obs")
        exp_start_time = datetime.fromisoformat(date_obs) if date_obs else datetime.now(UTC)

        exposure_time = context.get("exptime")
        exp_end_time = (
            exp_start_time + timedelta(seconds=float(exposure_time))
            if exposure_time is not None
            else exp_start_time
        )

        # sequenceId must be >= 1 (UDL requirement)
        frame_num = context.get("frame_num", 0)
        sequence_id = frame_num + 1

        image_set_length = request.num_frames or 1

        # Build SkyImagery metadata
        metadata = {
            "classificationMarking": request.classification_marking,
            "idSensor": program.config.api.id_sensor,
            "satNo": request.sat_no,
            "expStartTime": _udl_ts(exp_start_time),
            "expEndTime": _udl_ts(exp_end_time),
            "imageSetLength": image_set_length,
            "sequenceId": sequence_id,
            "frameWidthPixels": context.get("image_width"),
            "frameHeightPixels": context.get("image_height"),
            "pixelBitDepth": context.get("bits_per_pixel"),
            "filename": filename,
            "filesize": len(data),
            "source": program.config.api.source,
            "origin": request.origin,
            "dataMode": request.data_mode or "TEST",
            "imageType": context.get("image_type") or self._config.image_type,
        }

        # imageSetId groups multiple frames of one collect into a set. Per UDL:
        # a single-image set doesn't need an imageSetId, so only emit it when
        # the set has more than one frame.
        if image_set_length > 1:
            metadata["imageSetId"] = request.id

        if program._site:
            metadata["senlat"] = program._site.latitude_degrees
            metadata["senlon"] = program._site.longitude_degrees
            metadata["senalt"] = program._site.altitude_km
        else:
            logger.warning(
                f"Task ({request.id}): publishing SkyImagery without sensor location "
                f"(no SitePosition from controller {program.config.controller})"
            )

        # Remove None values
        metadata = {k: v for k, v in metadata.items() if v is not None}

        metadata_bytes = json.dumps(metadata).encode()
        metadata_fname = f"{Path(filename).stem}_skyimagery.json"

        # Save locally if configured
        if self._config.save_path:
            await asyncio.to_thread(
                self._save_archive_locally_sync,
                request.id,
                filename,
                data,
                metadata_fname,
                metadata_bytes,
            )

        # Create ZIP in memory and upload
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(metadata_fname, metadata_bytes)
            zf.writestr(filename, data)
        zip_buffer.seek(0)

        await self._upload_skyimagery_zip(zip_buffer.getvalue())
        logger.debug(
            f"task ({request.id}) uploaded skyimagery ({sequence_id}/{image_set_length})"
        )

    def _save_archive_locally_sync(
        self,
        task_id: str,
        data_fname: str,
        data: bytes,
        metadata_fname: str,
        metadata_bytes: bytes,
    ) -> None:
        """Save imagery archive to local filesystem."""
        try:
            save_path = Path(self._config.save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            zip_path = save_path / f"{Path(data_fname).stem}.zip"

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(data_fname, data)
                zf.writestr(metadata_fname, metadata_bytes)

            logger.debug(f"task ({task_id}) saved archive to {zip_path}")
        except Exception as e:
            logger.warning(f"Task ({task_id}) failed to save archive locally: {e}")

    def _upload_endpoint(self) -> UDLEndpointConfig:
        """Effective endpoint settings for SkyImagery uploads.

        The separate upload endpoint when configured, the primary otherwise.
        """
        return self._program.config.api.upload or self._program.config.api

    def _imagery_filedrop_url(self) -> str | None:
        """Resolve the SkyImagery filedrop URL.

        UDL serves the imagery filedrop on a dedicated subdomain. The SDK's
        sky_imagery.upload_zip() only targets it correctly for the production
        default; when base_url is overridden (e.g. the test environment) it
        POSTs to '{base_url}/filedrop/udl-skyimagery', which 404s. We therefore
        derive the correct host and POST the ZIP ourselves.

        Returns None for hosts we don't recognise (custom UDL-compliant
        endpoints), signalling the caller to fall back to the SDK's upload_zip().
        """
        base = self._upload_endpoint().base_url
        if not base:
            # SDK default → production UDL
            return "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"

        host = base.rstrip("/").split("://", 1)[-1]
        if host == "test.unifieddatalibrary.com":
            return "https://imagery-test.unifieddatalibrary.com/filedrop/udl-skyimagery"
        if host == "unifieddatalibrary.com":
            return "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"
        return None

    async def _upload_skyimagery_zip(self, zip_bytes: bytes) -> None:
        """Upload a SkyImagery ZIP to the UDL imagery filedrop.

        POSTs the raw ZIP as application/zip with Basic auth (or client cert)
        to the imagery subdomain — the approach proven against the live UDL
        filedrop. Falls back to the SDK when the filedrop host can't be derived
        (a custom base_url).
        """
        program = self._program
        endpoint = self._upload_endpoint()

        url = self._imagery_filedrop_url()
        if url is None:
            # The UDL filedrop contract is "a zip is all that's required" — no
            # multipart filename. We rely on that, and our integration tests
            # assert UDL-compliant endpoints keep parity by accepting the same
            # payload.
            await program.upload_client.sky_imagery.upload_zip(
                file=zip_bytes,
                timeout=endpoint.upload_timeout,
                extra_headers=program._upload_headers,
            )
            return

        if endpoint.use_certs:
            client_kwargs = {
                "cert": (endpoint.client_cert, endpoint.client_key),
                "verify": endpoint.client_verify,
            }
        else:
            client_kwargs = {"auth": (program._upload_username, program._upload_password)}

        async with httpx.AsyncClient(timeout=endpoint.upload_timeout, **client_kwargs) as http:
            resp = await http.post(
                url,
                content=zip_bytes,
                headers={"Content-Type": "application/zip"},
            )
            if resp.status_code >= 300:
                raise RuntimeError(
                    f"SkyImagery upload to {url} failed: HTTP {resp.status_code} {resp.text}"
                )


# ── EOObservation ──


@dataclass
class RequestSnapshot:
    """The CollectRequest fields an EOObservation needs, captured at request time.

    Snapshots outlive the program's task-reference cleanup so late-arriving
    SenpaiResults (SENPAI runs take minutes) can still correlate.
    """

    request_id: str
    classification_marking: str | None
    data_mode: str | None
    origin: str | None
    task_id: str | None

    @classmethod
    def from_request(cls, request: CollectRequestFull) -> RequestSnapshot:
        return cls(
            request_id=request.id,
            classification_marking=request.classification_marking,
            data_mode=request.data_mode,
            origin=request.origin,
            task_id=request.task_id,
        )


# Track-mode → Detection.kind values that represent the satellite. RATE-tracked
# frames image the target as a point source (stars streak); sidereal frames
# image it as a confirmed streak. Unconfirmed streak candidates and sidereal
# point detections (catalog-unmatched noise) are never posted.
_SATELLITE_KINDS = {
    "RATE": ("point",),
    "SIDEREAL": ("streak",),
}


def _add_annual_aberration(ra_deg: float, dec_deg: float, when: datetime) -> tuple[float, float]:
    """Add annual aberration to a star-relative position, in degrees.

    Earth's ~30 km/s barycentric velocity displaces starlight by up to ~20.5",
    so a plate solve — which fits the frame onto undisplaced catalog positions
    — subtracts that shift from everything it measures. A satellite shares
    Earth's orbital velocity and so was never displaced by it, leaving the
    solved target short by the same ~20.5". Adding it back recovers the
    geocentric direction UDL ingests.

    Diurnal aberration needs no such fix: the sensor's rotation velocity
    displaces the target and the stars alike, so the solve removes it
    correctly. Light time is likewise left in, per UDL.

    References:
        Vallado, Fundamentals of Astrodynamics and Applications, 5th ed., Eqn (4-25).
        Explanatory Supplement to the Astronomical Almanac, Eqn (3.253-1).
    """
    # Julian centuries since J2000. UTC stands in for UT1: DUT1 (< 0.9 s) moves
    # the correction by well under a microarcsecond.
    julian_date = when.timestamp() / 86400.0 + 2440587.5
    t = (julian_date - 2451545.0) / 36525.0
    sun_longitude = math.radians(280.460 + 36000.771285 * t)
    ra, dec = math.radians(ra_deg), math.radians(dec_deg)

    # Earth's barycentric velocity (AU/day) from the Sun's mean longitude, with
    # the obliquity folded into the y/z components.
    x_dot = 0.0172 * math.sin(sun_longitude)
    y_dot = -0.0158 * math.cos(sun_longitude)
    z_dot = -0.0068 * math.cos(sun_longitude)

    d_ra = (-x_dot * math.sin(ra) + y_dot * math.cos(ra)) / (_LIGHT_AU_PER_DAY * math.cos(dec))
    d_dec = (
        -x_dot * math.cos(ra) * math.sin(dec)
        - y_dot * math.sin(ra) * math.sin(dec)
        + z_dot * math.cos(dec)
    ) / _LIGHT_AU_PER_DAY

    return ra_deg + math.degrees(d_ra), dec_deg + math.degrees(d_dec)


def convert_wcs_observation(
    result: SenpaiResult, detection: Detection
) -> tuple[str, float, float]:
    """Convert a WCS-fit detection to UDL's EO observation conventions.

    Photon arrival time: the detection centroid is the mid-exposure image of
    the object, so its photons arrived at DATE-OBS plus half the exposure.
    Light time is not corrected (UDL's convention).

    Position: annual aberration is added back to the star-relative plate-solve
    position; see _add_annual_aberration.

    Returns (ob_time, ra, declination) for the record.
    """
    ob_time = result.timestamp
    if result.exposure_time_seconds:
        ob_time = ob_time + timedelta(seconds=result.exposure_time_seconds / 2.0)

    ra, declination = _add_annual_aberration(detection.ra, detection.dec, ob_time)
    return _udl_ts(ob_time), ra, declination


def build_eo_observations(
    result: SenpaiResult,
    snapshot: RequestSnapshot,
    *,
    site: SitePosition | None,
    api: UDLAPIConfig,
    config: EOObservationPublishConfig,
) -> list[dict]:
    """Map a solved SenpaiResult onto UDL EOObservation records (one per satellite detection).

    Records use the SDK's snake_case body keys (aliased to camelCase on the
    wire) with None values stripped. Observations are raw against the WCS: no
    catalog correlation is performed, so every record is an uncorrelated track
    (uct=true, no satNo).
    """
    kinds = _SATELLITE_KINDS.get(result.track_mode, ())
    if not kinds:
        return []

    records: list[dict] = []
    for det in result.detections:
        if det.kind not in kinds or det.ra is None or det.dec is None:
            continue

        ob_time, ra, declination = convert_wcs_observation(result, det)
        record = {
            "classification_marking": snapshot.classification_marking,
            "data_mode": snapshot.data_mode or "TEST",
            "ob_time": ob_time,
            "source": api.source,
            "id_sensor": api.id_sensor,
            "orig_sensor_id": api.id_sensor,
            "ra": ra,
            "declination": declination,
            "reference_frame": "J2000",
            "uct": True,
            "track_id": snapshot.request_id,
            "task_id": snapshot.task_id,
            "origin": snapshot.origin,
            "exp_duration": result.exposure_time_seconds,
            "descriptor": Path(result.file_path).name,
        }

        if site:
            record["senlat"] = site.latitude_degrees
            record["senlon"] = site.longitude_degrees
            record["senalt"] = site.altitude_km

        for band in config.mag_bands:
            if det.calibrated_magnitudes and band in det.calibrated_magnitudes:
                record["mag"] = det.calibrated_magnitudes[band]
                if det.magnitude_errs and band in det.magnitude_errs:
                    record["mag_unc"] = det.magnitude_errs[band]
                break

        records.append({k: v for k, v in record.items() if v is not None})

    return records


class EOObservationPublisher(Publisher):
    """Posts senpai satellite detections to UDL as EOObservations.

    Keyword-driven: consumes SenpaiResult keywords from the backend stream (a
    cross-entity wildcard subscription, so the senpai entity needs no naming
    here), correlates each result to its CollectRequest by task_id, and POSTs
    one createBulk batch per frame. Frames senpai processed that were not
    UDL-tasked (no task_id) are dropped.
    """

    _SNAPSHOT_TTL_S = 6 * 3600.0
    _SEEN_CAP = 512
    _WATCHDOG_INTERVAL_S = 600.0
    _RECONNECT_DELAY_S = 5.0

    def __init__(self, program: UDLProgram):
        self._program = program
        self._config = program.config.publish.eo_observation

        # CollectRequest snapshots by task id, with insertion timestamps for
        # TTL pruning.
        self._snapshots: dict[str, tuple[RequestSnapshot, float]] = {}

        # Recently handled result file_paths (transport redelivery dedupe).
        self._seen: dict[str, None] = {}

        self.results_received = 0
        self.posted = 0

        self._intake: asyncio.Task | None = None
        self._watchdog: asyncio.Task | None = None

        # Set once the intake consumer is established, cleared while it reconnects.
        self.intake_ready = asyncio.Event()

    @property
    def name(self) -> str:
        return "eo_observation"

    async def publish(
        self, context: dict, data: bytes, request: CollectRequestFull | None = None
    ) -> None:
        """No-op: EOObservations are keyword-driven, not frame-driven."""

    def note_request(self, request: CollectRequestFull) -> RequestSnapshot:
        """Snapshot a CollectRequest for later correlation by task_id."""
        snapshot = RequestSnapshot.from_request(request)
        now = time.monotonic()
        self._snapshots[request.id] = (snapshot, now)

        for task_id, (_, added) in list(self._snapshots.items()):
            if now - added > self._SNAPSHOT_TTL_S:
                self._snapshots.pop(task_id, None)

        return snapshot

    async def start(self) -> None:
        await self._warn_if_senpai_missing()
        self._intake = asyncio.create_task(self._intake_loop())
        self._watchdog = asyncio.create_task(self._watchdog_loop())

    async def close(self) -> None:
        for task in (self._intake, self._watchdog):
            if task and not task.done():
                task.cancel()

    async def _warn_if_senpai_missing(self) -> None:
        """Warn loudly when EO posting is enabled but senpai isn't configured."""
        try:
            kit = self._program.program.sensorkit()
            entries = await kit.backend.key_value().get_all(deep=True)
            if not any(entry.key.prop == "SenpaiConfig" for entry in entries):
                logger.warning(
                    "eo_observation publishing is enabled but no senpai config "
                    "section was found; no SenpaiResults will arrive until the "
                    "senpai service is configured and running"
                )
        except Exception as e:
            logger.debug(f"senpai presence check failed: {e}")

    async def _watchdog_loop(self) -> None:
        """Warn periodically while frames flow but no SenpaiResult has ever arrived."""
        while True:
            await asyncio.sleep(self._WATCHDOG_INTERVAL_S)
            if self.results_received > 0:
                return
            if self._program.frames_seen > 0:
                logger.warning(
                    f"eo_observation publishing is enabled and {self._program.frames_seen} "
                    f"frame(s) have flowed, but no SenpaiResults have been received — "
                    f"is the senpai service running?"
                )

    def _durable_name(self) -> str:
        """Sanitise the program entity into a valid NATS durable consumer name."""
        entity = str(self._program.program.entity)
        return "udl-eo-" + re.sub(r"[^A-Za-z0-9_-]", "_", entity)

    async def _intake_loop(self) -> None:
        """Consume SenpaiResult keywords from all entities and post EOObservations.

        Uses a durable wildcard subscription: the senpai entity needs no naming
        (senpai is single-instance), and results published while this service
        is down are delivered on reconnect.
        """
        info = get_keyword_info(SenpaiResult)
        keyword_key = info.key if info else SenpaiResult.__name__

        while True:
            try:
                kit = self._program.program.sensorkit()
                subject = Subject(path=(), prop=SpecialProperty.ALL_DESCENDANTS)
                consumer = await kit.backend.impl.stream_consume(
                    subject, durable_name=self._durable_name()
                )
                logger.debug("EO intake consuming SenpaiResults")
                self.intake_ready.set()

                async for msg in consumer:
                    if msg.subject.prop != keyword_key:
                        continue
                    try:
                        result = validate_keyword_json(keyword_key, msg.data)
                    except Exception:
                        logger.debug("Ignoring unparseable SenpaiResult payload")
                        continue
                    if not isinstance(result, SenpaiResult):
                        continue
                    try:
                        await self._handle_result(result)
                    except Exception as e:
                        logger.warning(f"Error handling SenpaiResult: {e}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    f"EO intake stream error: {e}; reconnecting in "
                    f"{self._RECONNECT_DELAY_S:.0f}s"
                )
                await asyncio.sleep(self._RECONNECT_DELAY_S)
            finally:
                self.intake_ready.clear()

    def _mark_seen(self, file_path: str) -> bool:
        """Record a handled result; returns False when it was already seen."""
        if file_path in self._seen:
            return False
        self._seen[file_path] = None
        while len(self._seen) > self._SEEN_CAP:
            self._seen.pop(next(iter(self._seen)))
        return True

    async def _handle_result(self, result: SenpaiResult) -> None:
        self.results_received += 1
        name = Path(result.file_path).name

        if not self._mark_seen(result.file_path):
            logger.debug(f"EO: duplicate SenpaiResult for {name}; skipping")
            return

        if result.task_id is None:
            logger.debug(f"EO: SenpaiResult for {name} is untasked; skipping")
            return

        if not result.solved:
            logger.debug(f"EO: SenpaiResult for {name} is unsolved; skipping")
            return

        if self._config.sequence_only and not result.from_sequence:
            logger.debug(
                f"EO: SenpaiResult for {name} is per-frame (not sequence-derived); skipping"
            )
            return

        entry = self._snapshots.get(result.task_id)
        snapshot = entry[0] if entry else None
        if snapshot is None:
            request = self._program.tasks.get(result.task_id)
            if request is not None:
                snapshot = self.note_request(request)
            else:
                logger.warning(
                    f"EO: no CollectRequest known for task_id {result.task_id}; "
                    f"dropping SenpaiResult for {name}"
                )
                return

        records = build_eo_observations(
            result,
            snapshot,
            site=self._program._site,
            api=self._program.config.api,
            config=self._config,
        )
        if not records:
            logger.debug(f"EO: no postable satellite detections in {name}")
            return

        if self._config.save_path:
            await asyncio.to_thread(self._save_locally_sync, name, records)

        await self._program.upload_client.observations.eo_observations.create_bulk(
            body=records,
            extra_headers=self._program._upload_headers,
        )
        self.posted += len(records)
        logger.info(
            f"task ({snapshot.request_id}): posted {len(records)} EO observation(s) "
            f"for {name}"
        )

    def _save_locally_sync(self, frame_name: str, records: list[dict]) -> None:
        """Save posted EOObservation records to the local filesystem."""
        try:
            save_path = Path(self._config.save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            out_path = save_path / f"{Path(frame_name).stem}_eoobs.json"
            out_path.write_text(json.dumps(records, indent=2))
            logger.debug(f"saved EO observations to {out_path}")
        except Exception as e:
            logger.warning(f"Failed to save EO observations locally: {e}")
