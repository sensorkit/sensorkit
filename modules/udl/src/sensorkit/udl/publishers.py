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
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from loguru import logger
from pydantic import TypeAdapter, ValidationError
from unifieddatalibrary.types.observations.eo_observation_unvalidated_publish_params import (
    Body as EOObservationRecord,
)
from unifieddatalibrary.types.sky_imagery_get_response import (
    SkyImageryGetResponse as SkyImageryRecord,
)

from sensorkit.backend.base import SpecialProperty, Subject
from sensorkit.common.keyword import get_keyword_info, validate_keyword_json
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.models import Detection, SenpaiResult
from sensorkit.udl.models import (
    UDLEndpointConfig,
)

if TYPE_CHECKING:
    from unifieddatalibrary.types import CollectRequestFull

    from sensorkit.udl.program import UDLProgram


_LIGHT_AU_PER_DAY = 173.1446  # speed of light in AU/day

# Both publishers build their records as plain dicts and both deliver through
# filedrop ingests, which accept payloads without reporting validation results
# synchronously — so UDL-schema compliance is enforced here, each record
# validated against the SDK's transcription of the schema before anything
# ships. (EO records use the SDK's snake_case body keys; SkyImagery metadata
# uses camelCase wire keys, which the response model's aliases accept.)
_EO_OBSERVATION_VALIDATOR = TypeAdapter(EOObservationRecord)
_SKY_IMAGERY_VALIDATOR = TypeAdapter(SkyImageryRecord)


def _to_udl_timestamp(dt: datetime) -> str:
    """Format a datetime as UDL expects: ISO 8601 UTC with trailing 'Z' (no offset)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SkyImageryPublisher:
    """Uploads FITS frames to the UDL SkyImagery filedrop.

    Each frame is zipped together with a SkyImagery metadata JSON and POSTed as
    raw ``application/zip`` — the payload proven against the live UDL filedrop.
    Raises on upload failure so the program can track delivered counts.
    """

    def __init__(self, program: UDLProgram):
        self._program = program
        self._config = program.config.publish.sky_imagery

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
            "origSensorId": program.config.api.id_sensor,
            "satNo": request.sat_no,
            "expStartTime": _to_udl_timestamp(exp_start_time),
            "expEndTime": _to_udl_timestamp(exp_end_time),
            "imageSetLength": image_set_length,
            "sequenceId": sequence_id,
            "frameWidthPixels": context.get("image_width"),
            "frameHeightPixels": context.get("image_height"),
            "pixelBitDepth": context.get("bits_per_pixel"),
            "filename": filename,
            "filesize": len(data),
            "source": program.config.api.source,
            "origin": request.origin,
            "dataMode": request.data_mode,
            "imageType": context.get("image_type") or self._config.image_type,
            # Correlate to the originating tasking: idRequest = this
            # CollectRequest; taskId echoes the request's own taskId.
            # Not yet in the published UDL schema (issue #12) — UDL's filedrop
            # tolerates unknown fields until Bluestaq's addition lands.
            "idRequest": request.id,
            "taskId": request.task_id,
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

        # UDL-schema compliance before shipping (see module header). Raises so
        # the program counts this frame as undelivered.
        _SKY_IMAGERY_VALIDATOR.validate_python(metadata)

        metadata_bytes = json.dumps(metadata).encode()
        metadata_fname = f"{Path(filename).stem}_skyimagery.json"

        # Save locally if configured
        if self._config.save_path:
            await asyncio.to_thread(
                self._save_locally,
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

        await self._upload(zip_buffer.getvalue())
        logger.debug(
            f"task {request.id}: uploaded SkyImagery {sequence_id}/{image_set_length}"
        )

    def _save_locally(
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
            out_path = save_path / f"{Path(data_fname).stem}.zip"

            with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(data_fname, data)
                zf.writestr(metadata_fname, metadata_bytes)

            logger.debug(f"saved SkyImagery locally to {out_path} for task {task_id}")
        except Exception as e:
            logger.warning(f"Failed to save SkyImagery locally for task {task_id}: {e}")

    def _resolve_upload_url(self, endpoint: UDLEndpointConfig) -> str:
        """SkyImagery filedrop URL for the endpoint.

        UDL proper serves the imagery filedrop on a dedicated subdomain, so the
        known UDL hosts are mapped there. Any other configured base_url is
        presumed to serve the UDL-compliant route itself.
        """
        base = endpoint.base_url
        if not base:
            # No override → production UDL
            return "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"

        host = base.rstrip("/").split("://", 1)[-1]
        if host == "test.unifieddatalibrary.com":
            return "https://imagery-test.unifieddatalibrary.com/filedrop/udl-skyimagery"
        if host == "unifieddatalibrary.com":
            return "https://imagery.unifieddatalibrary.com/filedrop/udl-skyimagery"
        return base.rstrip("/") + "/filedrop/udl-skyimagery"

    async def _upload(self, zip_bytes: bytes) -> None:
        """Upload a SkyImagery ZIP to the imagery filedrop.

        POSTs the raw ZIP as application/zip with Basic auth (or client cert) —
        the payload proven against the live UDL filedrop ("a zip is all that's
        required"), which UDL-compliant endpoints are presumed to accept alike.
        """
        program = self._program
        endpoint = program.config.api.upload or program.config.api
        url = self._resolve_upload_url(endpoint)

        if endpoint.use_certs:
            client_kwargs = {
                "cert": (endpoint.client_cert, endpoint.client_key),
                "verify": endpoint.client_verify,
            }
        else:
            client_kwargs = {"verify": endpoint.client_verify}
            if program._upload_username:
                client_kwargs["auth"] = (
                    program._upload_username,
                    program._upload_password,
                )

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


def _to_udl_eoobservation(
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
    return _to_udl_timestamp(ob_time), ra, declination


class EOObservationPublisher:
    """Posts senpai satellite detections to UDL as EOObservations.

    Keyword-driven: consumes SenpaiResult keywords from the backend stream (a
    cross-entity wildcard subscription, so the senpai entity needs no naming
    here), correlates each result to its CollectRequest by task_id, and POSTs
    one filedrop batch per frame. Frames senpai processed that were not
    UDL-tasked (no task_id) are dropped.
    """

    _REQUEST_TTL_S = 6 * 3600.0
    _SEEN_CAP = 512
    _WATCHDOG_INTERVAL_S = 600.0
    _RECONNECT_DELAY_S = 5.0

    def __init__(self, program: UDLProgram):
        self._program = program
        self._config = program.config.publish.eo_observation

        # CollectRequests by request id, with insertion timestamps for TTL
        # pruning: retained past the program's task-reference cleanup so
        # late-arriving SenpaiResults (SENPAI runs take minutes) can still
        # correlate.
        self._requests: dict[str, tuple[CollectRequestFull, float]] = {}

        # Recently handled result file_paths (transport redelivery dedupe).
        self._seen: dict[str, None] = {}

        self.results_received = 0
        self.posted = 0

        self._intake: asyncio.Task | None = None
        self._watchdog: asyncio.Task | None = None

        # Set once the intake consumer is established, cleared while it reconnects.
        self.intake_ready = asyncio.Event()

    def note_request(self, request: CollectRequestFull) -> None:
        """Retain a CollectRequest for later correlation by task_id."""
        now = time.monotonic()
        self._requests[request.id] = (request, now)

        for request_id, (_, added) in list(self._requests.items()):
            if now - added > self._REQUEST_TTL_S:
                self._requests.pop(request_id, None)

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
            frames_seen = len(self._program._seen_frames)
            if frames_seen > 0:
                logger.warning(
                    f"eo_observation publishing is enabled and {frames_seen} "
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

        # SenpaiResults carry the framework's execution id (from the frame
        # headers); translate to the CollectRequest it served, falling back to
        # a direct match for pipelines that already carry the request id.
        request_id = self._program.state.task_requests.get(result.task_id, result.task_id)

        entry = self._requests.get(request_id)
        request = entry[0] if entry else None
        if request is None:
            request = self._program.tasks.get(request_id)
            if request is not None:
                self.note_request(request)
            else:
                logger.warning(
                    f"EO: no CollectRequest known for task_id {result.task_id}; "
                    f"dropping SenpaiResult for {name}"
                )
                return

        await self.publish(result, request)

    async def publish(self, result: SenpaiResult, request: CollectRequestFull) -> None:
        """Build and post EOObservations for a solved SenpaiResult.

        One record per satellite detection, using the SDK's snake_case body
        keys (aliased to camelCase on the wire) with None values stripped.
        Observations are raw against the WCS: no catalog correlation is
        performed, so every record is an uncorrelated track (uct=true, no
        satNo).
        """
        program = self._program
        name = Path(result.file_path).name
        kinds = _SATELLITE_KINDS.get(result.track_mode, ())

        # Build EOObservation records
        records: list[dict] = []
        for det in result.detections:
            if det.kind not in kinds or det.ra is None or det.dec is None:
                continue

            ob_time, ra, declination = _to_udl_eoobservation(result, det)
            record = {
                "classification_marking": request.classification_marking,
                "data_mode": request.data_mode,
                "ob_time": ob_time,
                "source": program.config.api.source,
                "id_sensor": program.config.api.id_sensor,
                "orig_sensor_id": program.config.api.id_sensor,
                "ra": ra,
                "declination": declination,
                "reference_frame": "J2000",
                "uct": True,
                "track_id": request.id,
                "task_id": request.task_id,
                "origin": request.origin,
                "exp_duration": result.exposure_time_seconds,
                "descriptor": name,
            }

            if program._site:
                record["senlat"] = program._site.latitude_degrees
                record["senlon"] = program._site.longitude_degrees
                record["senalt"] = program._site.altitude_km

            for band in self._config.mag_bands:
                if det.calibrated_magnitudes and band in det.calibrated_magnitudes:
                    record["mag"] = det.calibrated_magnitudes[band]
                    if det.magnitude_errs and band in det.magnitude_errs:
                        record["mag_unc"] = det.magnitude_errs[band]
                    break

            # Remove None values
            record = {k: v for k, v in record.items() if v is not None}

            # UDL-schema compliance before shipping (see module header). Drops
            # so one bad record doesn't sink the frame's batch.
            try:
                _EO_OBSERVATION_VALIDATOR.validate_python(record)
            except ValidationError as e:
                logger.error(f"EO: record for {name} violates the UDL schema; dropped: {e}")
                continue

            records.append(record)

        if not records:
            # Show why nothing posted: 0 detections is a SENPAI/optics problem;
            # detections present but of the wrong kind is a track-mode/filter
            # mismatch (RATE wants point, SIDEREAL wants confirmed streak).
            kind_counts = Counter(det.kind for det in result.detections)
            logger.debug(
                f"EO: no postable satellite detections in {name} "
                f"(track_mode={result.track_mode}, {len(result.detections)} "
                f"detection(s): {dict(kind_counts)})"
            )
            return

        # Save locally if configured
        if self._config.save_path:
            await asyncio.to_thread(self._save_locally, request.id, name, records)

        # UDL and UDL-compliant endpoints expose EO writes only through the filedrop
        # (/filedrop/udl-eo); the /udl/eoobservation REST create is query-only.
        await program.upload_client.observations.eo_observations.unvalidated_publish(
            body=records,
            extra_headers=program._upload_client_headers,
        )
        self.posted += len(records)
        logger.info(
            f"task {request.id}: sent {len(records)} EOObservation(s) for {name}"
        )

    def _save_locally(self, task_id: str, frame_name: str, records: list[dict]) -> None:
        """Save posted EOObservation records to the local filesystem."""
        try:
            save_path = Path(self._config.save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            out_path = save_path / f"{Path(frame_name).stem}_eoobs.json"
            out_path.write_text(json.dumps(records, indent=2))
            logger.debug(f"saved EOObservations locally to {out_path} for task {task_id}")
        except Exception as e:
            logger.warning(f"Failed to save EOObservations locally for task {task_id}: {e}")
