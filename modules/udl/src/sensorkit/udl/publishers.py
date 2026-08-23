# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import io
import json
import math
import re
import time
import zipfile
from collections import Counter
from datetime import datetime, timedelta
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


# Neither the UDL nor the `udl_sdk` module validate payloads, so we choose to handle
# that within this module.
_EO_OBSERVATION_VALIDATOR = TypeAdapter(EOObservationRecord)
_SKY_IMAGERY_VALIDATOR = TypeAdapter(SkyImageryRecord)


def _to_udl_timestamp(dt: datetime) -> str:
    """Format a datetime as UDL expects: ISO 8601 UTC with trailing 'Z' (no offset)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SkyImageryPublisher:
    """Uploads FITS frames to the UDL SkyImagery filedrop."""

    def __init__(self, program: UDLProgram):
        self._program = program
        self._config = program.config.publish.sky_imagery

    async def publish(
        self, context: dict, data: bytes, request: CollectRequestFull | None = None
    ) -> None:
        program = self._program

        # Extract required context values
        info = context.get(FileInfo)
        required = {
            "FileInfo": info,
            "date_obs": context.get("date_obs"),
            "exptime": context.get("exptime"),
            "image_width": context.get("image_width"),
            "image_height": context.get("image_height"),
            "bits_per_pixel": context.get("bits_per_pixel"),
        }
        missing = [key for key, value in required.items() if value is None]

        if request:
            ident = f"CollectRequest {request.id}"
            image_set_length = request.num_frames or 1
            image_set_id = request.id
            provenance = {
                "classificationMarking": request.classification_marking,
                "dataMode": request.data_mode,
                "origin": request.origin,
                "satNo": request.sat_no,
                "idRequest": request.id,
                "taskId": request.task_id,
            }
        else:
            ident = "untasked frame"
            image_set_length = int(context.get("frame_count") or 1)
            image_set_id = context.get("task_id")
            provenance = {
                "classificationMarking": self._config.classification_marking,
                "dataMode": self._config.data_mode,
                "origin": self._config.origin,
            }

        if missing:
            raise ValueError(
                f"DataGraph context is missing {missing} for {ident}; "
                f"check the udl entity's data_flow keyword_map"
            )

        filename = info.path.name
        exp_start_time = datetime.fromisoformat(context["date_obs"])
        exp_end_time = exp_start_time + timedelta(seconds=float(context["exptime"]))

        # Per UDL: sequenceId must be >= 1
        frame_num = context.get("frame_num", 0)
        sequence_id = frame_num + 1

        # Build SkyImagery metadata
        metadata = {
            **provenance,
            "idSensor": program.config.api.id_sensor,
            "origSensorId": program.config.api.id_sensor,
            "expStartTime": _to_udl_timestamp(exp_start_time),
            "expEndTime": _to_udl_timestamp(exp_end_time),
            "imageSetLength": image_set_length,
            "sequenceId": sequence_id,
            "frameWidthPixels": context["image_width"],
            "frameHeightPixels": context["image_height"],
            "pixelBitDepth": context["bits_per_pixel"],
            "filename": filename,
            "filesize": len(data),
            "source": program.config.api.source,
            "imageType": context.get("image_type") or self._config.image_type,
        }

        # imageSetId groups multiple frames of one collect into a set.
        # Per UDL: a single-image set does not need an imageSetId, so only emit it when
        # the set has more than one frame.
        if image_set_length > 1 and image_set_id:
            metadata["imageSetId"] = image_set_id

        if program._site:
            metadata["senlat"] = program._site.latitude_degrees
            metadata["senlon"] = program._site.longitude_degrees
            metadata["senalt"] = program._site.altitude_km
        else:
            logger.warning(
                f"Publishing SkyImagery without sensor location for {ident} "
                f"(no SitePosition from controller {program.config.controller})"
            )

        # Remove None values before validating
        metadata = {k: v for k, v in metadata.items() if v is not None}
        _SKY_IMAGERY_VALIDATOR.validate_python(metadata)

        metadata_bytes = json.dumps(metadata).encode()
        metadata_fname = f"{Path(filename).stem}_skyimagery.json"

        # Save locally if configured
        if self._config.save_path:
            await asyncio.to_thread(
                self._save_locally,
                ident,
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
            f"uploaded SkyImagery {sequence_id}/{image_set_length} for {ident}"
        )

    def _save_locally(
        self,
        ident: str,
        data_fname: str,
        data: bytes,
        metadata_fname: str,
        metadata_bytes: bytes,
    ) -> None:
        """Save SkyImagery to local filesystem."""
        try:
            save_path = Path(self._config.save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            out_path = save_path / f"{Path(data_fname).stem}.zip"

            with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(data_fname, data)
                zf.writestr(metadata_fname, metadata_bytes)

            logger.debug(f"saved SkyImagery locally to {out_path} for {ident}")
        except Exception as e:
            logger.warning(f"Failed to save SkyImagery locally for {ident}: {e}")

    def _get_upload_url(self, endpoint: UDLEndpointConfig) -> str:
        """Create the SkyImagery filedrop URL."""
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
        """Upload a SkyImagery ZIP to the filedrop URL."""
        endpoint = self._program.config.api.upload or self._program.config.api
        url = self._get_upload_url(endpoint)

        if endpoint.client_cert and endpoint.client_key:
            client_kwargs = {
                "cert": (endpoint.client_cert, endpoint.client_key),
                "verify": endpoint.client_verify,
            }
        else:
            client_kwargs = {"verify": endpoint.client_verify}
            if self._program._upload_username:
                client_kwargs["auth"] = (
                    self._program._upload_username,
                    self._program._upload_password,
                )

        async with httpx.AsyncClient(timeout=endpoint.upload_timeout, **client_kwargs) as http:
            resp = await http.post(
                url,
                content=zip_bytes,
                headers={"Content-Type": "application/zip"},
            )
            if resp.status_code >= 300:
                raise RuntimeError(
                    f"Failed SkyImagery upload to {url}: HTTP {resp.status_code} {resp.text}"
                )


# Only accept 'point' or 'streak' detections from a SenpaiResult
_SATELLITE_KINDS = {
    "RATE": ("point",),
    "SIDEREAL": ("streak",),
}


class EOObservationPublisher:
    """Uploads SENPAI satellite detections to the UDL EOObservation endpoint."""

    _REQUEST_TTL_S = 6 * 3600.0  # CollectRequest retention for late correlation [s]
    _RECONNECT_DELAY_S = 5.0  # SenpaiResult stream reconnect backoff [s]

    # Constants for the annual-aberration correction
    _LIGHT_AU_PER_DAY = 173.1446  # speed of light [AU/day]
    _SECONDS_PER_DAY = 86400.0
    _UNIX_EPOCH_JD = 2440587.5  # Julian date of the Unix epoch (1970-01-01T00:00 UTC)
    _J2000_JD = 2451545.0  # Julian date of the J2000 epoch (2000-01-01T12:00 TT)
    _DAYS_PER_JULIAN_CENTURY = 36525.0
    # Sun's mean longitude at J2000 [deg] and its rate [deg per Julian century],
    # from the low-precision solar ephemeris (see references in _add_annual_aberration).
    _SUN_MEAN_LONGITUDE_J2000_DEG = 280.460
    _SUN_MEAN_LONGITUDE_RATE_DEG_PER_CENTURY = 36000.771285
    # Earth's mean orbital speed [AU/day] (numerically ≈ the Gaussian
    # gravitational constant k = 0.0172021), and its equatorial y/z
    # projections: -speed·cos(ε) and -speed·sin(ε) for the J2000 obliquity
    # ε ≈ 23.44°.
    _EARTH_SPEED_AU_PER_DAY = 0.0172
    _EARTH_SPEED_Y_AU_PER_DAY = -0.0158
    _EARTH_SPEED_Z_AU_PER_DAY = -0.0068

    def __init__(self, program: UDLProgram):
        self._program = program
        self._config = program.config.publish.eo_observation

        # CollectRequests by ID with insertion timestamps for TTL pruning.
        # Retained past the program's task-reference cleanup so late-arriving
        # SenpaiResults can still correlate.
        self._collect_requests: dict[str, tuple[CollectRequestFull, float]] = {}

        self.results_received = 0
        self.posted = 0

        self._post: asyncio.Task | None = None

        # Set once the stream consumer is established, cleared while it reconnects.
        self.post_ready = asyncio.Event()

    def get_collect_request(self, request: CollectRequestFull) -> None:
        """Retain a CollectRequest for later correlation by task_id."""
        now = time.monotonic()
        self._collect_requests[request.id] = (request, now)

        for request_id, (_, added) in list(self._collect_requests.items()):
            if now - added > self._REQUEST_TTL_S:
                self._collect_requests.pop(request_id, None)

    async def start(self) -> None:
        # Warn loudly when EO posting is enabled but SENPAI isn't configured.
        try:
            kit = self._program.program.sensorkit()
            entries = await kit.backend.key_value().get_all(deep=True)
            if not any(entry.key.prop == "SenpaiConfig" for entry in entries):
                logger.warning(
                    "EOObservation publishing is enabled but no `senpai` config "
                    "section was found; no SenpaiResults will arrive until the "
                    "`senpai` service is configured and running."
                )
        except Exception as e:
            logger.debug(f"`senpai` presence check failed: {e}")

        self._post = asyncio.create_task(self._post_loop())

    async def stop(self) -> None:
        if self._post and not self._post.done():
            self._post.cancel()

    @staticmethod
    def _add_annual_aberration(
        ra_deg: float, dec_deg: float, when: datetime
    ) -> tuple[float, float]:
        """Add annual aberration to a star-relative position, in degrees.

        Earth's ~30 km/s barycentric velocity displaces starlight by up to
        ~20.5", so a plate solve — which fits the frame onto undisplaced
        catalog positions — subtracts that shift from everything it measures.
        A satellite shares Earth's orbital velocity and so was never displaced
        by it, leaving the solved target short by the same ~20.5". Adding it
        back recovers the geocentric direction UDL ingests.

        Diurnal aberration needs no such fix: the sensor's rotation velocity
        displaces the target and the stars alike, so the solve removes it
        correctly. Light time is likewise left in, per UDL.

        References:
            Vallado, Fundamentals of Astrodynamics and Applications, 5th ed., Eqn (4-25).
            Explanatory Supplement to the Astronomical Almanac, Eqn (3.253-1).
        """
        cls = EOObservationPublisher

        # Julian centuries since J2000. UTC stands in for UT1: DUT1 (< 0.9 s)
        # moves the correction by well under a microarcsecond.
        julian_date = when.timestamp() / cls._SECONDS_PER_DAY + cls._UNIX_EPOCH_JD
        t = (julian_date - cls._J2000_JD) / cls._DAYS_PER_JULIAN_CENTURY
        sun_longitude = math.radians(
            cls._SUN_MEAN_LONGITUDE_J2000_DEG
            + cls._SUN_MEAN_LONGITUDE_RATE_DEG_PER_CENTURY * t
        )
        ra, dec = math.radians(ra_deg), math.radians(dec_deg)

        # Earth's barycentric velocity (AU/day) from the Sun's mean longitude,
        # with the obliquity folded into the y/z components.
        x_dot = cls._EARTH_SPEED_AU_PER_DAY * math.sin(sun_longitude)
        y_dot = cls._EARTH_SPEED_Y_AU_PER_DAY * math.cos(sun_longitude)
        z_dot = cls._EARTH_SPEED_Z_AU_PER_DAY * math.cos(sun_longitude)

        d_ra = (-x_dot * math.sin(ra) + y_dot * math.cos(ra)) / (
            cls._LIGHT_AU_PER_DAY * math.cos(dec)
        )
        d_dec = (
            -x_dot * math.cos(ra) * math.sin(dec)
            - y_dot * math.sin(ra) * math.sin(dec)
            + z_dot * math.cos(dec)
        ) / cls._LIGHT_AU_PER_DAY

        return ra_deg + math.degrees(d_ra), dec_deg + math.degrees(d_dec)

    @staticmethod
    def _to_udl_eo_observation(
        result: SenpaiResult, detection: Detection
    ) -> tuple[str, float, float]:
        """Convert a WCS-fit detection to UDL's EO observation conventions.

        Photon arrival time: the detection centroid is the mid-exposure image
        of the object, so its photons arrived at DATE-OBS plus half the
        exposure. Light time is not corrected (UDL's convention).

        Position: annual aberration is added back to the star-relative
        plate-solve position; see _add_annual_aberration.

        Returns (ob_time, ra, declination) for the record.
        """
        ob_time = result.timestamp
        if result.exposure_time_seconds:
            ob_time = ob_time + timedelta(seconds=result.exposure_time_seconds / 2.0)

        ra, declination = EOObservationPublisher._add_annual_aberration(
            detection.ra, detection.dec, ob_time
        )
        return _to_udl_timestamp(ob_time), ra, declination

    async def _post_loop(self) -> None:
        """Post EOObservations to the UDL for the configured sensor."""
        info = get_keyword_info(SenpaiResult)
        keyword_key = info.key if info else SenpaiResult.__name__
        # Sanitise the program entity into a valid NATS durable consumer name.
        durable_name = "udl-eo-" + re.sub(
            r"[^A-Za-z0-9_-]", "_", str(self._program.program.entity)
        )

        while True:
            try:
                kit = self._program.program.sensorkit()
                subject = Subject(path=(), prop=SpecialProperty.ALL_DESCENDANTS)
                consumer = await kit.backend.impl.stream_consume(
                    subject, durable_name=durable_name
                )
                logger.debug("_post_loop consuming SenpaiResults")
                self.post_ready.set()

                async for msg in consumer:
                    if msg.subject.prop != keyword_key:
                        continue
                    try:
                        result = validate_keyword_json(keyword_key, msg.data)
                    except Exception:
                        result = None
                    try:
                        await self._handle_senpai_result(result)
                    except Exception as e:
                        logger.warning(f"Error handling SenpaiResult: {e}")
            except Exception as e:
                # CancelledError is a BaseException, so stop() still tears the
                # loop down; everything else reconnects.
                logger.warning(
                    f"EO post stream error: {e}; reconnecting in "
                    f"{self._RECONNECT_DELAY_S:.0f}s"
                )
                await asyncio.sleep(self._RECONNECT_DELAY_S)
            finally:
                self.post_ready.clear()

    async def _handle_senpai_result(self, result: SenpaiResult | None) -> None:
        if not isinstance(result, SenpaiResult):
            logger.debug("ignoring non-SenpaiResult payload")
            return

        self.results_received += 1
        name = Path(result.file_path).name

        if not result.solved:
            logger.debug(f"SenpaiResult for {name} is unsolved; skipping")
            return

        if self._config.sequence_only and not result.from_sequence:
            logger.debug(
                f"SenpaiResult for {name} is per-frame (not sequence-derived); skipping"
            )
            return

        # Translate the execution task ID to its CollectRequest ID (a pipeline
        # may already carry the CollectRequest ID, hence the fallback).
        request_id = self._program.state.collect_request_ids.get(result.task_id, result.task_id)

        entry = self._collect_requests.get(request_id)
        request = entry[0] if entry else None
        if request is None:
            request = self._program.tasks.get(request_id)

        if request is not None:
            self.get_collect_request(request)
        elif result.task_id in self._program.state.collect_request_ids:
            # Tasked, but the CollectRequest reference is gone — dropping beats
            # posting it with the untasked provenance.
            logger.warning(
                f"No CollectRequest known for task_id {result.task_id}; "
                f"dropping SenpaiResult for {name}"
            )
            return
        elif not (self._config.classification_marking and self._config.data_mode):
            logger.debug(f"SenpaiResult for {name} is untasked; skipping")
            return

        await self.publish(result, request)

    async def publish(
        self, result: SenpaiResult, request: CollectRequestFull | None = None
    ) -> None:
        program = self._program
        name = Path(result.file_path).name
        kinds = _SATELLITE_KINDS.get(result.track_mode, ())

        if request:
            ident = f"CollectRequest {request.id}"
            provenance = {
                "classification_marking": request.classification_marking,
                "data_mode": request.data_mode,
                "origin": request.origin,
                "track_id": request.id,
                "task_id": request.task_id,
            }
        else:
            ident = "untasked result"
            provenance = {
                "classification_marking": self._config.classification_marking,
                "data_mode": self._config.data_mode,
                "origin": self._config.origin,
            }

        site_fields = {}
        if program._site:
            site_fields = {
                "senlat": program._site.latitude_degrees,
                "senlon": program._site.longitude_degrees,
                "senalt": program._site.altitude_km,
            }

        # Build EOObservation records
        records: list[dict] = []
        for det in result.detections:
            if det.kind not in kinds or det.ra is None or det.dec is None:
                continue

            ob_time, ra, declination = self._to_udl_eo_observation(result, det)
            # First configured band with a calibrated magnitude; None misses
            # fall away with the None-strip below.
            mags = det.calibrated_magnitudes or {}
            errs = det.magnitude_errs or {}
            band = next(filter(mags.__contains__, self._config.mag_bands), None)
            record = {
                **provenance,
                "ob_time": ob_time,
                "source": program.config.api.source,
                "id_sensor": program.config.api.id_sensor,
                "orig_sensor_id": program.config.api.id_sensor,
                "ra": ra,
                "declination": declination,
                "reference_frame": "J2000",
                "uct": True,
                "exp_duration": result.exposure_time_seconds,
                "descriptor": name,
                "mag": mags.get(band),
                "mag_unc": errs.get(band),
                **site_fields,
            }

            # Remove None values
            record = dict(filter(lambda kv: kv[1] is not None, record.items()))

            # UDL-schema compliance before shipping (see module header). Drops
            # so one bad record doesn't sink the frame's batch.
            try:
                _EO_OBSERVATION_VALIDATOR.validate_python(record)
            except ValidationError as e:
                logger.error(f"Unable to send EOObservation for {name} due to schema violation: {e}")
            else:
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
            await asyncio.to_thread(self._save_locally, ident, name, records)

        await program.upload_client.observations.eo_observations.unvalidated_publish(
            body=records,
            extra_headers=program._upload_client_headers,
        )
        self.posted += len(records)
        logger.info(f"Sent {len(records)} EOObservation(s) for {ident} ({name})")

    def _save_locally(self, ident: str, frame_name: str, records: list[dict]) -> None:
        """Save posted EOObservations to the local filesystem."""
        try:
            save_path = Path(self._config.save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            out_path = save_path / f"{Path(frame_name).stem}_eoobs.json"
            out_path.write_text(json.dumps(records, indent=2))
            logger.debug(f"saved EOObservations locally to {out_path} for {ident}")
        except Exception as e:
            logger.warning(f"Failed to save EOObservations locally for {ident}: {e}")
