# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any, Dict

import httpx
from dotenv import dotenv_values
from loguru import logger
from pydantic import Base64Bytes, BaseModel, Field, TypeAdapter, ValidationError
from unifieddatalibrary import AsyncUnifieddatalibrary, omit
from unifieddatalibrary.types import CollectRequestFull
from unifieddatalibrary.types.collect_response_create_params import (
    CollectResponseCreateParams,
)

import sensorkit.api as sk
from sensorkit.astro.common import TLE, SitePosition
from sensorkit.astro.coords import Cartesian, Equatorial, StateVector
from sensorkit.astro.target import ICRSTarget, StateVectorTarget, Target, TLETarget
from sensorkit.data.filesys import FileInfo
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask
from sensorkit.udl.models import (
    ResponseStatus,
    UDLConfig,
    UDLEndpointConfig,
    UDLReferenceFrame,
)
from sensorkit.udl.publishers import (
    EOObservationPublisher,
    SkyImageryPublisher,
    _to_udl_timestamp,
)
from sensorkit.udl.task_queue import TaskQueue

# Status values that are persisted to state and, upon restore, prevent
# reacceptance (CollectResponse=ACCEPT) by the poller when the CollectRequest
# window is still open.
_RESOLUTION_STATUSES = frozenset(
    {
        ResponseStatus.COLLECTED,
        ResponseStatus.COMPLETED,
        ResponseStatus.REJECTED,
        ResponseStatus.FAILED,
    }
)

# Neither the UDL nor the `udl_sdk` module validate payloads, so we choose to handle
# that within this module.
_COLLECT_RESPONSE_VALIDATOR = TypeAdapter(CollectResponseCreateParams)


# Max entries per persisted cache, oldest evicted first. Sized to outlive
# late-arriving SENPAI results.
_STATE_CACHE_CAP = 512


class _PublishProgress(BaseModel):
    """Per-CollectRequest frame upload progress, used to send COMPLETED.

    window is the (start, end) execution window from the task factory, set
    only when the collect succeeded; COMPLETED is not sent without it.
    """

    attempted: int = 0
    uploaded: int = 0
    window: tuple[datetime | None, datetime | None] | None = None


class UDLState(BaseModel):
    """Persistent state for UDL program."""

    # Compressed JSON array of full CollectRequests. Note, the default NATS max
    # payload size is 1 MB such that if a deployment / program implementation
    # begins to see "Failed to save state: MaxPayloadError: nats: maximum
    # payload exceeded" warnings, the max payload size must be increased via
    # `max_payload` in the NATS server configuration file, presuming that there
    # are no upstream means to reduce the number of CollectRequests in the
    # polling horizon.
    pending_collect_requests: Base64Bytes = Field(
        default_factory=lambda: gzip.compress(b"[]", mtime=0)
    )

    # Requests already answered with a resolving CollectResponse, mapped to
    # their window end; pruned once the poll filter (endTime > now) can no
    # longer return them.
    resolved_collect_requests: Dict[str, datetime] = Field(default_factory=dict)

    # Maps the framework's execution task ID (stamped on frames and
    # SenpaiResults) to the CollectRequest ID. Persisted because SENPAI results
    # arrive minutes after the collect, possibly across a restart.
    collect_request_ids: Dict[str, str] = Field(default_factory=dict)

    # CollectResponses, SkyImagery, and/or EOObservations that did not finish
    # uploading before a service interruption are continued.
    publish_progress: Dict[str, _PublishProgress] = Field(default_factory=dict)


@sk.declare_program
class UDLProgram:
    """SensorKit program for UDL (Unified Data Library) integration.

    Polls UDL for CollectRequests assigned to our sensor, executes them as
    StandardCollectTasks, and delivers SkyImagery and/or EOObservations per the
    publish config (see publishers.py).

    CollectResponse lifecycle: ACCEPTED on receipt, COLLECTED when the task
    finishes executing, COMPLETED once the frame set has been delivered;
    REJECTED, CANCELLED, or FAILED otherwise. Resolved requests are remembered
    in state until their windows close so the poller cannot re-accept them.

    CollectRequests are schema-validated at receipt and on restore, and the
    target is validated at receipt so the tasker hears REJECTED while it can
    still re-assign. The base_url can be UDL itself or any UDL-compliant
    endpoint.
    """

    def __init__(self):
        self.program: sk.ProgramImpl | None = None

        # Main client
        self.client: AsyncUnifieddatalibrary | None = None
        # Optionally unique SkyImagery upload client
        self.upload_client: AsyncUnifieddatalibrary | None = None

        self._username: str | None = None
        self._password: str | None = None
        self._upload_username: str | None = None
        self._upload_password: str | None = None

        self._client_headers: dict[str, Any] = {}
        self._upload_client_headers: dict[str, Any] = {}

        # Task management
        self.queue: TaskQueue | None = None
        self.tasks: Dict[str, CollectRequestFull] = {}

        # Request ID of the currently executing CollectRequest
        self._in_flight: str | None = None

        # Recently handled frame files: directory watchers can deliver the same
        # file more than once (create + data-landed events), and a duplicate
        # would double-count set progress and double-upload SkyImagery.
        self._seen_frames: Dict[str, None] = {}

        # Background CollectRequest poller
        self._poller: asyncio.Task | None = None

        # SkyImagery and EOObservation publishers
        self._publisher: asyncio.Task | None = None
        self._sky_imagery: SkyImageryPublisher | None = None
        self._eo_observation: EOObservationPublisher | None = None

        # Site location (populated from controller)
        self._site: SitePosition | None = None

        self.state = UDLState()

    async def _save_state(self) -> None:
        """Persist state to KV."""
        try:
            # Save ACCEPTED tasks that have not yet been COLLECTED
            pending = (
                [request.model_dump(mode="json", by_alias=True) for request in self.queue.iter()]
                if self.queue
                else []
            )
            self.state.pending_collect_requests = gzip.compress(json.dumps(pending).encode())

            # Save publish progress
            now = datetime.now(UTC)
            self.state.resolved_collect_requests = {k: v for k, v in self.state.resolved_collect_requests.items() if v > now}
            for cache in (self.state.collect_request_ids, self.state.publish_progress):
                while len(cache) > _STATE_CACHE_CAP:
                    cache.pop(next(iter(cache)))
            await self.program.kv_put_model(self.state)
            logger.debug(f"saved state for {self.program.entity}")
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    async def _restore_state(self) -> None:
        """Restore state from KV."""
        try:
            self.state = await self.program.kv_get_model(UDLState)
            now = datetime.now(UTC)
            self.state.resolved_collect_requests = {k: v for k, v in self.state.resolved_collect_requests.items() if v > now}
            logger.debug(f"restored state for {self.program.entity}")
        except Exception:
            logger.warning(f"No saved state for {self.program.entity}")
            self.state = UDLState()

    @staticmethod
    def _load_credentials(endpoint: UDLEndpointConfig) -> tuple[str | None, str | None]:
        """Read UDL_USERNAME/UDL_PASSWORD from .env."""
        env = dotenv_values(endpoint.env_file)
        username = env.get("UDL_USERNAME") or os.environ.get("UDL_USERNAME")
        password = env.get("UDL_PASSWORD") or os.environ.get("UDL_PASSWORD")

        if bool(username) != bool(password):
            raise RuntimeError(
                f"UDL_USERNAME and UDL_PASSWORD must both be set "
                f"in {endpoint.env_file} or as environment variables"
            )

        return username or None, password or None

    @staticmethod
    def _use_certs(endpoint: UDLEndpointConfig) -> bool:
        """Cert-based auth is selected by configuring BOTH client_cert and client_key."""
        if bool(endpoint.client_cert) != bool(endpoint.client_key):
            raise RuntimeError(
                f"client_cert and client_key must both be set for cert-based auth "
                f"(endpoint {endpoint.base_url})"
            )
        return bool(endpoint.client_cert)

    def _get_client(
        self, endpoint: UDLEndpointConfig, username: str | None, password: str | None
    ) -> AsyncUnifieddatalibrary:
        """Create a `udl_sdk` client."""
        if self._use_certs(endpoint):
            http_client = httpx.AsyncClient(
                cert=(endpoint.client_cert, endpoint.client_key),
                verify=endpoint.client_verify,
                timeout=endpoint.timeout,
            )
            return AsyncUnifieddatalibrary(
                http_client=http_client,
                base_url=endpoint.base_url,
            )

        client_kwargs: dict[str, Any] = {"timeout": endpoint.timeout}
        if endpoint.base_url:
            client_kwargs["base_url"] = endpoint.base_url
        if not endpoint.client_verify:
            client_kwargs["http_client"] = httpx.AsyncClient(verify=False)

        if not (username and password):
            logger.warning(
                f"No UDL credentials configured; issuing unauthenticated requests "
                f"to {endpoint.base_url}"
            )

        return AsyncUnifieddatalibrary(
            username=username, password=password, **client_kwargs
        )

    @sk.on_attach
    async def program_init(self) -> None:
        """Restore state, create SDK clients, start poller and image publisher."""
        self.program = sk.program()

        self.config = await self.program.kv_get_model(UDLConfig)

        # Restore last known state
        await self._restore_state()

        # Create main client: CollectRequest polling and CollectResponse posting
        if not self._use_certs(self.config.api):
            self._username, self._password = self._load_credentials(self.config.api)
        self.client = self._get_client(
            self.config.api, self._username, self._password
        )

        # Optionally create upload client: SkyImagery posting
        if self.config.api.upload:
            if not self._use_certs(self.config.api.upload):
                self._upload_username, self._upload_password = self._load_credentials(
                    self.config.api.upload
                )
            self.upload_client = self._get_client(
                self.config.api.upload, self._upload_username, self._upload_password
            )
            logger.debug(
                f"posting SkyImagery to {self.config.api.upload.base_url}"
            )
        else:
            self.upload_client = self.client
            self._upload_username = self._username
            self._upload_password = self._password

        self._client_headers = {} if self._username else {"Authorization": omit}
        self._upload_client_headers = (
            {} if self._upload_username else {"Authorization": omit}
        )

        logger.debug(f"starting UDL program for {self.program.entity}")

        # Get site location from controller. SkyImagery accepts uploads without
        # senlat/senlon/senalt, so warn but proceed if it's not available.
        try:
            controller_client = self.program.sensorkit().controller(self.config.controller)
            self._site = await controller_client.kv_get_model(SitePosition)
            logger.debug(
                f"site location: lat={self._site.latitude_degrees}, "
                f"lon={self._site.longitude_degrees}, alt={self._site.altitude_km } km"
            )
        except Exception as e:
            logger.warning(
                f"Unable to read SitePosition from {self.config.controller}. "
                f"Uploading SkyImagery without senlat/senlon/senalt: {e}"
            )

        # Initialize task queue with offer window integration
        self.queue = TaskQueue(
            self.program,
            on_expired=self._cancel_collect_request,
            end_time_deadband_s=self.config.end_time_deadband_s,
        )

        # Restore pending tasks from state
        try:
            pending = json.loads(gzip.decompress(self.state.pending_collect_requests))
        except Exception as e:
            logger.warning(f"Failed to restore pending tasks: {e}")
            pending = []
        logger.debug(f"restoring {len(pending)} pending tasks")
        for task_dict in pending:
            try:
                request = self._validate_collect_request(task_dict)
                if request.id and request.end_time and request.end_time > datetime.now(UTC):
                    self.tasks[request.id] = request
                    await self.queue.push_task(request)
                    logger.debug(f"restored task {request.id}")
            except Exception as e:
                logger.warning(f"Failed to restore task {task_dict.get('id', '<unknown>')}: {e}")

        await self._publishers_init()

        # Start poller
        self._poller = asyncio.create_task(self._poll_loop())

        # Start publishers
        self._publisher = asyncio.create_task(self._publish_loop())

    async def _publishers_init(self) -> None:
        """Create the data publishers."""
        if self.config.publish.sky_imagery:
            self._sky_imagery = SkyImageryPublisher(self)
        if self.config.publish.eo_observation:
            self._eo_observation = EOObservationPublisher(self)

        if self._eo_observation:
            for request in self.tasks.values():
                self._eo_observation.get_collect_request(request)
            await self._eo_observation.start()

    async def _cancel_collect_request(self, request: CollectRequestFull) -> None:
        """Cancel a CollectRequest whose window has expired."""
        self.tasks.pop(request.id, None)
        await self._send_response(
            request, ResponseStatus.CANCELLED, notes="Expired before execution"
        )

    @sk.on_detach
    async def program_deinit(self) -> None:
        """Cancel tasks, save state, close connections."""
        logger.debug(f"stopping UDL program {self.program.entity}")

        # Send a CANCELLED response for any in-flight CollectRequest executions
        if self._in_flight:
            request = self.tasks.get(self._in_flight)
            self._in_flight = None
            if request is not None:
                await self._send_response(
                    request,
                    ResponseStatus.CANCELLED,
                    notes="Interrupted by service shutdown",
                )

        for task in [self._poller, self._publisher]:
            if task and not task.done():
                task.cancel()

                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if self._eo_observation:
            await self._eo_observation.stop()

        await self._save_state()

        if self.upload_client and self.upload_client is not self.client:
            await self.upload_client.close()
        if self.client:
            await self.client.close()

    async def _poll_loop(self) -> None:
        """Poll UDL for CollectRequests assigned to the configured sensor."""
        logger.debug("poller started")

        while True:
            try:
                now = datetime.now(UTC)
                horizon = now + timedelta(seconds=self.config.poll_horizon)
                page = await self.client.collect_requests.list(
                    start_time=f"<{_to_udl_timestamp(horizon)}",
                    extra_query={
                        self.config.api.poll_filter: self.config.api.id_sensor,
                        "endTime": f">{_to_udl_timestamp(now)}",
                    },
                    extra_headers=self._client_headers,
                )

                for request in page.items:
                    await self._handle_collect_request(request)

            except Exception as e:
                logger.warning(f"Error polling collect requests: {e}")

            await asyncio.sleep(self.config.poll_frequency)

    @staticmethod
    def _validate_collect_request(payload: Dict) -> CollectRequestFull:
        """Schema-validate a CollectRequest payload.

        Blank sub-objects are treated as absent (see prune_blank). Note, the
        `udl_sdk` constructs poll responses leniently, so this is the only
        enforcement point.
        """

        def prune_blank(value):
            """Remove dict members that carry no data.

            UDL-compliant endpoints have been observed attaching placeholder
            sub-objects (e.g. ``"stateVector": {}``) whose required members
            would fail validation of an otherwise compliant request.
            """
            if isinstance(value, dict):
                pruned = {k: prune_blank(v) for k, v in value.items()}
                pruned = {k: v for k, v in pruned.items() if v is not None}
                return pruned or None
            return value

        return CollectRequestFull.model_validate(prune_blank(payload))

    async def _handle_collect_request(self, request: CollectRequestFull) -> None:
        """Process a CollectRequest."""
        if not request.id:
            logger.error("Ignoring CollectRequest without an ID")
            return

        if request.id in self.tasks or request.id in self.state.resolved_collect_requests:
            return

        if request.end_time and request.end_time < datetime.now(UTC):
            logger.debug(f"skipping expired request {request.id}")
            await self._send_response(request, ResponseStatus.REJECTED, notes="Request expired")
            return

        try:
            request = self._validate_collect_request(request.model_dump(mode="json", by_alias=True))
        except ValidationError as e:
            logger.error(f"CollectRequest {request.id} violates the UDL schema; rejecting: {e}")
            await self._send_response(
                request,
                ResponseStatus.REJECTED,
                notes="Request violates the UDL CollectRequest schema",
            )
            return

        # Validate/create the target now, before the window opens, to allow the original
        # tasker to receive a REJECTED response early.
        if self._get_target(request) is None:
            await self._send_response(
                request, ResponseStatus.REJECTED, notes="Unsupported target type"
            )
            return

        logger.debug(f"received CollectRequest {request.id}")

        self.tasks[request.id] = request
        if self._eo_observation:
            self._eo_observation.get_collect_request(request)
        await self.queue.push_task(request)

        await self._send_response(request, ResponseStatus.ACCEPTED)
        await self._save_state()

    async def _send_response(
        self,
        request: CollectRequestFull,
        status: ResponseStatus,
        *,
        actual_start_time: datetime | None = None,
        actual_end_time: datetime | None = None,
        notes: str | None = None,
    ) -> None:
        """Send a CollectResponse."""
        if status in _RESOLUTION_STATUSES and request.id:
            # No window end → retire the resolved record after the poll horizon
            self.state.resolved_collect_requests[request.id] = request.end_time or (
                datetime.now(UTC) + timedelta(seconds=self.config.poll_horizon)
            )
            await self._save_state()

        params: Dict[str, Any] = {
            "classification_marking": request.classification_marking,
            "data_mode": request.data_mode,
            "source": self.config.api.source,
            "id_request": request.id,
            "status": status.value,
            "sat_no": request.sat_no,
            "task_id": request.task_id,
            "id_plan": request.id_plan,
            "external_id": request.external_id,
            "id_sensor": self.config.api.id_sensor,
            "orig_sensor_id": self.config.api.id_sensor,
            "actual_start_time": (
                _to_udl_timestamp(actual_start_time) if actual_start_time else None
            ),
            "actual_end_time": _to_udl_timestamp(actual_end_time) if actual_end_time else None,
            "notes": notes,
        }
        params = {k: v for k, v in params.items() if v is not None}

        try:
            _COLLECT_RESPONSE_VALIDATOR.validate_python(params)
        except ValidationError as e:
            logger.error(
                f"Unable to send CollectResponse for {request.id} due to schema violation: {e}"
            )
            return

        try:
            await self.client.collect_responses.create(
                **params,
                extra_headers=self._client_headers,
            )
            logger.debug(f"sent {status.value} response for {request.id}")
        except Exception as e:
            logger.warning(f"Failed to send CollectResponse for {request.id}: {e}")

    async def _publish_loop(self) -> None:
        logger.debug("publisher started")

        graph = await self.program.data_graph()
        if not graph:
            logger.debug("no data graph bound; publisher exiting")
            return

        sink = graph.app_sink()
        async for context, data in sink.consume():
            try:
                await self._handle_frame(context, data)
            except Exception as e:
                logger.warning(f"Error in publish loop: {e}")

    async def _handle_frame(self, context: dict, data: bytes) -> None:
        """Track set progress for a collected frame and hand it to the publishers."""
        task_id: str | None = context.get("task_id")

        # The pipeline stamps the framework's execution ID; translate to the
        # CollectRequest it served.
        request_id = self.state.collect_request_ids.get(task_id, task_id)
        request = self.tasks.get(request_id)

        info = context.get(FileInfo)
        frame_key = str(info.path) if info else f"{request_id}:{context.get('frame_num')}"
        if frame_key in self._seen_frames:
            logger.debug(f"duplicate frame event for {frame_key}; skipping")
            return
        self._seen_frames[frame_key] = None
        while len(self._seen_frames) > _STATE_CACHE_CAP:
            self._seen_frames.pop(next(iter(self._seen_frames)))

        if request is None:
            cfg = self.config.publish.sky_imagery
            if task_id in self.state.collect_request_ids:
                logger.warning(f"No CollectRequest found for {task_id}")
            elif self._sky_imagery and cfg.classification_marking and cfg.data_mode:
                await self._sky_imagery.publish(context, data)
            return

        image_set_length = request.num_frames or 1
        progress = self.state.publish_progress.setdefault(request.id, _PublishProgress())
        progress.attempted += 1

        if self._sky_imagery:
            try:
                await self._sky_imagery.publish(context, data, request)
                progress.uploaded += 1
            except Exception as e:
                logger.warning(f"Failed to upload SkyImagery for {request.id}: {e}")

        await self._send_completed_collect_response(request, progress, image_set_length)

    async def _send_completed_collect_response(
        self, request: CollectRequestFull, progress: _PublishProgress, image_set_length: int
    ) -> None:
        """Send COMPLETED once every frame in the set has been seen.

        With imagery publishing enabled, at least one frame must have been
        uploaded (per-frame failures are tolerated). The window guard limits
        COMPLETED to tasks that collected successfully. EO posting never gates
        COMPLETED (SENPAI results arrive minutes later, best-effort).
        """
        if progress.attempted < image_set_length:
            return

        self.state.publish_progress.pop(request.id, None)
        delivered = self._sky_imagery is None or progress.uploaded > 0

        if delivered and progress.window is not None:
            start_time, end_time = progress.window
            await self._send_response(
                request,
                ResponseStatus.COMPLETED,
                actual_start_time=start_time,
                actual_end_time=end_time,
            )
            detail = (
                f": uploaded {progress.uploaded}/{image_set_length} frames"
                if self._sky_imagery
                else ""
            )
            logger.info(f"{request.id} COMPLETED{detail}")
        elif self._sky_imagery and progress.uploaded == 0:
            logger.warning(
                f"Failed to upload all frames for {request.id}; COMPLETED not sent"
            )

    @sk.task_factory
    async def generate(self):
        """Convert CollectRequests to StandardCollectTasks."""
        request = await self.queue.pop_task()
        if not request:
            yield None
            return

        target = self._get_target(request)
        if target is None:
            logger.warning(f"Task ({request.id}): Could not build target, skipping")
            await self._send_response(
                request, ResponseStatus.REJECTED, notes="Unsupported target type"
            )
            await self.queue.remove_task(request.id)
            yield None
            return

        end_time = (
            request.end_time + timedelta(seconds=self.config.end_time_deadband_s)
            if request.end_time
            else datetime.now(UTC) + timedelta(seconds=self.config.end_time_deadband_s)
        )

        task = StandardCollectTask(
            target=target,
            end_time=end_time,
            camera_params=CameraParameterSet(
                integration_time_seconds=(request.integration_time / 1000.0)
                if request.integration_time
                else 1.0,
                frame_count=request.num_frames or 1,
                filter_name=self.config.collect.filter_name,
                readout_mode=self.config.collect.readout_mode,
                gain=self.config.collect.gain,
                binning_x=self.config.collect.binning,
                binning_y=self.config.collect.binning,
            ),
            sidereal_frames=self._get_sidereal_frames(request),
        )

        logger.info(f"Executing {request.id} with end_time={task.end_time}")

        # A fresh execution is a fresh frame set: drop bookkeeping left by any
        # earlier interrupted attempt of this request.
        self.state.publish_progress.pop(request.id, None)
        self._in_flight = request.id

        try:
            # The yield returns the minted TaskExecution; awaiting it returns a
            # TaskExecutionResult whose start/end times bracket the execution
            # (reported as the CollectResponse actual window).
            execution = yield task.submit(expiry_time=end_time)
            # Frames (and thus SenpaiResults) are stamped with the framework's
            # execution ID, not the CollectRequest ID; record the pairing for
            # the publishers.
            self.state.collect_request_ids[str(execution.task_id)] = request.id
            result = await execution
            logger.info(f"Finished executing {request.id}")
            # Stash the execution window so the later COMPLETED response
            # reports the same actual times as COLLECTED.
            progress = self.state.publish_progress.setdefault(request.id, _PublishProgress())
            progress.window = (
                result.start_time if result else None,
                result.end_time if result else None,
            )
            await self._send_response(
                request,
                ResponseStatus.COLLECTED,
                actual_start_time=result.start_time if result else None,
                actual_end_time=result.end_time if result else None,
            )
        except asyncio.CancelledError as e:
            logger.warning(f"{request.id} cancelled: {e}")
            if self._in_flight == request.id:
                await self._send_response(
                    request,
                    ResponseStatus.CANCELLED,
                    notes=str(e),
                )
            raise
        except Exception as e:
            logger.warning(f"{request.id} failed: {e}")
            await self._send_response(
                request,
                ResponseStatus.FAILED,
                notes=str(e),
            )
            raise
        finally:
            self._in_flight = None
            await self.queue.remove_task(request.id)
            # Drop the task reference and publish progress after a grace
            # period (bounds leaks when a set never finishes).
            asyncio.get_event_loop().call_later(
                300,
                lambda rid=request.id: (
                    self.tasks.pop(rid, None),
                    self.state.publish_progress.pop(rid, None),
                ),
            )

    def _get_sidereal_frames(self, request: CollectRequestFull) -> list[int]:
        """Sidereal frame indices for StandardCollectTask."""
        num_frames = request.num_frames or 1
        request_type = (request.type or "").upper()
        # Check the compound type before the bare SIDEREAL/STARE match, since it
        # contains the substring "SIDEREAL".
        if "RATE TRACK SIDEREAL" in request_type:
            return [num_frames - 1]
        if "SIDEREAL" in request_type or "STARE" in request_type:
            return list(range(num_frames))
        return []

    def _get_target(self, request: CollectRequestFull) -> Target | None:
        """Build a SensorKit Target from a CollectRequest."""
        match request:
            case CollectRequestFull(elset=elset) if elset and elset.line1 and elset.line2:
                return TLETarget(
                    tle=TLE(
                        line0=f"0 {elset.sat_no or request.orig_object_id or 'UNKNOWN'}",
                        line1=elset.line1,
                        line2=elset.line2,
                    )
                )

            case CollectRequestFull(state_vector=sv) if sv and None not in (
                sv.xpos, sv.ypos, sv.zpos, sv.epoch
            ):
                frame_str = sv.reference_frame or "J2000"
                try:
                    ref_frame = UDLReferenceFrame(frame_str).to_sensorkit_frame()
                except ValueError:
                    logger.warning(f"Unknown reference frame {frame_str}, defaulting to GCRF")
                    from sensorkit.astro.common import ReferenceFrame

                    ref_frame = ReferenceFrame.GCRF

                return StateVectorTarget(
                    frame=ref_frame,
                    sv=StateVector(
                        t=sv.epoch,
                        r=Cartesian(
                            x=sv.xpos * 1000,  # km to m
                            y=sv.ypos * 1000,
                            z=sv.zpos * 1000,
                        ),
                        v=Cartesian(
                            x=(sv.xvel or 0) * 1000,  # km/s to m/s
                            y=(sv.yvel or 0) * 1000,
                            z=(sv.zvel or 0) * 1000,
                        ),
                    ),
                )

            case CollectRequestFull(ra=ra, dec=dec) if ra is not None and dec is not None:
                return ICRSTarget(coords=Equatorial(ra=ra, dec=dec))

            case _:
                logger.warning(f"No supported target data found in CollectRequest {request.id}")
                return None
