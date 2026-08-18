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

# Statuses that resolve a request for good: once one is sent, the request must
# never be re-accepted while its window is still open (the poll re-fetches it
# after a restart or after the task-reference cleanup). CANCELLED is deliberately
# absent so an interrupted collect is re-collected on restart.
_RESOLUTION_STATUSES = frozenset(
    {
        ResponseStatus.COLLECTED,
        ResponseStatus.COMPLETED,
        ResponseStatus.REJECTED,
        ResponseStatus.FAILED,
    }
)

# Outbound CollectResponses are checked against the SDK's transcription of the
# UDL schema before they ship; the SDK itself never validates at runtime.
_COLLECT_RESPONSE_VALIDATOR = TypeAdapter(CollectResponseCreateParams)


def _prune_blank(value):
    """Drop dict members that carry no data (None, or objects of only blanks).

    UDL-compliant endpoints have been observed attaching placeholder sub-objects
    (e.g. ``"stateVector": {}``) to requests whose target is carried elsewhere.
    Schema-wise those fields are optional, but their object type has required
    members, so a placeholder would fail validation of an otherwise compliant
    request. Treating all-blank objects as absent keeps such requests valid
    while leaving partially-filled objects to fail loudly.
    """
    if isinstance(value, dict):
        pruned = {k: _prune_blank(v) for k, v in value.items()}
        pruned = {k: v for k, v in pruned.items() if v is not None}
        return pruned or None
    return value


# Bound on the persisted correlation/progress caches (insertion-ordered, oldest
# evicted first). Sized to outlive SENPAI's minutes-later results by a wide
# margin without letting state grow unbounded.
_STATE_CACHE_CAP = 512


class _PublishProgress(BaseModel):
    """Per-request imagery-publishing bookkeeping used to finalize a collect.

    attempted/uploaded count the frames the publisher has tried and landed in
    UDL, used to detect set completion and whether the set was fully delivered.
    window is the (start, end) execution window stashed by the task factory on
    the success path, so the deferred COMPLETED response can stamp the same
    actual times COLLECTED reported; it stays None until the collect succeeds,
    which also gates whether COMPLETED is sent at all.
    """

    attempted: int = 0
    uploaded: int = 0
    window: tuple[datetime | None, datetime | None] | None = None


class UDLState(BaseModel):
    """Persistent state for UDL program."""

    # Gzipped JSON array of full CollectRequest dumps. The whole state rides in
    # one NATS message (1 MB max payload); ~290 raw bodies hit that cap, and
    # gzip on this schema-repetitive JSON buys ~38x.
    pending_tasks: Base64Bytes = b""

    # Requests already answered with a resolving CollectResponse, mapped to
    # their window end; pruned once the poll filter (endTime > now) can no
    # longer return them.
    resolved: Dict[str, datetime] = Field(default_factory=dict)

    # Frame/EO correlation: the pipeline stamps frames (and thus SenpaiResults)
    # with the framework's execution task id, while everything here is keyed by
    # CollectRequest id. Recorded at dispatch, capped at _STATE_CACHE_CAP.
    task_requests: Dict[str, str] = Field(default_factory=dict)

    # Imagery-delivery bookkeeping by request id; persisted so a set whose
    # uploads were interrupted by a restart can still be finalized.
    publish_progress: Dict[str, _PublishProgress] = Field(default_factory=dict)


@sk.declare_program
class UDLProgram:
    """SensorKit program for UDL (Unified Data Library) integration.

    This program:
    - Polls UDL for CollectRequests assigned to our sensor
    - Converts CollectRequests to StandardCollectTasks
    - Delivers data products per the publish config: SkyImagery frame uploads
      and/or EOObservations built from senpai detections (see publishers.py)

    CollectResponse lifecycle for a request: ACCEPTED on receipt, then COLLECTED
    once the collect task finishes executing, then COMPLETED once the frame set
    has been delivered (see _finalize_set). REJECTED (unusable/expired request),
    CANCELLED, or FAILED replace the success path as appropriate.

    Requests are validated against the UDL schema at receipt and on restore
    (see _validate_collect_request), and target viability is triaged at receipt so an
    unusable request is REJECTED while the originating tasker can still re-assign it.
    Outbound CollectResponses are schema-validated before they ship. Once a
    request has a resolving response (see _RESOLUTION_STATUSES) it is
    remembered in state until its window closes, so a poll re-fetch — after a
    restart or the post-task reference cleanup — cannot re-accept it.

    Supports both username/password and cert-based authentication. The base_url
    can be pointed at UDL itself or any UDL-compliant endpoint.
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

        # Background tasks
        self._poller: asyncio.Task | None = None
        self._publisher: asyncio.Task | None = None

        # Data publishers
        self._imagery: SkyImageryPublisher | None = None
        self._eo_observation: EOObservationPublisher | None = None

        # Site location (populated from controller)
        self._site: SitePosition | None = None

        # State
        self.state = UDLState()

    async def _save_state(self) -> None:
        """Persist current state to KV store."""
        try:
            # Save ACCEPTED tasks that have not yet been COLLECTED.
            pending = (
                [request.model_dump(mode="json", by_alias=True) for request in self.queue.iter()]
                if self.queue
                else []
            )
            self.state.pending_tasks = gzip.compress(json.dumps(pending).encode())

            # Save publish progress so that we can continue when the service returns.
            now = datetime.now(UTC)
            self.state.resolved = {k: v for k, v in self.state.resolved.items() if v > now}
            for cache in (self.state.task_requests, self.state.publish_progress):
                while len(cache) > _STATE_CACHE_CAP:
                    cache.pop(next(iter(cache)))
            await self.program.kv_put_model(self.state)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    async def _restore_state(self) -> None:
        """Restore state from KV store."""
        try:
            self.state = await self.program.kv_get_model(UDLState)
            now = datetime.now(UTC)
            self.state.resolved = {k: v for k, v in self.state.resolved.items() if v > now}
            logger.debug(f"restored state for {self.program.entity}")
        except Exception:
            logger.warning(f"No saved state for {self.program.entity}")
            self.state = UDLState()

    @staticmethod
    def _load_credentials(endpoint: UDLEndpointConfig) -> tuple[str | None, str | None]:
        """Read UDL_USERNAME/UDL_PASSWORD for an endpoint (use_certs=False).

        Returns (None, None) when no credentials are configured, allowing
        unauthenticated requests against local UDL-compliant endpoints that
        don't enforce auth. When exactly one of username/password is provided,
        the partial config is treated as a mistake and rejected.
        """
        env = dotenv_values(endpoint.env_file)
        username = env.get("UDL_USERNAME") or os.environ.get("UDL_USERNAME")
        password = env.get("UDL_PASSWORD") or os.environ.get("UDL_PASSWORD")

        if bool(username) != bool(password):
            raise RuntimeError(
                f"UDL_USERNAME and UDL_PASSWORD must both be set "
                f"in {endpoint.env_file} or as environment variables"
            )

        return username or None, password or None

    def _create_client(
        self, endpoint: UDLEndpointConfig, username: str | None, password: str | None
    ) -> AsyncUnifieddatalibrary:
        """Create an SDK client for an endpoint.

        Auth that doesn't ride the Authorization header (cert/TLS, or none) is
        handled per request; see program_init.
        """
        if endpoint.use_certs:
            http_client = httpx.AsyncClient(
                cert=(endpoint.client_cert, endpoint.client_key),
                verify=endpoint.client_verify,
                timeout=endpoint.timeout,
            )
            logger.debug(f"using cert-based auth for {endpoint.base_url}")
            return AsyncUnifieddatalibrary(
                http_client=http_client,
                base_url=endpoint.base_url,
            )

        client_kwargs: dict[str, Any] = {"timeout": endpoint.timeout}
        if endpoint.base_url:
            client_kwargs["base_url"] = endpoint.base_url
        if not endpoint.client_verify:
            # Basic-auth against a self-signed endpoint (e.g. a local mock):
            # TLS verification settings only ride on a custom transport.
            client_kwargs["http_client"] = httpx.AsyncClient(verify=False)

        if not (username and password):
            logger.warning(
                f"no UDL credentials configured; issuing unauthenticated requests "
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

        # Primary client: CollectRequest polling and CollectResponses
        if not self.config.api.use_certs:
            self._username, self._password = self._load_credentials(self.config.api)
        self.client = self._create_client(
            self.config.api, self._username, self._password
        )

        # Upload client: SkyImagery only. Aliases the primary client unless a
        # separate upload endpoint is configured.
        if self.config.api.upload:
            if not self.config.api.upload.use_certs:
                self._upload_username, self._upload_password = self._load_credentials(
                    self.config.api.upload
                )
            self.upload_client = self._create_client(
                self.config.api.upload, self._upload_username, self._upload_password
            )
            logger.debug(
                f"SkyImagery uploads routed to {self.config.api.upload.base_url}"
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
                f"lon={self._site.longitude_degrees}, alt={self._site.altitude_km}km"
            )
        except Exception as e:
            logger.warning(
                f"Could not read SitePosition from controller {self.config.controller}: {e}. "
                f"SkyImagery will be uploaded without senlat/senlon/senalt."
            )

        # Initialize task queue with offer window integration.
        self.queue = TaskQueue(
            self.program,
            on_expired=self._cancel_collect_request,
            executing_deadband_s=self.config.end_time_deadband_s,
        )

        # Restore pending tasks from state.
        try:
            pending = (
                json.loads(gzip.decompress(self.state.pending_tasks))
                if self.state.pending_tasks
                else []
            )
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

        await self._init_publishers()

        # Start background poller
        self._poller = asyncio.create_task(self._poll_loop())

        # Start frame publisher (feeds the data publishers from the graph sink)
        self._publisher = asyncio.create_task(self._publish_loop())

    async def _init_publishers(self) -> None:
        """Create the data publishers enabled by the publish config."""
        if self.config.publish.sky_imagery:
            self._imagery = SkyImageryPublisher(self)
        if self.config.publish.eo_observation:
            self._eo_observation = EOObservationPublisher(self)

        if self._eo_observation:
            for request in self.tasks.values():
                self._eo_observation.note_request(request)
            await self._eo_observation.start()

    async def _cancel_collect_request(self, request: CollectRequestFull) -> None:
        """A queued request ran out its window without ever executing."""
        self.tasks.pop(request.id, None)
        await self._send_response(
            request, ResponseStatus.CANCELLED, notes="Expired before execution"
        )

    @sk.on_detach
    async def program_deinit(self) -> None:
        """Cancel tasks, save state, close connections."""
        logger.debug(f"stopping UDL program {self.program.entity}")

        # Send a CANCELLED response for any in-flight CollectRequest executions.
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
            await self._eo_observation.close()

        await self._save_state()

        if self.upload_client and self.upload_client is not self.client:
            await self.upload_client.close()

        if self.client:
            await self.client.close()

    # ── Polling ──

    async def _poll_loop(self) -> None:
        """Poll UDL for CollectRequests assigned to our sensor."""
        logger.debug("poller started")

        while True:
            try:
                now = datetime.now(UTC)
                horizon = now + timedelta(seconds=self.config.poll_horizon)
                filter_field = (
                    "origSensorId"
                    if self.config.api.poll_filter == "orig_sensor_id"
                    else "idSensor"
                )
                page = await self.client.collect_requests.list(
                    start_time=f"<{_to_udl_timestamp(horizon)}",
                    extra_query={
                        filter_field: self.config.api.id_sensor,
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
        """Schema-validate a CollectRequest payload (UDL schema via the SDK models).

        Blank sub-objects are treated as absent (see _prune_blank); everything
        else must validate in full. The SDK never validates at runtime — its
        poll responses are constructed leniently — so this is the only
        enforcement point.
        """
        return CollectRequestFull.model_validate(_prune_blank(payload))

    async def _handle_collect_request(self, request: CollectRequestFull) -> None:
        """Validate a polled CollectRequest, then accept or reject it."""
        if not request.id:
            logger.error("Ignoring CollectRequest without an id")
            return

        if request.id in self.tasks or request.id in self.state.resolved:
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

        # Triage the target now rather than at window open, so the tasker
        # hears REJECTED while it can still re-assign the collect.
        if self._build_target(request) is None:
            await self._send_response(
                request, ResponseStatus.REJECTED, notes="Unsupported target type"
            )
            return

        logger.debug(f"received CollectRequest {request.id}")

        self.tasks[request.id] = request
        if self._eo_observation:
            self._eo_observation.note_request(request)
        await self.queue.push_task(request)

        await self._send_response(request, ResponseStatus.ACCEPTED)
        await self._save_state()

    # ── Responses ──

    async def _send_response(
        self,
        request: CollectRequestFull,
        status: ResponseStatus,
        *,
        actual_start_time: datetime | None = None,
        actual_end_time: datetime | None = None,
        notes: str | None = None,
    ) -> None:
        """Send a CollectResponse.

        Resolving statuses are recorded (and persisted) before the send is
        attempted: the disposition of the request is decided regardless of
        whether this particular response reaches the endpoint.
        """
        if status in _RESOLUTION_STATUSES and request.id:
            # No window end → retire the resolved record after the poll horizon.
            self.state.resolved[request.id] = request.end_time or (
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
        # Optional fields ride only when set: the SDK serializes an explicit
        # None as JSON null, which is off the UDL schema.
        params = {k: v for k, v in params.items() if v is not None}

        try:
            _COLLECT_RESPONSE_VALIDATOR.validate_python(params)
        except ValidationError as e:
            logger.error(
                f"CollectResponse for {request.id} violates the UDL schema; not sent: {e}"
            )
            return

        try:
            await self.client.collect_responses.create(
                **params,
                extra_headers=self._client_headers,
            )
            logger.debug(f"sent {status.value} response for request {request.id}")
        except Exception as e:
            logger.warning(f"Failed to send response for {request.id}: {e}")

    # ── Publishing ──

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
        if not task_id:
            return

        # The pipeline stamps the framework's execution id; translate to the
        # CollectRequest it served (falling back to a direct match for
        # pipelines that already carry the request id).
        request_id = self.state.task_requests.get(task_id, task_id)
        request = self.tasks.get(request_id)
        if not request:
            logger.warning(f"No CollectRequest found for task_id {task_id}")
            return

        info = context.get(FileInfo)
        frame_key = str(info.path) if info else f"{request_id}:{context.get('frame_num')}"
        if frame_key in self._seen_frames:
            logger.debug(f"task ({request.id}): duplicate frame event for {frame_key}; skipping")
            return
        self._seen_frames[frame_key] = None
        while len(self._seen_frames) > _STATE_CACHE_CAP:
            self._seen_frames.pop(next(iter(self._seen_frames)))

        image_set_length = request.num_frames or 1
        progress = self.state.publish_progress.setdefault(request.id, _PublishProgress())
        progress.attempted += 1

        if self._imagery:
            try:
                await self._imagery.publish(context, data, request)
                progress.uploaded += 1
            except Exception as e:
                logger.warning(f"Task ({request.id}) failed to upload skyimagery: {e}")

        await self._finalize_set(request, progress, image_set_length)

    async def _finalize_set(
        self, request: CollectRequestFull, progress: _PublishProgress, image_set_length: int
    ) -> None:
        """Send COMPLETED once every frame in the set has been seen.

        With imagery publishing enabled, at least one frame must have reached
        UDL — a partially-delivered set is still "completed" enough to ack, so
        per-frame upload failures (including the final frame) are tolerated.
        Without it (including EO-only publishing), seeing the full set is
        completion. The window guard scopes COMPLETED to tasks that collected
        successfully (the factory only stashes a window on the success path).
        EO posting never gates COMPLETED: SENPAI results arrive minutes later
        and are best-effort.
        """
        if progress.attempted < image_set_length:
            return

        self.state.publish_progress.pop(request.id, None)
        delivered = self._imagery is None or progress.uploaded > 0

        if delivered and progress.window is not None:
            start_time, end_time = progress.window
            await self._send_response(
                request,
                ResponseStatus.COMPLETED,
                actual_start_time=start_time,
                actual_end_time=end_time,
            )
            detail = (
                f"uploaded {progress.uploaded}/{image_set_length} frames"
                if self._imagery
                else f"{image_set_length} frames seen (imagery publishing disabled)"
            )
            logger.info(f"task {request.id} COMPLETED: {detail}")
        elif self._imagery and progress.uploaded == 0:
            logger.warning(
                f"task ({request.id}): all {image_set_length} frame uploads "
                f"failed; COMPLETED not sent"
            )

    # ── Task generation ──

    @sk.task_factory
    async def generate(self):
        """Convert CollectRequests to StandardCollectTasks."""
        request = await self.queue.pop_task()
        if not request:
            yield None
            return

        target = self._build_target(request)
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
            ),
            sidereal_frames=self._track_mode(request),
        )

        logger.info(f"Task ({request.id}): starting execution with end_time={task.end_time}")

        # A fresh execution is a fresh frame set: drop bookkeeping left by any
        # earlier interrupted attempt of this request.
        self.state.publish_progress.pop(request.id, None)
        self._in_flight = request.id

        try:
            # The yield hands back the minted TaskExecution; awaiting it yields
            # a TaskExecutionResult whose start_time/end_time bracket the
            # controller's task execution (before slew … after the mount stop),
            # which we report as the CollectResponse's actual window.
            # Per-exposure precision is carried separately by SkyImagery's
            # expStartTime/expEndTime.
            execution = yield task.submit(expiry_time=end_time)
            # Frames (and thus SenpaiResults) are stamped with the framework's
            # execution id, not the CollectRequest id; record the pairing for
            # the publishers.
            self.state.task_requests[str(execution.task_id)] = request.id
            result = await execution
            logger.info(f"Task ({request.id}): finished execution successfully")
            # Stash the execution window so the COMPLETED response — sent later
            # from _finalize_set once the imagery set finishes uploading — can
            # report the same actual times COLLECTED carries here. setdefault
            # preserves any frame counts the publisher already recorded.
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
            logger.warning(f"Task ({request.id}): cancelled. {e=}")
            # program_deinit clears _in_flight after sending its own CANCELLED;
            # only respond here for cancellations it didn't already cover.
            if self._in_flight == request.id:
                await self._send_response(
                    request,
                    ResponseStatus.CANCELLED,
                    notes=str(e),
                )
            raise
        except Exception as e:
            logger.warning(f"Task ({request.id}): failed. {e=}")
            await self._send_response(
                request,
                ResponseStatus.FAILED,
                notes=str(e),
            )
            raise
        finally:
            self._in_flight = None
            await self.queue.remove_task(request.id)
            # Keep the task reference and publish progress for imagery-publishing
            # correlation, then drop them after a grace period (bounds leaks when
            # a set never finishes, e.g. dropped frames).
            asyncio.get_event_loop().call_later(
                300,
                lambda rid=request.id: (
                    self.tasks.pop(rid, None),
                    self.state.publish_progress.pop(rid, None),
                ),
            )

    # ── Target building ──

    def _track_mode(self, request: CollectRequestFull) -> list[int]:
        """Frame indices (0-based) to hold under sidereal tracking.

        The mount rate-tracks a satellite target by default (stars streak);
        listing a frame here instead holds RA/Dec fixed for it (the satellite
        streaks). UDL's ``type`` is a free string, so one match serves UDL and
        UDL-compliant endpoints alike: ``STARE``/``SIDEREAL`` request the whole
        collect sidereally, while the compound ``RATE TRACK SIDEREAL`` means
        rate-track with only the final frame sidereal. Everything else
        (``RATE TRACK``, ``OBJECT``, …) is all-rate. A RA/Dec target is already
        sidereal at the handler regardless of this.
        """
        num_frames = request.num_frames or 1
        request_type = (request.type or "").upper()
        # Check the compound type before the bare SIDEREAL/STARE match, since it
        # contains the substring "SIDEREAL".
        if "RATE TRACK SIDEREAL" in request_type:
            return [num_frames - 1]
        if "SIDEREAL" in request_type or "STARE" in request_type:
            return list(range(num_frames))
        return []

    def _build_target(self, request: CollectRequestFull) -> Target | None:
        """Build a SensorKit Target from a CollectRequest.

        Supports Elset (TLE), StateVector, and RA/Dec targets.
        """
        # Try Elset (TLE) first
        if request.elset and request.elset.line1 and request.elset.line2:
            elset = request.elset
            line0 = f"0 {elset.sat_no or request.orig_object_id or 'UNKNOWN'}"

            return TLETarget(
                tle=TLE(
                    line0=line0,
                    line1=elset.line1,
                    line2=elset.line2,
                )
            )

        # Try StateVector. A position without its epoch is not a usable target:
        # propagating from a made-up time yields confidently wrong pointing.
        if request.state_vector:
            sv = request.state_vector
            if None not in (sv.xpos, sv.ypos, sv.zpos, sv.epoch):
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

        # Fallback: RA/Dec pointing
        if request.ra is not None and request.dec is not None:
            return ICRSTarget(
                coords=Equatorial(ra=request.ra, dec=request.dec),
            )

        logger.warning(f"Task ({request.id}): No supported target data found")
        return None
