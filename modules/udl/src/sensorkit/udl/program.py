# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List

import httpx
from dotenv import dotenv_values
from loguru import logger
from pydantic import BaseModel, Field
from unifieddatalibrary import AsyncUnifieddatalibrary, omit
from unifieddatalibrary.types import CollectRequestFull

import sensorkit.api as sk
from sensorkit.astro.common import TLE, SitePosition
from sensorkit.astro.coords import Cartesian, Equatorial, StateVector
from sensorkit.astro.target import ICRSTarget, StateVectorTarget, Target, TLETarget
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
    _udl_ts,
)
from sensorkit.udl.task_queue import TaskQueue


class UDLState(BaseModel):
    """Persistent state for UDL program."""

    pending_tasks: List[Dict] = Field(default_factory=list)


@dataclass
class _PublishProgress:
    """Per-task imagery-publishing bookkeeping used to finalize a collect.

    attempted/uploaded count the frames the publisher has tried and landed in
    UDL, used to detect set completion and whether anything was delivered.
    window is the (start, end) execution window stashed by the task factory on
    the success path, so the deferred COMPLETED response can stamp the same
    actual times COLLECTED reported; it stays None until the collect succeeds,
    which also gates whether COMPLETED is sent at all.
    """

    attempted: int = 0
    uploaded: int = 0
    window: tuple[datetime | None, datetime | None] | None = None


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

    Supports both username/password and cert-based authentication. The base_url
    can be pointed at UDL itself or any UDL-compliant endpoint.
    """

    def __init__(self):
        self.program: sk.ProgramImpl | None = None

        # SDK clients (created in program_init). upload_client targets the
        # SkyImagery upload endpoint; it aliases client unless api.upload is
        # configured.
        self.client: AsyncUnifieddatalibrary | None = None
        self.upload_client: AsyncUnifieddatalibrary | None = None

        self._udl_username: str | None = None
        self._udl_password: str | None = None
        self._upload_username: str | None = None
        self._upload_password: str | None = None

        # Per-request headers for each client. Unauthenticated and cert-auth
        # clients send no Authorization header, which the SDK refuses unless we
        # explicitly omit it per request; credentialed clients pass {} so their
        # Basic-auth header is preserved. Populated in program_init.
        self._client_headers: dict[str, Any] = {}
        self._upload_headers: dict[str, Any] = {}

        # Task management
        self.queue: TaskQueue | None = None
        self.tasks: Dict[str, CollectRequestFull] = {}

        # Per-task publishing bookkeeping, keyed by request id and cleared once
        # the set completes or the task reference expires.
        self._publish_progress: Dict[str, _PublishProgress] = {}

        # Background tasks
        self._poller: asyncio.Task | None = None
        self._publisher: asyncio.Task | None = None

        # Data publishers (created in program_init per config.publish)
        self._imagery: SkyImageryPublisher | None = None
        self._eo: EOObservationPublisher | None = None

        # Frames consumed from the data graph (feeds the EO watchdog)
        self.frames_seen = 0

        # Site location (populated from controller)
        self._site: SitePosition | None = None

        # State
        self.state = UDLState()

    async def _save_state(self) -> None:
        """Persist current state to KV store."""
        try:
            self.state.pending_tasks = (
                [request.model_dump(mode="json") for request in self.queue.iter()]
                if self.queue
                else []
            )
            await self.program.kv_put_model(self.state)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    async def _restore_state(self) -> None:
        """Restore state from KV store."""
        try:
            self.state = await self.program.kv_get_model(UDLState)
            logger.debug(f"restored state with {len(self.state.pending_tasks)} pending tasks")
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
                f"UDL_USERNAME and UDL_PASSWORD must both be set (or both omitted) "
                f"in {endpoint.env_file} or as environment variables"
            )

        return username or None, password or None

    @staticmethod
    def _omit_auth_headers(username: str | None) -> dict[str, Any]:
        """Per-request headers for a client built with the given username.

        Without credentials (unauthenticated or cert-auth) we send no
        Authorization header; the SDK refuses that unless it's explicitly
        omitted per request, so pass `omit`. Credentialed clients pass nothing,
        leaving their Basic-auth header intact (a per-request Omit would
        override and strip it).
        """
        return {} if username else {"Authorization": omit}

    def _create_client(
        self, endpoint: UDLEndpointConfig, username: str | None, password: str | None
    ) -> AsyncUnifieddatalibrary:
        """Create an SDK client for an endpoint.

        Auth that doesn't ride the Authorization header (cert/TLS, or none) is
        handled per request via _omit_auth_headers; see program_init.
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
            self._udl_username, self._udl_password = self._load_credentials(self.config.api)
        self.client = self._create_client(
            self.config.api, self._udl_username, self._udl_password
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
            self._upload_username = self._udl_username
            self._upload_password = self._udl_password

        # Resolve per-request auth-header omission for each client. Keying off
        # the resolved username also covers the aliased case, where
        # _upload_username == _udl_username.
        self._client_headers = self._omit_auth_headers(self._udl_username)
        self._upload_headers = self._omit_auth_headers(self._upload_username)

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

        # Initialize task queue with offer window integration
        self.queue = TaskQueue(self.program)

        # Restore pending tasks from state
        for task_dict in self.state.pending_tasks:
            try:
                request = CollectRequestFull.model_validate(task_dict)
                if request.end_time and request.end_time > datetime.now(UTC):
                    self.tasks[request.id] = request
                    await self.queue.push_task(request)
                    logger.debug(f"restored task {request.id}")
            except Exception as e:
                logger.warning(f"Failed to restore task: {e}")

        await self._init_publishers()

        # Start background poller
        self._poller = asyncio.create_task(self._poll_loop())

        # Start frame publisher (feeds the data publishers from the graph sink)
        self._publisher = asyncio.create_task(self._publish_loop())

    async def _init_publishers(self) -> None:
        """Create the data publishers enabled by the publish config."""
        if self.config.publish.upload:
            if self.config.publish.sky_imagery:
                self._imagery = SkyImageryPublisher(self)
            if self.config.publish.eo_observation:
                self._eo = EOObservationPublisher(self)

        if self._eo:
            for request in self.tasks.values():
                self._eo.note_request(request)
            await self._eo.start()

    @sk.on_detach
    async def program_deinit(self) -> None:
        """Cancel tasks, save state, close connections."""
        logger.debug(f"stopping UDL program {self.program.entity}")

        for task in [self._poller, self._publisher]:
            if task and not task.done():
                task.cancel()

                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if self._eo:
            await self._eo.close()
        if self._imagery:
            await self._imagery.close()

        await self._save_state()

        if self.upload_client and self.upload_client is not self.client:
            await self.upload_client.close()

        if self.client:
            await self.client.close()

    # ── Polling ──

    async def _poll_loop(self) -> None:
        logger.debug("poller started")

        while True:
            try:
                await self._poll_collect_requests()
            except Exception as e:
                logger.exception(f"Error in poll loop: {e}")

            await asyncio.sleep(self.config.poll_frequency)

    async def _poll_collect_requests(self) -> None:
        """Poll UDL for CollectRequests assigned to our sensor."""
        try:
            now = datetime.now(UTC)
            horizon = now + timedelta(days=1)
            filter_field = (
                "origSensorId"
                if self.config.api.poll_filter == "orig_sensor_id"
                else "idSensor"
            )
            page = await self.client.collect_requests.list(
                start_time=f"<{_udl_ts(horizon)}",
                extra_query={
                    filter_field: self.config.api.id_sensor,
                    "endTime": f">{_udl_ts(now)}",
                },
                extra_headers=self._client_headers,
            )

            for request in page.items:
                await self._handle_collect_request(request)

        except Exception as e:
            logger.warning(f"Error polling collect requests: {e}")

    async def _handle_collect_request(self, request: CollectRequestFull) -> None:
        """Process a new CollectRequest."""
        if request.id in self.tasks:
            return

        if request.end_time and request.end_time < datetime.now(UTC):
            logger.debug(f"skipping expired request {request.id}")
            await self._send_response(request, ResponseStatus.REJECTED, notes="Request expired")
            return

        logger.debug(f"received CollectRequest {request.id}")

        self.tasks[request.id] = request
        if self._eo:
            self._eo.note_request(request)
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
        """Send a CollectResponse."""
        try:
            await self.client.collect_responses.create(
                classification_marking=request.classification_marking,
                data_mode=request.data_mode or "TEST",
                source=self.config.api.source,
                id_request=request.id,
                status=status.value,
                sat_no=request.sat_no,
                task_id=request.task_id,
                id_plan=request.id_plan,
                external_id=request.external_id,
                id_sensor=self.config.api.id_sensor,
                orig_sensor_id=self.config.api.id_sensor,
                actual_start_time=_udl_ts(actual_start_time) if actual_start_time else None,
                actual_end_time=_udl_ts(actual_end_time) if actual_end_time else None,
                notes=notes,
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

        request = self.tasks.get(task_id)
        if not request:
            logger.warning(f"No CollectRequest found for task_id {task_id}")
            return

        self.frames_seen += 1
        image_set_length = request.num_frames or 1
        progress = self._publish_progress.setdefault(request.id, _PublishProgress())
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
        Without it, seeing the full set is completion. The window guard scopes
        COMPLETED to tasks that collected successfully (the factory only
        stashes a window on the success path). EO posting never gates
        COMPLETED: SENPAI results arrive minutes later and are best-effort.
        """
        if progress.attempted < image_set_length:
            return

        self._publish_progress.pop(request.id, None)
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
                f"{progress.uploaded}/{image_set_length} frames delivered"
                if self._imagery
                else f"all {image_set_length} frames seen; imagery publishing disabled"
            )
            logger.info(f"task ({request.id}): sent COMPLETED ({detail})")
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
        )

        logger.info(f"Task ({request.id}): starting execution with end_time={task.end_time}")

        try:
            # The framework sends back a TaskExecutionResult on success; its
            # start_time/end_time bracket the controller's task execution
            # (before slew … after the mount stop), which we report as the
            # CollectResponse's actual window. Per-exposure precision is carried
            # separately by SkyImagery's expStartTime/expEndTime.
            result = await (yield task.submit(expiry_time=end_time))
            logger.info(f"Task ({request.id}): finished execution successfully")
            # Stash the execution window so the COMPLETED response — sent later
            # from _publish_imagery once the imagery set finishes uploading — can
            # report the same actual times COLLECTED carries here. setdefault
            # preserves any frame counts the publisher already recorded.
            progress = self._publish_progress.setdefault(request.id, _PublishProgress())
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
            await self.queue.remove_task(request.id)
            # Keep the task reference and publish progress for imagery-publishing
            # correlation, then drop them after a grace period (bounds leaks when
            # a set never finishes, e.g. dropped frames).
            asyncio.get_event_loop().call_later(
                300,
                lambda rid=request.id: (
                    self.tasks.pop(rid, None),
                    self._publish_progress.pop(rid, None),
                ),
            )

    # ── Target building ──

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

        # Try StateVector
        if request.state_vector:
            sv = request.state_vector
            if sv.xpos is not None and sv.ypos is not None and sv.zpos is not None:
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
                        t=sv.epoch or datetime.now(UTC),
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
