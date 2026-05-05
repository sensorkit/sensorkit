from __future__ import annotations

import asyncio
import io
import json
import os
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Dict, List

import httpx
from dotenv import dotenv_values
from loguru import logger
from pydantic import BaseModel, Field
from unifieddatalibrary import AsyncUnifieddatalibrary
from unifieddatalibrary.types import CollectRequestFull

import sensorkit.api as sk
from sensorkit.astro.common import TLE, Cartesian, Equatorial, StateVector
from sensorkit.astro.target import ICRSTarget, StateVectorTarget, TLETarget
from sensorkit.models.devices import SitePosition, Target
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask
from sensorkit.udl.models import ResponseStatus, UDLConfig, UDLReferenceFrame
from sensorkit.udl.task_queue import TaskQueue


def _udl_ts(dt: datetime) -> str:
    """Format a datetime as UDL expects: ISO 8601 UTC with trailing 'Z' (no offset)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class UDLState(BaseModel):
    """Persistent state for UDL program."""

    pending_tasks: List[Dict] = Field(default_factory=list)


@sk.declare_program
class UDLProgram:
    """SensorKit program for UDL (Unified Data Library) integration.

    This program:
    - Polls UDL for CollectRequests assigned to our sensor
    - Acknowledges requests with ACCEPTED/REJECTED/COMPLETED status
    - Converts CollectRequests to StandardCollectTasks
    - Publishes imagery back to UDL as SkyImagery

    Supports both UDL (username/password) and MACHINA (cert-based) authentication.
    The base_url can be pointed at either endpoint.
    """

    def __init__(self, config: UDLConfig):
        self.config = config
        self.program: sk.ProgramImpl | None = None

        # SDK client (created in program_init)
        self.client: AsyncUnifieddatalibrary | None = None

        # Task management
        self.queue: TaskQueue | None = None
        self.tasks: Dict[str, CollectRequestFull] = {}

        # Background tasks
        self._poller: asyncio.Task | None = None
        self._publisher: asyncio.Task | None = None

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

    @sk.on_attach
    async def program_init(self) -> None:
        """Restore state, create SDK client, start poller and image publisher."""
        self.program = sk.program()

        # Restore last known state
        await self._restore_state()

        # Create SDK client with appropriate auth method
        if self.config.api.use_certs:
            http_client = httpx.AsyncClient(
                cert=(self.config.api.client_cert, self.config.api.client_key),
                verify=self.config.api.client_verify,
                timeout=self.config.api.timeout,
            )
            self.client = AsyncUnifieddatalibrary(
                http_client=http_client,
                base_url=self.config.api.base_url,
            )
            logger.debug(f"using cert-based auth for {self.config.api.base_url}")
        else:
            env = dotenv_values(self.config.api.env_file)
            username = env.get("UDL_USERNAME") or os.environ.get("UDL_USERNAME")
            password = env.get("UDL_PASSWORD") or os.environ.get("UDL_PASSWORD")

            if not username or not password:
                raise RuntimeError(
                    f"UDL_USERNAME and UDL_PASSWORD must be set in "
                    f"{self.config.api.env_file} or as environment variables"
                )

            client_kwargs = {
                "username": username,
                "password": password,
                "timeout": self.config.api.timeout,
            }
            if self.config.api.base_url:
                client_kwargs["base_url"] = self.config.api.base_url

            self.client = AsyncUnifieddatalibrary(**client_kwargs)

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

        # Start background poller
        self._poller = asyncio.create_task(self._poll_loop())

        # Start imagery publisher
        self._publisher = asyncio.create_task(self._publish_loop())

    @sk.on_detach
    async def program_deinit(self) -> None:
        """Cancel tasks, save state, close connections."""
        logger.debug(f"stopping UDL program {self.program.entity}")

        for task in [self._poller, self._publisher]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        await self._save_state()

        if self.client:
            await self.client.close()

    # ── Polling ──

    async def _poll_loop(self) -> None:
        logger.debug("poller started")

        while True:
            try:
                await self._poll_collect_requests()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Error in poll loop: {e}")

            await asyncio.sleep(self.config.poll_frequency)

    async def _poll_collect_requests(self) -> None:
        """Poll UDL for CollectRequests assigned to our sensor."""
        try:
            now = datetime.now(UTC)
            page = await self.client.collect_requests.list(
                start_time=f"<{_udl_ts(now)}",
                extra_query={
                    "origSensorId": self.config.api.id_sensor,
                    "endTime": f">{_udl_ts(now)}",
                },
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
                actual_start_time=actual_start_time,
                actual_end_time=actual_end_time,
                notes=notes,
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
                await self._publish_imagery(context, data)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error in publish loop: {e}")

    async def _publish_imagery(self, context: dict, data: bytes) -> None:
        """Build and upload SkyImagery for a completed frame."""
        task_id = context.get("task_id")
        if not task_id:
            return

        request = self.tasks.get(task_id)
        if not request:
            logger.warning(f"No CollectRequest found for task_id {task_id}")
            return

        # Extract context values
        file_path = context.get("file_path")
        path = Path(file_path) if isinstance(file_path, (str, Path)) else None
        filename = (
            path.name
            if path
            else context.get("file_name", f"{request.id}_{context.get('frame_num', 0)}.fits")
        )

        date_obs = context.get("date_obs")
        exp_start_time = datetime.fromisoformat(date_obs) if date_obs else datetime.now(UTC)

        exposure_time = context.get("exptime")
        exp_end_time = exp_start_time + timedelta(seconds=float(exposure_time))

        # sequenceId must be >= 1 (UDL requirement)
        frame_num = context.get("frame_num", 0)
        sequence_id = frame_num + 1

        # Build SkyImagery metadata
        metadata = {
            "classificationMarking": request.classification_marking,
            "idSensor": self.config.api.id_sensor,
            "satNo": request.sat_no,
            "expStartTime": _udl_ts(exp_start_time),
            "expEndTime": _udl_ts(exp_end_time),
            "imageSetId": request.id,
            "imageSetLength": request.num_frames or 1,
            "sequenceId": sequence_id,
            "frameWidthPixels": context.get("image_width"),
            "frameHeightPixels": context.get("image_height"),
            "pixelBitDepth": context.get("bits_per_pixel"),
            "filename": filename,
            "filesize": len(data),
            "source": self.config.api.source,
            "dataMode": request.data_mode or "TEST",
            "imageType": context.get("image_type") or self.config.image_type,
        }

        if self._site:
            metadata["senlat"] = self._site.latitude_degrees
            metadata["senlon"] = self._site.longitude_degrees
            metadata["senalt"] = self._site.altitude_km
        else:
            logger.warning(
                f"Task ({request.id}): publishing SkyImagery without sensor location "
                f"(no SitePosition from controller {self.config.controller})"
            )

        # Remove None values
        metadata = {k: v for k, v in metadata.items() if v is not None}

        metadata_bytes = json.dumps(metadata).encode()
        metadata_fname = f"{Path(filename).stem}_skyimagery.json"

        # Save locally if configured
        if self.config.skyimagery_save_path:
            await self._save_archive_locally(
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

        try:
            await self.client.sky_imagery.upload_zip(file=zip_buffer.getvalue())
            logger.debug(
                f"task ({request.id}) uploaded skyimagery "
                f"({sequence_id}/{metadata.get('imageSetLength', 1)})"
            )
        except Exception as e:
            logger.warning(f"Task ({request.id}) failed to upload skyimagery: {e}")

    async def _save_archive_locally(
        self,
        task_id: str,
        data_fname: str,
        data: bytes,
        metadata_fname: str,
        metadata_bytes: bytes,
    ) -> None:
        """Save imagery archive to local filesystem."""
        try:
            save_path = Path(self.config.skyimagery_save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            zip_path = save_path / f"{Path(data_fname).stem}.zip"

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(data_fname, data)
                zf.writestr(metadata_fname, metadata_bytes)

            logger.debug(f"task ({task_id}) saved archive to {zip_path}")
        except Exception as e:
            logger.warning(f"Task ({task_id}) failed to save archive locally: {e}")

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
            else datetime.now(UTC) + timedelta(minutes=self.config.end_time_deadband_s)
        )

        task = StandardCollectTask(
            task_id=request.id,
            controller_id=str(self.config.controller),
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
            yield task
            logger.info(f"Task ({request.id}): finished execution successfully")
            await self._send_response(request, ResponseStatus.COLLECTED)
        except asyncio.CancelledError as e:
            logger.warning(f"Task ({request.id}): cancelled. {e=}")
            await self._send_response(
                request,
                ResponseStatus.CANCELLED,
                notes=str(e),
            )
        except Exception as e:
            logger.warning(f"Task ({request.id}): failed. {e=}")
            await self._send_response(
                request,
                ResponseStatus.FAILED,
                notes=str(e),
            )
        finally:
            await self.queue.remove_task(request.id)
            # Keep task reference for imagery publishing correlation
            asyncio.get_event_loop().call_later(300, lambda: self.tasks.pop(request.id, None))

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
