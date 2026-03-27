import asyncio
from datetime import datetime, timedelta, UTC
import random
from typing import Dict, List
import uuid

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.core.controller import ControllerClient
from sensorkit.models.devices import SitePosition
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask

from sensorkit.otto.models import OttoConfig
from sensorkit.otto.task_queue import TaskQueue, start_fastapi
from sensorkit.otto.utils import (
    fetch_tles,
    check_satellite_visibility,
    dither_tle,
    ObjectListManager,
    ListType,
)


class OttoState(BaseModel):
    """Persistent state for Otto program."""
    blacklist: List[str] = Field(default_factory=list)
    graylist: List[str] = Field(default_factory=list)
    whitelist: List[str] = Field(default_factory=list)


class TLECache(BaseModel):
    """Cached TLE data."""
    tles: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    dt: datetime


@sk.declare_program
class OttoProgram:

    def __init__(self, config: OttoConfig):
        self.config = config

        self.task_queue: TaskQueue | None = None

        self.tles: Dict[str, Dict[str, str]] = {}
        self._tle_updater: asyncio.Task | None = None

        self._fastapi_server: asyncio.Task | None = None
        self._task_generator: asyncio.Task | None = None
        self._publisher: asyncio.Task | None = None

        self.state: OttoState = OttoState()
        self.list_manager: ObjectListManager | None = None

    async def _save_state(self):
        try:
            await self.program.kv_put_model(self.state)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    @sk.on_enable
    async def program_enable(self):
        pass
    #     self.controller_client = ControllerClient(self.program.binding.backend, sk.Entity.at(self.config.controller))
    #     self._controller_location = await self.controller_client.kv_get_model(SitePosition)
    #     self.latitude = self._controller_location.latitude_degrees
    #     self.longitude = self._controller_location.longitude_degrees
    #     self.altitude_km = self._controller_location.altitude_km
    #
    #     # Start automated task generation
    #     self._task_generator = asyncio.create_task(self.generate_tasks())

    @sk.on_attach
    async def program_init(self):
        """Load state, set up the controller, task queue, and optionally start task generation and image publishing."""
        self.program = sk.program()

        # Restore last known state
        try:
            self.state = await self.program.kv_get_model(OttoState)
        except Exception:
            logger.warning(f"No saved state for {self.program.entity}")
            self.state.whitelist, self.state.graylist, self.state.blacklist = self.config.task.objects, [], []

        # Initialize list manager
        self.list_manager = ObjectListManager(self.state, self._save_state)

        # Restore TLE cache
        # Useful for proceeding during rate-limit exception on Spacebook fetch
        try:
            tle_cache = await self.program.kv_get_model(TLECache)
            self.tles = tle_cache.tles
            self.tles_dt = tle_cache.dt
        except Exception:
            logger.debug("no cached TLEs found, will fetch on startup")

        logger.debug("starting Otto program")

        # Initialize task queue
        self.task_queue = TaskQueue(self.program)

        # Start TLE updater (updates on startup and every 24 hours)
        self._tle_updater = asyncio.create_task(self.update_tles_loop())

        # Start graylist promoter
        self._graylist_promoter = asyncio.create_task(self.promote_graylist_loop())

        # Start FastAPI server for manual tasking additions
        server = getattr(self.config, "server", None)
        fastapi_config = {
            "host": getattr(server, "host", "0.0.0.0"),
            "port": getattr(server, "port", 8001),
            "log_level": getattr(server, "log_level", "info"),
        }
        self._fastapi_server = asyncio.create_task(
            start_fastapi(fastapi_config, self.task_queue)
        )

        # Get site location
        self.controller_client = ControllerClient(self.program.backend, sk.Entity.at(self.config.controller))
        self._controller_location = await self.controller_client.kv_get_model(SitePosition)
        self.latitude = self._controller_location.latitude_degrees
        self.longitude = self._controller_location.longitude_degrees
        self.altitude_km = self._controller_location.altitude_km

        # Start automated task generation
        self._task_generator = asyncio.create_task(self.generate_tasks())

        # Start data publishing (if configured)
        if self.config.publish.upload:
            logger.debug(f"publishing Otto data for {self.config.publish.sensor_name}")
            self._publisher = asyncio.create_task(self.data_publisher())

        await asyncio.sleep(0.1)

    @sk.on_detach
    async def program_deinit(self):
        """Cancel background tasks."""
        logger.debug("stopping Otto program")

        # Cancel background tasks
        for task in [
            self._fastapi_server,
            self._task_generator,
            self._publisher,
            self._tle_updater
        ]:
            if task and not task.done():
                task.cancel()

        # Save final state
        await self._save_state()

    async def update_tles_loop(self):
        """ Background task that updates TLEs on startup and every 24 hours (default)."""
        while True:
            try:
                logger.debug("updating TLEs from Spacebook")

                # Fetch TLEs from Spacebook
                self.tles, response = await fetch_tles(
                    objects=self.config.task.objects
                )
                if response == 200:
                    logger.debug(f"updated {len(self.tles)} TLEs from Spacebook")
                    self.tles_dt = datetime.now(UTC)
                    tle_cache = TLECache(tles=self.tles, dt=self.tles_dt)
                    await self.program.kv_put_model(tle_cache)
                elif response == 429:
                    logger.warning("Too many requests to Spacebook, using last saved TLEs")
                else:
                    logger.warning(f"Failed to fetch TLEs from Spacebook (status: {response})")

            except Exception as e:
                logger.exception(f"Error updating TLEs: {e}")

            # Wait configured hours before next update
            update_interval = getattr(
                self.config.task,
                'tle_update_interval_hours',
                24
            )
            await asyncio.sleep(update_interval * 60 * 60)

    async def promote_graylist_loop(self):
        """Background task that promotes graylisted objects back to whitelist."""
        graylist_interval = getattr(self.config.task, 'graylist_interval_minutes', 15)

        while True:
            try:
                await asyncio.sleep(graylist_interval * 60)

                if self.state.graylist:
                    logger.debug(f"promoting {len(self.state.graylist)} objects from graylist to whitelist")

                    # Move all graylisted objects back to whitelist
                    for obj in self.state.graylist.copy():
                        await self.list_manager.move_object(
                            obj,
                            ListType.GRAYLIST,
                            ListType.WHITELIST
                        )

            except Exception as e:
                logger.exception(f"Error in graylist promoter: {e}")

    @sk.task_factory
    async def generate(self):
        """
        Program task generator.

        Pulls a task from the queue and yields it for execution.

        Yields:
            StandardCollectTask: The next task to be executed.
            None: If no task is available.
        """
        if task := await self.task_queue.pop_task():
            logger.info(
                f"task ({task.task_id}): target -> {task.target}, "
                f"camera -> {task.camera_params}"
            )

            try:
                yield task
                logger.info(f"task ({task.task_id}) finished execution successfully.")
            except asyncio.CancelledError as e:
                logger.warning(f"Task ({task.task_id}) cancelled: {e}")
            except Exception as e:
                logger.exception(f"Task ({task.task_id}) failed with exception: {e}")
        else:
            # Peek at next task to provide info
            if next_task := await self.task_queue.peek_task():
                time_until = (next_task.end_time - datetime.now()).total_seconds()
                logger.info(
                    f"next task {next_task.task_id} available for {time_until:.1f}s"
                )
            else:
                logger.debug("no tasks available")
            yield None

    async def generate_tasks(self):
        """
        Background task that automatically generates observation tasks.

        Implement your automated task generation logic here:
        - Reading from Spacebook
        - Parsing TLE files
        - Scheduling based on visibility windows
        - etc.
        """

        # TODO: Implement smart object selection features using graylist and infrequently visited objects from whitelist

        while True:
            try:
                # Wait for a fresh batch of TLEs
                if not self.tles or (datetime.now(UTC) - self.tles_dt).days > 1:
                    logger.debug("no TLEs available yet")
                    await asyncio.sleep(5)
                    continue

                if not self.state.whitelist and not self.state.graylist:
                    logger.warning("No viewable objects on whitelist nor graylist. Please pick new objects and reload Otto")
                    return
                else:
                    if self.state.whitelist:
                        object = random.choice(self.state.whitelist)
                    else:
                        graylist_interval = getattr(self.config.task, 'graylist_interval_minutes', 15)
                        logger.warning(f"No viewable objects on whitelist but {len(self.state.graylist)} objects on graylist. Sleeping {graylist_interval} minutes")
                        await asyncio.sleep(graylist_interval * 60)
                        continue

                # Check current altitude and rising status
                result = check_satellite_visibility(
                    tles=self.tles,
                    object=object,
                    latitude=self.latitude,
                    longitude=self.longitude,
                    elevation=self.altitude_km*1000,
                )
                if result is None:
                    continue
                altitude, rising = result

                # Determine which list this object should be on
                if altitude < self.config.collect.altitude_min and not rising:
                    await self.list_manager.move_object(
                        object,
                        ListType.WHITELIST,
                        ListType.BLACKLIST
                    )
                    logger.debug(f"object {object} blacklisted (alt={altitude:.1f}°, falling)")
                    continue

                if altitude < self.config.collect.altitude_min and rising:
                    await self.list_manager.move_object(
                        object,
                        ListType.WHITELIST,
                        ListType.GRAYLIST
                    )
                    logger.debug(f"object {object} graylisted (alt={altitude:.1f}°, rising)")
                    continue

                tle_data = self.tles[object]
                tle_obj = TLE(
                    line0=tle_data["line0"],
                    line1=tle_data["line1"],
                    line2=tle_data["line2"],
                )
                if self.config.collect.dither and self.config.collect.dither_amount_arcsec > 0:
                    tle_obj = dither_tle(
                        tle_obj,
                        self.config.collect.dither_amount_arcsec,
                        latitude=self.latitude,
                        longitude=self.longitude,
                        elevation=self.altitude_km * 1000,
                    )

                # Account for rate-sidereal mode
                sidereal_kwargs = (
                    {"sidereal_track_from_frame": self.config.collect.num_frames-1}
                    if self.config.collect.track_mode == "rate_sidereal"
                    else {}
                )

                # FIXME: may want a custom controller for Otto. The standard controller re-slews between each different
                # filter, binning, and exposure (but not frame number) setting.

                # Create tasks for each combination of camera parameters
                exposures = list(range(
                    self.config.collect.exposure_min,
                    self.config.collect.exposure_max,
                    self.config.collect.exposure_delta
                ))
                random.shuffle(exposures)

                filters = list(self.config.collect.filters or [None])
                random.shuffle(filters)

                binnings = list(self.config.collect.binning)
                random.shuffle(binnings)

                now = datetime.now(UTC)
                cumulative_exposure = 0
                for filter in filters:
                    for exposure in exposures:
                        for binning in binnings:
                            cumulative_exposure += exposure * self.config.collect.num_frames
                            task = StandardCollectTask(
                                task_id=uuid.uuid1(),
                                controller_id=str(self.config.controller),
                                target=TLETarget(tle=tle_obj),
                                end_time=(
                                    now
                                    + timedelta(seconds=cumulative_exposure)
                                    + timedelta(seconds=self.config.task.end_time_deadband_seconds)
                                ),
                                camera_params=CameraParameterSet(
                                    filter_name=filter,
                                    integration_time_seconds=exposure,
                                    binning_x=binning,
                                    binning_y=binning,
                                    frame_count=self.config.collect.num_frames,
                                ),
                                **sidereal_kwargs
                            )
                            await self.task_queue.push_task(task)
            except Exception as e:
                logger.exception(f"Error in task generator: {e}")

            finally:
                while await self.task_queue.peek_task():
                    await asyncio.sleep(1)

    async def data_publisher(self):
        logger.debug("data publisher started")

        # Initialize configured publishers
        publishers = []
        cfg = self.config.publish
        if cfg.gdrive:
            from sensorkit.otto.publishers import GDrivePublisher
            publishers.append(GDrivePublisher(cfg.gdrive))
        if cfg.dropbox:
            from sensorkit.otto.publishers import DropboxPublisher
            publishers.append(DropboxPublisher(cfg.dropbox))

        if not publishers:
            logger.warning("publish.upload is true but no destinations configured")
            return

        logger.info(f"publishing to {len(publishers)} destination(s)")

        try:
            if graph := await self.program.data_graph():
                sink = graph.app_sink()
                async for context, data in sink.consume():
                    task_id = context.get("task_id")
                    for pub in publishers:
                        try:
                            await pub.publish(context, data)
                        except Exception as e:
                            logger.exception(
                                f"Error publishing task {task_id} to {pub.name}: {e}"
                            )
        finally:
            for pub in publishers:
                await pub.close()
