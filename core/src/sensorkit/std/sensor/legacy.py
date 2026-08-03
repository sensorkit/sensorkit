# SPDX-License-Identifier: Apache-2.0
"""Hand-written sensor orchestration: the implementation sites run today.

Selected by `implementation: 1`. Retained beside the workflow implementation for
one release, as the rollback and as the live oracle its differential tests run
against, then deleted whole.

Nothing here is shared with the workflow implementation, deliberately: the two
are compared by the commands their devices receive, so a helper reached from both
would weaken exactly the assertion that matters.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Coroutine
from typing import Any

from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing, RADecPointing, ReferenceFrame
from sensorkit.astro.target import CatalogTarget, FrameTarget, ICRSTarget, TLETarget
from sensorkit.std.collect import Collect, StandardCollectTask
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.std.instrument import Binning, CameraCapture, ConfigureCameraSensor
from sensorkit.std.mount import AxisRates, FollowTarget
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover, SetFilter
from sensorkit.std.sensor.compat import Capabilities, add_compat_context
from sensorkit.std.sensor.config import SensorConfig, SensorDevices, SensorPolicies
from sensorkit.std.traits import Connect, Deinit, Init, Stop


class LegacyDevices:
    """High-level client to a set of devices comprising a logical sensor."""

    def __init__(self, impl: sk.ControllerImpl, devices: SensorDevices, policies: SensorPolicies):
        self.policies = policies
        self.mount = impl.use_device(
            devices.mount,
            subscribe=[AltAzPointing, RADecPointing, AxisRates],
        ) if devices.mount else None
        self.camera = impl.use_device(devices.camera) if devices.camera else None
        self.focuser = impl.use_device(devices.focuser) if devices.focuser else None
        self.rotator = impl.use_device(devices.rotator) if devices.rotator else None
        self.mirror_cover = impl.use_device(devices.mirror_cover) if devices.mirror_cover else None
        self.filter_wheel = impl.use_device(devices.filter_wheel) if devices.filter_wheel else None
        self.dome = impl.use_device(devices.dome) if devices.dome else None

    async def init_dome(self):
        """Initialize the dome and open it, if one is configured."""
        if not self.dome:
            return

        if self.policies.concurrent_dome_init_open:
            logger.info("Initializing and opening the dome")
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    asyncio.wait_for(
                        self.dome.command(Init()),
                        self.policies.dome_init_timeout,
                    )
                )
                tg.create_task(
                    asyncio.wait_for(
                        self.dome.command(OpenEnclosure()),
                        self.policies.dome_open_close_timeout,
                    )
                )
            return

        logger.info("Initializing the dome")
        async with asyncio.timeout(self.policies.dome_init_timeout):
            await self.dome.command(Init())

        logger.info("Opening the dome")
        async with asyncio.timeout(self.policies.dome_open_close_timeout):
            await self.dome.command(OpenEnclosure())

    async def deinit_dome(self):
        """Stop the dome, close it, and deinitialize it, if one is configured."""
        if not self.dome:
            return

        # Halt any leftover motion before closing, so nothing aborts the close mid-flight.
        with contextlib.suppress(Exception):
            await self.dome.command(Stop())

        if self.policies.concurrent_dome_deinit_close:
            logger.info("Closing and deinitializing the dome")
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    asyncio.wait_for(
                        self.dome.command(CloseEnclosure()),
                        self.policies.dome_open_close_timeout,
                    )
                )
                tg.create_task(
                    asyncio.wait_for(
                        self.dome.command(Deinit()),
                        self.policies.dome_deinit_timeout,
                    )
                )
            return

        logger.info("Closing the dome")
        async with asyncio.timeout(self.policies.dome_open_close_timeout):
            await self.dome.command(CloseEnclosure())

        logger.info("Deinitializing the dome")
        async with asyncio.timeout(self.policies.dome_deinit_timeout):
            await self.dome.command(Deinit())

    async def init_mount(self):
        """Initialize the mount."""
        if self.mount:
            logger.info("Initializing the mount")
            async with asyncio.timeout(self.policies.mount_init_timeout):
                await self.mount.command(Init())

    async def deinit_mount(self):
        """Deinitialize the mount."""
        if self.mount:
            logger.info("Deinitializing the mount")
            await self.mount.command(Deinit())

    async def init_mirror_cover(self):
        """Open the mirror cover if one is configured."""
        if self.mirror_cover:
            # Start opening the mirror cover.
            logger.info("Opening mirror cover")
            async with asyncio.timeout(self.policies.mirror_cover_open_close_timeout):
                await self.mirror_cover.command(OpenMirrorCover())

    async def deinit_mirror_cover(self):
        """Close the mirror cover if one is configured."""
        if self.mirror_cover:
            # Start closing the mirror cover.
            logger.info("Closing mirror cover")
            async with asyncio.timeout(self.policies.mirror_cover_open_close_timeout):
                await self.mirror_cover.command(CloseMirrorCover())

    async def init_all(self, tg: asyncio.TaskGroup):
        """Init the mount and open the dome and mirror cover, if configured."""
        tasks = []

        # Open the dome, if any.
        tasks.append(tg.create_task(self.init_dome()))

        if not self.policies.concurrent_dome_and_mount_init:
            await asyncio.wait(tasks)

        # Initialize the mount.
        tasks.append(tg.create_task(self.init_mount()))

        if not self.policies.concurrent_mount_and_mirror_cover_init:
            await asyncio.wait(tasks)

        # Open the mirror cover, if any.
        tasks.append(tg.create_task(self.init_mirror_cover()))

        # Wait for all operations to complete.
        await asyncio.wait(tasks)

    async def stop_all(self):
        """Issue Stop commands to the mount, dome, and mirror cover, suppressing any errors."""
        if self.mount:
            with contextlib.suppress(Exception):
                await self.mount.command(Stop())

        if self.dome:
            with contextlib.suppress(Exception):
                await self.dome.command(Stop())

        if self.mirror_cover:
            with contextlib.suppress(Exception):
                await self.mirror_cover.command(Stop())


@sk.declare_controller
class LegacySensor:
    """A Controller that controls a mount and camera."""

    def __init__(self, config: SensorConfig):
        self.config = config

    @sk.on_attach
    async def controller_init(self):
        controller = sk.controller()

        # TODO: Phase out when UI code is updated to use ControllerInfo and SensorConfig for this
        #       info.
        await controller.kv_put_model(
            Capabilities(
                tasks=[h.__name__ for h in controller._task_handlers.keys()],
                devices=self.config.devices,
            )
        )

        self.sensor = LegacyDevices(
            controller,
            self.config.devices,
            self.config.policies,
        )

        await controller.kv_put_model(self.config.site_position)

    @sk.task_handler
    async def sensor_init(self, task: sk.InitTask):
        """Attempt to start the sensor."""
        try:
            async with asyncio.TaskGroup() as tg:
                await self.sensor.init_all(tg)
        except* BaseException:
            with contextlib.suppress(BaseException):
                await self.sensor.stop_all()

            raise

        logger.info(f"Sensor '{sk.controller().entity}' is ready to operate")

    @sk.task_handler
    async def sensor_standby(self, task: sk.StandbyTask):
        """Put the sensor in standby mode."""
        # FIXME: Presently this is a synonym for init. Semantics should be dictated by config.
        try:
            async with asyncio.TaskGroup() as tg:
                await self.sensor.init_all(tg)
        except* BaseException:
            with contextlib.suppress(BaseException):
                await self.sensor.stop_all()

            raise

        logger.info(f"Sensor '{sk.controller().entity}' is standing by")

    @sk.task_handler
    async def sensor_collect(self, task: StandardCollectTask):
        """Execute a StandardCollectTask: slew, configure camera, capture frames."""
        if not self.sensor.mount or not self.sensor.camera:
            raise RuntimeError("Standard collect requires a mount and a camera")

        # Perform the device commands to do the collect!
        logger.info("Moving to target")
        await self.sensor.mount.command(FollowTarget(target=task.target))

        logger.info("Reached target")

        if task.camera_params.filter_name is not None and self.sensor.filter_wheel is not None:
            await self.sensor.filter_wheel.command(
                SetFilter(filter=task.camera_params.filter_name)
            )

        # Configure camera capture parameters.
        if None not in (task.camera_params.binning_x, task.camera_params.binning_y):
            await self.sensor.camera.command(
                ConfigureCameraSensor(
                    binning=Binning(
                        x=task.camera_params.binning_x,
                        y=task.camera_params.binning_y,
                    ),
                )
            )

        # Set base context for all frames.
        collect = Collect(
            target=task.target,
            params=task.camera_params,
            target_id=(
                task.target_id
                if task.target_id
                else task.target.tle.norad_id
                if isinstance(task.target, TLETarget)
                else task.target.object
                if isinstance(task.target, CatalogTarget)
                else None
            ),
        )
        await sk.controller().update_context(self.config.site_position, collect)

        # For inherently sidereal targets (stars), the initial FollowTarget already
        # establishes sidereal tracking — mark as sidereal from the start so the frame
        # loop never issues a redundant tracking switch.
        target_is_sidereal = isinstance(task.target, (ICRSTarget, CatalogTarget))
        currently_sidereal = target_is_sidereal

        # Capture the requested frames.
        for frame_num in range(0, task.camera_params.frame_count):
            collect.frame_number = frame_num
            want_sidereal = target_is_sidereal or frame_num in task.sidereal_frames

            if want_sidereal and not currently_sidereal:
                # Hold the current RA/Dec under sidereal tracking.
                logger.info(f"Frame #{frame_num+1} of {task.camera_params.frame_count}: switching to sidereal track")
                collect.target = FrameTarget(frame=ReferenceFrame.ICRF)
                await self.sensor.mount.command(FollowTarget(target=collect.target))
                currently_sidereal = True
            elif not want_sidereal and currently_sidereal:
                # Resume following the original target.
                logger.info(f"Frame #{frame_num+1} of {task.camera_params.frame_count}: resuming target track")
                collect.target = task.target
                await self.sensor.mount.command(FollowTarget(target=collect.target))
                currently_sidereal = False

            logger.info(f"Acquiring frame #{frame_num+1} of {task.camera_params.frame_count}")
            context = await sk.controller().update_context(collect)
            add_compat_context(context)

            await self.sensor.camera.command(
                CameraCapture(
                    integration_time=task.camera_params.integration_time_seconds,
                    context=context,
                )
            )

        # Stop motion when collection completes
        await self.sensor.mount.command(Stop())

    @sk.task_handler
    async def sensor_recover(self, task: sk.RecoverTask):
        """Reconnect to all devices and stop any in-progress motion."""

        def assert_success_or_unsupported(results: tuple[Any]):
            logger.debug(f"{results=}")

            for result in results:
                if isinstance(result, BaseException) and (
                    not isinstance(result, sk.CallError) or "Request rejected" not in str(result)
                ):
                    raise result

        # Try to reconnect to all devices that support it.
        devices = sk.controller().all_devices()

        logger.info("Reconnecting to devices...")
        assert_success_or_unsupported(
            await asyncio.gather(
                *(dev.client.command(Connect()) for dev in devices),
                return_exceptions=True,
            )
        )

        # Try to stop all devices that support it.
        logger.info("Stopping device activity...")
        assert_success_or_unsupported(
            await asyncio.gather(
                *(dev.client.command(Stop()) for dev in devices),
                return_exceptions=True,
            )
        )

    @sk.task_handler
    async def sensor_shutdown(self, task: sk.ShutdownTask):
        """Shut down the sensor.

        Under the always_deinit_dome policy every step is attempted even if an earlier one
        fails, so the dome comes down regardless; the failures are raised once it is closed.
        Otherwise the first failure ends the shutdown.
        """
        policies = self.config.policies
        failures: list[Exception] = []

        async def attempt(step: Coroutine[Any, Any, None]):
            """Run a shutdown step, recording rather than propagating its failure."""
            try:
                await step
            except Exception as exc:
                logger.exception("Sensor shutdown step failed")
                failures.append(exc)

        def raise_failures():
            match failures:
                case []:
                    return
                case [failure]:
                    raise failure
                case _:
                    raise ExceptionGroup("Sensor shutdown failed", failures)

        await attempt(self.sensor.deinit_mirror_cover())

        if not policies.always_deinit_dome:
            raise_failures()

        if policies.concurrent_dome_and_mount_deinit:
            # attempt() absorbs failures, so both steps always run to completion.
            await asyncio.gather(
                attempt(self.sensor.deinit_dome()),
                attempt(self.sensor.deinit_mount()),
            )
        else:
            await attempt(self.sensor.deinit_mount())

            if not policies.always_deinit_dome:
                raise_failures()

            await attempt(self.sensor.deinit_dome())

        raise_failures()
