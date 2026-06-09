from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Literal

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing, RADecPointing, ReferenceFrame, SitePosition
from sensorkit.astro.target import (
    AltAzTarget,
    CatalogTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    RateTarget,
    StateVectorTarget,
    TLETarget,
)
from sensorkit.models.devices import (
    AxisRates,
    Deinit,
    FollowTarget,
    Init,
    Stop,
)
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.std.instrument import Binning, CameraCapture, ConfigureCameraSensor
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover, SetFilter
from sensorkit.std.traits import Connect


class Sensor:
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
        """Open the dome if one is configured."""
        if self.dome:
            logger.info("Opening the dome")
            async with asyncio.timeout(self.policies.dome_open_close_timeout):
                await self.dome.command(OpenEnclosure())

    async def deinit_dome(self):
        """Close the dome if one is configured."""
        if self.dome:
            logger.info("Closing the dome")
            async with asyncio.timeout(self.policies.dome_open_close_timeout):
                await self.dome.command(CloseEnclosure())

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
class SensorControl:
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

        self.sensor = Sensor(
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

        # FIXME: Ad hoc context should be formalized as Keywords.
        adhoc = dict(
            binning_x=task.camera_params.binning_x,
            binning_y=task.camera_params.binning_y,
            elevation=self.config.site_position.altitude_km,
            filter_name=task.camera_params.filter_name,
            frame_count=task.camera_params.frame_count,
            latitude=self.config.site_position.latitude_degrees,
            longitude=self.config.site_position.longitude_degrees,
            task_id=task.execution.task_id,
        )

        # TLE fields default to None; only populated for TLE targets.
        adhoc["tle_line0"] = None
        adhoc["tle_line1"] = None
        adhoc["tle_line2"] = None

        match task.target:
            case ICRSTarget():
                adhoc["track_mode"] = "sidereal"
                adhoc["target_id"] = "ICRSTarget"
            case AltAzTarget():
                adhoc["track_mode"] = "fixed"
                adhoc["target_id"] = "AltAzTarget"
            case TLETarget():
                adhoc["track_mode"] = "rate"
                adhoc["target_name"] = task.target.tle.line0
                adhoc["target_id"] = task.target.tle.line0.split()[1]
                adhoc["tle_line0"] = task.target.tle.line0
                adhoc["tle_line1"] = task.target.tle.line1
                adhoc["tle_line2"] = task.target.tle.line2
            case RateTarget():
                adhoc["track_mode"] = "rate"
                adhoc["target_id"] = "RateTarget"
            case EphemerisTarget():
                adhoc["track_mode"] = "rate"
                adhoc["target_id"] = "EphemerisTarget"
            case StateVectorTarget():
                adhoc["track_mode"] = "rate"
                adhoc["target_id"] = "StateVectorTarget"
            case CatalogTarget():
                adhoc["track_mode"] = "sidereal"
                adhoc["target_id"] = task.target.object
            case FrameTarget():
                adhoc["track_mode"] = str(task.target.frame.value)
                adhoc["target_id"] = str(task.target.frame.value)
            case _:
                adhoc["track_mode"] = "unknown"
                adhoc["target_id"] = "unknown"

        await sk.controller().update_context(
            task.execution.get_context(),
            **adhoc,
        )

        # For inherently sidereal targets (stars), the initial FollowTarget already
        # establishes sidereal tracking — mark as sidereal from the start so the frame
        # loop never issues a redundant tracking switch.
        target_is_sidereal = isinstance(task.target, (ICRSTarget, CatalogTarget))
        currently_sidereal = target_is_sidereal

        # Capture the requested frames.
        for frame_num in range(0, task.camera_params.frame_count):
            want_sidereal = target_is_sidereal or frame_num in task.sidereal_frames

            if want_sidereal and not currently_sidereal:
                # Hold the current RA/Dec under sidereal tracking.
                logger.info(f"Frame #{frame_num+1} of {task.camera_params.frame_count}: switching to sidereal track")
                await self.sensor.mount.command(
                    FollowTarget(target=FrameTarget(frame=ReferenceFrame.ICRF))
                )
                currently_sidereal = True
            elif not want_sidereal and currently_sidereal:
                # Resume following the original target.
                logger.info(f"Frame #{frame_num+1} of {task.camera_params.frame_count}: resuming target track")
                await self.sensor.mount.command(FollowTarget(target=task.target))
                currently_sidereal = False

            logger.info(f"Acquiring frame #{frame_num+1} of {task.camera_params.frame_count}")
            context = await sk.controller().update_context(
                frame_num=frame_num,
                track_mode="sidereal" if want_sidereal else adhoc["track_mode"],
            )
            _add_compat_context(context)

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
        """Shut down the sensor."""
        # TODO: Handle concurrency as in sensor_startup.
        await self.sensor.deinit_mirror_cover()

        deinit_mount = self.sensor.deinit_mount()
        deinit_dome = self.sensor.deinit_dome()

        if self.config.policies.concurrent_dome_and_mount_deinit:
            await asyncio.gather(deinit_dome, deinit_mount)
        else:
            await deinit_mount
            await deinit_dome


class SensorDevices(BaseModel):
    """Device entity references for sensor control."""

    mount: str | None = None
    camera: str | None = None
    focuser: str | None = None
    rotator: str | None = None
    filter_wheel: str | None = None
    mirror_cover: str | None = None
    dome: str | None = None


class SensorPolicies(BaseModel):
    """Policies for sensor control."""

    concurrent_dome_and_mount_init: bool = False
    """Whether the dome and mount can be initialized concurrently."""

    concurrent_dome_and_mount_deinit: bool = False
    """Whether the dome and mount can be deinitialized concurrently."""

    dome_open_close_timeout: float = 120.0
    """Timeout for fully opening or closing the dome."""

    concurrent_mount_and_mirror_cover_init: bool = False
    """Whether the mount and mirror cover can be initialized concurrently."""

    mirror_cover_open_close_timeout: float = 60.0
    """Timeout for fully opening or closing the mirror cover."""

    mount_init_timeout: float = 30.0
    """Timeout for powering the mount and enabling axis control."""

    mount_home_timeout: float = 300.0
    """Timeout for homing the mount."""

    minimum_target_altitude_degrees: float | None = None
    """Minimum altitude for a target to be tracked during collection."""

    sun_separation_degrees: float | None = None
    """Distance a target must be away from the sun during collection."""

    moon_separation_degrees: float | None = None
    """Distance a target must be away from the moon during collection"""


class SensorConfig(BaseModel):
    """Configuration for standard sensor control."""

    controller_name: str
    devices: SensorDevices
    site_position: SitePosition
    policies: SensorPolicies = Field(default_factory=SensorPolicies)


sk.declare_config_section(
    "sensors",
    list[SensorConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


# TODO: Phase out when UI code is updated to use ControllerInfo and SensorConfig for this info.
class Capabilities(BaseModel):
    """Deprecated controller capability descriptor for the sensor service."""

    type: Literal["controller"] = "controller"
    tasks: list[str]
    devices: SensorDevices


@sk.service_entrypoint(version=sk.VERSION)
async def sensor_control_service(service: sk.Service):
    await service.register()

    try:
        # Read configuration.
        config = await service.context.kv_get_model(SensorConfig)
    except sk.KVError as e:
        logger.error(f"Service {service.name} could not get SensorConfig ({e})")
        return

    sc = SensorControl(config=config)
    service.include(
        sc,
        name=config.controller_name,
    )

    await service.run()


def _add_compat_context(context: sk.Context):
    # FIXME: temporary for compat
    compat = dict(
        ra="RADecPointing.right_ascension_hours * 15",
        ra_hms="RADecPointing.ra_hms",
        ra_rate="AxisRates.right_ascension.velocity * 3600",
        dec="RADecPointing.declination_degrees",
        dec_dms="RADecPointing.dec_dms",
        dec_rate="AxisRates.declination.velocity * 3600",
        alt="AltAzPointing.altitude_degrees",
        alt_rate="AxisRates.altitude.velocity * 3600",
        az="AltAzPointing.azimuth_degrees",
        az_rate="AxisRates.azimuth.velocity * 3600",
    )
    for key, expr in compat.items():
        with contextlib.suppress(Exception):
            context[key] = context.eval(expr)

    # Fold legacy flat file_name/file_path keys into FileNameTemplate / FileInfo keywords.
    # A file_name is an input naming template; a file_path is an explicit output location.
    import pathlib

    from sensorkit.data.filesys import FileInfo, FileNameTemplate

    file_name = context.pop("file_name", None)
    file_path = context.pop("file_path", None)

    if file_name is not None:
        logger.warning("The 'file_name' context key is deprecated. Use FileNameTemplate instead.")
        context.set(FileNameTemplate(template=file_name))

    if file_path is not None:
        logger.warning("The 'file_path' context key is deprecated. Use FileInfo instead.")
        context.set(FileInfo(path=pathlib.Path(file_path)))
