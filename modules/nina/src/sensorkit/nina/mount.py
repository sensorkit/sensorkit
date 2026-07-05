# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

import astropy.units as u
from astropy.coordinates import EarthLocation
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition, AltAzPointing, RADecPointing
from sensorkit.astro.coords import Geodetic
from sensorkit.astro.target import (
    AltAzTarget,
    FrameTarget,
    ICRSTarget,
    TLETarget,
)
from sensorkit.astro.common import ReferenceFrame
from sensorkit.models.devices import (
    AxisRate,
    AxisRates,
    Deinit,
    FollowTarget,
    Home,
    Init,
    MoveToPark,
    MountAxis,
    SetParkPosition,
    Slewing,
    Stop,
    Tracking,
)
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.nina.device import (
    NinaDevice,
    NinaDeviceConfig,
    NinaDeviceState,
)


@sk.declare_keyword
class NinaMountStatus(BaseModel):
    tracking: bool = False
    slewing: bool = False
    at_home: bool = False
    at_park: bool = False
    side_of_pier: str | None = None
    sidereal_time: float | None = None


@sk.declare_device
class NinaMount(NinaDevice):
    """NINA mount implementation."""

    config: NinaMountConfig
    device_name = "Mount"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaMountState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaMountState()

        self._tracking: bool | None = None
        self._slewing: bool | None = None
        self._fast_status_task: asyncio.Task | None = None
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency_slow)
        await self.stop_status_loop()
        await self.mount_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def mount_init(self, cmd: Init):
        # Connect to the hardware
        self._reconnect = lambda: self.mount_connect(Connect())
        await self.mount_connect(Connect())
        self.start_status_loop(self.status_publish_slow())

        # Wait for initial status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency_slow)

        # Read site position from mount info
        info = await self.info("mount")
        self._site_lat = info.get("SiteLatitude")
        self._site_lon = info.get("SiteLongitude")
        self._site_elev = info.get("SiteElevation")
        if self._site_lat is not None and self._site_lon is not None:
            await sk.device().publish(
                SitePosition(
                    latitude_degrees=self._site_lat,
                    longitude_degrees=self._site_lon,
                    altitude_km=(self._site_elev or 0.0) / 1000.0,
                )
            )
            self._geodetic = Geodetic(
                lon=self._site_lon,
                lat=self._site_lat,
                elev=self._site_elev,
            )
            self._location = EarthLocation(
                lat=self._site_lat * u.deg,
                lon=self._site_lon * u.deg,
                height=(self._site_elev) * u.m,
            )
            logger.debug(
                f"site info: lat={self._site_lat}, lon={self._site_lon}, "
                f"height={self._site_elev} m"
            )

        # Home as needed
        if not self.state.has_been_homed:
            await self.mount_home(Home())

    @sk.command_handler
    async def mount_deinit(self, cmd: Deinit):
        if not self.device_connected:
            # Init may not have run, but mount may still be tracking from a failed run.
            # Connect so we can ensure it's parked.
            try:
                await self.mount_connect(Connect())
            except Exception:
                logger.warning("Unable to connect mount for Deinit park; skipping")
                return

        self._stop_fast_status()
        await self.mount_stop(Stop())
        await self.mount_park(MoveToPark())

    @sk.command_handler
    async def mount_connect(self, cmd: Connect):
        await self.connect("mount")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def mount_disconnect(self, cmd: Disconnect):
        await self.disconnect("mount")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def mount_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping mount")

        await self.client.get("/equipment/mount/slew/stop")
        await self.client.get("/equipment/mount/tracking", enabled=False)

        await self._wait_for_mount()

        self._stop_fast_status()
        logger.debug("stopped mount")

    @sk.command_handler
    async def mount_home(self, cmd: Home):
        await self.require_connected()
        await self.mount_unpark()
        logger.debug("homing mount")

        try:
            await self.client.get("/equipment/mount/home")
        except Exception:
            logger.warning("Unable to home")
            self.state.has_been_homed = True
            await sk.device().kv_put_model(self.state)
            return

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("mount")
                if info.get("AtHome", False) and not info.get("Slewing", True):
                    break
                await asyncio.sleep(0.2)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed mount")

    @sk.command_handler
    async def mount_park(self, cmd: MoveToPark):
        await self.require_connected()
        logger.debug("parking mount")

        await self.client.get("/equipment/mount/park")

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("mount")
                if info.get("AtPark", False):
                    break
                await asyncio.sleep(0.2)

        logger.debug("parked mount")

    @sk.command_handler
    async def mount_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        logger.debug("setting park position")
        await self.client.get("/equipment/mount/set-park-position")
        logger.debug("set park position")

    async def mount_unpark(self):
        await self.require_connected()
        logger.debug("unparking mount")

        info = await self.info("mount")
        if info.get("AtPark", False):
            await self.client.get("/equipment/mount/unpark")

        logger.debug("unparked mount")

    @sk.command_handler
    async def mount_follow_target(self, cmd: FollowTarget):
        await self.require_connected()
        await self.mount_unpark()

        # target = await cmd.target.adapt(
        #     ICRSTarget,
        #     AltAzTarget,
        #     (FrameTarget, ReferenceFrame.ICRF),
        #     (FrameTarget, ReferenceFrame.ALTAZ),
        #     TLETarget,
        #     (RateTarget, ReferenceFrame.ICRF),
        #     (RateTarget, ReferenceFrame.ICRF),
        #     (EphemerisTarget, ReferenceFrame.ICRF),
        #     observer=self._geodetic,
        # )
        target = cmd.target

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                ra_hours = target.coords.ra / 15.0
                dec_deg = target.coords.dec

                await self.client.get("/equipment/mount/tracking", enabled=True)

                await self.client.get(
                    "/equipment/mount/slew-radec",
                    ra=ra_hours,
                    dec=dec_deg,
                    waitToFinish=True,
                )

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following RADec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                alt_deg = target.coords.alt
                az_deg = target.coords.az

                await self.client.get(
                    "/equipment/mount/slew-altaz",
                    altitude=alt_deg,
                    azimuth=az_deg,
                    waitToFinish=True,
                )

                await self._wait_for_mount(tracking=False)
                self._start_fast_status()

                logger.debug("following AltAz target")

            case TLETarget():
                # TODO: Implement PID-based rate tracking via core.
                # For now, adapt to ICRSTarget and slew to current position.
                logger.warning("TLE tracking via NINA is best-effort (no native offset rates)")

                observer = None
                if self._site_lat is not None and self._site_lon is not None:
                    from sensorkit.astro.coords import Geodetic

                    observer = Geodetic(
                        lon=self._site_lon,
                        lat=self._site_lat,
                        elev=(self._site_elev or 0.0) / 1000.0,
                    )

                adapted = await target.adapt(
                    ICRSTarget,
                    (FrameTarget, ReferenceFrame.ICRF),
                    observer=observer,
                )

                if isinstance(adapted, ICRSTarget):
                    ra_hours = adapted.coords.ra / 15.0
                    dec_deg = adapted.coords.dec

                    await self.client.get("/equipment/mount/tracking", enabled=True)
                    await self.client.get(
                        "/equipment/mount/slew-radec",
                        ra=ra_hours,
                        dec=dec_deg,
                        waitToFinish=True,
                    )

                    await self._wait_for_mount(tracking=True)
                    self._start_fast_status()

                    logger.debug("following TLE target")
                else:
                    logger.warning(f"Could not adapt TLE to ICRSTarget: {type(adapted).__name__}")

            case FrameTarget():
                if target.frame == ReferenceFrame.ALTAZ:
                    self._stop_fast_status()
                    logger.debug("disabling tracking")
                    await self.client.get("/equipment/mount/tracking", enabled=False)
                    await self._wait_for_mount()
                    logger.debug("disabled tracking")
                else:
                    logger.debug("enabling sidereal tracking")
                    await self.client.get("/equipment/mount/tracking", enabled=True)
                    await self._wait_for_mount(tracking=True)
                    self._start_fast_status()
                    logger.debug("enabled sidereal tracking")

            case _:
                logger.warning(f"Unsupported target type: {type(target).__name__}")

        try:
            await self._publish_mount_status()
        except Exception as e:
            logger.warning(f"Immediate mount status publish failed: {e}")

    async def _wait_for_mount(
        self,
        *,
        slewing: bool = False,
        tracking: bool = False,
    ):
        """Poll /equipment/mount/info until Slewing and Tracking both match.

        Bounded by config.timeout; polls every 0.1 s.
        """

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("mount")
                if (
                    info.get("Slewing", False) == slewing
                    and info.get("Tracking", False) == tracking
                ):
                    break
                await asyncio.sleep(0.1)

    def _start_fast_status(self):
        if self._fast_status_task is None or self._fast_status_task.done():
            logger.debug("starting fast mount status loop")
            self._fast_status_task = asyncio.create_task(self.status_publish_fast())

    def _stop_fast_status(self):
        if self._fast_status_task is not None and not self._fast_status_task.done():
            logger.debug("stopping fast mount status loop")
            self._fast_status_task.cancel()
            self._fast_status_task = None

    @property
    def _fast_status_active(self) -> bool:
        return self._fast_status_task is not None and not self._fast_status_task.done()

    async def _publish_mount_status(self):
        info = await self.info("mount")
        ra_hours = info.get("RightAscension", 0.0)
        dec_deg = info.get("Declination", 0.0)
        alt_deg = info.get("Altitude", 0.0)
        az_deg = info.get("Azimuth", 0.0)

        device = sk.device()
        await device.publish(
            RADecPointing(
                right_ascension_hours=ra_hours,
                declination_degrees=dec_deg,
            )
        )
        await device.publish(
            AltAzPointing(
                altitude_degrees=alt_deg,
                azimuth_degrees=az_deg,
            )
        )

        await device.publish(
            AxisRates(
                azimuth=AxisRate(axis=MountAxis.AZIMUTH),
                altitude=AxisRate(axis=MountAxis.ALTITUDE),
                right_ascension=AxisRate(
                    axis=MountAxis.RIGHT_ASCENSION,
                    velocity=0.0,
                ),
                declination=AxisRate(
                    axis=MountAxis.DECLINATION,
                    velocity=0.0,
                ),
            )
        )

    async def status_publish_slow(self):
        while True:
            try:
                info = await self.info("mount")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if not connected:
                    await asyncio.sleep(self.config.status_frequency_slow)
                    continue

                self._slewing = info.get("Slewing", False)
                self._tracking = info.get("Tracking", False)

                await device.publish(Slewing(is_slewing=self._slewing))
                await device.publish(Tracking(is_tracking=self._tracking))

                # Only publish pointing/rates if fast loop isn't handling it
                if not self._fast_status_active:
                    await self._publish_mount_status()

                fields: dict = {
                    "tracking": self._tracking or False,
                    "slewing": self._slewing or False,
                    "at_home": info.get("AtHome", False),
                    "at_park": info.get("AtPark", False),
                }

                side_of_pier = info.get("SideOfPier")
                if side_of_pier is not None:
                    fields["side_of_pier"] = str(side_of_pier)

                sidereal_time = info.get("SiderealTime")
                if sidereal_time is not None:
                    fields["sidereal_time"] = sidereal_time

                await device.publish(NinaMountStatus(**fields))

                fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                # logger.debug(f"NINA mount status: connected={connected}, {fields_str}")
            except Exception as e:
                logger.exception(f"Error in slow mount status publish: {e}")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            await asyncio.sleep(self.config.status_frequency_slow)

    async def status_publish_fast(self):
        while True:
            try:
                await self._publish_mount_status()
            except Exception as e:
                logger.warning(f"Error in fast mount status publish ({e})")
                await asyncio.sleep(self.config.status_frequency_fast)
                continue

            await asyncio.sleep(self.config.status_frequency_fast)


class NinaMountConfig(NinaDeviceConfig[NinaMount]):
    device_type: Literal["mount"] = "mount"
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1
    timeout: float = 300.0

    @override
    def create_device(self):
        return NinaMount(self)


class NinaMountState(NinaDeviceState):
    device_type: Literal["mount"] = "mount"
    has_been_homed: bool = False
