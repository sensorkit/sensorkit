from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.target import AltAzTarget, FrameTarget, ICRSTarget, TLETarget
from sensorkit.models.devices import (
    AltAzPointing,
    AxisRate,
    AxisRates,
    Connected,
    MountAxis,
    RADecPointing,
    SitePosition,
)
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

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaMountState)
        except Exception:
            self.state = NinaMountState()

        await self.mount_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.mount_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def mount_init(self, cmd: sk.Init):
        self.device_name = "Mount"
        self._tracking: bool | None = None
        self._slewing: bool | None = None

        await self.connect("mount")
        await sk.device().publish(Connected(is_connected=True))

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

        self.start_status_loop(self.status_publish())

        if self.config.needs_homed and not self.state.has_been_homed:
            await self.mount_home(sk.Home())

    @sk.command_handler
    async def mount_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()

        if not self.device_connected:
            return

        try:
            info = await self.info("mount")
            if not info.get("AtPark", False):
                await self.client.get("/equipment/mount/tracking", enabled=False)
                self._tracking = False
                await self.client.get("/equipment/mount/park")
        except Exception as e:
            logger.warning(f"Error during mount deinit: {e}")

        try:
            await self.disconnect("mount")
            await sk.device().publish(Connected(is_connected=False))
        except Exception as e:
            logger.warning(f"Error during mount disconnect: {e}")

    @sk.command_handler
    async def mount_connect(self, cmd: sk.Connect):
        await self.connect("mount")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def mount_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("mount")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def mount_home(self, cmd: sk.Home):
        self.require_connected()
        logger.debug("homing mount")
        await self.client.get("/equipment/mount/find-home")

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("mount")
                if info.get("AtHome", False) and not info.get("Slewing", True):
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed mount")

    @sk.command_handler
    async def mount_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping mount")
        await self.client.get("/equipment/mount/stop-slew")
        await self.client.get("/equipment/mount/tracking", enabled=False)
        self._tracking = False
        logger.debug("stopped mount")

    @sk.command_handler
    async def mount_park(self, cmd: sk.MoveToPark):
        self.require_connected()
        logger.debug("parking mount")
        await self.client.get("/equipment/mount/park")

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("mount")
                if info.get("AtPark", False):
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("parked mount")

    @sk.command_handler
    async def mount_set_park(self, cmd: sk.SetParkPosition):
        self.require_connected()
        await self.client.get("/equipment/mount/set-park-position")

    @sk.command_handler
    async def mount_follow_target(self, cmd: sk.FollowTarget):
        self.require_connected()
        from sensorkit.astro.common import ReferenceFrame

        target = cmd.target

        # Unpark if needed
        info = await self.info("mount")
        if info.get("AtPark", False):
            await self.client.get("/equipment/mount/unpark")

        match target:
            case ICRSTarget():
                ra_hours = target.coords.ra / 15.0
                dec_deg = target.coords.dec

                logger.debug(f"slewing to RA={ra_hours:.4f}h, Dec={dec_deg:.4f}°")

                await self.client.get("/equipment/mount/tracking", enabled=True)
                self._tracking = True

                await self.client.get(
                    "/equipment/mount/slew-radec",
                    ra=ra_hours,
                    dec=dec_deg,
                    waitToFinish=True,
                )

                await self._publish_pointing()

            case AltAzTarget():
                alt_deg = target.coords.alt
                az_deg = target.coords.az

                logger.debug(f"slewing to Alt={alt_deg:.4f}°, Az={az_deg:.4f}°")

                await self.client.get(
                    "/equipment/mount/slew-altaz",
                    altitude=alt_deg,
                    azimuth=az_deg,
                    waitToFinish=True,
                )

                await self._publish_pointing()

            case TLETarget():
                # TODO: Implement PID-based rate tracking via core.
                # For now, adapt to ICRSTarget and slew to current position.
                logger.warning("TLE tracking via NINA is best-effort (no native offset rates)")

                observer = None
                if self._site_lat is not None and self._site_lon is not None:
                    from sensorkit.astro.common import Geodetic

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
                    self._tracking = True
                    await self.client.get(
                        "/equipment/mount/slew-radec",
                        ra=ra_hours,
                        dec=dec_deg,
                        waitToFinish=True,
                    )

                    await self._publish_pointing()
                else:
                    logger.warning(f"Could not adapt TLE to ICRSTarget: {type(adapted).__name__}")

            case FrameTarget():
                if target.frame == ReferenceFrame.ALTAZ:
                    await self.client.get("/equipment/mount/tracking", enabled=False)
                    self._tracking = False
                    logger.debug("disabled tracking (ALTAZ frame)")
                else:
                    await self.client.get("/equipment/mount/tracking", enabled=True)
                    self._tracking = True
                    logger.debug("enabled sidereal tracking (ICRF frame)")

                await self._publish_rates()

            case _:
                logger.warning(f"Unsupported target type: {type(target).__name__}")

        logger.debug("slewed to target")

    async def _publish_pointing(self) -> tuple[float, float, float, float]:
        """Read and publish current pointing."""

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
        return ra_hours, dec_deg, alt_deg, az_deg

    async def _publish_rates(
        self,
        ra_hours: float | None = None,
        dec_deg: float | None = None,
    ) -> tuple[float, float]:
        """Publish rates. NINA doesn't expose offset rates, so rates are sidereal-only."""

        _SIDEREAL_RATE_DEG_S = 15.04107 / 3600.0
        tracking = self._tracking or False

        total_ra_deg_s = _SIDEREAL_RATE_DEG_S if tracking else 0.0
        total_dec_deg_s = 0.0

        await sk.device().publish(
            AxisRates(
                azimuth=AxisRate(axis=MountAxis.AZIMUTH),
                altitude=AxisRate(axis=MountAxis.ALTITUDE),
                right_ascension=AxisRate(
                    axis=MountAxis.RIGHT_ASCENSION,
                    velocity=total_ra_deg_s,
                ),
                declination=AxisRate(
                    axis=MountAxis.DECLINATION,
                    velocity=total_dec_deg_s,
                ),
            )
        )

        return total_ra_deg_s, total_dec_deg_s

    async def status_publish(self):
        while True:
            try:
                info = await self.info("mount")
                connected = info.get("Connected", False)
                self.device_connected = connected

                if not connected:
                    await sk.device().publish(Connected(is_connected=False))
                    await asyncio.sleep(self.config.status_frequency)
                    continue

                self._slewing = info.get("Slewing", False)
                self._tracking = info.get("Tracking", False)

                device = sk.device()
                await device.publish(Connected(is_connected=True))

                right_ascension, declination, altitude, azimuth = await self._publish_pointing()
                right_ascension_rate, declination_rate = await self._publish_rates(
                    ra_hours=right_ascension, dec_deg=declination
                )

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

                fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                logger.debug(
                    f"NINA telescope status: connected={connected}, slewing={self._slewing}, "
                    f"tracking={self._tracking}, RA={right_ascension}, Dec={declination}, "
                    f"RA_rate={right_ascension_rate}, Dec_rate={declination_rate}, "
                    f"alt={altitude}, az={azimuth}, "
                    f"{fields_str}"
                )

                await device.publish(NinaMountStatus(**fields))
            except Exception as e:
                logger.exception(f"Error in mount status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class NinaMountConfig(NinaDeviceConfig[NinaMount]):
    device_type: Literal["mount"] = "mount"
    needs_homed: bool = False
    timeout: float = 300.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NinaMount(self)


class NinaMountState(NinaDeviceState):
    device_type: Literal["mount"] = "mount"
    has_been_homed: bool = False
