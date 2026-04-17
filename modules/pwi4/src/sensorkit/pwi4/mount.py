from __future__ import annotations

import asyncio
import math
from typing import Any, Literal, override

from astropy import units as u
from astropy.coordinates import ICRS, EarthLocation, SkyCoord
from astropy.coordinates import AltAz as AstropyAltAz
from astropy.time import Time
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.common import Geodetic
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    TLETarget,
)
from sensorkit.models.devices import (
    AltAzArcseconds,
    AltAzPointing,
    ApplyOffset,
    AxisEnabled,
    AxisRate,
    AxisRates,
    AzimuthWrapRange,
    Connected,
    ModelAddPoint,
    ModelClearPoints,
    ModelDeletePoint,
    ModelDisablePoint,
    ModelEnablePoint,
    ModelLoad,
    ModelSave,
    MountAxis,
    RADecArcseconds,
    RADecPointing,
    ReferenceFrame,
    SetAzimuthWrapRangeMin,
    SetParkPosition,
)
from sensorkit.pwi4.device import (
    PWI4Client,
    PWI4Device,
    PWI4DeviceConfig,
    PWI4DeviceState,
)


@sk.declare_keyword
class MountAxisEnabled(BaseModel):
    axis: list[AxisEnabled]


@sk.declare_keyword
class MountTargetDistance(BaseModel):
    axis: list[sk.AxisTargetDistance]


async def wrap_autocenter_loop(
    client: PWI4Client,
    interval: float = 60.0,
    deadband_deg: float = 10.0,
):
    """Background task that keeps azimuth wrap centered.

    Periodically reads the current axis0 position and adjusts the wrap
    range minimum to keep it centered, preventing the mount from hitting
    a cable wrap limit during long tracking sessions.
    """

    while True:
        try:
            st = await client.status()

            pos = client.get_float(st, "mount.axis0.position_degs")
            min_mech = client.get_float(st, "mount.axis0.min_mech_position_degs")
            max_mech = client.get_float(st, "mount.axis0.max_mech_position_degs")
            current_min = client.get_float(st, "mount.axis0_wrap_range_min_degs")

            desired_min = pos - 180.0
            desired_min = max(desired_min, min_mech)
            desired_min = min(desired_min, max_mech - 360.0)

            if abs(desired_min - current_min) >= deadband_deg:
                await client.request(
                    "/mount/set_axis0_wrap_range_min",
                    params={"degs": desired_min},
                )
        except Exception:
            pass

        await asyncio.sleep(interval)


def altaz_rates_to_radec_rates(
    alt_deg: float,
    az_deg: float,
    alt_rate: float,
    az_rate: float,
    location: EarthLocation,
    time: Time,
) -> tuple[float, float, float, float]:
    """Convert Alt/Az rates to RA/Dec rates via numerical differentiation.

    Returns (ra_deg, dec_deg, ra_rate_deg_per_sec, dec_rate_deg_per_sec).
    """

    frame = AstropyAltAz(obstime=time, location=location)
    coord = SkyCoord(alt=alt_deg * u.deg, az=az_deg * u.deg, frame=frame)
    radec = coord.transform_to(ICRS())

    dt = 0.01
    new_coord = SkyCoord(
        alt=(alt_deg + alt_rate * dt) * u.deg,
        az=(az_deg + az_rate * dt) * u.deg,
        frame=frame,
    )
    new_radec = new_coord.transform_to(ICRS())

    ra_rate_dps = (new_radec.ra.deg - radec.ra.deg) / dt
    dec_rate_dps = (new_radec.dec.deg - radec.dec.deg) / dt

    return radec.ra.deg, radec.dec.deg, ra_rate_dps, dec_rate_dps


@sk.declare_device
class PWI4Mount(PWI4Device):
    """PWI4 mount implementation."""

    config: PWI4MountConfig

    def __init__(self, config: PWI4MountConfig, client: PWI4Client):
        super().__init__(config=config, client=client)
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None
        self._wrap_task: asyncio.Task | None = None

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(PWI4MountState)
        except Exception:
            self.state = PWI4MountState()

        # Site location
        st = await self.client.status()
        lat = self.client.get_float(st, "site.latitude_degs")
        lon = self.client.get_float(st, "site.longitude_degs")
        height_m = self.client.get_float(st, "site.height_meters")
        self._geodetic = Geodetic(lon=lon, lat=lat, elev=height_m / 1000)
        self._location = EarthLocation.from_geodetic(lon=lon, lat=lat, height=height_m)

    @sk.on_detach
    async def entity_deinit(self):
        await self.mount_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def mount_init(self, cmd: sk.Init):
        self.device_name = "Mount"

        await self.mount_connect(sk.Connect())
        self.start_status_loop(self.status_publish())

        await asyncio.gather(
            self.mount_enable_axis(sk.EnableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_enable_axis(sk.EnableAxis(axis=MountAxis.ALTITUDE)),
        )

        await self.mount_home(sk.Home())

        await self.setup_ota()

        if self.config.wrap_autocenter:
            self._wrap_task = asyncio.create_task(
                wrap_autocenter_loop(
                    self.client,
                    interval=self.config.wrap_interval,
                    deadband_deg=self.config.wrap_deadband_deg,
                )
            )

        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def mount_deinit(self, cmd: sk.Deinit):
        if self._wrap_task is not None:
            self._wrap_task.cancel()
            try:
                await self._wrap_task
            except asyncio.CancelledError:
                pass

        await self.mount_park(sk.MoveToPark())

        await asyncio.gather(
            self.mount_disable_axis(sk.DisableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_disable_axis(sk.DisableAxis(axis=MountAxis.ALTITUDE)),
        )

        await self.stop_status_loop()

    @sk.command_handler
    async def mount_connect(self, cmd: sk.Connect):
        logger.debug("connecting to mount")
        await self.client.request("/mount/connect")

        async with asyncio.timeout(self.config.timeout):
            await self.client.poll(
                lambda s: self.client.get_bool(s, "mount.is_connected"),
            )

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))
        logger.debug("connected to mount")

    @sk.command_handler
    async def mount_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from mount")
        await self.client.request("/mount/disconnect")

        async with asyncio.timeout(self.config.timeout):
            await self.client.poll(
                lambda s: not self.client.get_bool(s, "mount.is_connected"),
            )

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))
        logger.debug("disconnected from mount")

    @sk.command_handler
    async def mount_home(self, cmd: sk.Home):
        self.require_connected()

        # Check if mount has already homed
        st = await self.client.status()
        if self.client.get_bool(
            st, "mount.axis0.is_position_initialized"
        ) and self.client.get_bool(st, "mount.axis1.is_position_initialized"):
            return

        logger.debug("homing mount")
        await self.client.request("/mount/find_home")
        await self.client.poll(
            lambda s: (
                self.client.get_bool(s, "mount.axis0.is_position_initialized")
                and self.client.get_bool(s, "mount.axis1.is_position_initialized")
            ),
        )
        logger.debug("homed mount")

    @sk.command_handler
    async def mount_stop(self, cmd: sk.Stop):
        logger.debug("stopping mount")
        await self.client.request("/mount/stop")
        await self.client.poll(
            lambda s: (
                not self.client.get_bool(s, "mount.is_slewing")
                and not self.client.get_bool(s, "mount.is_tracking")
            ),
        )
        logger.debug("stopped mount")

    @sk.command_handler
    async def mount_park(self, cmd: sk.MoveToPark):
        logger.debug("parking mount")
        await self.client.request("/mount/park")
        await self.client.poll(
            lambda s: (
                not self.client.get_bool(s, "mount.is_slewing")
                and not self.client.get_bool(s, "mount.is_tracking")
            ),
        )
        logger.debug("parked mount")

    @sk.command_handler
    async def mount_enable_axis(self, cmd: sk.EnableAxis):
        axis = 0 if cmd.axis == MountAxis.AZIMUTH else 1
        await self.client.request("/mount/enable", params={"axis": axis})

    @sk.command_handler
    async def mount_disable_axis(self, cmd: sk.DisableAxis):
        axis = 0 if cmd.axis == MountAxis.AZIMUTH else 1
        await self.client.request("/mount/disable", params={"axis": axis})

    @sk.command_handler
    async def mount_follow_target(self, cmd: sk.FollowTarget):
        self.require_connected()

        target = await cmd.target.adapt(
            ICRSTarget,
            AltAzTarget,
            (FrameTarget, ReferenceFrame.ICRF),
            (FrameTarget, ReferenceFrame.ALTAZ),
            TLETarget,
            (EphemerisTarget, ReferenceFrame.ICRF),
            observer=self._geodetic,
        )

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")
                await self.client.request(
                    "/mount/goto_ra_dec_j2000",
                    params={
                        "ra_hours": target.coords.ra,
                        "dec_degs": target.coords.dec,
                    },
                )

                await self.client.poll(
                    lambda s: (
                        not self.client.get_bool(s, "mount.is_slewing")
                        and self.client.get_bool(s, "mount.is_tracking")
                    ),
                    delay=1,
                )

                logger.debug("following RADec target")

            case AltAzTarget():
                await self.client.request(
                    "/mount/goto_alt_az",
                    params={
                        "alt_degs": target.coords.alt,
                        "az_degs": target.coords.az,
                    },
                )

                await self.client.poll(
                    lambda s: not self.client.get_bool(s, "mount.is_slewing"),
                    delay=1,
                )

            case TLETarget():
                await self.client.request(
                    "/mount/follow_tle",
                    params={
                        "line1": target.tle.line0,
                        "line2": target.tle.line1,
                        "line3": target.tle.line2,
                    },
                )

                await self.client.poll(
                    lambda s: (
                        not self.client.get_bool(s, "mount.is_slewing")
                        and self.client.get_bool(s, "mount.is_tracking")
                    ),
                    delay=1,
                )

            case EphemerisTarget():
                await self.client.request("/mount/radecpath/new")
                for i in range(len(target.jds)):
                    await self.client.request(
                        "/mount/radecpath/add_point",
                        params={
                            "jd": target.jds[i],
                            "ra_j2000_hours": target.points[i].ra / 15,
                            "dec_j2000_degs": target.points[i].dec,
                        },
                    )
                await self.client.request("/mount/radecpath/apply")

                await self.client.poll(
                    lambda s: (
                        not self.client.get_bool(s, "mount.is_slewing")
                        and self.client.get_bool(s, "mount.is_tracking")
                    ),
                    delay=1,
                )

            case FrameTarget():
                match target.frame:
                    case ReferenceFrame.ALTAZ:
                        await self.client.request("/mount/tracking_off")
                        await self.client.poll(
                            lambda s: not self.client.get_bool(s, "mount.is_tracking"),
                        )
                    case ReferenceFrame.ICRF:
                        await self.client.request("/mount/tracking_on")
                        await self.client.poll(
                            lambda s: self.client.get_bool(s, "mount.is_tracking"),
                        )

    @sk.command_handler
    async def mount_offset(self, cmd: ApplyOffset):
        self.require_connected()
        if isinstance(cmd.offset, RADecArcseconds):
            await self.client.request(
                "/mount/offset",
                params={
                    "ra_add_arcsec": cmd.offset.right_ascension_arcseconds,
                    "dec_add_arcsec": cmd.offset.declination_arcseconds,
                },
            )
        elif isinstance(cmd.offset, AltAzArcseconds):
            await self.client.request(
                "/mount/offset",
                params={
                    "axis0_add_arcsec": cmd.offset.azimuth_arcseconds,
                    "axis1_add_arcsec": cmd.offset.altitude_arcseconds,
                },
            )

    @sk.command_handler
    async def mount_set_wrap_range_min(self, cmd: SetAzimuthWrapRangeMin):
        await self.client.request("/mount/set_axis0_wrap_range_min", params={"degs": cmd.min})
        await sk.device().publish(AzimuthWrapRange(min=cmd.min, max=cmd.min + 360))

    @sk.command_handler
    async def model_add_point(self, cmd: ModelAddPoint):
        if cmd.reference_frame != ReferenceFrame.ICRF:
            raise ValueError("Mount model points must be ICRF")
        await self.client.request(
            "/mount/model/add_point",
            params={
                "ra_j2000_hours": cmd.right_ascension_hours,
                "dec_j2000_degs": cmd.declination_degrees,
            },
        )

    @sk.command_handler
    async def model_delete_point(self, cmd: ModelDeletePoint):
        await self.client.request(
            "/mount/model/delete_point",
            params={"index": ",".join(str(i) for i in cmd.indexes)},
        )

    @sk.command_handler
    async def model_enable_point(self, cmd: ModelEnablePoint):
        await self.client.request(
            "/mount/model/enable_point",
            params={"index": ",".join(str(i) for i in cmd.indexes)},
        )

    @sk.command_handler
    async def model_disable_point(self, cmd: ModelDisablePoint):
        await self.client.request(
            "/mount/model/disable_point",
            params={"index": ",".join(str(i) for i in cmd.indexes)},
        )

    @sk.command_handler
    async def model_clear_points(self, cmd: ModelClearPoints):
        await self.client.request("/mount/model/clear_points")

    @sk.command_handler
    async def model_save(self, cmd: ModelSave):
        await self.client.request("/mount/model/save", params={"filename": cmd.filename})

    @sk.command_handler
    async def model_load(self, cmd: ModelLoad):
        await self.client.request("/mount/model/load", params={"filename": cmd.filename})

    async def status_publish(self):
        backoff = 1.0
        while True:
            try:
                st = await self.client.status()
                backoff = 1.0
            except Exception as e:
                logger.warning(f"PWI4 mount status poll failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)
                continue

            try:
                connected = self.client.get_bool(st, "mount.is_connected")
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    ra_hours = self.client.get_float(st, "mount.ra_j2000_hours")
                    dec_degs = self.client.get_float(st, "mount.dec_j2000_degs")
                    alt_degs = self.client.get_float(st, "mount.altitude_degs")
                    az_degs = self.client.get_float(st, "mount.azimuth_degs")

                    await device.publish(
                        RADecPointing(
                            right_ascension_hours=ra_hours,
                            declination_degrees=dec_degs,
                            reference_frame=ReferenceFrame.ICRF,
                        )
                    )
                    await device.publish(
                        AltAzPointing(
                            altitude_degrees=alt_degs,
                            azimuth_degrees=az_degs,
                        )
                    )
                    if self._geodetic is not None:
                        await device.publish(
                            sk.SitePosition(
                                latitude_degrees=self._geodetic.lat,
                                longitude_degrees=self._geodetic.lon,
                                altitude_km=self._geodetic.elev,
                            )
                        )

                    # Axis rates + RA/Dec rate conversion
                    az_rate = self.client.get_float(
                        st, "mount.axis0.measured_velocity_degs_per_sec"
                    )
                    alt_rate = self.client.get_float(
                        st, "mount.axis1.measured_velocity_degs_per_sec"
                    )

                    if self._location is not None:
                        _, _, ra_rate, dec_rate = altaz_rates_to_radec_rates(
                            alt_degs, az_degs, alt_rate, az_rate, self._location, Time.now()
                        )

                        await device.publish(
                            AxisRates(
                                azimuth=AxisRate(
                                    velocity=az_rate,
                                    max_velocity=self.client.get_float(
                                        st, "mount.axis0.max_velocity_degs_per_sec"
                                    ),
                                    max_acceleration=self.client.get_float(
                                        st, "mount.axis0.acceleration_degs_per_sec_sqr"
                                    ),
                                    mechanical_position=self.client.get_float(
                                        st, "mount.axis0.position_degs"
                                    ),
                                    min_mechanical_position=self.client.get_float(
                                        st, "mount.axis0.min_mech_position_degs"
                                    ),
                                    max_mechanical_position=self.client.get_float(
                                        st, "mount.axis0.max_mech_position_degs"
                                    ),
                                    axis=MountAxis.AZIMUTH,
                                ),
                                altitude=AxisRate(
                                    velocity=alt_rate,
                                    max_velocity=self.client.get_float(
                                        st, "mount.axis1.max_velocity_degs_per_sec"
                                    ),
                                    max_acceleration=self.client.get_float(
                                        st, "mount.axis1.acceleration_degs_per_sec_sqr"
                                    ),
                                    mechanical_position=self.client.get_float(
                                        st, "mount.axis1.position_degs"
                                    ),
                                    min_mechanical_position=self.client.get_float(
                                        st, "mount.axis1.min_mech_position_degs"
                                    ),
                                    max_mechanical_position=self.client.get_float(
                                        st, "mount.axis1.max_mech_position_degs"
                                    ),
                                    axis=MountAxis.ALTITUDE,
                                ),
                                right_ascension=AxisRate(
                                    velocity=ra_rate, axis=MountAxis.RIGHT_ASCENSION
                                ),
                                declination=AxisRate(
                                    velocity=dec_rate, axis=MountAxis.DECLINATION
                                ),
                            )
                        )

                    await device.publish(
                        MountTargetDistance(
                            axis=[
                                sk.AxisTargetDistance(
                                    distance_arcseconds=self.client.get_float(
                                        st, "mount.axis0.dist_to_target_arcsec"
                                    ),
                                    rms_error_arcseconds=self.client.get_float(
                                        st, "mount.axis0.rms_error_arcsec"
                                    ),
                                    axis=MountAxis.AZIMUTH,
                                ),
                                sk.AxisTargetDistance(
                                    distance_arcseconds=self.client.get_float(
                                        st, "mount.axis1.dist_to_target_arcsec"
                                    ),
                                    rms_error_arcseconds=self.client.get_float(
                                        st, "mount.axis1.rms_error_arcsec"
                                    ),
                                    axis=MountAxis.ALTITUDE,
                                ),
                            ]
                        )
                    )

                    await device.publish(
                        MountAxisEnabled(
                            axis=[
                                AxisEnabled(
                                    axis=MountAxis.AZIMUTH,
                                    enabled=self.client.get_bool(st, "mount.axis0.is_enabled"),
                                ),
                                AxisEnabled(
                                    axis=MountAxis.ALTITUDE,
                                    enabled=self.client.get_bool(st, "mount.axis1.is_enabled"),
                                ),
                            ]
                        )
                    )

            except Exception as e:
                logger.warning(f"PWI4 mount status publish failed: {e}")

            await asyncio.sleep(self.config.status_frequency)

    async def setup_ota(self):
        """Set heater power levels and turn on fans."""

        for role, power in self.config.heaters.items():
            await self.client.request("/heaters/set", params={"role": role, "power": int(power)})
            logger.debug(f"set {role} heater power to {power:.0f}%")

        if self.config.fans:
            roles = ",".join(self.config.fans)
            await self.client.request("/fans/on", params={"roles": roles})
            logger.debug(f"set {roles} fans to on")


class PWI4MountConfig(PWI4DeviceConfig[PWI4Mount]):
    device_type: Literal["mount"] = "mount"
    status_frequency: float = 1.0
    wrap_autocenter: bool = False
    wrap_interval: float = 60.0
    wrap_deadband_deg: float = 10.0
    fans: list[str] = []
    heaters: dict[str, float] = {}

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Mount(config=self, client=client)


class PWI4MountState(PWI4DeviceState):
    device_type: Literal["mount"] = "mount"
