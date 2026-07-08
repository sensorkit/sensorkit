# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
from typing import Literal, override

from astropy import units as u
from astropy.coordinates import ICRS, AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from astropy.utils import iers
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing, RADecPointing, ReferenceFrame, SitePosition
from sensorkit.astro.coords import Geodetic
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    RateTarget,
    TLETarget,
)
from sensorkit.models.devices import (
    AltAzArcseconds,
    ApplyOffset,
    AxisEnabled,
    AxisRate,
    AxisRates,
    AxisTargetDistance,
    AzimuthWrapRange,
    Deinit,
    DisableAxis,
    EnableAxis,
    FollowTarget,
    Home,
    Init,
    ModelAddPoint,
    ModelClearPoints,
    ModelDeletePoint,
    ModelDisablePoint,
    ModelEnablePoint,
    ModelLoad,
    ModelSave,
    MountAxis,
    MoveToPark,
    RADecArcseconds,
    SetAzimuthWrapRangeMin,
    SetParkPosition,
    Slewing,
    Stop,
    Tracking,
)
from sensorkit.pwi4.device import (
    PWI4Client,
    PWI4Device,
    PWI4DeviceConfig,
    PWI4DeviceState,
)
from sensorkit.std import Connect, Connected, Disconnect

iers.conf.auto_download = False
iers.conf.auto_max_age = None


# Max time to wait for a commanded slew to *begin* (is_slewing -> True) before
# waiting for it to settle. Also the worst-case stall on a positional no-op
# (re-follow to an unchanged position, park-when-already-parked).
_MOUNT_ONSET_TIMEOUT = 2.0


@sk.declare_keyword
class MountAxisEnabled(BaseModel):
    axis: list[AxisEnabled]


@sk.declare_keyword
class MountTargetDistance(BaseModel):
    axis: list[AxisTargetDistance]


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
    alt_rate_deg_per_sec: float,
    az_rate_deg_per_sec: float,
    location: EarthLocation,
    time: Time,
) -> tuple[float, float]:
    """
    Convert Alt/Az position and angular rates (deg/sec) into RA/Dec rates.

    Returns
    -------
    ra_rate_deg_per_sec : float
        d(RA)/dt in deg/sec
    dec_rate_deg_per_sec : float
        d(Dec)/dt in deg/sec
    """

    # Numerical derivative for rates
    dt = 0.01  # seconds

    # Current AltAz coordinate
    coord_altaz = SkyCoord(
        alt=alt_deg * u.deg,
        az=az_deg * u.deg,
        frame=AltAz(obstime=time, location=location)
    )

    # Convert to RA/Dec
    coord_radec = coord_altaz.transform_to(ICRS())
    ra_deg = coord_radec.ra.deg
    dec_deg = coord_radec.dec.deg

    # New alt/az after dt
    new_alt = alt_deg + alt_rate_deg_per_sec * dt
    new_az = az_deg + az_rate_deg_per_sec * dt

    # Create coordinate at new Alt/Az
    new_coord_altaz = SkyCoord(
        alt=new_alt * u.deg,
        az=new_az * u.deg,
        frame=AltAz(obstime=time + dt * u.s, location=location)
    )

    # Transform to RA/Dec
    new_coord_radec = new_coord_altaz.transform_to(ICRS())

    # Compute rates in deg/sec
    ra_rate_deg_per_sec = (new_coord_radec.ra.deg - ra_deg) / dt
    dec_rate_deg_per_sec = (new_coord_radec.dec.deg - dec_deg) / dt

    return ra_rate_deg_per_sec, dec_rate_deg_per_sec


@sk.declare_device
class PWI4Mount(PWI4Device):
    """PWI4 mount implementation."""

    config: PWI4MountConfig
    device_name = "Mount"

    def __init__(self, config: PWI4MountConfig, client: PWI4Client):
        super().__init__(config=config, client=client)
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None
        self._wrap_task: asyncio.Task | None = None
        self._fast_status_task: asyncio.Task | None = None
        self._sidereal = False

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(PWI4MountState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            self.state = PWI4MountState()
            logger.warning(f"No saved state for {device.entity}")

        # Site location
        st = await self.client.status()
        self._site_lat = self.client.get_float(st, "site.latitude_degs")
        self._site_lon = self.client.get_float(st, "site.longitude_degs")
        self._site_elev = self.client.get_float(st, "site.height_meters")
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

        # Enable the motors
        await asyncio.gather(
            self.mount_enable_axis(EnableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_enable_axis(EnableAxis(axis=MountAxis.ALTITUDE)),
        )

        # Zero out any residual rates
        await self.client.request(
            "/mount/offset",
            params={
                "ra_set_rate_arcsec_per_sec": 0,
                "dec_set_rate_arcsec_per_sec": 0,
            },
        )
        await self.client.request("/mount/tracking_off")

        # Home, as needed
        await self.mount_home(Home())

        # Initialize the optical tube
        await self.init_ot()

        # Keep the mount azimuth centered w.r.t. the wrap
        if self.config.wrap_autocenter:
            self._wrap_task = asyncio.create_task(
                wrap_autocenter_loop(
                    self.client,
                    interval=self.config.wrap_interval,
                    deadband_deg=self.config.wrap_deadband_deg,
                )
            )

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

        if self._wrap_task is not None:
            self._wrap_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await self._wrap_task

        await self.mount_park(MoveToPark())
        await asyncio.gather(
            self.mount_disable_axis(DisableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_disable_axis(DisableAxis(axis=MountAxis.ALTITUDE)),
        )
        await self.deinit_ot()

    @sk.command_handler
    async def mount_connect(self, cmd: Connect):
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
    async def mount_disconnect(self, cmd: Disconnect):
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
    async def mount_enable_axis(self, cmd: EnableAxis):
        await self.require_connected()
        axis = 0 if cmd.axis == MountAxis.AZIMUTH else 1
        logger.debug(f"enabling axis {axis}")
        await self.client.request("/mount/enable", params={"axis": axis})
        logger.debug(f"enabled axis {axis}")

    @sk.command_handler
    async def mount_disable_axis(self, cmd: DisableAxis):
        await self.require_connected()
        axis = 0 if cmd.axis == MountAxis.AZIMUTH else 1
        logger.debug(f"disabling axis {axis}")
        await self.client.request("/mount/disable", params={"axis": axis})
        logger.debug(f"disabled axis {axis}")

    @sk.command_handler
    async def mount_stop(self, cmd: Stop):
        await self.require_connected()

        logger.debug("stopping mount")
        await self.client.request("/mount/stop")
        await self._wait_for_mount(await_onset=False)
        logger.debug("stopped mount")
        self._sidereal = False

        self._stop_fast_status()

    @sk.command_handler
    async def mount_home(self, cmd: Home):
        await self.require_connected()

        # Check if mount has already homed
        st = await self.client.status()
        if self.client.get_bool(
            st, "mount.axis0.is_position_initialized"
        ) and self.client.get_bool(st, "mount.axis1.is_position_initialized"):
            return

        await asyncio.gather(
            self.mount_enable_axis(EnableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_enable_axis(EnableAxis(axis=MountAxis.ALTITUDE)),
        )

        logger.debug("homing mount")
        await self.client.request("/mount/find_home")

        # async with asyncio.timeout(self.config.timeout):
        #     await self.client.poll(
        #         lambda s: (
        #             self.client.get_bool(s, "mount.axis0.is_position_initialized")
        #             and self.client.get_bool(s, "mount.axis1.is_position_initialized")
        #         ),
        #     )

        logger.debug("homed mount")

    @sk.command_handler
    async def mount_park(self, cmd: MoveToPark):
        await self.require_connected()

        await asyncio.gather(
            self.mount_enable_axis(EnableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_enable_axis(EnableAxis(axis=MountAxis.ALTITUDE)),
        )

        logger.debug("parking mount")
        await self.client.request("/mount/park")
        await self._wait_for_mount()
        logger.debug("parked mount")

    @sk.command_handler
    async def mount_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        logger.debug("setting park position")
        await self.client.request("/mount/set_park_here")
        logger.debug("set park position")

    @sk.command_handler
    async def mount_follow_target(self, cmd: FollowTarget):
        await self.require_connected()
        await asyncio.gather(
            self.mount_enable_axis(EnableAxis(axis=MountAxis.AZIMUTH)),
            self.mount_enable_axis(EnableAxis(axis=MountAxis.ALTITUDE)),
        )

        target = await cmd.target.adapt(
            ICRSTarget,
            AltAzTarget,
            (FrameTarget, ReferenceFrame.ICRF),
            (FrameTarget, ReferenceFrame.ALTAZ),
            TLETarget,
            (RateTarget, ReferenceFrame.ICRF),
            (EphemerisTarget, ReferenceFrame.ICRF),
            observer=self._geodetic,
        )

        self._sidereal = False

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                await self.client.request(
                    "/mount/goto_ra_dec_j2000",
                    params={
                        "ra_hours": target.coords.ra / 15,
                        "dec_degs": target.coords.dec,
                    },
                )

                await self._wait_for_mount(tracking=True)
                self._sidereal = True
                self._start_fast_status()

                logger.debug("following RADec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                await self.client.request(
                    "/mount/goto_alt_az",
                    params={
                        "alt_degs": target.coords.alt,
                        "az_degs": target.coords.az,
                    },
                )

                await self._wait_for_mount(tracking=False)
                self._start_fast_status()

                logger.debug("following AltAz target")

            case TLETarget():
                logger.debug("executing TLE follow")

                await self.client.request(
                    "/mount/follow_tle",
                    params={
                        "line1": target.tle.line0,
                        "line2": target.tle.line1,
                        "line3": target.tle.line2,
                    },
                )

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following TLE target")

            case RateTarget():
                logger.debug("executing Rate follow")

                # Slew to initial position
                await self.client.request(
                    "/mount/goto_ra_dec_j2000",
                    params={
                        "ra_hours": target.initial_coords.ra / 15,
                        "dec_degs": target.initial_coords.dec,
                    },
                )

                await self._wait_for_mount(tracking=True)

                # Apply offset rates (degrees/sec -> arcsec/sec)
                await self.client.request(
                    "/mount/offset",
                    params={
                        "ra_set_rate_arcsec_per_sec": target.rates.ra * 3600,
                        "dec_set_rate_arcsec_per_sec": target.rates.dec * 3600,
                    },
                )
                self._start_fast_status()

                logger.debug("following Rate target")

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow")

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

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following Ephemeris target")

            case FrameTarget():
                match target.frame:
                    case ReferenceFrame.ALTAZ:
                        self._stop_fast_status()
                        logger.debug("disabling tracking")
                        await self.client.request("/mount/tracking_off")
                        await self._wait_for_mount(tracking=False, await_onset=False)
                        logger.debug("disabled tracking")
                    case ReferenceFrame.ICRF:
                        logger.debug("enabling sidereal tracking")
                        await self.client.request("/mount/tracking_on")
                        await self._wait_for_mount(tracking=True, await_onset=False)
                        self._sidereal = True
                        self._start_fast_status()
                        logger.debug("enabled sidereal tracking")

            case _:
                track_type = type(cmd.target).__name__
                raise NotImplementedError(f"{track_type} tracking via PWI4 is not supported")

        try:
            st = await self.client.status()
            await self._publish_mount_status(st)
        except Exception as e:
            logger.warning(f"Immediate mount status publish failed: {e}")

    @sk.command_handler
    async def mount_offset(self, cmd: ApplyOffset):
        await self.require_connected()
        logger.debug("applying offset")

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

        logger.debug("applied offset")

    @sk.command_handler
    async def mount_set_wrap_range_min(self, cmd: SetAzimuthWrapRangeMin):
        await self.require_connected()
        logger.debug("setting azimuth wrap range min")
        await self.client.request("/mount/set_axis0_wrap_range_min", params={"degs": cmd.min})
        await sk.device().publish(AzimuthWrapRange(min=cmd.min, max=cmd.min + 360))
        logger.debug("set azimuth wrap range min")

    @sk.command_handler
    async def model_add_point(self, cmd: ModelAddPoint):
        await self.require_connected()
        logger.debug("adding model point")
        if cmd.reference_frame != ReferenceFrame.ICRF:
            raise ValueError("Mount model points must be ICRF")
        await self.client.request(
            "/mount/model/add_point",
            params={
                "ra_j2000_hours": cmd.right_ascension_hours,
                "dec_j2000_degs": cmd.declination_degrees,
            },
        )
        logger.debug("added model point")

    @sk.command_handler
    async def model_delete_point(self, cmd: ModelDeletePoint):
        await self.require_connected()
        logger.debug("deleting model point")
        await self.client.request(
            "/mount/model/delete_point",
            params={"index": ",".join(str(i) for i in cmd.indexes)},
        )
        logger.debug("deleted model point")

    @sk.command_handler
    async def model_enable_point(self, cmd: ModelEnablePoint):
        await self.require_connected()
        logger.debug("enabling model point")
        await self.client.request(
            "/mount/model/enable_point",
            params={"index": ",".join(str(i) for i in cmd.indexes)},
        )
        logger.debug("enabled model point")

    @sk.command_handler
    async def model_disable_point(self, cmd: ModelDisablePoint):
        await self.require_connected()
        logger.debug("disabling model point")
        await self.client.request(
            "/mount/model/disable_point",
            params={"index": ",".join(str(i) for i in cmd.indexes)},
        )
        logger.debug("disabled model point")

    @sk.command_handler
    async def model_clear_points(self, cmd: ModelClearPoints):
        await self.require_connected()
        logger.debug("clearing model points")
        await self.client.request("/mount/model/clear_points")
        logger.debug("cleared model points")

    @sk.command_handler
    async def model_save(self, cmd: ModelSave):
        await self.require_connected()
        logger.debug("saving model")
        await self.client.request("/mount/model/save", params={"filename": cmd.filename})
        logger.debug("saved model")

    @sk.command_handler
    async def model_load(self, cmd: ModelLoad):
        await self.require_connected()
        logger.debug("loading model")
        await self.client.request("/mount/model/load", params={"filename": cmd.filename})
        logger.debug("loaded model")

    async def _wait_for_mount(
        self,
        *,
        slewing: bool = False,
        tracking: bool = False,
        await_onset: bool = True,
    ):
        """Poll /status until mount.is_slewing and mount.is_tracking both match.

        When `await_onset` (the default, for commands that slew), first wait
        briefly for the mount to *start* slewing. Without this, a command whose
        target flags already equal the current flags (e.g. re-following while
        already tracking) would match the stale pre-command state and return
        before motion begins. If no slew is observed within _MOUNT_ONSET_TIMEOUT,
        the command was a positional no-op, and we fall through to the settle check.

        Non-slewing commands (stop, enable/disable tracking) must pass
        `await_onset=False`: they never raise is_slewing, so onset would
        otherwise burn the full timeout on every call.

        The settle wait is bounded by config.timeout; both phases poll every 0.1 s.
        """

        if await_onset:
            try:
                async with asyncio.timeout(_MOUNT_ONSET_TIMEOUT):
                    while True:
                        st = await self.client.status()
                        if self.client.get_bool(st, "mount.is_slewing"):
                            break
                        await asyncio.sleep(0.1)
            except TimeoutError:
                # No slew observed -> positional no-op. Fall through to the
                # settle check, which returns at once if already on target or
                # still waits out any flag change (e.g. tracking engaging).
                pass

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                is_slewing = self.client.get_bool(st, "mount.is_slewing")
                is_tracking = self.client.get_bool(st, "mount.is_tracking")
                if is_slewing == slewing and is_tracking == tracking:
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

    async def _publish_mount_status(self, st: dict[str, str]):
        connected = self.client.get_bool(st, "mount.is_connected")
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if not connected:
            return

        await device.publish(
            Slewing(is_slewing=self.client.get_bool(st, "mount.is_slewing"))
        )
        await device.publish(
            Tracking(is_tracking=self.client.get_bool(st, "mount.is_tracking"))
        )

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

        # Axis rates + RA/Dec rate conversion
        az_rate = self.client.get_float(st, "mount.axis0.measured_velocity_degs_per_sec")
        alt_rate = self.client.get_float(st, "mount.axis1.measured_velocity_degs_per_sec")

        if self._location is not None:
            ra_rate, dec_rate = altaz_rates_to_radec_rates(
                alt_degs, az_degs, alt_rate, az_rate, self._location, Time.now()
            )
            if self._sidereal:
                ra_rate = dec_rate = 0.0

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
                        mechanical_position=self.client.get_float(st, "mount.axis0.position_degs"),
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
                        mechanical_position=self.client.get_float(st, "mount.axis1.position_degs"),
                        min_mechanical_position=self.client.get_float(
                            st, "mount.axis1.min_mech_position_degs"
                        ),
                        max_mechanical_position=self.client.get_float(
                            st, "mount.axis1.max_mech_position_degs"
                        ),
                        axis=MountAxis.ALTITUDE,
                    ),
                    right_ascension=AxisRate(
                        velocity=ra_rate,
                        axis=MountAxis.RIGHT_ASCENSION,
                    ),
                    declination=AxisRate(
                        velocity=dec_rate,
                        axis=MountAxis.DECLINATION,
                    ),
                )
            )

        await device.publish(
            MountTargetDistance(
                axis=[
                    AxisTargetDistance(
                        distance_arcseconds=self.client.get_float(
                            st, "mount.axis0.dist_to_target_arcsec"
                        ),
                        rms_error_arcseconds=self.client.get_float(
                            st, "mount.axis0.rms_error_arcsec"
                        ),
                        axis=MountAxis.AZIMUTH,
                    ),
                    AxisTargetDistance(
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

        if self._geodetic is not None:
            await device.publish(
                SitePosition(
                    latitude_degrees=self._geodetic.lat,
                    longitude_degrees=self._geodetic.lon,
                    altitude_km=self._geodetic.elev,
                )
            )

    async def status_publish_slow(self):
        while True:
            try:
                st = await self.client.status()
                if not self._fast_status_active:
                    await self._publish_mount_status(st)
                else:
                    self.device_connected = self.client.get_bool(st, "mount.is_connected")
                    await sk.device().publish(Connected(is_connected=self.device_connected))

                # logger.debug(
                #     f"PWI4 mount status: connected={self.device_connected}"
                # )
            except Exception as e:
                logger.warning(f"Error in slow mount status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            await asyncio.sleep(self.config.status_frequency_slow)

    async def status_publish_fast(self):
        while True:
            try:
                st = await self.client.status()
                await self._publish_mount_status(st)
            except Exception as e:
                logger.warning(f"Error in fast mount status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_fast)
                continue

            await asyncio.sleep(self.config.status_frequency_fast)

    async def init_ot(self):
        """Set heater power levels and turn on fans."""

        for role, power in self.config.heaters.items():
            await self.client.request("/heaters/set", params={"role": role, "power": int(power)})
            logger.debug(f"set {role} heater power to {power:.0f}%")

        if self.config.fans:
            roles = ",".join(self.config.fans)
            await self.client.request("/fans/on", params={"roles": roles})
            logger.debug(f"turned on fans: {roles}")

    async def deinit_ot(self):
        """Turn off heaters and fans."""

        for role in self.config.heaters:
            await self.client.request("/heaters/set", params={"role": role, "power": 0})
            logger.debug(f"set {role} heater power to 0%")

        if self.config.fans:
            roles = ",".join(self.config.fans)
            await self.client.request("/fans/off", params={"roles": roles})
            logger.debug(f"turned off fans: {roles}")


class PWI4MountConfig(PWI4DeviceConfig[PWI4Mount]):
    """PWI4 Mount configuration."""

    device_type: Literal["mount"] = "mount"
    wrap_autocenter: bool = False
    wrap_interval: float = 60.0
    wrap_deadband_deg: float = 10.0
    fans: list[str] = []
    heaters: dict[str, float] = {}
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Mount(config=self, client=client)


class PWI4MountState(PWI4DeviceState):
    """PWI4 Mount state."""

    device_type: Literal["mount"] = "mount"
