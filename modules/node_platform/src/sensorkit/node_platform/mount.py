from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Literal, override

import astropy.units as u
import numpy as np
import ourskyai_node_platform_api as osapi
from astropy.coordinates import ICRS, AltAz, CartesianRepresentation, EarthLocation, SkyCoord
from astropy.time import Time
from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing, RADecPointing, ReferenceFrame
from sensorkit.astro.coords import Coordinates, Equatorial, Geodetic, Horizontal
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    RateTarget,
    TLETarget,
)
from sensorkit.models.devices import (
    AxisRate,
    AxisRates,
    Deinit,
    FollowTarget,
    Home,
    Init,
    MountAxis,
    MoveToPark,
    Slewing,
    Stop,
    Tracking,
)
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)
from sensorkit.std import Connected


@sk.declare_keyword
class OTStatus(BaseModel):
    """Optical tube status (fans, heaters, temperature sensors, cover, M3)."""

    model_config = {"extra": "allow"}


@sk.declare_device
class NodePlatformMount(NodePlatformDevice):
    """Node Platform Mount implementation."""

    config: NodePlatformMountConfig
    device_name = "Mount"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NodePlatformMountState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformMountState()

        self.mount_slewing: bool | None = None
        self.mount_tracking: bool | None = None
        self._fast_status_task: asyncio.Task | None = None

        # Cache for site info (location + time)
        self._site_info: dict[str, object] = {}
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None

        # Start slow status publishing
        self.start_status_loop(self.status_publish_slow())

        # Wait for initial status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency_slow)

        # Site location
        location: osapi.V1SystemLocation = await self.api.call("v1_get_system_location")
        self._site_info["latitude_degrees"] = location.latitude_degrees
        self._site_info["longitude_degrees"] = location.longitude_degrees
        self._site_info["height_meters"] = location.height_meters
        self._site_info["location_updated_at"] = location.updated_at
        # if location.gps_details:
        #     self._site_info["gps_fix"] = location.gps_details.fix
        #     self._site_info["gps_position_error_meters"] = location.gps_details.position_error_meters
        self._geodetic = Geodetic(
            lon=self._site_info["longitude_degrees"],
            lat=self._site_info["latitude_degrees"],
            elev=self._site_info["height_meters"],
        )
        self._location = EarthLocation(
            lat=self._site_info["latitude_degrees"] * u.deg,
            lon=self._site_info["longitude_degrees"] * u.deg,
            height=self._site_info["height_meters"] * u.m,
        )

        # System time
        system_time: osapi.V1SystemTime = await self.api.call("v1_get_system_time")
        self._site_info["time_source"] = system_time.current_time_source
        self._site_info["system_time"] = system_time.current_system_time
        self._site_info["time_uncertainty_seconds"] = system_time.time_uncertainty_seconds

        logger.debug(
            f"site info: lat={self._site_info['latitude_degrees']}, "
            f"lon={self._site_info['longitude_degrees']}, "
            f"height={self._site_info['height_meters']} m, "
            f"time_source={self._site_info['time_source']}, "
            f"time_unc={self._site_info['time_uncertainty_seconds']} s"
        )

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency_slow)
        await self.stop_status_loop()
        await self.api.close()
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def mount_init(self, cmd: Init):
        await self.require_connected()

        # Enable the motors
        await self.mount_enable(sk.Enable())

        # Home, as needed
        if not self.state.has_been_homed:
            await self.mount_home(Home())

        # Go to park position for cover opening
        await self.mount_park(MoveToPark())

        # Initialize the optical tube
        await self.init_ot()

    @sk.command_handler
    async def mount_deinit(self, cmd: Deinit):
        self._stop_fast_status()
        await self.mount_stop(Stop())
        await self.mount_park(MoveToPark())
        await self.mount_disable(sk.Disable())
        await self.deinit_ot()

    @sk.command_handler
    async def mount_enable(self, cmd: sk.Enable):
        await self.require_connected()

        status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
        if not status.motor_a.is_enabled:
            logger.debug("enabling mount motors")
            await self.api.call("v1_enable_mount_motors")
            logger.debug("enabled mount motors")

    @sk.command_handler
    async def mount_disable(self, cmd: sk.Disable):
        await self.require_connected()

        status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
        if status.motor_a.is_enabled:
            logger.debug("disabling mount motors")
            await self.api.call("v1_disable_mount_motors")
            logger.debug("disabled mount motors")

    @sk.command_handler
    async def mount_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping mount")

        await self.api.call("v1_halt_mount")
        await asyncio.sleep(1)

        await self._wait_for_mount()

        self._stop_fast_status()
        logger.debug("stopped mount")

    @sk.command_handler
    async def mount_home(self, cmd: Home):
        await self.require_connected()
        logger.debug("homing mount")

        await self.api.call("v1_mount_go_to_home")
        await asyncio.sleep(1)

        await self._wait_for_mount()

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed mount")

    @sk.command_handler
    async def mount_park(self, cmd: MoveToPark):
        await self.require_connected()
        logger.debug("parking mount")

        self._stop_fast_status()
        await self.api.call("v1_park_mount")
        await asyncio.sleep(1)

        await self._wait_for_mount()

        logger.debug("parked mount")

    @sk.command_handler
    async def mount_set_park_position(self, cmd: sk.SetParkPosition):
        await self.require_connected()
        logger.debug("setting park position")
        await self.api.call("v1_set_mount_park_position_here")
        logger.debug("set park position")

    @sk.command_handler
    async def mount_follow_target(self, cmd: FollowTarget):
        await self.require_connected()

        target = await cmd.target.adapt(
            ICRSTarget,
            AltAzTarget,
            (FrameTarget, ReferenceFrame.ICRF),
            (FrameTarget, ReferenceFrame.ALTAZ),
            TLETarget,
            (RateTarget, ReferenceFrame.CIRF),
            (EphemerisTarget, ReferenceFrame.ICRF),
            observer=self._geodetic,
        )

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                req = osapi.V1GoToMountCoordinatesRequest(
                    ra=target.coords.ra,
                    dec=target.coords.dec,
                )
                await self.api.call("v1_go_to_mount_coordinates", req)
                await asyncio.sleep(1)

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following RA/Dec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                req = osapi.V1GoToMountCoordinatesRequest(
                    altitude=target.coords.alt,
                    azimuth=target.coords.az,
                )
                await self.api.call("v1_go_to_mount_coordinates", req)
                await asyncio.sleep(1)

                await self._wait_for_mount(tracking=False)
                self._start_fast_status()

                logger.debug("following Alt/Az target")

            case TLETarget():
                logger.debug("executing TLE follow")

                req = osapi.V1FollowTLERequest(
                    tle_line1=target.tle.line1,
                    tle_line2=target.tle.line2,
                )
                await self.api.call("v1_mount_follow_tle", req)
                await asyncio.sleep(1)

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following TLE target")

            case RateTarget():
                logger.debug("executing Rate follow")

                # Sample the constant-rate path as singularity-free ENU unit
                # vectors so motion through the pole/zenith is handled correctly
                # (see _enu_along_rate). The astropy transform is CPU-bound, so
                # build the samples off the event loop.
                samples = await asyncio.to_thread(
                    self._rate_track_samples,
                    target.initial_coords,
                    target.rates,
                    target.initial_frame,
                    target.initial_time,
                )
                req = osapi.V1StartMountTrackPathRequest(
                    frame=osapi.V1MountTrackPathFrame.ENU_UNIT_VECTOR,
                    enu_samples=samples,
                )
                await self.api.call("v1_start_mount_track_path", req)
                await asyncio.sleep(1)

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following Rate target")

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow")

                # Resolve the precomputed points to singularity-free ENU unit
                # vectors, so the controller interpolates smoothly through the
                # pole and the RA=0 seam. CPU-bound astropy work runs off the event loop.
                jds = np.asarray(target.jds, dtype=float)
                east, north, up = await asyncio.to_thread(
                    self._points_to_enu,
                    target.points,
                    target.frame,
                    Time(jds, format="jd"),
                )
                samples = [
                    osapi.V1MountTrackPathEnuSample(
                        julian_day=float(jds[i]),
                        east=float(east[i]),
                        north=float(north[i]),
                        up=float(up[i]),
                    )
                    for i in range(len(jds))
                ]
                req = osapi.V1StartMountTrackPathRequest(
                    frame=osapi.V1MountTrackPathFrame.ENU_UNIT_VECTOR,
                    enu_samples=samples,
                )
                await self.api.call("v1_start_mount_track_path", req)
                await asyncio.sleep(1)

                await self._wait_for_mount(tracking=True)
                self._start_fast_status()

                logger.debug("following Ephemeris target")

            case FrameTarget():
                match cmd.target.frame:
                    case ReferenceFrame.ALTAZ:
                        logger.debug("disabling tracking")
                        self._stop_fast_status()
                        await self.api.call("v1_disable_mount_tracking")
                        await self._wait_for_mount(tracking=False)
                        logger.debug("disabled tracking")
                    case ReferenceFrame.ICRF:
                        logger.debug("enabling sidereal tracking")
                        await self.api.call("v1_enable_mount_tracking")
                        await self._wait_for_mount(tracking=True)
                        self._start_fast_status()
                        logger.debug("enabled sidereal tracking")

                    case _:
                        raise RuntimeError(f"Need specific target to track {cmd.target.frame}")

            case _:
                track_type = type(cmd.target).__name__
                raise NotImplementedError(
                    f"{track_type} tracking via Node Platform is not supported"
                )

    async def _wait_for_mount(
        self,
        *,
        slewing: bool = False,
        tracking: bool = False,
    ):
        """Poll v2_get_mount_status until is_slewing and is_tracking both match.

        Bounded by config.timeout; polls every 0.2 s.
        """

        async with asyncio.timeout(self.config.timeout):
            while True:
                status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
                if status.is_slewing == slewing and status.is_tracking == tracking:
                    break
                await asyncio.sleep(0.2)

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

    async def _publish_mount_status(self, status: osapi.V2MountStatus):
        self.mount_slewing = status.is_slewing
        self.mount_tracking = status.is_tracking

        device = sk.device()
        await device.publish(
            RADecPointing(
                right_ascension_hours=status.ra_j2000_degrees / 15.0,
                declination_degrees=status.dec_j2000_degrees,
                reference_frame=ReferenceFrame.ICRF,
            )
        )
        await device.publish(
            AltAzPointing(
                altitude_degrees=status.altitude_degrees,
                azimuth_degrees=status.azimuth_degrees,
            )
        )

        if self._location is not None:
            rate_a = status.motor_a.measured_velocity_degrees_per_second
            rate_b = status.motor_b.measured_velocity_degrees_per_second
            ra_rate, dec_rate = self.altaz_rates_to_radec_rates(
                status.altitude_degrees,
                status.azimuth_degrees,
                rate_b,
                rate_a,
                location=self._location,
                time=Time.now(),
            )
            await device.publish(
                AxisRates(
                    azimuth=AxisRate(velocity=rate_a, axis=MountAxis.AZIMUTH),
                    altitude=AxisRate(velocity=rate_b, axis=MountAxis.ALTITUDE),
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

    async def _get_ot_temperatures(self) -> list[dict]:
        """Get OT temperature sensors via raw HTTP (not yet in SDK model)."""

        try:
            resp = await asyncio.to_thread(
                self.api._client.rest_client.request,
                "GET",
                f"{self.api._configuration.host}/node-platform/v1/optical-tube/status",
                headers={"Authorization": f"Bearer {self.api._configuration.access_token}"},
            )
            raw = json.loads(resp.read())
            return raw.get("temperatureSensors", {}).get("statuses", [])
        except Exception as e:
            logger.warning(f"Failed to get OT temperatures ({e})")
            return []

    async def _publish_ot_status(
        self, ot: osapi.V1OpticalTubeStatus, temps: list[dict] | None = None
    ):
        props: dict = {}

        if ot.fans:
            for fan in ot.fans.statuses:
                role = fan.role.value if hasattr(fan.role, "value") else str(fan.role)
                props[f"fan_{role}_connected"] = fan.connected
                props[f"fan_{role}_on"] = fan.is_on

        if ot.heaters:
            for heater in ot.heaters.statuses:
                role = heater.role.value if hasattr(heater.role, "value") else str(heater.role)
                props[f"heater_{role}_connected"] = heater.connected
                props[f"heater_{role}_power"] = heater.power

        for sensor in temps or []:
            role = sensor["role"]
            props[f"temp_{role}_celsius"] = sensor["temperatureCelsius"]

        if ot.cover:
            props["cover_state"] = str(ot.cover.state) if hasattr(ot.cover, "state") else None

        if ot.m3:
            if hasattr(ot.m3, "port"):
                props["m3_port"] = ot.m3.port

        if props:
            await sk.device().publish(OTStatus(**props))

    async def status_publish_slow(self):
        while True:
            # Mount
            try:
                # Always get mount status for connection/slewing/tracking state
                status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
                self.device_connected = status.connected
                self.mount_slewing = status.is_slewing
                self.mount_tracking = status.is_tracking

                await sk.device().publish(Connected(is_connected=status.connected))
                await sk.device().publish(Slewing(is_slewing=self.mount_slewing))
                await sk.device().publish(Tracking(is_tracking=self.mount_tracking))

                # Only publish pointing/rates if the fast loop isn't handling it
                if not self._fast_status_active:
                    await self._publish_mount_status(status)
            except Exception as e:
                logger.exception(f"Error in slow status_publish: {e}")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            # Optical tube
            try:
                ot: osapi.V1OpticalTubeStatus = await self.api.call("v1_get_optical_tube_status")
                temps = await self._get_ot_temperatures()
                await self._publish_ot_status(ot, temps)
            except Exception as e:
                logger.warning(f"Failed to update OT status ({e})")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            # ot_parts = []
            # if ot.fans:
            #     for fan in ot.fans.statuses:
            #         role = fan.role.value if hasattr(fan.role, "value") else str(fan.role)
            #         ot_parts.append(f"fan_{role}={'on' if fan.is_on else 'off'}")
            # if ot.heaters:
            #     for heater in ot.heaters.statuses:
            #         role = heater.role.value if hasattr(heater.role, "value") else str(heater.role)
            #         ot_parts.append(f"heater_{role}={heater.power}%")
            # for sensor in temps or []:
            #     ot_parts.append(f"temp_{sensor['role']}={sensor['temperatureCelsius']}°C")
            # ot_str = ", ".join(ot_parts)
            # logger.debug(
            #     f"NodePlatform mount status: connected={self.device_connected}, "
            #     f"slewing={self.mount_slewing}, tracking={self.mount_tracking}, "
            #     f"{ot_str}"
            # )

            await asyncio.sleep(self.config.status_frequency_slow)

    async def status_publish_fast(self):
        while True:
            # Mount
            try:
                status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
                await self._publish_mount_status(status)
            except Exception as e:
                logger.warning(f"Error in fast status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_fast)
                continue

            await asyncio.sleep(self.config.status_frequency_fast)

    def _rate_track_samples(
        self,
        coords: Coordinates,
        rates: Coordinates,
        frame: ReferenceFrame,
        t0: datetime,
        *,
        step: float = 30.0,
        max_window: float = 24 * 3600.0,
        min_coverage: float = 60.0,
    ) -> list[osapi.V1MountTrackPathEnuSample]:
        """Sample a constant-rate target as topocentric ENU unit vectors.

        The position is extrapolated linearly in the rate's native frame, embedded
        as a 3-D unit vector (finite past 90 deg, so the pole/zenith folds to the
        antipodal meridian instead of crashing or clamping), resolved to topocentric
        East/North/Up, and trimmed at horizon-set. The edge controller renormalizes
        the vectors, so they need only be finite and nonzero.

        Raises ValueError if the target is below the horizon at t0 or stays up for
        less than ``min_coverage`` seconds (the Node Platform rejects paths with
        under 60 s of trackable coverage).
        """
        times_s = np.arange(0.0, max_window + step, step)
        obstimes = Time(t0) + times_s * u.s

        east, north, up = self._enu_along_rate(coords, rates, frame, obstimes, times_s)

        visible = up >= 0.0
        if not visible[0]:
            raise ValueError("rate target is below the horizon at t0; not trackable")
        set_idx = len(times_s) if visible.all() else int(np.argmin(visible))
        coverage = float(times_s[set_idx - 1])
        if coverage < min_coverage:
            raise ValueError(
                f"rate target sets after {coverage:.0f}s, below the "
                f"{min_coverage:.0f}s minimum track coverage"
            )

        jd = obstimes.jd
        return [
            osapi.V1MountTrackPathEnuSample(
                julian_day=float(jd[i]),
                east=float(east[i]),
                north=float(north[i]),
                up=float(up[i]),
            )
            for i in range(set_idx)
        ]

    @staticmethod
    def _altaz_to_enu(
        alt_rad: np.ndarray, az_rad: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Topocentric (East, North, Up) from alt/az in radians.

        Azimuth is measured clockwise from north and Up is positive above the
        horizon, matching the Node Platform ENU convention. Finite for any alt,
        so a path crossing the zenith folds to the opposite azimuth rather than
        diverging.
        """
        return (
            np.cos(alt_rad) * np.sin(az_rad),
            np.cos(alt_rad) * np.cos(az_rad),
            np.sin(alt_rad),
        )

    def _radec_to_enu(
        self,
        ra_deg: np.ndarray,
        dec_deg: np.ndarray,
        frame: ReferenceFrame,
        obstimes: Time,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Topocentric ENU from equatorial angles (degrees) in ``frame``.

        The direction is built as a unit vector (finite for any dec, so motion
        through the pole folds correctly) and rotated to topocentric by astropy.
        """
        ra = np.deg2rad(ra_deg)
        dec = np.deg2rad(dec_deg)
        vec = CartesianRepresentation(
            np.cos(dec) * np.cos(ra),
            np.cos(dec) * np.sin(ra),
            np.sin(dec),
        )
        altaz = SkyCoord(vec, frame=frame.to_astropy(), obstime=obstimes).transform_to(
            AltAz(obstime=obstimes, location=self._location)
        )
        return self._altaz_to_enu(altaz.alt.rad, altaz.az.rad)

    def _enu_along_rate(
        self,
        coords: Coordinates,
        rates: Coordinates,
        frame: ReferenceFrame,
        obstimes: Time,
        times_s: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Extrapolate position at a constant rate in its native frame, as ENU."""
        match coords, rates:
            case Horizontal(alt=alt0, az=az0), Horizontal(alt=alt_rate, az=az_rate):
                return self._altaz_to_enu(
                    np.deg2rad(alt0 + alt_rate * times_s),
                    np.deg2rad(az0 + az_rate * times_s),
                )

            case Equatorial(ra=ra0, dec=dec0), Equatorial(ra=ra_rate, dec=dec_rate):
                return self._radec_to_enu(
                    ra0 + ra_rate * times_s, dec0 + dec_rate * times_s, frame, obstimes
                )

            case _:
                raise NotImplementedError(
                    f"rate follow for {type(coords).__name__} position with "
                    f"{type(rates).__name__} rates is not supported"
                )

    def _points_to_enu(
        self,
        points: Sequence[Coordinates],
        frame: ReferenceFrame,
        obstimes: Time,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Resolve a homogeneous sequence of ephemeris points to topocentric ENU."""
        match points[0]:
            case Horizontal():
                return self._altaz_to_enu(
                    np.deg2rad(np.fromiter((p.alt for p in points), float)),
                    np.deg2rad(np.fromiter((p.az for p in points), float)),
                )

            case Equatorial():
                return self._radec_to_enu(
                    np.fromiter((p.ra for p in points), float),
                    np.fromiter((p.dec for p in points), float),
                    frame,
                    obstimes,
                )

            case _:
                raise NotImplementedError(
                    f"ephemeris follow for {type(points[0]).__name__} points is not supported"
                )

    def altaz_rates_to_radec_rates(
        self,
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

        # Current AltAz coordinate
        altaz_frame = AltAz(obstime=time, location=location)
        coord_altaz = SkyCoord(alt=alt_deg * u.deg, az=az_deg * u.deg, frame=altaz_frame)

        # Convert to RA/Dec
        coord_radec = coord_altaz.transform_to(ICRS())
        ra_deg = coord_radec.ra.deg
        dec_deg = coord_radec.dec.deg

        # === Numerical derivative for rates ===
        dt = 0.01  # seconds

        # New alt/az after dt
        new_alt = alt_deg + alt_rate_deg_per_sec * dt
        new_az = az_deg + az_rate_deg_per_sec * dt

        # Create coordinate at new Alt/Az
        new_coord_altaz = SkyCoord(alt=new_alt * u.deg, az=new_az * u.deg, frame=altaz_frame)

        # Transform to RA/Dec
        new_coord_radec = new_coord_altaz.transform_to(ICRS())

        # Compute rates in deg/sec
        ra_rate_deg_per_sec = (new_coord_radec.ra.deg - ra_deg) / dt
        dec_rate_deg_per_sec = (new_coord_radec.dec.deg - dec_deg) / dt

        return ra_rate_deg_per_sec, dec_rate_deg_per_sec

    async def init_ot(self):
        """Set heater power levels and turn on fans."""

        heater_role_map = {
            "M1": osapi.V1OpticalTubeHeaterRole.M1,
            "M2": osapi.V1OpticalTubeHeaterRole.M2,
            "M3": osapi.V1OpticalTubeHeaterRole.M3,
        }
        for mirror, power in self.config.heater_power.items():
            role = heater_role_map.get(mirror.upper())
            await self.api.call(
                "v1_set_optical_tube_heater_power",
                osapi.V1SetOpticalTubeHeaterPowerRequest(
                    role=role,
                    power=int(power),
                ),
            )
            logger.debug(f"set {mirror} heater power to {power:.0f}%")

        # Turn on all OT fans
        if self.config.fans:
            all_fan_roles = list(osapi.V1OpticalTubeFanRole)
            await self.api.call(
                "v1_turn_on_optical_tube_fans",
                osapi.V1TurnOnOpticalTubeFansRequest(roles=all_fan_roles),
            )
            logger.debug(f"turned on all OT fans: {[r.value for r in all_fan_roles]}")

    async def deinit_ot(self):
        """Turn off heaters and fans."""

        heater_role_map = {
            "M1": osapi.V1OpticalTubeHeaterRole.M1,
            "M2": osapi.V1OpticalTubeHeaterRole.M2,
            "M3": osapi.V1OpticalTubeHeaterRole.M3,
        }
        for mirror in self.config.heater_power:
            role = heater_role_map.get(mirror.upper())
            await self.api.call(
                "v1_set_optical_tube_heater_power",
                osapi.V1SetOpticalTubeHeaterPowerRequest(
                    role=role,
                    power=0,
                ),
            )
            logger.debug(f"set {mirror} heater power to 0%")

        # Turn off all OT fans
        if self.config.fans:
            all_fan_roles = list(osapi.V1OpticalTubeFanRole)
            await self.api.call(
                "v1_turn_off_optical_tube_fans",
                osapi.V1TurnOffOpticalTubeFansRequest(roles=all_fan_roles),
            )
            logger.debug(f"turned off all OT fans: {[r.value for r in all_fan_roles]}")


class NodePlatformMountConfig(NodePlatformDeviceConfig[NodePlatformMount]):
    """Node Platform Mount configuration."""

    device_type: Literal["mount"] = "mount"
    fans: bool = False
    heater_power: dict[str, float] = Field(default_factory=dict)
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1
    timeout: float = 300.0

    @override
    def create_device(self):
        return NodePlatformMount(self)


class NodePlatformMountState(NodePlatformDeviceState):
    """Node Platform Mount state."""

    device_type: Literal["mount"] = "mount"
    has_been_homed: bool = False
