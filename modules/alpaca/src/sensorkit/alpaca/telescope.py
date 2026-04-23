from __future__ import annotations

import asyncio
from typing import Literal, override

import astropy.units as u
from astropy.coordinates import ICRS, AltAz, EarthLocation, SkyCoord
from astropy.time import Time

# Prevent IERS-A (Earth orientation parameters) download, which can take long enough to cause a lease expiry
from astropy.utils import iers
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.telescope import Telescope
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
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

iers.conf.auto_download = False
# Suppress the warning that results from the above decision
from astropy.utils.iers import conf

conf.auto_max_age = None


@sk.declare_keyword
class AlpacaTelescopeStatus(BaseModel):
    """ITelescopeV4 properties."""

    # Tracking
    tracking: bool = False
    tracking_rate: int | None = None
    right_ascension_rate: float = 0.0
    declination_rate: float = 0.0
    slewing: bool = False
    side_of_pier: str | None = None
    sidereal_time: float | None = None
    at_home: bool = False
    at_park: bool = False

    # Optics
    aperture_diameter: float | None = None
    aperture_area: float | None = None
    focal_length: float | None = None

    # System
    equatorial_system: str | None = None
    alignment_mode: str | None = None
    does_refraction: bool | None = None


_ALIGNMENT_MODES = {0: "AltAz", 1: "Polar", 2: "GermanPolar"}
_DRIVE_RATES = {0: "Sidereal", 1: "Lunar", 2: "Solar", 3: "King"}
_PIER_SIDES = {-1: "Unknown", 0: "East", 1: "West"}


@sk.declare_device
class AlpacaTelescope(AlpacaDevice):
    """Alpaca Telescope implementation."""

    config: AlpacaTelescopeConfig
    _home_task: asyncio.Task | None = None

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaTelescopeState)
        except Exception:
            self.state = AlpacaTelescopeState()

        await self.telescope_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.telescope_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def telescope_init(self, cmd: sk.Init):
        self.device_name = "Telescope"
        self.telescope = Telescope(self.address, self.config.device_number, self.config.protocol)
        self._tracking: bool | None = None
        self._slewing: bool | None = None

        await self.telescope_connect(sk.Connect())

        t = self.telescope

        # Read capabilities
        self._can_slew = await self.get(t, "CanSlew", False)
        self._can_slew_async = await self.get(t, "CanSlewAsync", False)
        self._can_slew_altaz = await self.get(t, "CanSlewAltAz", False)
        self._can_slew_altaz_async = await self.get(t, "CanSlewAltAzAsync", False)
        self._can_park = await self.get(t, "CanPark", False)
        self._can_unpark = await self.get(t, "CanUnpark", False)
        self._can_find_home = await self.get(t, "CanFindHome", False)
        self._can_set_tracking = await self.get(t, "CanSetTracking", False)
        self._can_set_park = await self.get(t, "CanSetPark", False)
        self._can_pulse_guide = await self.get(t, "CanPulseGuide", False)
        self._can_set_right_ascension_rate = await self.get(t, "CanSetRightAscensionRate", False)
        self._can_set_declination_rate = await self.get(t, "CanSetDeclinationRate", False)
        self._can_set_guide_rates = await self.get(t, "CanSetGuideRates", False)
        self._can_set_pier_side = await self.get(t, "CanSetPierSide", False)
        self._can_sync = await self.get(t, "CanSync", False)
        self._can_sync_altaz = await self.get(t, "CanSyncAltAz", False)
        self._can_move_axis = [await self._check_can_move_axis(i) for i in range(3)]

        # Read static telescope properties
        self._aperture_diameter = await self.get(t, "ApertureDiameter", None)
        self._aperture_area = await self.get(t, "ApertureArea", None)
        self._focal_length = await self.get(t, "FocalLength", None)
        self._equatorial_system = self._eq_system_str(await self.get(t, "EquatorialSystem", None))
        self._alignment_mode = _ALIGNMENT_MODES.get(await self.get(t, "AlignmentMode", -1))
        self._does_refraction = await self.get(t, "DoesRefraction", None)

        # Read and publish site position
        self._site_lat = await self.get(t, "SiteLatitude", None)
        self._site_lon = await self.get(t, "SiteLongitude", None)
        self._site_elev = await self.get(t, "SiteElevation", None)
        if self._site_lat is not None and self._site_lon is not None:
            await sk.device().publish(
                SitePosition(
                    latitude_degrees=self._site_lat,
                    longitude_degrees=self._site_lon,
                    altitude_km=(self._site_elev or 0.0) / 1000.0,
                )
            )
        self._location = EarthLocation(
            lat=self._site_lat * u.deg,
            lon=self._site_lon * u.deg,
            height=self._site_elev * u.m,
        )

        # Read available tracking rates
        self._tracking_rates = await self.get(t, "TrackingRates", [])

        self.start_status_loop(self.status_publish())

        # Home as needed
        if not self.state.has_been_homed:
            self._home_task = asyncio.create_task(self.telescope_home(sk.Home()))

    async def _check_can_move_axis(self, axis: int) -> bool:
        try:
            return await asyncio.to_thread(self.telescope.CanMoveAxis, axis)
        except Exception:
            return False

    @staticmethod
    def _eq_system_str(val) -> str | None:
        if val is None:
            return None
        mapping = {0: "Other", 1: "Topocentric", 2: "J2000", 3: "J2050", 4: "B1950"}
        return mapping.get(val, f"Unknown({val})")

    @sk.command_handler
    async def telescope_deinit(self, cmd: sk.Deinit):
        if self._home_task is not None:
            self._home_task.cancel()
            try:
                await self._home_task
            except asyncio.CancelledError:
                pass

        try:
            if self._can_set_tracking:
                await self.put(self.telescope, "Tracking", False)
                self._tracking = False
            if self._can_park:
                await self.telescope_park(sk.MoveToPark())
            await self.telescope_disconnect(sk.Disconnect())
        except Exception as e:
            logger.warning(f"Error during telescope deinit: {e}")

        # Stop telescope status publishing
        await self.stop_status_loop()

        # Disconnect from the hardware
        await self.telescope_disconnect(sk.Disconnect())

    @sk.command_handler
    async def telescope_connect(self, cmd: sk.Connect):
        await self.connect(self.telescope, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def telescope_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.telescope)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def telescope_home(self, cmd: sk.Home):
        self.require_connected()
        if not self._can_find_home:
            logger.warning("Cannot find home")
            return

        logger.debug("homing telescope")

        await self.call(self.telescope, "FindHome")
        await asyncio.sleep(self.config.status_frequency)

        async with asyncio.timeout(self.config.timeout):
            while self._slewing is None or self._slewing:
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed telescope")

    @sk.command_handler
    async def telescope_stop(self, cmd: sk.Stop):
        self.require_connected()

        logger.debug("stopping telescope")

        await self.call(self.telescope, "AbortSlew")
        if self._can_set_tracking:
            await self.put(self.telescope, "Tracking", False)
            self._tracking = False

        logger.debug("stopped telescope")

    @sk.command_handler
    async def telescope_park(self, cmd: sk.MoveToPark):
        self.require_connected()
        if not self._can_park:
            logger.warning("Cannot park")
            return

        logger.debug("parking telescope")

        await self.call(self.telescope, "Park")

        async with asyncio.timeout(self.config.timeout):
            while True:
                at_park = await self.get(self.telescope, "AtPark", False)
                if at_park:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("parked telescope")

    @sk.command_handler
    async def telescope_set_park(self, cmd: sk.SetParkPosition):
        self.require_connected()
        if not self._can_set_park:
            logger.warning("Cannot set park")
            return
        await self.call(self.telescope, "SetPark")

    def _resolve_tle_position_and_rates(self, tle_target: TLETarget):
        """Compute current ICRS position and RA/Dec offset rates from a TLE.

        Returns (ra_hours, dec_deg, ascom_ra_rate, ascom_dec_rate) where:
        - ascom_ra_rate: seconds of RA time per sidereal second (ASCOM RightAscensionRate)
        - ascom_dec_rate: arcseconds per second (ASCOM DeclinationRate)

        These are *offsets from sidereal tracking*, per the ASCOM spec.
        This is a best-effort snapshot — rates will drift over time.
        """
        from datetime import UTC, datetime, timedelta

        from skyfield.api import EarthSatellite, load, wgs84

        _UTC_TO_SIDEREAL = 1.00273791

        ts = load.timescale()
        sat = EarthSatellite(tle_target.tle.line1, tle_target.tle.line2, ts=ts)
        loc = wgs84.latlon(self._site_lat, self._site_lon, (self._site_elev or 0.0))

        now = datetime.now(UTC)
        dt = 1.0  # seconds (UTC)
        t0 = ts.from_datetime(now)
        t1 = ts.from_datetime(now + timedelta(seconds=dt))

        difference = sat - loc
        topo0 = difference.at(t0)
        topo1 = difference.at(t1)
        ra0, dec0, _ = topo0.radec()
        ra1, dec1, _ = topo1.radec()

        ra_hours = ra0.hours
        dec_deg = dec0.degrees

        # RA offset rate: RA seconds per sidereal second
        # apparent().radec() includes aberration, matching ICRS star catalogs
        # and WCS solutions. (ra1 - ra0)/dt is motion relative to the celestial
        # sphere (i.e., already the offset from sidereal).
        # Convert from per-UTC-second to per-sidereal-second.
        ra_rate_ascom = (ra1.hours - ra0.hours) * 3600.0 / dt * _UTC_TO_SIDEREAL

        # Dec offset rate: arcseconds per second (ASCOM convention)
        dec_rate_ascom = (dec1.degrees - dec0.degrees) / dt * 3600.0

        return ra_hours, dec_deg, ra_rate_ascom, dec_rate_ascom

    @sk.command_handler
    async def telescope_follow_target(self, cmd: sk.FollowTarget):
        if not self.device_connected:
            await self.telescope_init(sk.Init())
        from sensorkit.astro.common import ReferenceFrame

        target = cmd.target

        # Unpark if needed
        at_park = await self.get(self.telescope, "AtPark", False)
        if at_park and self._can_unpark:
            await self.call(self.telescope, "Unpark")

        match target:
            case ICRSTarget():
                ra_hours = target.coords.ra / 15.0
                dec_deg = target.coords.dec

                logger.debug(f"slewing to RA={ra_hours:.4f}h, Dec={dec_deg:.4f}°")

                # Tracking must be enabled before equatorial slews
                if self._can_set_tracking:
                    await self.put(self.telescope, "Tracking", True)
                    self._tracking = True

                if self._can_slew_async:
                    await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                elif self._can_slew:
                    await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self._slewing is None or self._slewing:
                        await asyncio.sleep(self.config.status_frequency)

                await self._publish_pointing()

            case AltAzTarget():
                alt_deg = target.coords.alt
                az_deg = target.coords.az

                logger.debug(f"slewing to Alt={alt_deg:.4f}°, Az={az_deg:.4f}°")

                if self._can_slew_altaz_async:
                    await self.call(self.telescope, "SlewToAltAzAsync", az_deg, alt_deg)
                elif self._can_slew_altaz:
                    await asyncio.to_thread(self.telescope.SlewToAltAz, az_deg, alt_deg)
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self._slewing is None or self._slewing:
                        await asyncio.sleep(self.config.status_frequency)

                await self._publish_pointing()

            case TLETarget():
                ra_hours, dec_deg, ra_rate, dec_rate = await asyncio.to_thread(
                    self._resolve_tle_position_and_rates, target
                )

                logger.debug(
                    f"slewing to TLE position RA={ra_hours:.4f}h, Dec={dec_deg:.4f}°, "
                    f"RA rate={ra_rate:.4f} s/s, Dec rate={dec_rate:.6f} °/s"
                )

                # Tracking must be enabled before slewing
                if self._can_set_tracking:
                    await self.put(self.telescope, "Tracking", True)
                    self._tracking = True

                if self._can_slew_async:
                    await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                elif self._can_slew:
                    await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self._slewing is None or self._slewing:
                        await asyncio.sleep(self.config.status_frequency)

                if self._can_set_right_ascension_rate:
                    await self.put(self.telescope, "RightAscensionRate", ra_rate)
                if self._can_set_declination_rate:
                    await self.put(self.telescope, "DeclinationRate", dec_rate)

                # Force immediate publish so subscribers see post-slew position and TLE rates
                await self._publish_pointing()
                await self._publish_rates()

            case FrameTarget():
                # Clear any non-sidereal offset rates
                if self._can_set_right_ascension_rate:
                    await self.put(self.telescope, "RightAscensionRate", 0.0)
                if self._can_set_declination_rate:
                    await self.put(self.telescope, "DeclinationRate", 0.0)

                if target.frame == ReferenceFrame.ALTAZ:
                    if self._can_set_tracking:
                        await self.put(self.telescope, "Tracking", False)
                        self._tracking = False
                        logger.debug("disabled tracking (ALTAZ frame)")
                else:
                    if self._can_set_tracking:
                        await self.put(self.telescope, "Tracking", True)
                        self._tracking = True
                        logger.debug("enabled sidereal tracking (ICRF frame)")

                # Force immediate rate publish so subscribers see cleared rates
                await self._publish_rates()

            case _:
                logger.warning(f"Unsupported target type: {type(target).__name__}")

        logger.debug("slewed to target")

    async def _publish_pointing(self) -> tuple[float, float, float, float]:
        """Read and publish current pointing."""

        t = self.telescope
        ra_hours = await self.get(t, "RightAscension", 0.0)
        dec_deg = await self.get(t, "Declination", 0.0)
        alt_deg = await self.get(t, "Altitude", 0.0)
        az_deg = await self.get(t, "Azimuth", 0.0)

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
        """Read Alpaca offset rates, compute total inertial rates, publish AxisRates."""

        _SIDEREAL_RATE_DEG_S = 15.04107 / 3600.0

        t = self.telescope

        # Read Alpaca offset rates
        alpaca_ra_rate = (
            await self.get(t, "RightAscensionRate", 0.0)
            if self._can_set_right_ascension_rate
            else 0.0
        )
        alpaca_dec_rate = (
            await self.get(t, "DeclinationRate", 0.0) if self._can_set_declination_rate else 0.0
        )

        # Convert to deg/s offsets
        ra_offset_deg_s = alpaca_ra_rate * 15.0 / 3600.0
        dec_offset_deg_s = alpaca_dec_rate / 3600.0

        # Total rates in deg/s (inertial frame)
        tracking = self._tracking or False
        total_ra_deg_s = (_SIDEREAL_RATE_DEG_S + ra_offset_deg_s) if tracking else 0.0
        total_dec_deg_s = dec_offset_deg_s if tracking else 0.0

        # Compute and publish alt/az rates if location is available
        if self._location is not None:
            if ra_hours is None:
                ra_hours = await self.get(t, "RightAscension", 0.0)
            if dec_deg is None:
                dec_deg = await self.get(t, "Declination", 0.0)

            alt_rate_deg_s, az_rate_deg_s = self.radec_rates_to_altaz_rates(
                ra_hr=ra_hours,
                dec_deg=dec_deg,
                ra_rate_deg_per_sec=total_ra_deg_s,
                dec_rate_deg_per_sec=total_dec_deg_s,
                location=self._location,
                time=Time.now(),
            )

            await sk.device().publish(
                AxisRates(
                    azimuth=AxisRate(axis=MountAxis.AZIMUTH, velocity=az_rate_deg_s),
                    altitude=AxisRate(axis=MountAxis.ALTITUDE, velocity=alt_rate_deg_s),
                    right_ascension=AxisRate(
                        axis=MountAxis.RIGHT_ASCENSION, velocity=total_ra_deg_s
                    ),
                    declination=AxisRate(axis=MountAxis.DECLINATION, velocity=total_dec_deg_s),
                )
            )

        return alpaca_ra_rate, alpaca_dec_rate

    async def status_publish(self):
        while True:
            try:
                t = self.telescope

                connected = await self.get(t, "Connected", False)
                self.device_connected = connected

                if not connected:
                    await sk.device().publish(Connected(is_connected=False))
                    await asyncio.sleep(self.config.status_frequency)
                    continue

                self._slewing = await self.get(t, "Slewing", False)
                self._tracking = await self.get(t, "Tracking", False)

                tracking_rate = await self.get(t, "TrackingRate", None)

                # Pier side
                side_of_pier = await self.get(t, "SideOfPier", -1)
                side_of_pier = _PIER_SIDES.get(side_of_pier, "Unknown")

                # Sidereal time
                sidereal_time = await self.get(t, "SiderealTime", None)

                device = sk.device()
                await device.publish(Connected(is_connected=True))

                # Publish position and rates
                (
                    right_ascension,
                    declination,
                    altitude,
                    azimuth,
                ) = await self._publish_pointing()
                right_ascension_rate, declination_rate = await self._publish_rates(
                    ra_hours=right_ascension, dec_deg=declination
                )

                # Full ITelescopeV4 status — only include supported properties
                properties: dict = {
                    "tracking": self._tracking or False,
                    "right_ascension_rate": right_ascension_rate,
                    "declination_rate": declination_rate,
                    "slewing": self._slewing or False,
                    "at_home": await self.get(t, "AtHome", False),
                    "at_park": await self.get(t, "AtPark", False),
                }

                if tracking_rate is not None:
                    properties["tracking_rate"] = tracking_rate
                if side_of_pier != "Unknown":
                    properties["side_of_pier"] = side_of_pier
                if sidereal_time is not None:
                    properties["sidereal_time"] = sidereal_time

                # Static optics (only include if available)
                for attr, key in (
                    ("_aperture_diameter", "aperture_diameter"),
                    ("_aperture_area", "aperture_area"),
                    ("_focal_length", "focal_length"),
                    ("_equatorial_system", "equatorial_system"),
                    ("_alignment_mode", "alignment_mode"),
                    ("_does_refraction", "does_refraction"),
                ):
                    val = getattr(self, attr, None)
                    if val is not None:
                        properties[key] = val

                properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                logger.debug(
                    f"Alpaca telescope status: connected={connected}, slewing={self._slewing}, "
                    f"tracking={self._tracking}, RA={right_ascension}, Dec={declination}, "
                    f"RA_rate={right_ascension_rate}, Dec_rate={declination_rate}, "
                    f"alt={altitude}, az={azimuth}, "
                    f"{properties_str}"
                )

                await device.publish(AlpacaTelescopeStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in telescope status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)

    def radec_rates_to_altaz_rates(
        self,
        ra_hr: float,
        dec_deg: float,
        ra_rate_deg_per_sec: float,
        dec_rate_deg_per_sec: float,
        location: EarthLocation,
        time: Time,
    ) -> tuple[float, float]:
        """
        Convert RA/Dec rates to Alt/Az rates.

        Parameters
        ----------
        ra_hr : float
            Right ascension in hours.
        dec_deg : float
            Declination in degrees.
        ra_rate_deg_per_sec : float
            RA rate in degrees per second.
        dec_rate_deg_per_sec : float
            Dec rate in degrees per second.
        location : EarthLocation
            Observer location.
        time : Time
            Observation time.

        Returns
        -------
        tuple[float, float]
            (alt_rate, az_rate) all in degrees per second.
        """
        # Convert RA from hours to degrees
        ra_deg = ra_hr * 15.0

        # Create the AltAz frame
        altaz_frame = AltAz(obstime=time, location=location)

        # Create coordinate at current RA/Dec position
        coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame=ICRS())

        # Small time step for numerical differentiation
        dt = 0.01  # seconds

        # Calculate new RA/Dec position after dt
        new_ra = ra_deg + ra_rate_deg_per_sec * dt
        new_dec = dec_deg + dec_rate_deg_per_sec * dt

        # Create coordinate at new position
        new_coord = SkyCoord(ra=new_ra * u.deg, dec=new_dec * u.deg, frame=ICRS())

        # Transform both to AltAz
        altaz = coord.transform_to(altaz_frame)
        new_altaz = new_coord.transform_to(altaz_frame)

        # Calculate alt/az rates in deg/sec
        alt_rate_deg_per_sec = (new_altaz.alt.deg - altaz.alt.deg) / dt
        az_rate_deg_per_sec = (new_altaz.az.deg - altaz.az.deg) / dt

        return alt_rate_deg_per_sec, az_rate_deg_per_sec


class AlpacaTelescopeConfig(AlpacaDeviceConfig[AlpacaTelescope]):
    device_type: Literal["telescope"] = "telescope"
    timeout: float = 300.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return AlpacaTelescope(self)


class AlpacaTelescopeState(AlpacaDeviceState):
    device_type: Literal["telescope"] = "telescope"
    has_been_homed: bool = False
