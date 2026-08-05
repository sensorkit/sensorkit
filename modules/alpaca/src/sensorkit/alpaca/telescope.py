# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

import astropy.units as u
from astropy.coordinates import ICRS, AltAz, EarthLocation, SkyCoord
from astropy.time import Time
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
from sensorkit.std import (
    AxisRate,
    AxisRates,
    Connect,
    Connected,
    Deinit,
    Disconnect,
    FollowTarget,
    Home,
    Init,
    MountAxis,
    MoveToPark,
    SetParkPosition,
    Slewing,
    Stop,
    Tracking,
)

# Prevent IERS-A (Earth orientation parameters) download
iers.conf.auto_download = False
iers.conf.auto_max_age = None


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
_EQUATORIAL_SYSTEMS = {0: "Other", 1: "Topocentric", 2: "J2000", 3: "J2050", 4: "B1950"}

# Max time to wait for a commanded slew to *start* (Slewing -> True) before
# treating the command as a positional no-op. See _wait_for_telescope.
_TELESCOPE_ONSET_TIMEOUT = 2.0

# Closed-loop TLE re-acquisition: after the initial slew the target has moved on
# (the mount settled behind it), so re-resolve and correct until the residual is
# small. One pass suffices for slow/GEO targets; faster ones converge in a few.
_TLE_REACQUIRE_MAX_PASSES = 3
_TLE_REACQUIRE_TOL_ARCSEC = 5.0


def radec_rates_to_altaz_rates(
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

    # Transform both to AltAz (new position at an obstime advanced by dt)
    altaz = coord.transform_to(altaz_frame)
    new_altaz = new_coord.transform_to(AltAz(obstime=time + dt * u.s, location=location))

    # Calculate alt/az rates in deg/sec
    alt_rate_deg_per_sec = (new_altaz.alt.deg - altaz.alt.deg) / dt
    az_rate_deg_per_sec = (new_altaz.az.deg - altaz.az.deg) / dt

    return alt_rate_deg_per_sec, az_rate_deg_per_sec


class AlpacaTelescopeState(AlpacaDeviceState):
    device_type: Literal["telescope"] = "telescope"
    has_been_homed: bool = False


@sk.declare_device
class AlpacaTelescope(AlpacaDevice):
    """Alpaca Telescope implementation."""

    config: AlpacaTelescopeConfig
    device_name = "Telescope"
    state_model = AlpacaTelescopeState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        self._tracking: bool | None = None
        self._slewing: bool | None = None
        self._fast_status_task: asyncio.Task | None = None
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None

        # Initialize capabilities
        self._can_slew = self._can_slew_async = False
        self._can_slew_altaz = self._can_slew_altaz_async = False
        self._can_park = self._can_unpark = self._can_find_home = False
        self._can_set_tracking = self._can_set_park = self._can_pulse_guide = False
        self._can_set_right_ascension_rate = self._can_set_declination_rate = False
        self._can_set_guide_rates = self._can_set_pier_side = False
        self._can_sync = self._can_sync_altaz = False
        self._can_move_axis = [False, False, False]

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency_slow)
        await self.stop_status_loop()
        await self.telescope_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def telescope_init(self, cmd: Init):
        # Connect to the hardware
        self._reconnect = lambda: self.telescope_connect(Connect())
        self.telescope = Telescope(self.address, self.config.device_number, self.config.protocol)
        await self.telescope_connect(Connect())

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
        self._can_move_axis = []
        for axis in range(3):
            try:
                can = await asyncio.to_thread(self.telescope.CanMoveAxis, axis)
            except Exception:
                can = False
            self._can_move_axis.append(can)

        # Normalize the mount to a clean state on connect
        if self._can_set_right_ascension_rate:
            await self.put(t, "RightAscensionRate", 0.0)
        if self._can_set_declination_rate:
            await self.put(t, "DeclinationRate", 0.0)
        if self._can_set_tracking:
            await self.put(t, "Tracking", False)
            self._tracking = False

        # Read static telescope properties
        self._aperture_diameter = await self.get(t, "ApertureDiameter", None)
        self._aperture_area = await self.get(t, "ApertureArea", None)
        self._focal_length = await self.get(t, "FocalLength", None)
        eq_sys = await self.get(t, "EquatorialSystem", None)
        self._equatorial_system = (
            None if eq_sys is None else _EQUATORIAL_SYSTEMS.get(eq_sys, f"Unknown({eq_sys})")
        )
        self._alignment_mode = _ALIGNMENT_MODES.get(await self.get(t, "AlignmentMode", -1))
        self._does_refraction = await self.get(t, "DoesRefraction", None)

        # Site location
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

        # Read available tracking rates
        self._tracking_rates = await self.get(t, "TrackingRates", [])

        self.start_status_loop(self.status_publish_slow())

        # Home, as needed
        if not self.state.has_been_homed:
            await self.telescope_home(Home())

    @sk.command_handler
    async def telescope_deinit(self, cmd: Deinit):
        if not self.device_connected:
            # Init may not have run, but telescope may still be tracking from a failed run.
            # Connect so we can ensure it's parked.
            try:
                await self.telescope_connect(Connect())
            except Exception:
                logger.warning("Unable to connect telescope for Deinit park; skipping")
                return

        self._stop_fast_status()
        await self.telescope_stop(Stop())

        if self._can_set_tracking:
            await self.put(self.telescope, "Tracking", False)
            self._tracking = False
        if self._can_park:
            await self.telescope_park(MoveToPark())

    @sk.command_handler
    async def telescope_connect(self, cmd: Connect):
        await self.connect(self.telescope, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def telescope_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.telescope)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def telescope_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping telescope")

        await self.call(self.telescope, "AbortSlew")
        if self._can_set_tracking:
            await self.put(self.telescope, "Tracking", False)
            self._tracking = False

        await self._wait_for_telescope(await_onset=False)

        self._stop_fast_status()
        logger.debug("stopped telescope")

    @sk.command_handler
    async def telescope_home(self, cmd: Home):
        await self.require_connected()
        if not self._can_find_home:
            logger.warning("Cannot find home")
            return

        logger.debug("homing telescope")

        await self.call(self.telescope, "FindHome")
        await asyncio.sleep(self.config.status_frequency_slow)

        async with asyncio.timeout(self.config.timeout):
            while True:
                at_home = await self.get(self.telescope, "AtHome", False)
                if at_home:
                    break
                await asyncio.sleep(0.2)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed telescope")

    @sk.command_handler
    async def telescope_park(self, cmd: MoveToPark):
        await self.require_connected()
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
                await asyncio.sleep(0.2)

        logger.debug("parked telescope")

    @sk.command_handler
    async def telescope_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        if not self._can_set_park:
            logger.warning("Cannot set park")
            return
        logger.debug("setting park position")
        await self.call(self.telescope, "SetPark")
        logger.debug("set park position")

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
        # Convert from per-UTC-second to per-sidereal-second
        ra_rate_ascom = (ra1.hours - ra0.hours) * 3600.0 / dt * _UTC_TO_SIDEREAL

        # Dec offset rate: arcseconds per second
        dec_rate_ascom = (dec1.degrees - dec0.degrees) / dt * 3600.0

        return ra_hours, dec_deg, ra_rate_ascom, dec_rate_ascom

    @sk.command_handler
    async def telescope_follow_target(self, cmd: FollowTarget):
        await self.require_connected()

        # ASCOM/Alpaca has no path-follow primitive, only slew + constant offset
        # rate, so we accept an ICRF EphemerisTarget and collapse it to a
        # position-plus-rate below. Propagatable inputs (TLE, state vector) are
        # normalized to that ephemeris by adapt(); a direct TLE still passes
        # through to the native skyfield handler.
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

        # Unpark if needed
        at_park = await self.get(self.telescope, "AtPark", False)
        if at_park and self._can_unpark:
            await self.call(self.telescope, "Unpark")

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                ra_hours = target.coords.ra / 15.0
                dec_deg = target.coords.dec

                if self._can_set_right_ascension_rate:
                    await self.put(self.telescope, "RightAscensionRate", 0.0)
                if self._can_set_declination_rate:
                    await self.put(self.telescope, "DeclinationRate", 0.0)

                # Tracking must be enabled before equatorial slews
                if self._can_set_tracking:
                    await self.put(self.telescope, "Tracking", True)
                    self._tracking = True

                if self._can_slew_async:
                    await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                elif self._can_slew:
                    await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)

                await self._wait_for_telescope(tracking=True)
                self._start_fast_status()

                logger.debug("following RADec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                alt_deg = target.coords.alt
                az_deg = target.coords.az

                if self._can_slew_altaz_async:
                    await self.call(self.telescope, "SlewToAltAzAsync", az_deg, alt_deg)
                elif self._can_slew_altaz:
                    await asyncio.to_thread(self.telescope.SlewToAltAz, az_deg, alt_deg)

                await self._wait_for_telescope()
                self._start_fast_status()

                logger.debug("following AltAz target")

            case TLETarget():
                # FIXME: Leaving TLETarget for now because it has been tested against sdasim.
                # Once EphemerisTarget has been tested, TLETarget can be adapted to it.
                logger.debug("executing TLE follow")

                ra_hours, dec_deg, ra_rate, dec_rate = await asyncio.to_thread(
                    self._resolve_tle_position_and_rates, target
                )

                # Tracking must be enabled before slewing
                if self._can_set_tracking:
                    await self.put(self.telescope, "Tracking", True)
                    self._tracking = True

                if self._can_slew_async:
                    await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                elif self._can_slew:
                    await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)
                await self._wait_for_telescope(tracking=True)

                # Close the loop: the target moved during the slew, so the mount
                # settled behind it. Re-resolve at the now-current time and slew to
                # correct until the residual is small (one pass suffices for a
                # slow/GEO target; faster ones converge in a few). The offset rate
                # then only has to hold position, not also recover the slew lag.
                for _ in range(_TLE_REACQUIRE_MAX_PASSES):
                    ra_hours, dec_deg, ra_rate, dec_rate = await asyncio.to_thread(
                        self._resolve_tle_position_and_rates, target
                    )
                    cur_ra = await self.get(self.telescope, "RightAscension", ra_hours)
                    cur_dec = await self.get(self.telescope, "Declination", dec_deg)
                    sep = SkyCoord(ra=cur_ra * 15.0 * u.deg, dec=cur_dec * u.deg).separation(
                        SkyCoord(ra=ra_hours * 15.0 * u.deg, dec=dec_deg * u.deg)
                    )
                    if sep.arcsec <= _TLE_REACQUIRE_TOL_ARCSEC:
                        break
                    if self._can_slew_async:
                        await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                    elif self._can_slew:
                        await asyncio.to_thread(
                            self.telescope.SlewToCoordinates, ra_hours, dec_deg
                        )
                    await self._wait_for_telescope(tracking=True)

                if self._can_set_right_ascension_rate:
                    await self.put(self.telescope, "RightAscensionRate", ra_rate)
                if self._can_set_declination_rate:
                    await self.put(self.telescope, "DeclinationRate", dec_rate)
                self._start_fast_status()

                logger.debug("following TLE target")

            case RateTarget():
                logger.debug("executing Rate follow")

                ra_hours = target.initial_coords.ra / 15.0
                dec_deg = target.initial_coords.dec

                # Tracking must be enabled before slewing
                if self._can_set_tracking:
                    await self.put(self.telescope, "Tracking", True)
                    self._tracking = True

                if self._can_slew_async:
                    await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                elif self._can_slew:
                    await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)

                await self._wait_for_telescope(tracking=True)

                # Apply offset rates
                # ASCOM RightAscensionRate: seconds of RA per sidereal second
                # ASCOM DeclinationRate: arcseconds per second
                _UTC_TO_SIDEREAL = 1.00273791
                if self._can_set_right_ascension_rate:
                    ra_rate = target.rates.ra / 15.0 * 3600.0 * _UTC_TO_SIDEREAL
                    await self.put(self.telescope, "RightAscensionRate", ra_rate)
                if self._can_set_declination_rate:
                    dec_rate = target.rates.dec * 3600.0
                    await self.put(self.telescope, "DeclinationRate", dec_rate)
                self._start_fast_status()

                logger.debug("following Rate target")

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow")

                # No path-follow primitive in ASCOM, so collapse the precomputed
                # path to an initial position plus a constant offset rate: slew to
                # the sample nearest now and finite-difference the two adjacent
                # samples for the instantaneous RA/Dec rate. Like the TLE case,
                # this is a best-effort snapshot that drifts over time.
                jds = target.jds
                points = target.points
                now_jd = Time.now().jd
                i = min(range(len(jds) - 1), key=lambda j: abs(jds[j] - now_jd))

                ra_hours = points[i].ra / 15.0
                dec_deg = points[i].dec

                dt_sec = (jds[i + 1] - jds[i]) * 86400.0
                # Wrap the RA difference into [-180, 180] deg to survive 0/360 crossings.
                dra_deg = (points[i + 1].ra - points[i].ra + 180.0) % 360.0 - 180.0
                ddec_deg = points[i + 1].dec - points[i].dec

                _UTC_TO_SIDEREAL = 1.00273791
                ra_rate = dra_deg / dt_sec / 15.0 * 3600.0 * _UTC_TO_SIDEREAL
                dec_rate = ddec_deg / dt_sec * 3600.0

                # Tracking must be enabled before slewing
                if self._can_set_tracking:
                    await self.put(self.telescope, "Tracking", True)
                    self._tracking = True

                if self._can_slew_async:
                    await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
                elif self._can_slew:
                    await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)
                await self._wait_for_telescope(tracking=True)

                if self._can_set_right_ascension_rate:
                    await self.put(self.telescope, "RightAscensionRate", ra_rate)
                if self._can_set_declination_rate:
                    await self.put(self.telescope, "DeclinationRate", dec_rate)
                self._start_fast_status()

                logger.debug("following Ephemeris target")

            case FrameTarget():
                # Clear any non-sidereal offset rates
                if self._can_set_right_ascension_rate:
                    await self.put(self.telescope, "RightAscensionRate", 0.0)
                if self._can_set_declination_rate:
                    await self.put(self.telescope, "DeclinationRate", 0.0)

                if target.frame == ReferenceFrame.ALTAZ:
                    self._stop_fast_status()
                    if self._can_set_tracking:
                        logger.debug("disabling tracking")
                        await self.put(self.telescope, "Tracking", False)
                        self._tracking = False
                        logger.debug("disabled tracking")
                else:
                    if self._can_set_tracking:
                        logger.debug("enabling sidereal tracking")
                        await self.put(self.telescope, "Tracking", True)
                        self._tracking = True
                        self._start_fast_status()
                        logger.debug("enabled sidereal tracking")

            case _:
                track_type = type(cmd.target).__name__
                raise NotImplementedError(f"{track_type} tracking via Alpaca is not supported")

        try:
            await self._publish_telescope_status()
        except Exception as e:
            logger.warning(f"Immediate telescope status publish failed: {e}")

    async def _wait_for_telescope(
        self,
        *,
        slewing: bool = False,
        tracking: bool = False,
        await_onset: bool = True,
    ):
        """Poll the telescope until Slewing and Tracking both match.

        When `await_onset` (the default, for commands that slew), first wait
        briefly for the mount to *start* slewing. Without this, a command whose
        target flags already equal the current flags (e.g. re-following while
        already tracking) would match the stale pre-command state and return
        before motion begins. If no slew is observed within
        `_TELESCOPE_ONSET_TIMEOUT`, the command was a positional no-op and we
        fall through to the settle check (this replaces the old manual `sleep(1)`
        after each slew).

        Non-slewing commands (stop, enable/disable tracking) must pass
        `await_onset=False`: they never raise Slewing, so onset would otherwise
        burn the full onset timeout on every call.

        The settle wait is bounded by config.timeout; both phases poll every 0.1 s.
        """

        if await_onset:
            try:
                async with asyncio.timeout(_TELESCOPE_ONSET_TIMEOUT):
                    while True:
                        if await self.get(self.telescope, "Slewing", False):
                            break
                        await asyncio.sleep(0.1)
            except TimeoutError:
                # No slew observed -> positional no-op. Fall through to the settle
                # check, which returns at once if already on target.
                pass

        async with asyncio.timeout(self.config.timeout):
            while True:
                is_slewing = await self.get(self.telescope, "Slewing", False)
                is_tracking = await self.get(self.telescope, "Tracking", False)
                if is_slewing == slewing and is_tracking == tracking:
                    break
                await asyncio.sleep(0.1)

    def _start_fast_status(self):
        if self._fast_status_task is None or self._fast_status_task.done():
            logger.debug("starting fast telescope status loop")
            self._fast_status_task = asyncio.create_task(self.status_publish_fast())

    def _stop_fast_status(self):
        if self._fast_status_task is not None and not self._fast_status_task.done():
            logger.debug("stopping fast telescope status loop")
            self._fast_status_task.cancel()
            self._fast_status_task = None

    @property
    def _fast_status_active(self) -> bool:
        return self._fast_status_task is not None and not self._fast_status_task.done()

    async def _publish_telescope_status(self):
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

        tracking = self._tracking or False
        icrf_ra_deg_s = ra_offset_deg_s if tracking else 0.0
        icrf_dec_deg_s = dec_offset_deg_s if tracking else 0.0

        # Compute and publish alt/az rates if location is available
        if self._location is not None:
            if tracking:
                alt_rate_deg_s, az_rate_deg_s = radec_rates_to_altaz_rates(
                    ra_hr=ra_hours,
                    dec_deg=dec_deg,
                    ra_rate_deg_per_sec=icrf_ra_deg_s,
                    dec_rate_deg_per_sec=icrf_dec_deg_s,
                    location=self._location,
                    time=Time.now(),
                )
            else:
                alt_rate_deg_s = az_rate_deg_s = 0.0

            await device.publish(
                AxisRates(
                    azimuth=AxisRate(axis=MountAxis.AZIMUTH, velocity=az_rate_deg_s),
                    altitude=AxisRate(axis=MountAxis.ALTITUDE, velocity=alt_rate_deg_s),
                    right_ascension=AxisRate(
                        axis=MountAxis.RIGHT_ASCENSION, velocity=icrf_ra_deg_s
                    ),
                    declination=AxisRate(axis=MountAxis.DECLINATION, velocity=icrf_dec_deg_s),
                )
            )

    async def status_publish_slow(self):
        while True:
            try:
                t = self.telescope

                connected = await self.get(t, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if not connected:
                    await asyncio.sleep(self.config.status_frequency_slow)
                    continue

                self._slewing = await self.get(t, "Slewing", False)
                self._tracking = await self.get(t, "Tracking", False)

                await device.publish(Slewing(is_slewing=self._slewing))
                await device.publish(Tracking(is_tracking=self._tracking))

                # Only publish pointing/rates if fast loop isn't handling it
                if not self._fast_status_active:
                    await self._publish_telescope_status()

                tracking_rate = await self.get(t, "TrackingRate", None)

                # Pier side
                side_of_pier = await self.get(t, "SideOfPier", -1)
                side_of_pier = _PIER_SIDES.get(side_of_pier, "Unknown")

                # Sidereal time
                sidereal_time = await self.get(t, "SiderealTime", None)

                # Full ITelescopeV4 status — only include supported properties
                properties: dict = {
                    "tracking": self._tracking or False,
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

                await device.publish(AlpacaTelescopeStatus(**properties))

                # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                # logger.debug(
                #     f"Alpaca telescope status: connected={connected}, {properties_str}"
                # )
            except Exception as e:
                logger.exception(f"Error in slow telescope status publish: {e}")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            await asyncio.sleep(self.config.status_frequency_slow)

    async def status_publish_fast(self):
        while True:
            try:
                await self._publish_telescope_status()
            except Exception as e:
                logger.warning(f"Error in fast telescope status publish ({e})")
                await asyncio.sleep(self.config.status_frequency_fast)
                continue

            await asyncio.sleep(self.config.status_frequency_fast)


class AlpacaTelescopeConfig(AlpacaDeviceConfig[AlpacaTelescope]):
    device_type: Literal["telescope"] = "telescope"
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1
    timeout: float = 300.0

    @override
    def create_device(self):
        return AlpacaTelescope(self)
