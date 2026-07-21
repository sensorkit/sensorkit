# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew mount (ASCOM Alpaca Telescope + ASA extensions).

Subclasses `sensorkit.alpaca`'s `AlpacaTelescope`, inheriting connect/disconnect,
home/park, and the slew-settle poll unchanged. The parts that differ are overridden
wholesale (re-decorated so they hook back into SensorKit — see
`sensorkit.api.declarative`):

  * **Init/stop/follow.** Autoslew needs its own capability probe (no RA/Dec-rate or
    pulse-guide flags to cache), a motors-on + stale-track cleanup on init, and an
    `AbortSlew` + ``sat:stop`` on stop.

  * **Frames.** Autoslew's ASCOM interface is JNow / topocentric-of-date on *both*
    read and command, regardless of its "Used Ascom Epoch" setting (verified live).
    SensorKit standardizes on ICRS, so we convert at exactly two boundaries: JNow ->
    ICRS on status out, ICRS -> JNow on goto in. The ``jnow_to_icrs`` / ``icrs_to_jnow``
    helpers below are module-level (like pwi4's ``altaz_rates_to_radec_rates``).

  * **Native TLE following.** Autoslew runs the satellite ephemeris on the controller,
    so a ``TLETarget`` maps straight onto the ``sat:*`` action flow rather than the
    slew+offset-rate dance the generic Alpaca path uses. Motors must be ON first
    (``sat:start`` returns 0x40B "Motors are not on" otherwise), and ``motoron`` is
    asynchronous — poll ``MotStat`` until it engages.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime, timedelta
from typing import override

import astropy.units as u
from alpaca.telescope import Telescope
from astropy.coordinates import ICRS, TETE, AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from astropy.utils import iers
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.telescope import AlpacaTelescope, AlpacaTelescopeConfig, AlpacaTelescopeState
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
from sensorkit.autoslew.device import AutoslewMixin, _num, _pick
from sensorkit.std import (
    AxisRate,
    AxisRates,
    Connect,
    Connected,
    Deinit,
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

# Prevent IERS-A (Earth orientation) download, which can stall long enough to
# trip a lease expiry. Sub-arcsecond frame accuracy is irrelevant to pointing.
iers.conf.auto_download = False
iers.conf.auto_max_age = None

# Max time to wait for sat:start to engage tracking (or at least begin the
# acquisition slew) before handing off to the fast status loop.
_SAT_ACQUIRE_TIMEOUT = 8.0


# --------------------------------------------------------------------------- #
# Frame conversion — JNow (apparent, of-date) <-> ICRS.
# TETE = true-equator/true-equinox-of-date, astropy's apparent-place frame (~JNow).
# `location` gives the topocentric term; negligible for stars, passed for rigor.
# --------------------------------------------------------------------------- #
def jnow_to_icrs(
    ra_deg: float, dec_deg: float, obstime: Time, location: EarthLocation | None = None
) -> tuple[float, float]:
    coord = SkyCoord(
        ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame=TETE(obstime=obstime, location=location)
    )
    icrs = coord.transform_to(ICRS())
    return icrs.ra.deg, icrs.dec.deg


def icrs_to_jnow(
    ra_deg: float, dec_deg: float, obstime: Time, location: EarthLocation | None = None
) -> tuple[float, float]:
    coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame=ICRS())
    jnow = coord.transform_to(TETE(obstime=obstime, location=location))
    return jnow.ra.deg, jnow.dec.deg


def _icrf_rate_from_samples(jds, points) -> tuple[float, float]:
    """Finite-difference an ICRF ephemeris into an (RA, Dec) rate in deg/s.

    RA differences are wrapped the shortest way around, so a sample pair straddling
    the RA=0 seam doesn't produce a ~360 deg/s spike.
    """
    if len(points) < 2:
        return 0.0, 0.0
    dt = (jds[1] - jds[0]) * 86400.0
    if dt <= 0:
        return 0.0, 0.0
    dra = (((points[1].ra - points[0].ra) + 180.0) % 360.0) - 180.0
    return dra / dt, (points[1].dec - points[0].dec) / dt


def radec_rates_to_altaz_rates(
    ra_deg: float,
    dec_deg: float,
    ra_rate_deg_s: float,
    dec_rate_deg_s: float,
    location: EarthLocation,
    time: Time,
) -> tuple[float, float]:
    """Convert ICRS RA/Dec rates (deg/s) to Alt/Az rates (deg/s) by numerical difference."""
    dt = 0.01
    now = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame=ICRS()).transform_to(
        AltAz(obstime=time, location=location)
    )
    later = SkyCoord(
        ra=(ra_deg + ra_rate_deg_s * dt) * u.deg,
        dec=(dec_deg + dec_rate_deg_s * dt) * u.deg,
        frame=ICRS(),
    ).transform_to(AltAz(obstime=time + dt * u.s, location=location))
    return (later.alt.deg - now.alt.deg) / dt, (later.az.deg - now.az.deg) / dt


@sk.declare_keyword
class SatTrackError(BaseModel):
    """ASA satellite tracking status/quality, from getSatStatus."""

    tracking: bool = False
    sunlit: bool = False  # not valid for EPH-driven passes
    track_error_ax1_millirad: float = 0.0
    track_error_ax2_millirad: float = 0.0


@sk.declare_device
class AutoslewTelescope(AutoslewMixin, AlpacaTelescope):
    """ASA Autoslew mount implementation.

    Inherits `entity_deinit`/`telescope_connect`/`telescope_disconnect`/
    `telescope_home`/`telescope_park`/`_wait_for_telescope`/`status_publish_fast`/
    the fast-status task helpers from `AlpacaTelescope` unchanged — Autoslew's
    capability probe still populates the `_can_find_home`/`_can_park` flags those
    rely on. Everything else is overridden below for the ASA-specific
    init/stop/follow/status behavior.
    """

    config: AutoslewTelescopeConfig
    device_name = "Telescope"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(AutoslewTelescopeState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AutoslewTelescopeState()

        self._tracking: bool | None = None
        self._slewing: bool | None = None
        self._sidereal = False
        # Inertial (ICRF) RA/Dec rate of whatever we're following, deg/s. Zero while
        # sidereal/fixed; published as AxisRates so FITS RA_RATE stays inertial.
        self._icrf_rate: tuple[float, float] = (0.0, 0.0)
        # Set while following a TLE so the status loop can refresh the rate mid-pass.
        self._tle_target: TLETarget | None = None
        self._fast_status_task: asyncio.Task | None = None
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None
        self._can_slew_async = self._can_slew = False
        self._can_slew_altaz_async = self._can_slew_altaz = False
        self._can_park = self._can_unpark = self._can_find_home = False

    @sk.command_handler
    async def telescope_init(self, cmd: Init):
        self._reconnect = lambda: self.telescope_connect(Connect())
        self.telescope = Telescope(self.address, self.config.device_number, self.config.protocol)
        await self.telescope_connect(Connect())

        t = self.telescope
        self._can_slew = await self.get(t, "CanSlew", False)
        self._can_slew_async = await self.get(t, "CanSlewAsync", False)
        self._can_slew_altaz = await self.get(t, "CanSlewAltAz", False)
        self._can_slew_altaz_async = await self.get(t, "CanSlewAltAzAsync", False)
        self._can_park = await self.get(t, "CanPark", False)
        self._can_unpark = await self.get(t, "CanUnpark", False)
        self._can_find_home = await self.get(t, "CanFindHome", False)

        # Clean start: abort any in-flight slew AND clear a stale satellite track left
        # by a prior/crashed session, then engage the motors (required before any slew
        # or sat:start). The abort matters: a TLE follow leaves the mount slewing to
        # acquire, and inheriting that state wedges the next goto into a crawl.
        with contextlib.suppress(Exception):
            await self.call(t, "AbortSlew")
        with contextlib.suppress(Exception):
            await self.action("sat:stop")
        await self._ensure_motors_on()

        # Site — used as the observer for adapt() propagation and the topocentric
        # term of the frame conversion. Autoslew's SiteLongitude is used verbatim so
        # our transforms stay consistent with the driver's own AltAz computation.
        self._site_lat = await self.get(t, "SiteLatitude", None)
        self._site_lon = await self.get(t, "SiteLongitude", None)
        self._site_elev = await self.get(t, "SiteElevation", None)
        if self._site_lat is not None and self._site_lon is not None:
            self._geodetic = Geodetic(lon=self._site_lon, lat=self._site_lat, elev=self._site_elev)
            self._location = EarthLocation(
                lat=self._site_lat * u.deg,
                lon=self._site_lon * u.deg,
                height=(self._site_elev or 0.0) * u.m,
            )
            await sk.device().publish(
                SitePosition(
                    latitude_degrees=self._site_lat,
                    longitude_degrees=self._site_lon,
                    altitude_km=(self._site_elev or 0.0) / 1000.0,
                )
            )
            logger.debug(f"site: lat={self._site_lat} lon={self._site_lon} elev={self._site_elev}")

        self.start_status_loop(self.status_publish_slow())

        if self._can_find_home and not self.state.has_been_homed:
            await self.telescope_home(Home())

    @sk.command_handler
    async def telescope_deinit(self, cmd: Deinit):
        if not self.device_connected:
            try:
                await self.telescope_connect(Connect())
            except Exception:
                logger.warning("Unable to connect mount for Deinit park; skipping")
                return

        self._stop_fast_status()
        await self.telescope_stop(Stop())
        if self._can_park:
            await self.telescope_park(MoveToPark())

    @sk.command_handler
    async def telescope_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping mount")
        with contextlib.suppress(Exception):
            await self.action("sat:stop")
        await self.call(self.telescope, "AbortSlew")
        await self.put(self.telescope, "Tracking", False)
        self._tracking = False
        self._sidereal = False
        await self._wait_for_telescope(await_onset=False)
        self._stop_fast_status()
        logger.debug("stopped mount")

    @sk.command_handler
    async def telescope_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        if not await self.get(self.telescope, "CanSetPark", False):
            logger.warning("Cannot set park")
            return
        await self.call(self.telescope, "SetPark")

    # ---- ASA mount helpers ------------------------------------------------ #
    async def _ensure_motors_on(self):
        """Engage the motors and wait for MotStat — motoron is asynchronous."""
        await self.action("telescope:motoron")
        async with asyncio.timeout(self.config.timeout):
            while True:
                if await self.command_bool("MotStat"):
                    return
                await asyncio.sleep(0.5)

    async def _sat_tracking(self) -> bool:
        """The getSatStatus tracking bit (b0). Key is lowercase 'status' on the wire."""
        try:
            d = json.loads(await self.command_string("getSatStatus"))
        except Exception:
            return False
        return bool(int(_pick(d, "Status", default=0)) & 0b01)

    async def _tle_icrf_rate(self, tle_target: TLETarget) -> tuple[float, float]:
        """Instantaneous inertial (topocentric ICRF) RA/Dec rate of a TLE, in deg/s.

        Autoslew flies the pass itself, but SensorKit still needs the target's inertial
        rate for AxisRates (and therefore FITS RA_RATE). Reuses core's propagation over a
        minimal two-sample window rather than hand-rolling SGP4 and frame handling.
        """
        if self._geodetic is None:
            return 0.0, 0.0
        try:
            eph = await tle_target.to_ephemeris_target(
                start_time=datetime.now(UTC),
                duration=timedelta(seconds=2),
                step=timedelta(seconds=1),
                frame=ReferenceFrame.ICRF,
                observer=self._geodetic,
            )
        except Exception as e:
            logger.warning(f"TLE inertial-rate computation failed: {e}")
            return 0.0, 0.0
        return _icrf_rate_from_samples(eph.jds, eph.points)

    # ---- follow / tracking ------------------------------------------------ #
    @sk.command_handler
    async def telescope_follow_target(self, cmd: FollowTarget):  # noqa: C901
        await self.require_connected()

        target = await cmd.target.adapt(
            ICRSTarget,
            AltAzTarget,
            (FrameTarget, ReferenceFrame.ICRF),
            (FrameTarget, ReferenceFrame.ALTAZ),
            TLETarget,  # native sat:* follow
            (RateTarget, ReferenceFrame.ICRF),
            (EphemerisTarget, ReferenceFrame.ICRF),
            observer=self._geodetic,
        )

        if await self.get(self.telescope, "AtPark", False) and self._can_unpark:
            await self.call(self.telescope, "Unpark")

        self._sidereal = False
        # Default to a zero inertial rate (sidereal/fixed); moving-target cases below
        # override it so the published AxisRates reflect the real motion.
        self._icrf_rate = (0.0, 0.0)
        self._tle_target = None

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")
                # ICRS -> JNow: Autoslew interprets slew INPUT as of-date.
                ra_jnow, dec_jnow = icrs_to_jnow(
                    target.coords.ra, target.coords.dec, Time.now(), self._location
                )
                await self.put(self.telescope, "Tracking", True)
                self._tracking = True
                await self._slew_radec(ra_jnow / 15.0, dec_jnow)
                await self._wait_for_telescope(tracking=True)
                self._sidereal = True
                self._start_fast_status()

            case AltAzTarget():
                logger.debug("executing AltAz follow")
                if self._can_slew_altaz_async:
                    await self.call(
                        self.telescope, "SlewToAltAzAsync", target.coords.az, target.coords.alt
                    )
                elif self._can_slew_altaz:
                    await asyncio.to_thread(
                        self.telescope.SlewToAltAz, target.coords.az, target.coords.alt
                    )
                await self._wait_for_telescope()
                self._start_fast_status()

            case TLETarget():
                logger.debug("executing native TLE follow")
                await self._ensure_motors_on()
                # Stage the pass. Keep 'cpf' out of the name (that switches modes).
                name = target.tle.line0 or "SENSORKIT"
                await self.action("sat:name", name.replace("cpf", "").replace("CPF", ""))
                await self.action("sat:startalt", repr(float(self.config.min_altitude_degrees)))
                await self.action("sat:delay", "0")
                await self.action("sat:line1", target.tle.line1)
                await self.action("sat:line2", target.tle.line2)
                await self.action("sat:start")
                # Wait for the tracking bit, or at least for the acquisition slew to
                # begin (sat:start slews to the rise point and waits if not yet up).
                try:
                    async with asyncio.timeout(_SAT_ACQUIRE_TIMEOUT):
                        while True:
                            if await self._sat_tracking() or await self.get(
                                self.telescope, "Slewing", False
                            ):
                                break
                            await asyncio.sleep(0.2)
                except TimeoutError:
                    logger.warning("sat:start did not report tracking/slewing within timeout")
                # Autoslew flies the elements itself, but SensorKit still needs the
                # target's inertial rate for AxisRates/FITS; refreshed in the slow loop.
                self._tle_target = target
                self._icrf_rate = await self._tle_icrf_rate(target)
                self._start_fast_status()

            case RateTarget():
                logger.debug("executing Rate follow")
                ra_jnow, dec_jnow = icrs_to_jnow(
                    target.initial_coords.ra, target.initial_coords.dec, Time.now(), self._location
                )
                await self.put(self.telescope, "Tracking", True)
                self._tracking = True
                await self._slew_radec(ra_jnow / 15.0, dec_jnow)
                await self._wait_for_telescope(tracking=True)
                # ASCOM offset rates (deg/s -> RA sec/sidereal-sec, arcsec/s)
                await self.put(
                    self.telescope, "RightAscensionRate", target.rates.ra * 3600.0 / 15.0
                )
                await self.put(self.telescope, "DeclinationRate", target.rates.dec * 3600.0)
                self._icrf_rate = (target.rates.ra, target.rates.dec)
                self._start_fast_status()

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow (reduced to position + offset rate)")
                # ASCOM/Alpaca has no path-follow primitive: use the first sample as
                # the initial position and a finite difference of the first two as a
                # constant offset rate (see the RateTarget case). Good for a slow/GEO
                # target; a fast one drifts until the next FollowTarget refreshes it.
                # NB: EphemerisTarget subclasses FrameTarget, so this case MUST precede
                # the bare FrameTarget case below or it would be caught as a sidereal hold.
                p0 = target.points[0]
                ra_jnow, dec_jnow = icrs_to_jnow(p0.ra, p0.dec, Time.now(), self._location)
                await self.put(self.telescope, "Tracking", True)
                self._tracking = True
                await self._slew_radec(ra_jnow / 15.0, dec_jnow)
                await self._wait_for_telescope(tracking=True)
                ra_rate, dec_rate = _icrf_rate_from_samples(target.jds, target.points)
                self._icrf_rate = (ra_rate, dec_rate)
                await self.put(self.telescope, "RightAscensionRate", ra_rate * 3600.0 / 15.0)
                await self.put(self.telescope, "DeclinationRate", dec_rate * 3600.0)
                self._start_fast_status()

            case FrameTarget():
                match target.frame:
                    case ReferenceFrame.ALTAZ:
                        logger.debug("disabling tracking")
                        await self.put(self.telescope, "Tracking", False)
                        self._tracking = False
                        await self._wait_for_telescope(tracking=False, await_onset=False)
                        self._stop_fast_status()
                    case ReferenceFrame.ICRF:
                        logger.debug("enabling sidereal tracking")
                        await self.put(self.telescope, "Tracking", True)
                        self._tracking = True
                        self._sidereal = True
                        await self._wait_for_telescope(tracking=True, await_onset=False)
                        self._start_fast_status()

            case _:
                raise NotImplementedError(
                    f"{type(cmd.target).__name__} tracking via Autoslew is not supported"
                )

        try:
            await self._publish_telescope_status()
        except Exception as e:
            logger.warning(f"Immediate mount status publish failed: {e}")

    async def _slew_radec(self, ra_hours: float, dec_deg: float):
        if self._can_slew_async:
            await self.call(self.telescope, "SlewToCoordinatesAsync", ra_hours, dec_deg)
        elif self._can_slew:
            await asyncio.to_thread(self.telescope.SlewToCoordinates, ra_hours, dec_deg)

    # ---- status ----------------------------------------------------------- #
    # _wait_for_telescope / _start_fast_status / _stop_fast_status /
    # _fast_status_active / status_publish_fast are inherited from AlpacaTelescope
    # unchanged (status_publish_fast drives the overridden _publish_telescope_status).

    async def _publish_telescope_status(self):
        t = self.telescope
        ra_jnow_h = await self.get(t, "RightAscension", 0.0)  # JNow, hours
        dec_jnow = await self.get(t, "Declination", 0.0)  # JNow, deg
        alt_deg = await self.get(t, "Altitude", 0.0)
        az_deg = await self.get(t, "Azimuth", 0.0)

        # JNow -> ICRS, then publish explicit ICRF (never the implicit default).
        ra_icrs, dec_icrs = jnow_to_icrs(ra_jnow_h * 15.0, dec_jnow, Time.now(), self._location)

        device = sk.device()
        await device.publish(
            RADecPointing(
                right_ascension_hours=ra_icrs / 15.0,
                declination_degrees=dec_icrs,
                reference_frame=ReferenceFrame.ICRF,
            )
        )
        await device.publish(AltAzPointing(altitude_degrees=alt_deg, azimuth_degrees=az_deg))

        # Inertial (ICRF) RA/Dec rates: zero while sidereal/fixed, the target's real
        # rate while following a moving target — FITS RA_RATE must be inertial.
        ra_rate, dec_rate = self._icrf_rate
        alt_rate = az_rate = 0.0
        if self._location is not None and (ra_rate or dec_rate):
            with contextlib.suppress(Exception):
                alt_rate, az_rate = radec_rates_to_altaz_rates(
                    ra_icrs, dec_icrs, ra_rate, dec_rate, self._location, Time.now()
                )
        await device.publish(
            AxisRates(
                azimuth=AxisRate(axis=MountAxis.AZIMUTH, velocity=az_rate),
                altitude=AxisRate(axis=MountAxis.ALTITUDE, velocity=alt_rate),
                right_ascension=AxisRate(axis=MountAxis.RIGHT_ASCENSION, velocity=ra_rate),
                declination=AxisRate(axis=MountAxis.DECLINATION, velocity=dec_rate),
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

                # While following a TLE, refresh the inertial rate (it changes through
                # the pass) and surface the closed-loop tracking error.
                if self._tle_target is not None:
                    self._icrf_rate = await self._tle_icrf_rate(self._tle_target)
                    with contextlib.suppress(Exception):
                        sat = json.loads(await self.command_string("getSatStatus"))
                        status = int(_pick(sat, "Status", default=0))
                        await device.publish(
                            SatTrackError(
                                tracking=bool(status & 0b01),
                                sunlit=bool(status & 0b10),
                                track_error_ax1_millirad=_num(
                                    _pick(sat, "TrackErrAx1", default=0.0)
                                ),
                                track_error_ax2_millirad=_num(
                                    _pick(sat, "TrackErrAx2", default=0.0)
                                ),
                            )
                        )

                if not self._fast_status_active:
                    await self._publish_telescope_status()
            except Exception as e:
                logger.warning(f"Error in slow mount status publish: {e}")
            await asyncio.sleep(self.config.status_frequency_slow)


class AutoslewTelescopeConfig(AlpacaTelescopeConfig):
    """Autoslew mount configuration."""

    # Floor for sat:startalt; must exceed the driver's horizon limit + 0.5.
    min_altitude_degrees: float = 20.0

    @override
    def create_device(self):
        return AutoslewTelescope(self)


class AutoslewTelescopeState(AlpacaTelescopeState):
    """Autoslew mount state."""
