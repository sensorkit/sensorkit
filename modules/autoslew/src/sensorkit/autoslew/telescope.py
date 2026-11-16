# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew mount with Alpaca commands and ASA extensions.

Initialization probes capabilities, clears stale motion, and enables the
motors. Stopping also ends satellite tracking.

Commanded positions and tracking offsets are converted from ICRS to the
reported equatorial system. Pointing is published in ICRF. Native TLE
following uses ASA satellite actions and waits for the motors before starting.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime, timedelta
from typing import override

import astropy.units as u
from alpaca.telescope import EquatorialCoordinateType, Telescope
from astropy.coordinates import EarthLocation
from astropy.time import Time
from astropy.utils import iers
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.telescope import (
    AlpacaTelescope,
    AlpacaTelescopeConfig,
    AlpacaTelescopeState,
    radec_rates_to_altaz_rates,
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
from sensorkit.autoslew.device import AutoslewMixin, _num, _pick
from sensorkit.common.aio import AsyncLoop
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
    StandardMount,
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


@sk.declare_keyword
class SatTrackError(BaseModel):
    """ASA satellite tracking status/quality, from getSatStatus."""

    tracking: bool = False
    sunlit: bool = False  # not valid for EPH-driven passes
    track_error_ax1_millirad: float = 0.0
    track_error_ax2_millirad: float = 0.0


class AutoslewTelescopeState(AlpacaTelescopeState):
    """Autoslew mount state."""


@sk.declare_device(type=StandardMount)
class AutoslewTelescope(AutoslewMixin, AlpacaTelescope):
    """ASA Autoslew mount control through Alpaca and ASA extensions."""

    config: AutoslewTelescopeConfig
    device_name = "Telescope"
    state_model = AutoslewTelescopeState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        self._tracking: bool | None = None
        self._slewing: bool | None = None
        self._sidereal = False
        # Inertial (ICRF) RA/Dec rate of whatever we're following, deg/s. Zero while
        # sidereal/fixed; published as AxisRates so FITS RA_RATE stays inertial.
        self._icrf_rate: tuple[float, float] = (0.0, 0.0)
        # Set while following a TLE so the status loop can refresh the rate mid-pass.
        self._tle_target: TLETarget | None = None
        self._geodetic: Geodetic | None = None
        self._location: EarthLocation | None = None
        self._equatorial_system: EquatorialCoordinateType | None = None
        self._can_slew_async = self._can_slew = False
        self._can_slew_altaz_async = self._can_slew_altaz = False
        self._can_park = self._can_unpark = self._can_find_home = False

        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        )
        self.fast_loop = AsyncLoop(
            self._publish_telescope_status, interval=self.config.status_frequency_fast, log=True
        )

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
        self._can_set_right_ascension_rate = await self.get(t, "CanSetRightAscensionRate", False)
        self._can_set_declination_rate = await self.get(t, "CanSetDeclinationRate", False)

        self._equatorial_system = await self.get(t, "EquatorialSystem", None)
        if self._equatorial_system != EquatorialCoordinateType.equTopocentric:
            logger.warning(
                f"Autoslew reports EquatorialSystem={self._equatorial_system}. "
                "Expected Topocentric. Conversions follow the reported system."
            )

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

        self.status_loop.start()

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

        await self.fast_loop.stop()
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
        await self.fast_loop.stop()
        logger.debug("stopped mount")

    @sk.command_handler
    async def telescope_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        if not await self.get(self.telescope, "CanSetPark", False):
            logger.warning("Cannot set park")
            return
        await self.call(self.telescope, "SetPark")

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

    @sk.command_handler
    async def telescope_follow_target(self, cmd: FollowTarget):
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
                await self.put(self.telescope, "Tracking", True)
                self._tracking = True
                await self._slew_to_icrs(target.coords.ra, target.coords.dec)
                await self._wait_for_telescope(tracking=True)
                self._sidereal = True
                self.fast_loop.start()

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
                self.fast_loop.start()

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
                self.fast_loop.start()

            case RateTarget():
                logger.debug("executing Rate follow")
                await self.put(self.telescope, "Tracking", True)
                self._tracking = True
                await self._slew_to_icrs(target.initial_coords.ra, target.initial_coords.dec)
                await self._wait_for_telescope(tracking=True)
                await self._set_offset_rates(
                    target.initial_coords.ra,
                    target.initial_coords.dec,
                    target.rates.ra,
                    target.rates.dec,
                )
                self._icrf_rate = (target.rates.ra, target.rates.dec)
                self.fast_loop.start()

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow (reduced to position + offset rate)")
                # ASCOM/Alpaca has no path-follow primitive: use the first sample as
                # the initial position and a finite difference of the first two as a
                # constant offset rate (see the RateTarget case). Good for a slow/GEO
                # target; a fast one drifts until the next FollowTarget refreshes it.
                # NB: EphemerisTarget subclasses FrameTarget, so this case MUST precede
                # the bare FrameTarget case below or it would be caught as a sidereal hold.
                p0 = target.points[0]
                await self.put(self.telescope, "Tracking", True)
                self._tracking = True
                await self._slew_to_icrs(p0.ra, p0.dec)
                await self._wait_for_telescope(tracking=True)
                ra_rate, dec_rate = _icrf_rate_from_samples(target.jds, target.points)
                self._icrf_rate = (ra_rate, dec_rate)
                await self._set_offset_rates(p0.ra, p0.dec, ra_rate, dec_rate)
                self.fast_loop.start()

            case FrameTarget():
                match target.frame:
                    case ReferenceFrame.ALTAZ:
                        logger.debug("disabling tracking")
                        await self.put(self.telescope, "Tracking", False)
                        self._tracking = False
                        await self._wait_for_telescope(tracking=False, await_onset=False)
                        await self.fast_loop.stop()
                    case ReferenceFrame.ICRF:
                        logger.debug("enabling sidereal tracking")
                        await self.put(self.telescope, "Tracking", True)
                        self._tracking = True
                        self._sidereal = True
                        await self._wait_for_telescope(tracking=True, await_onset=False)
                        self.fast_loop.start()

            case _:
                raise NotImplementedError(
                    f"{type(cmd.target).__name__} tracking via Autoslew is not supported"
                )

        try:
            await self._publish_telescope_status()
        except Exception as e:
            logger.warning(f"Immediate mount status publish failed: {e}")

    async def _publish_telescope_status(self):
        t = self.telescope
        mount_ra_hours = await self.get(t, "RightAscension", 0.0)
        mount_dec_deg = await self.get(t, "Declination", 0.0)
        alt_deg = await self.get(t, "Altitude", 0.0)
        az_deg = await self.get(t, "Azimuth", 0.0)

        # Convert reported pointing to ICRS for publication.
        ra_icrs_hours, dec_icrs = self._from_mount(mount_ra_hours, mount_dec_deg)

        device = sk.device()
        await device.publish(
            RADecPointing(
                right_ascension_hours=ra_icrs_hours,
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
                    ra_icrs_hours, dec_icrs, ra_rate, dec_rate, self._location, Time.now()
                )
        await device.publish(
            AxisRates(
                azimuth=AxisRate(axis=MountAxis.AZIMUTH, velocity=az_rate),
                altitude=AxisRate(axis=MountAxis.ALTITUDE, velocity=alt_rate),
                right_ascension=AxisRate(axis=MountAxis.RIGHT_ASCENSION, velocity=ra_rate),
                declination=AxisRate(axis=MountAxis.DECLINATION, velocity=dec_rate),
            )
        )

    async def status_publish(self):
        t = self.telescope
        connected = await self.get(t, "Connected", False)
        self.device_connected = connected
        device = sk.device()
        await device.publish(Connected(is_connected=connected))
        if not connected:
            return

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
                        track_error_ax1_millirad=_num(_pick(sat, "TrackErrAx1", default=0.0)),
                        track_error_ax2_millirad=_num(_pick(sat, "TrackErrAx2", default=0.0)),
                    )
                )

        if not self.fast_loop.active:
            await self._publish_telescope_status()


class AutoslewTelescopeConfig(AlpacaTelescopeConfig):
    """Autoslew mount configuration."""

    # Floor for sat:startalt; must exceed the driver's horizon limit + 0.5.
    min_altitude_degrees: float = 20.0

    @override
    def create_device(self):
        return AutoslewTelescope(self)
