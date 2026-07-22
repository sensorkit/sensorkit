# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Literal, override

import astropy.units as u
from astropy.coordinates import ICRS, AltAz, EarthLocation, SkyCoord
from astropy.time import Time

# Prevent IERS-A (Earth orientation parameters) download, which can take long enough to cause a lease expiry
from astropy.utils import iers
from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.common import TLE, AltAzPointing, RADecPointing, ReferenceFrame
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
from sensorkit.thesky.device import (
    CommandNotSupportedError,
    MountCommandInProgressError,
    ProcessAbortedError,
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
    TheSkyError,
)

iers.conf.auto_download = False
# Suppress the warning that results from the above decision
from astropy.utils.iers import conf  # noqa: E402

conf.auto_max_age = None
LEO_WAITING, LEO_SLEWING, LEO_SETTLING, LEO_TRACKING = 3, 4, 5, 6
# Max time to wait for a commanded slew to *start* before treating it as a
# positional no-op.
_MOUNT_ONSET_TIMEOUT = 2.0


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


@sk.declare_device
class TheSkyTelescope(TheSkyDevice):
    """TheSky Telescope implementation."""

    config: TheSkyTelescopeConfig
    device_name = "Telescope"
    _fast_status_task: asyncio.Task | None = None

    # Site info, populated from TheSky in entity_init. Defaulted here so a status
    # publish that lands before the attach completes skips the alt/az rate
    # conversion, which is what the `is not None` guard in status_publish assumes.
    _geodetic: Geodetic | None = None
    _location: EarthLocation | None = None

    # NOTE: For a TheSky telescope, you have to home it before any commands at all, and you have to "Unpark" it
    # (if in the "Park" position) before any motion. If you "Park" the telescope and "Disconnect" it, note that
    # it will not be in the software "Park" position upon a reconnection, even if it is actually in the hardware
    # "Park" position.

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyTelescopeState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyTelescopeState()

        # Get TheSky location
        resp = await self.execute(
            """
            var Out;
            var sk6DocProp_Latitude = 0;
            var sk6DocProp_Longitude = 1;
            var sk6DocProp_TimeZone = 2;
            var sk6DocProp_Elevation = 3;
            sky6StarChart.DocumentProperty(sk6DocProp_Latitude);
            dLat = sky6StarChart.DocPropOut
            sky6StarChart.DocumentProperty(sk6DocProp_Longitude);
            dLon = sky6StarChart.DocPropOut
            sky6StarChart.DocumentProperty(sk6DocProp_TimeZone);
            dTz = sky6StarChart.DocPropOut
            sky6StarChart.DocumentProperty(sk6DocProp_Elevation);
            dEle = sky6StarChart.DocPropOut
            Out = [
                dLat,
                dLon,
                dTz,
                dEle
            ];
            """
        )
        self._site_lat, self._site_lon, time_zone, self._site_elev = [float(x) for x in resp.split(",")]
        self._site_lon = -self._site_lon if time_zone < 0 else self._site_lon
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
        await self.telescope_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def telescope_init(self, cmd: Init):
        # Connect to the hardware
        self._reconnect = lambda: self.telescope_connect(Connect())

        await self.telescope_connect(Connect())
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
        await self.telescope_park(MoveToPark())

    @sk.command_handler
    async def telescope_connect(self, cmd: Connect):
        logger.debug("connecting to telescope")

        await self.execute(
            """
            sky6RASCOMTele.Asynchronous = 1;
            sky6RASCOMTele.ConnectAndDoNotUnpark();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to telescope")

    @sk.command_handler
    async def telescope_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from telescope")

        await self.execute(
            """
            sky6RASCOMTheSky.DisconnectTelescope();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from telescope")

    @sk.command_handler
    async def telescope_stop(self, cmd: Stop):
        self._stop_fast_status()
        await self.telescope_unpark()
        logger.debug("stopping telescope")

        try:
            await self.execute(
                """
                sky6RASCOMTele.Abort();
                sky6RASCOMTele.SetTracking(0,1,0,0);
                """
            )

            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6RASCOMTele.IsTracking;""", "0", interval=0.2)
        except CommandNotSupportedError:
            logger.warning("Mount does not support SetTracking; skipping")

        self._stop_fast_status()
        logger.debug("stopped telescope")

    @sk.command_handler
    async def telescope_park(self, cmd: MoveToPark):
        self._stop_fast_status()
        await self.require_connected()
        logger.debug("parking telescope")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""sky6RASCOMTele.ParkAndDoNotDisconnect();""")
                    break
                except MountCommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsParked();""", "true")

        logger.debug("parked telescope")

    @sk.command_handler
    async def telescope_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        logger.debug("setting park position")

        await self.execute(
            """
            sky6RASCOMTele.SetParkPosition();
            """
        )

        logger.debug("set park position")

    async def telescope_unpark(self):
        # This is unique to TheSky. It requires you to unpark the telescope before issuing any
        # other motion command.
        await self.require_connected()
        logger.debug("unparking telescope")

        await self.execute(
            """
            var Out;
            Out = sky6RASCOMTele.IsParked();
            if (Out) {
                sky6RASCOMTele.Unpark();
            }
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsParked();""", "false", interval=0.2)

        logger.debug("unparked telescope")

    @sk.command_handler
    async def telescope_home(self, cmd: Home):
        self._stop_fast_status()
        await self.telescope_unpark()
        logger.debug("homing telescope")

        try:
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute(
                            """
                            sky6RASCOMTele.FindHome();
                            """
                        )
                        break
                    except MountCommandInProgressError:
                        await asyncio.sleep(0.5)

            await self.poll("""sky6RASCOMTele.LastSlewError;""", "0")
        except CommandNotSupportedError:
            logger.warning("Mount does not support FindHome; skipping")

        # Turn off sidereal tracking
        try:
            await self.execute(
                """
                sky6RASCOMTele.SetTracking(0,1,0,0);
                """
            )

            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6RASCOMTele.IsTracking;""", "0")
        except CommandNotSupportedError:
            logger.warning("Mount does not support SetTracking; skipping")

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed telescope")

    @sk.command_handler
    async def telescope_follow_target(self, cmd: FollowTarget):
        self._stop_fast_status()
        await self.telescope_unpark()

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

        match target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToRaDec(
                        {target.coords.ra / 15:0.6f},
                        {target.coords.dec:0.6f},
                        "object"
                    );
                    """
                )

                tracked = await self._set_tracking(1, 1, 0, 0)
                await self._wait_for_mount(tracking=tracked)
                self._start_fast_status()

                logger.debug("following RADec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToAzAlt(
                        {target.coords.az:0.6f},
                        {target.coords.alt:0.6f},
                        "object"
                    );
                    """
                )

                await self._wait_for_mount(tracking=False)
                self._start_fast_status()

                logger.debug("following AltAz target")

            case TLETarget():
                logger.debug("executing TLE follow")

                # Update line0
                # NOTE: TheSky requires this syntax, so we force it here.
                tle = TLE(
                    line0=f"0 {target.tle.line2.split()[1]}",
                    line1=target.tle.line1,
                    line2=target.tle.line2,
                )

                # Update the TLE file
                tle_path = self.write_tle(tle)
                designator = tle.line1.split()[1]  # e.g. "12345U"
                await self.execute(
                    f"""
                    Raven3.trackLEODoCommand(
                        100,
                        '.'
                    );
                    Raven3.trackLEODoCommand(
                        100,
                        '{tle_path}'
                    );
                    """
                )
                await asyncio.sleep(0.5)

                # Find the just-loaded satellite, verify the chart selection
                # actually points at it, and only then call trackLEOBegin.
                # If Find returns no matches, TheSky raises ObjectNotFoundError
                # (250) at the script level. If it matches but the first hit
                # isn't our satellite, trackLEOBegin would error 7503 ("Target
                # not a satellite") -- detect that here and surface a diagnostic
                # message instead.
                result = await self.execute(
                    f"""
                    var Out;
                    sky6StarChart.Find('{designator}');
                    var n = sky6ObjectInformation.Count;
                    var parts = ['count=' + n];
                    var ok = false;
                    for (var i = 0; i < n; i++) {{
                        sky6ObjectInformation.Index = i;
                        sky6ObjectInformation.Property(0);
                        var name = sky6ObjectInformation.ObjInfoPropOut;
                        parts.push(i + ':' + name);
                        if (i === 0 && name.indexOf('#{designator}') >= 0) {{
                            ok = true;
                        }}
                    }}
                    if (ok) {{
                        Raven3.trackLEOBegin();
                        Out = 'OK|' + parts.join('|');
                    }} else {{
                        Out = 'BAD|' + parts.join('|');
                    }}
                    """
                )

                if result.startswith("BAD|"):
                    logger.warning(
                        f"TLE follow chart-selection mismatch for {designator};"
                        f" skipped trackLEOBegin. Find diagnostic: {result}"
                    )
                    raise TheSkyError(
                        message=(
                            f"chart selection isn't the loaded satellite for"
                            f" {designator}: {result}"
                        ),
                        code=-1,
                    )

                logger.debug(f"TLE follow Find diagnostic for {designator}: {result}")

                async with asyncio.timeout(self.config.timeout):
                    await self.poll("""Raven3.trackLEOStatus;""", str(LEO_TRACKING), interval=0.2)
                self._start_fast_status()

                logger.debug("tracking TLE follow")

            case RateTarget():
                logger.debug("executing Rate follow")

                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToRaDec(
                        {target.initial_coords.ra / 15:0.6f},
                        {target.initial_coords.dec:0.6f},
                        "object"
                    );
                    """
                )

                # Apply custom offset rates (degrees/sec -> arcsec/sec)
                ra_rate_arcsec = target.rates.ra * 3600
                dec_rate_arcsec = target.rates.dec * 3600
                tracked = await self._set_tracking(1, 0, ra_rate_arcsec, dec_rate_arcsec)
                await self._wait_for_mount(tracking=tracked)
                self._start_fast_status()

                logger.debug("following rate target")

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow")

                # TheSky has no path-follow primitive, so collapse the precomputed
                # path to an initial position plus a constant offset rate: slew to
                # the sample nearest now and finite-difference the two adjacent
                # samples for the instantaneous RA/Dec rate. Like the TLE case, this
                # is a best-effort snapshot that drifts over time.
                jds = target.jds
                points = target.points
                now_jd = Time.now().jd
                i = min(range(len(jds) - 1), key=lambda j: abs(jds[j] - now_jd))

                dt_sec = (jds[i + 1] - jds[i]) * 86400.0
                # Wrap the RA difference into [-180, 180] deg to survive 0/360 crossings.
                dra_deg = (points[i + 1].ra - points[i].ra + 180.0) % 360.0 - 180.0
                ddec_deg = points[i + 1].dec - points[i].dec

                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToRaDec(
                        {points[i].ra / 15:0.6f},
                        {points[i].dec:0.6f},
                        "object"
                    );
                    """
                )

                # Apply offset rates (degrees/sec -> arcsec/sec), matching RateTarget
                ra_rate_arcsec = dra_deg / dt_sec * 3600
                dec_rate_arcsec = ddec_deg / dt_sec * 3600
                tracked = await self._set_tracking(1, 0, ra_rate_arcsec, dec_rate_arcsec)
                await self._wait_for_mount(tracking=tracked)
                self._start_fast_status()

                logger.debug("following Ephemeris target")

            case FrameTarget():
                match target.frame:
                    case ReferenceFrame.ALTAZ:
                        self._stop_fast_status()
                        logger.debug("disabling tracking")
                        await self._set_tracking(0, 1, 0, 0)
                        await self._wait_for_mount(tracking=False, await_onset=False)
                        logger.debug("disabled tracking")
                    case ReferenceFrame.ICRF:
                        logger.debug("enabling sidereal tracking")
                        tracked = await self._set_tracking(1, 1, 0, 0)
                        await self._wait_for_mount(tracking=tracked, await_onset=False)
                        self._start_fast_status()
                        logger.debug("enabled sidereal tracking")

                    case _:
                        raise RuntimeError(f"Need specific target to track {target.frame}")

            case _:
                track_type = type(target).__name__
                raise NotImplementedError(f"{track_type} tracking via TheSky is not supported")

        try:
            await self._publish_telescope_status()
        except Exception as e:
            logger.warning(f"Immediate telescope status publish failed: {e}")

    def write_tle(self, target: TLE) -> str:
        """Write TLE to a temp file and return the path for TheSky use."""

        lines = []
        if target.line0 is not None:
            lines.append(target.line0)
        lines.extend([target.line1, target.line2])

        # Use satellite designator (e.g. "NNNNNU") for a unique filename
        designator = target.line1.split()[1]
        filename = f"tle_{designator}.txt"

        host_data_path = os.environ.get("SENSORKIT_DATA_PATH")

        if host_data_path:
            write_path = Path("/data") / filename
            thesky_path = f"{host_data_path.replace('\\', '/')}/{filename}"
        else:
            write_path = Path(tempfile.gettempdir()) / filename
            thesky_path = write_path.as_posix()

        write_path.write_text("\n".join(lines) + "\n")
        return thesky_path

    async def _wait_for_mount(
        self,
        *,
        slewing: bool = False,
        tracking: bool = False,
        await_onset: bool = True,
    ):
        """Poll until IsSlewComplete/IsTracking match the target flags.

        When `await_onset` (the default, for slewing commands)
        first wait briefly for the slew to *start*; if none is seen within
        _MOUNT_ONSET_TIMEOUT the command was a positional no-op and we fall
        through to the settle check. Non-slewing commands (enable/disable
        tracking) pass `await_onset=False`. The settle wait is bounded by
        config.timeout; both phases poll every 0.1 s.
        """
        flags = """var Out; Out=[sky6RASCOMTele.IsSlewComplete, sky6RASCOMTele.IsTracking];"""

        if await_onset:
            try:
                async with asyncio.timeout(_MOUNT_ONSET_TIMEOUT):
                    while True:
                        slew_complete, _ = (await self.execute(flags)).split(",")
                        if slew_complete.strip() != "1":
                            break
                        await asyncio.sleep(0.1)
            except TimeoutError:
                pass  # no slew observed -> positional no-op; fall through to settle

        async with asyncio.timeout(self.config.timeout):
            while True:
                slew_complete, is_tracking = (await self.execute(flags)).split(",")
                is_slewing = slew_complete.strip() != "1"
                if is_slewing == slewing and (is_tracking.strip() == "1") == tracking:
                    break
                await asyncio.sleep(0.1)

    async def _set_tracking(
        self, on: int, ignore_rates: int, ra_rate: float = 0.0, dec_rate: float = 0.0
    ) -> bool:
        """Set tracking mode/rates. Returns True if applied, False only if the
        mount doesn't implement SetTracking (error 228) -- in which case the
        tracking degrades to a bare slew instead of crashing.
        Any other error propagates.
        """
        try:
            await self.execute(
                f"""sky6RASCOMTele.SetTracking({on}, {ignore_rates}, {ra_rate}, {dec_rate});"""
            )
        except CommandNotSupportedError:
            logger.warning(
                "Mount does not support SetTracking; tracking no-op"
            )
            return False
        return True

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
        resp = await self.execute(
            """
            var LeoStatus = -1;
            try { LeoStatus = Raven3.trackLEOStatus; } catch (e) {}
            var SlewComplete = -1;
            try { SlewComplete = sky6RASCOMTele.IsSlewComplete; } catch (e) {}
            var Out;
            sky6RASCOMTele.GetRaDec();
            sky6RASCOMTele.GetAzAlt();
            Out = [
                sky6RASCOMTele.IsConnected,
                sky6RASCOMTele.dRa,
                sky6RASCOMTele.dRaTrackingRate,
                sky6RASCOMTele.dDec,
                sky6RASCOMTele.dDecTrackingRate,
                sky6RASCOMTele.dAlt,
                sky6RASCOMTele.dAz,
                SlewComplete,
                sky6RASCOMTele.IsTracking,
                LeoStatus
            ];
            """
        )

        (
            connected,
            ra,
            ra_rate,
            dec,
            dec_rate,
            alt,
            az,
            slew_complete,
            tracking,
            leo_status
        ) = [float(x) for x in resp.split(",")]

        connected = bool(connected)
        self.device_connected = connected

        leo_status = int(leo_status)
        if leo_status in (LEO_SLEWING, LEO_SETTLING):
            is_slewing, is_tracking = True, False
        elif leo_status == LEO_TRACKING:
            is_slewing, is_tracking = False, True
        else:
            if slew_complete < 0:
                is_slewing = False
            else:
                is_slewing = not bool(slew_complete)
            is_tracking = bool(tracking)

        device = sk.device()
        await device.publish(Connected(is_connected=connected))
        await device.publish(Slewing(is_slewing=is_slewing))
        await device.publish(Tracking(is_tracking=is_tracking))
        await device.publish(RADecPointing(right_ascension_hours=ra, declination_degrees=dec))
        await device.publish(AltAzPointing(altitude_degrees=alt, azimuth_degrees=az))

        # Convert RA/Dec rates from arcsec/sec to deg/sec
        ra_rate /= 3600
        dec_rate /= 3600

        icrf_ra = ra_rate if is_tracking else 0.0
        icrf_dec = dec_rate if is_tracking else 0.0

        if self._location is not None:
            if is_tracking:
                alt_rate, az_rate = radec_rates_to_altaz_rates(
                    ra_hr=ra,
                    dec_deg=dec,
                    ra_rate_deg_per_sec=icrf_ra,
                    dec_rate_deg_per_sec=icrf_dec,
                    location=self._location,
                    time=Time.now(),
                )
            else:
                alt_rate = az_rate = 0.0

            await device.publish(
                AxisRates(
                    azimuth=AxisRate(velocity=az_rate, axis=MountAxis.AZIMUTH),
                    altitude=AxisRate(velocity=alt_rate, axis=MountAxis.ALTITUDE),
                    right_ascension=AxisRate(
                        velocity=icrf_ra,
                        axis=MountAxis.RIGHT_ASCENSION,
                    ),
                    declination=AxisRate(
                        velocity=icrf_dec,
                        axis=MountAxis.DECLINATION,
                    ),
                )
            )

    async def status_publish_slow(self):
        while True:
            try:
                if not self._fast_status_active:
                    await self._publish_telescope_status()
                else:
                    await sk.device().publish(Connected(is_connected=self.device_connected))

                # logger.debug(
                #     f"TheSky telescope status: connected={self.device_connected}"
                # )
            except ProcessAbortedError:
                # TheSky returns 212 transiently after an `abort`; telescope is fine
                pass
            except Exception as e:
                logger.warning(f"Error in slow telescope status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            await asyncio.sleep(self.config.status_frequency_slow)

    async def status_publish_fast(self):
        while True:
            try:
                await self._publish_telescope_status()
            except Exception as e:
                logger.warning(f"Error in fast telescope status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_fast)
                continue

            await asyncio.sleep(self.config.status_frequency_fast)


class TheSkyTelescopeConfig(TheSkyDeviceConfig[TheSkyTelescope]):
    """TheSky Telescope configuration."""

    device_type: Literal["telescope"] = "telescope"
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1
    timeout: float = 300.0

    @override
    def create_device(self):
        return TheSkyTelescope(self)


class TheSkyTelescopeState(TheSkyDeviceState):
    """TheSky Telescope state."""

    device_type: Literal["telescope"] = "telescope"
    has_been_homed: bool = False
