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
from sensorkit.astro.common import TLE
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    RateTarget,
    StateVectorTarget,
    TLETarget,
)
from sensorkit.models.devices import (
    AltAzPointing,
    AxisRate,
    AxisRates,
    Connected,
    MountAxis,
    RADecPointing,
    ReferenceFrame,
    SetParkPosition,
)
from sensorkit.thesky.device import (
    MountCommandInProgressError,
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)

iers.conf.auto_download = False
# Suppress the warning that results from the above decision
from astropy.utils.iers import conf  # noqa: E402

conf.auto_max_age = None


@sk.declare_device
class TheSkyMount(TheSkyDevice):
    """TheSky Mount implementation."""

    config: TheSkyMountConfig
    device_name = "Mount"
    _fast_status_task: asyncio.Task | None = None

    # NOTE: For a TheSky mount, you have to home the mount before any commands at all, and you have to "Unpark" the
    # mount (if in the "Park" position) before any motion. If you "Park" the mount and "Disconnect" it, note that it
    # will not be in the software "Park" position upon a reconnection, even if it is actually in the hardware "Park"
    # position.

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(TheSkyMountState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyMountState()

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
        latitude, longitude, time_zone, elevation = [float(x) for x in resp.split(",")]
        longitude = -longitude if time_zone < 0 else longitude
        self._location = EarthLocation(
            lat=latitude * u.deg, lon=longitude * u.deg, height=elevation * u.m
        )

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.mount_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def mount_init(self, cmd: sk.Init):
        self._reconnect = lambda: self.mount_connect(sk.Connect())

        await self.mount_connect(sk.Connect())
        self.start_status_loop(self.status_publish_slow())

        if not self.state.has_been_homed:
            await self.mount_home(sk.Home())

    @sk.command_handler
    async def mount_deinit(self, cmd: sk.Deinit):
        if not self.device_connected:
            return
        await self.mount_stop(sk.Stop())
        await self.mount_park(sk.MoveToPark())
        self._stop_fast_status()
        await self.stop_status_loop()
        await self.mount_disconnect(sk.Disconnect())

    @sk.command_handler
    async def mount_connect(self, cmd: sk.Connect):
        logger.debug("connecting to Mount")

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

        logger.debug("connected to Mount")

    @sk.command_handler
    async def mount_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from Mount")

        await self.execute(
            """
            sky6RASCOMTheSky.DisconnectTelescope();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from Mount")

    @sk.command_handler
    async def mount_stop(self, cmd: sk.Stop):
        await self.mount_unpark()
        logger.debug("stopping mount")

        await self.execute(
            """
            sky6RASCOMTele.Abort();
            sky6RASCOMTele.SetTracking(0,1,0,0);
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsTracking;""", "0")

        self._stop_fast_status()
        logger.debug("stopped mount")

    @sk.command_handler
    async def mount_park(self, cmd: sk.MoveToPark):
        await self.require_connected()
        logger.debug("parking mount")

        await self.execute(
            """
            sky6RASCOMTele.ParkAndDoNotDisconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsParked();""", "true")

        logger.debug("parked mount")

    @sk.command_handler
    async def mount_set_park_position(self, cmd: SetParkPosition):
        await self.require_connected()
        logger.debug("setting park position")

        await self.execute(
            """
            sky6RASCOMTele.SetParkPosition();
            """
        )

        logger.debug("set park position")

    async def mount_unpark(self):
        # This is unique to TheSky. It requires you to unpark the mount before issuing any
        # other motion command.
        await self.require_connected()
        logger.debug("unparking mount")

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
            await self.poll("""sky6RASCOMTele.IsParked();""", "false")

        logger.debug("unparked mount")

    @sk.command_handler
    async def mount_home(self, cmd: sk.Home):
        await self.mount_unpark()
        logger.debug("homing mount")

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

        # Turn off sidereal tracking
        await self.execute(
            """
            sky6RASCOMTele.SetTracking(0,1,0,0);
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6RASCOMTele.IsTracking;""", "0")

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed mount")

    @sk.command_handler
    async def mount_follow_target(self, cmd: sk.FollowTarget):
        await self.mount_unpark()

        # target = await cmd.target.adapt(
        #     ICRSTarget,
        #     AltAzTarget,
        #     (FrameTarget, ReferenceFrame.ICRF),
        #     (FrameTarget, ReferenceFrame.ALTAZ),
        #     TLETarget,
        #     (RateTarget, ReferenceFrame.ICRF),
        #     (EphemerisTarget, ReferenceFrame.ICRF),
        #     observer=self._geodetic,
        # )
        target = cmd.target

        # Clear previous error(s)
        try:
            await self.execute("""Raven3.trackLEOAbort();""")
        except Exception as e:
            logger.warning(f"Unable to abort track: {e}")

        match cmd.target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToRaDec(
                        {cmd.target.coords.ra:0.6f},
                        {cmd.target.coords.dec:0.6f},
                        "object"
                    );
                    """
                )

                async with asyncio.timeout(self.config.timeout):
                    await self.poll("""sky6RASCOMTele.IsTracking;""", "1")

                self._start_fast_status()
                logger.debug("following RADec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToAzAlt(
                        {cmd.target.coords.az:0.6f},
                        {cmd.target.coords.alt:0.6f},
                        "object"
                    );
                    """
                )

                async with asyncio.timeout(self.config.timeout):
                    await self.poll("""sky6RASCOMTele.IsSlewComplete;""", "1")

                logger.debug("following AltAz target")

            case TLETarget():
                logger.debug("executing TLE follow")

                # Update line0
                # NOTE: TheSky requires this syntax, so we force it here.
                tle = TLE(
                    line0=f"0 {cmd.target.tle.line2.split()[1]}",
                    line1=cmd.target.tle.line1,
                    line2=cmd.target.tle.line2,
                )

                # Update the TLE file
                tle_path = self.write_tle(tle)
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

                # Select the target and begin the follow
                await self.execute(
                    f"""
                    sky6StarChart.Find(
                        '{tle.line1.split()[1]}'
                    );
                    Raven3.trackLEOBegin();
                    """
                )

                async with asyncio.timeout(self.config.timeout):
                    await self.poll("""Raven3.trackLEOStatus;""", "6")

                self._start_fast_status()
                logger.debug("tracking TLE follow")

            case RateTarget():
                logger.debug("executing Rate follow")

                # Slew to initial position
                await self.execute(
                    f"""
                    sky6RASCOMTele.SlewToRaDec(
                        {cmd.target.initial_coords.ra:0.6f},
                        {cmd.target.initial_coords.dec:0.6f},
                        "object"
                    );
                    """
                )

                async with asyncio.timeout(self.config.timeout):
                    await self.poll("""sky6RASCOMTele.IsTracking;""", "1")

                # Apply custom offset rates (degrees/sec -> arcsec/sec)
                ra_rate_arcsec = cmd.target.rates.ra * 3600
                dec_rate_arcsec = cmd.target.rates.dec * 3600
                await self.execute(
                    f"""
                    sky6RASCOMTele.SetTracking(1, 0, {ra_rate_arcsec}, {dec_rate_arcsec});
                    """
                )

                self._start_fast_status()
                logger.debug("following rate target")

            case StateVectorTarget():
                logger.debug("executing StateVector follow")
                # TODO: this will require PID control
                raise RuntimeError("No StateVector support")

            case EphemerisTarget():
                logger.debug("executing Ephemeris follow")
                # TODO: this will require PID control
                raise RuntimeError("No Ephemeris support")

            case FrameTarget():
                match cmd.target.frame:
                    case ReferenceFrame.ALTAZ:
                        self._stop_fast_status()
                        logger.debug("disabling tracking")
                        await self.execute(
                            """
                            sky6RASCOMTele.SetTracking(0,1,0,0);
                            """
                        )
                        async with asyncio.timeout(self.config.timeout):
                            await self.poll("""sky6RASCOMTele.IsTracking;""", "0")
                        logger.debug("disabled tracking")
                    case ReferenceFrame.ICRF:
                        logger.debug("enabling sidereal tracking")
                        await self.execute(
                            """
                            sky6RASCOMTele.SetTracking(1,1,0,0);
                            """
                        )
                        async with asyncio.timeout(self.config.timeout):
                            await self.poll("""sky6RASCOMTele.IsTracking;""", "1")
                        self._start_fast_status()
                        logger.debug("enabled sidereal tracking")

                    case _:
                        raise RuntimeError(f"Need specific target to track {cmd.target.frame}")

            case _:
                track_type = type(cmd.target).__name__
                raise NotImplementedError(f"{track_type} tracking via TheSky is not supported")

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
        resp = await self.execute(
            """
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
                sky6RASCOMTele.dAz
            ];
            """
        )

        connected, ra, ra_rate, dec, dec_rate, alt, az = [float(x) for x in resp.split(",")]

        connected = bool(connected)
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))
        await device.publish(RADecPointing(right_ascension_hours=ra, declination_degrees=dec))
        await device.publish(AltAzPointing(altitude_degrees=alt, azimuth_degrees=az))

        # Convert RA/Dec rates from arcsec/sec to deg/sec
        ra_rate /= 3600
        dec_rate /= 3600

        if self._location is not None:
            alt_rate, az_rate = self.radec_rates_to_altaz_rates(
                ra_hr=ra,
                dec_deg=dec,
                ra_rate_deg_per_sec=ra_rate,
                dec_rate_deg_per_sec=dec_rate,
                location=self._location,
                time=Time.now(),
            )

            await device.publish(
                AxisRates(
                    azimuth=AxisRate(velocity=az_rate, axis=MountAxis.AZIMUTH),
                    altitude=AxisRate(velocity=alt_rate, axis=MountAxis.ALTITUDE),
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

    async def status_publish_slow(self):
        while True:
            try:
                if not self._fast_status_active:
                    await self._publish_mount_status()
                else:
                    await sk.device().publish(Connected(is_connected=self.device_connected))

                # logger.debug(
                #     f"TheSky mount status: connected={self.device_connected}"
                # )
            except Exception as e:
                logger.warning(f"Error in slow mount status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_slow)
                continue

            await asyncio.sleep(self.config.status_frequency_slow)

    async def status_publish_fast(self):
        while True:
            try:
                await self._publish_mount_status()
            except Exception as e:
                logger.warning(f"Error in fast mount status_publish ({e})")
                await asyncio.sleep(self.config.status_frequency_fast)
                continue

            await asyncio.sleep(self.config.status_frequency_fast)

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


class TheSkyMountConfig(TheSkyDeviceConfig[TheSkyMount]):
    """TheSky Mount configuration."""

    device_type: Literal["mount"] = "mount"
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1
    timeout: float = 300.0

    @override
    def create_device(self):
        return TheSkyMount(self)


class TheSkyMountState(TheSkyDeviceState):
    """TheSky Mount state."""

    device_type: Literal["mount"] = "mount"
    has_been_homed: bool = False
