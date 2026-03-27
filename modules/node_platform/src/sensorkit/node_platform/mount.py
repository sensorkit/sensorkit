from __future__ import annotations

import asyncio
from pydantic import Field
from typing import Literal, override

from astropy.coordinates import ICRS, AltAz, EarthLocation, SkyCoord
from astropy.time import Time
import astropy.units as u
from loguru import logger

import ourskyai_node_platform_api as osapi

import sensorkit.api as sk
from sensorkit.astro.target import (
    AltAzTarget,
    FrameTarget,
    ICRSTarget,
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
)
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


@sk.declare_device
class NodePlatformMount(NodePlatformDevice):
    """Node Platform Mount implementation."""
    config: NodePlatformMountConfig
    device_name = "Mount"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state, start status publishing, define site location."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformMountState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformMountState()

        self.mount_slewing: bool | None = None
        self.mount_tracking: bool | None = None

        # Cache for site info (location + time)
        self._site_info: dict[str, object] = {}
        self._location: EarthLocation | None = None

        # Start mount status publishing
        logger.debug("starting node_platform mount status loop")
        self._status_task = asyncio.create_task(self.status_publish())

        # Wait for initial status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

        # Site location
        location: osapi.V1SystemLocation = await self.api.call("v1_get_system_location")
        self._site_info["latitude_degrees"] = location.latitude_degrees
        self._site_info["longitude_degrees"] = location.longitude_degrees
        self._site_info["height_meters"] = location.height_meters
        self._site_info["location_updated_at"] = location.updated_at
        # if location.gps_details:
        #     self._site_info["gps_fix"] = location.gps_details.fix
        #     self._site_info["gps_position_error_meters"] = location.gps_details.position_error_meters
        self._location = EarthLocation(
            lat=self._site_info["latitude_degrees"] * u.deg,
            lon=self._site_info["longitude_degrees"] * u.deg,
            height=self._site_info["height_meters"] * u.m
        )

        # System time
        system_time: osapi.V1SystemTime = await self.api.call("v1_get_system_time")
        self._site_info["time_source"] = system_time.current_time_source
        self._site_info["system_time"] = system_time.current_system_time
        self._site_info["time_uncertainty_seconds"] = system_time.time_uncertainty_seconds

        logger.debug(
            f"site info: lat={self._site_info['latitude_degrees']}, "
            f"lon={self._site_info['longitude_degrees']}, "
            f"height={self._site_info['height_meters']}m, "
            f"time_source={self._site_info['time_source']}, "
            f"time_unc={self._site_info['time_uncertainty_seconds']}s"
        )

    @sk.command_handler
    async def mount_init(self, cmd: sk.Init):
        """Home as needed, setup optical tube assembly."""
        self.require_connected()
        if self.config.needs_homed:
            if not self.state.has_been_homed:
                await self.mount_home(sk.Home())

        # Setup the OTA
        # await self.setup_ota()

    @sk.command_handler
    async def mount_deinit(self, cmd: sk.Deinit):
        """Stop all motion, send mount to park position."""
        await self.mount_stop(sk.Stop())
        await self.mount_park(sk.MoveToPark())

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state and stop status publishing."""
        logger.debug("stopping node_platform mount status loop")
        if hasattr(self, "_status_task"):
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def mount_home(self, cmd: sk.Home):
        self.require_connected()
        logger.debug("homing node_platform mount")

        await self.api.call("v1_mount_go_to_home")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.mount_slewing is None or self.mount_slewing:
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed node_platform mount")

    @sk.command_handler
    async def mount_park(self, cmd: sk.MoveToPark):
        self.require_connected()
        logger.debug("parking node_platform mount")

        await self.api.call("v1_park_mount")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.mount_slewing is None or self.mount_slewing:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("parked node_platform mount")

    @sk.command_handler
    async def mount_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform mount")

        await self.api.call("v1_halt_mount")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.mount_slewing is None or self.mount_slewing:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("stopped node_platform mount")

    @sk.command_handler
    async def mount_follow_target(self, cmd: sk.FollowTarget):
        self.require_connected()
        match cmd.target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                req = osapi.V1GoToMountCoordinatesRequest(
                    ra=cmd.target.right_ascension_hours * 15,
                    dec=cmd.target.declination,
                )
                await self.api.call("v1_go_to_mount_coordinates", req)
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self.mount_tracking is None or not self.mount_tracking:
                        await asyncio.sleep(self.config.status_frequency)

                logger.debug("following RA/Dec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                req = osapi.V1GoToMountCoordinatesRequest(
                    altitude=cmd.target.altitude_degrees,
                    azimuth=cmd.target.azimuth_degrees,
                )
                await self.api.call("v1_go_to_mount_coordinates", req)
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self.mount_slewing is None or self.mount_slewing:
                        await asyncio.sleep(self.config.status_frequency)

                logger.debug("following Alt/Az target")

            case TLETarget():
                logger.debug("executing TLE follow")

                req = osapi.V1FollowTLERequest(
                    tle_line1=cmd.target.tle.line1,
                    tle_line2=cmd.target.tle.line2,
                )
                await self.api.call("v1_mount_follow_tle", req)
                await asyncio.sleep(1)

                async with asyncio.timeout(self.config.timeout):
                    while self.mount_tracking is None or not self.mount_tracking:
                        await asyncio.sleep(self.config.status_frequency)

                logger.debug("following TLE target")

            case FrameTarget():
                match cmd.target.frame:
                    case ReferenceFrame.ALTAZ:
                        logger.debug("stopping tracking")
                        await self.api.call("v1_disable_mount_tracking")

                        async with asyncio.timeout(self.config.timeout):
                            while (self.mount_slewing is None or self.mount_slewing or
                                   self.mount_tracking is None or self.mount_tracking):
                                await asyncio.sleep(self.config.status_frequency)

                    case ReferenceFrame.ICRF:
                        logger.debug("executing sidereal track")
                        await self.api.call("v1_enable_mount_tracking")
                        await asyncio.sleep(1)

                        async with asyncio.timeout(self.config.timeout):
                            while self.mount_tracking is None or not self.mount_tracking:
                                await asyncio.sleep(self.config.status_frequency)

                    case _:
                        raise RuntimeError(f"Need specific target to track {cmd.target.frame}")

            case _:
                track_type = type(cmd.target).__name__
                raise NotImplementedError(f"{track_type} tracking via Node Platform is not supported")

    async def status_publish(self):
        while True:
            try:
                status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                self.device_connected = status.connected
                self.mount_slewing = status.is_slewing
                self.mount_tracking = status.is_tracking

                ra_deg = status.ra_j2000_degrees
                dec_deg = status.dec_j2000_degrees
                alt_deg = status.altitude_degrees
                az_deg = status.azimuth_degrees
                ra_hours = ra_deg / 15.0

                # Motor A = azimuth/RA axis, Motor B = altitude/Dec axis
                rate_a = status.motor_a.measured_velocity_degrees_per_second
                rate_b = status.motor_b.measured_velocity_degrees_per_second

                # logger.debug(
                #     f"NodePlatform mount status: connected={status.connected}, "
                #     f"slewing={status.is_slewing}, tracking={status.is_tracking}, "
                #     f"RA={ra_deg}°, Dec={dec_deg}°, Alt={alt_deg}°, Az={az_deg}°, "
                #     f"rate_a={rate_a}°/s, "
                #     f"rate_b={rate_b}°/s"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))
                await device.publish(
                    RADecPointing(
                        right_ascension_hours=ra_hours,
                        declination_degrees=dec_deg,
                        reference_frame=ReferenceFrame.ICRF,
                    )
                )
                await device.publish(
                    AltAzPointing(
                        altitude_degrees=alt_deg,
                        azimuth_degrees=az_deg,
                    )
                )

                if self._location is not None:
                    ra_rate, dec_rate = self.altaz_rates_to_radec_rates(
                        alt_deg,
                        az_deg,
                        rate_b,
                        rate_a,
                        location=self._location,
                        time=Time.now()
                    )

                    await device.publish(
                        AxisRates(
                            azimuth=AxisRate(velocity=rate_a, axis=MountAxis.AZIMUTH),
                            altitude=AxisRate(velocity=rate_b, axis=MountAxis.ALTITUDE),
                            right_ascension=AxisRate(velocity=ra_rate, axis=MountAxis.RIGHT_ASCENSION),
                            declination=AxisRate(velocity=dec_rate, axis=MountAxis.DECLINATION),
                        )
                    )
            except Exception as e:
                logger.warning(f"Failed to update Node Platform mount status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)

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

    async def setup_ota(self):
        # Set heater power levels from config
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

        # Turn on all fans
        all_fan_roles = list(osapi.V1OpticalTubeFanRole)
        await self.api.call(
            "v1_turn_on_optical_tube_fans",
            osapi.V1TurnOnOpticalTubeFansRequest(roles=all_fan_roles),
        )
        logger.debug(f"turned on all fans: {[r.value for r in all_fan_roles]}")


class NodePlatformMountConfig(NodePlatformDeviceConfig[NodePlatformMount]):
    """Node Platform Mount configuration."""
    device_type: Literal["mount"] = "mount"
    needs_homed: bool = False
    heater_power: dict[str, float] = Field(default_factory=dict)
    timeout: float = 300.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformMount(self)


class NodePlatformMountState(NodePlatformDeviceState):
    """Node Platform Mount state."""
    device_type: Literal["mount"] = "mount"
    has_been_homed: bool = False