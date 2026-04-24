from __future__ import annotations

import asyncio
import json
from typing import Literal, override

import astropy.units as u
import ourskyai_node_platform_api as osapi
from astropy.coordinates import ICRS, AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from loguru import logger
from pydantic import BaseModel, Field

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

        # Fast status task (started/stopped by command handlers)
        self._fast_status_task: asyncio.Task | None = None

        # Start slow status publishing
        logger.debug("starting node_platform mount status loop")
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
            f"height={self._site_info['height_meters']}m, "
            f"time_source={self._site_info['time_source']}, "
            f"time_unc={self._site_info['time_uncertainty_seconds']}s"
        )

    @sk.command_handler
    async def mount_init(self, cmd: sk.Init):
        self.require_connected()
        if not self.state.has_been_homed:
            await self.mount_home(sk.Home())

        # Initialize the optical tube
        await self.init_ot()

    @sk.command_handler
    async def mount_deinit(self, cmd: sk.Deinit):
        await self.mount_stop(sk.Stop())
        await self.mount_park(sk.MoveToPark())
        await self.api.call("v1_disable_mount_motors")
        logger.debug("disabled mount motors")
        await self.deinit_ot()

    @sk.on_detach
    async def entity_deinit(self):
        logger.debug("stopping node_platform mount status loop")
        self._stop_fast_status()
        await self.stop_status_loop()

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
                await asyncio.sleep(self.config.status_frequency_slow)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed node_platform mount")

    @sk.command_handler
    async def mount_park(self, cmd: sk.MoveToPark):
        self.require_connected()
        logger.debug("parking node_platform mount")

        self._stop_fast_status()
        await self.api.call("v1_park_mount")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.mount_slewing is None or self.mount_slewing:
                await asyncio.sleep(self.config.status_frequency_slow)

        logger.debug("parked node_platform mount")

    @sk.command_handler
    async def mount_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform mount")

        self._stop_fast_status()
        await self.api.call("v1_halt_mount")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.mount_slewing is None or self.mount_slewing:
                await asyncio.sleep(self.config.status_frequency_slow)

        logger.debug("stopped node_platform mount")

    @sk.command_handler
    async def mount_follow_target(self, cmd: sk.FollowTarget):
        if not self.device_connected:
            await self.mount_init(sk.Init())

        match cmd.target:
            case ICRSTarget():
                logger.debug("executing RADec follow")

                req = osapi.V1GoToMountCoordinatesRequest(
                    ra=cmd.target.right_ascension_hours * 15,
                    dec=cmd.target.declination,
                )
                await self.api.call("v1_go_to_mount_coordinates", req)
                self._start_fast_status()
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self.mount_tracking is None or not self.mount_tracking:
                        await asyncio.sleep(self.config.status_frequency_fast)

                logger.debug("following RA/Dec target")

            case AltAzTarget():
                logger.debug("executing AltAz follow")

                req = osapi.V1GoToMountCoordinatesRequest(
                    altitude=cmd.target.altitude_degrees,
                    azimuth=cmd.target.azimuth_degrees,
                )
                await self.api.call("v1_go_to_mount_coordinates", req)
                self._start_fast_status()
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self.mount_slewing is None or self.mount_slewing:
                        await asyncio.sleep(self.config.status_frequency_fast)

                logger.debug("following Alt/Az target")

            case TLETarget():
                logger.debug("executing TLE follow")

                req = osapi.V1FollowTLERequest(
                    tle_line1=cmd.target.tle.line1,
                    tle_line2=cmd.target.tle.line2,
                )
                await self.api.call("v1_mount_follow_tle", req)
                self._start_fast_status()
                await asyncio.sleep(0.1)

                async with asyncio.timeout(self.config.timeout):
                    while self.mount_tracking is None or not self.mount_tracking:
                        await asyncio.sleep(self.config.status_frequency_fast)

                logger.debug("following TLE target")

            case FrameTarget():
                match cmd.target.frame:
                    case ReferenceFrame.ALTAZ:
                        logger.debug("stopping tracking")
                        self._stop_fast_status()
                        await self.api.call("v1_disable_mount_tracking")

                        async with asyncio.timeout(self.config.timeout):
                            while (
                                self.mount_slewing is None
                                or self.mount_slewing
                                or self.mount_tracking is None
                                or self.mount_tracking
                            ):
                                await asyncio.sleep(self.config.status_frequency_slow)

                    case ReferenceFrame.ICRF:
                        logger.debug("executing sidereal track")
                        await self.api.call("v1_enable_mount_tracking")
                        self._start_fast_status()
                        await asyncio.sleep(0.1)

                        async with asyncio.timeout(self.config.timeout):
                            while self.mount_tracking is None or not self.mount_tracking:
                                await asyncio.sleep(self.config.status_frequency_fast)

                    case _:
                        raise RuntimeError(f"Need specific target to track {cmd.target.frame}")

            case _:
                track_type = type(cmd.target).__name__
                raise NotImplementedError(
                    f"{track_type} tracking via Node Platform is not supported"
                )

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

    async def status_publish_fast(self):
        while True:
            # Mount
            try:
                status: osapi.V2MountStatus = await self.api.call("v2_get_mount_status")
                await self._publish_mount_status(status)
            except Exception as e:
                logger.warning(f"Error in fast status_publish ({e})")

            await asyncio.sleep(self.config.status_frequency_fast)

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

        # Turn on all fans
        all_fan_roles = list(osapi.V1OpticalTubeFanRole)
        await self.api.call(
            "v1_turn_on_optical_tube_fans",
            osapi.V1TurnOnOpticalTubeFansRequest(roles=all_fan_roles),
        )
        logger.debug(f"turned on all fans: {[r.value for r in all_fan_roles]}")

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

        # Turn off all fans
        all_fan_roles = list(osapi.V1OpticalTubeFanRole)
        await self.api.call(
            "v1_turn_off_optical_tube_fans",
            osapi.V1TurnOffOpticalTubeFansRequest(roles=all_fan_roles),
        )
        logger.debug(f"turned off all fans: {[r.value for r in all_fan_roles]}")


class NodePlatformMountConfig(NodePlatformDeviceConfig[NodePlatformMount]):
    """Node Platform Mount configuration."""

    device_type: Literal["mount"] = "mount"
    heater_power: dict[str, float] = Field(default_factory=dict)
    timeout: float = 300.0
    status_frequency_slow: float = 1.0
    status_frequency_fast: float = 0.1

    @override
    def create_device(self):
        return NodePlatformMount(self)


class NodePlatformMountState(NodePlatformDeviceState):
    """Node Platform Mount state."""

    device_type: Literal["mount"] = "mount"
    has_been_homed: bool = False
