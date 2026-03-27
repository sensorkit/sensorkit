import asyncio
from typing import Self

from alpaca.telescope import Telescope as AscomMount
from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.target import AltAzTarget, ICRSTarget
from sensorkit.models.devices import (
    AltAzPointing,
    AxisRate,
    AxisRates,
    Connected,
    RADecPointing,
    ReferenceFrame,
)


class MountTelemetryPublisher:
    """Publishes mount telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        mount: AscomMount,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.mount = mount
        self.frequency = frequency
        self._task: asyncio.Task | None = None

    async def start(self):
        """Start the telemetry publisher background task."""
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        """Stop the telemetry publisher background task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        """Background loop publishing telemetry at the configured frequency."""
        backoff = 1.0
        while True:
            try:
                await asyncio.gather(
                    self.publish_connected(),
                    self.publish_site_position(),
                    self.publish_altaz_pointing(),
                    self.publish_radec_pointing(),
                    self.publish_axis_rates(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Mount telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.mount.Connected)
            await self.binding.publish(
                Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_site_position(self):
        try:
            lat = await asyncio.to_thread(lambda: self.mount.SiteLatitude)
            lon = await asyncio.to_thread(lambda: self.mount.SiteLongitude)
            elev = await asyncio.to_thread(lambda: self.mount.SiteElevation)
            await self.binding.publish(
                sk.SitePosition(
                    latitude_degrees=lat,
                    longitude_degrees=lon,
                    altitude_km=(float(elev) / 1000)
                )
            )
        except Exception:
            return None

    async def publish_altaz_pointing(self):
        try:
            alt = await asyncio.to_thread(lambda: self.mount.Altitude)
            az = await asyncio.to_thread(lambda: self.mount.Azimuth)
            await self.binding.publish(
                AltAzPointing(
                    altitude_degrees=alt,
                    azimuth_degrees=az,
                )
            )
        except Exception:
            return None

    async def publish_radec_pointing(self):
        try:
            ra = await asyncio.to_thread(lambda: self.mount.RightAscension)
            dec = await asyncio.to_thread(lambda: self.mount.Declination)
            await self.binding.publish(
                RADecPointing(
                    right_ascension_hours=ra,
                    declination_degrees=dec,
                    reference_frame=ReferenceFrame.J2000,
                )
            )
        except Exception:
            return None

    async def publish_axis_rates(self):
        """Publish axis rate information for each axis."""
        try:
            ra_rate = await asyncio.to_thread(getattr, self.mount, "RightAscensionRate", None)
            dec_rate = await asyncio.to_thread(getattr, self.mount, "DeclinationRate", None)

            rates = AxisRates()

            if ra_rate is not None:
                rates.azimuth = AxisRate(
                    velocity=float(ra_rate),
                    max_velocity=None,
                    max_acceleration=None,
                    min_mechanical_position=None,
                    max_mechanical_position=None,
                    axis=sk.MountAxis.AZIMUTH,
                )

            if dec_rate is not None:
                rates.altitude = AxisRate(
                    velocity=float(dec_rate),
                    max_velocity=None,
                    max_acceleration=None,
                    min_mechanical_position=None,
                    max_mechanical_position=None,
                    axis=sk.MountAxis.ALTITUDE,
                )

            await self.binding.publish(rates)

        except Exception:
            return None


@sk.declare_device
class MountService:

    def __init__(
        self,
        device: AscomMount,
        status_frequency: float,
    ):
        self.mount = device
        self.status_frequency = status_frequency
        self.telemetry_publisher: MountTelemetryPublisher | None = None

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config=None,
    ) -> Self:
        device_number = config.device_number if config else 0
        status_frequency = config.status_frequency if config else 1.0
        return cls(
            device=AscomMount(address=device_url, device_number=device_number),
            status_frequency=status_frequency,
        )

    @sk.on_attach
    async def startup(self):
        self.mount.Connected = True
        await sk.device().publish(sk.Connected(is_connected=True))

        # Initialize and start telemetry publisher
        self.telemetry_publisher = MountTelemetryPublisher(
            binding=sk.device(),
            mount=self.mount,
            frequency=self.status_frequency
        )
        await self.telemetry_publisher.start()

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()
        self.mount.Connected = False
        await sk.device().publish(sk.Connected(is_connected=False))

    @sk.command_handler
    async def mount_connect(self, cmd: sk.Connect):
        self.mount.Connected = True
        await sk.device().publish(sk.Connected(is_connected=True))

    @sk.command_handler
    async def mount_disconnect(self, cmd: sk.Disconnect):
        self.mount.Connected = False
        await sk.device().publish(sk.Connected(is_connected=False))

    @sk.command_handler
    async def mount_park(self, cmd: sk.MoveToPark):
        self.mount.Park()

    @sk.command_handler
    async def mount_move_home(self, cmd: sk.Home):
        self.mount.FindHome()

    @sk.command_handler
    async def mount_slew_alt_az(self, cmd: sk.FollowTarget):
        async def _target_acquisition():
            while self.mount.Slewing:
                await asyncio.sleep(0.1)

        if isinstance(cmd.target, AltAzTarget):
            self.mount.Tracking = False  # cant slew alt az with tracking on
            self.mount.SlewToAltAzAsync(
                Azimuth=cmd.target.coords.az,
                Altitude=cmd.target.coords.alt,
            )
        elif isinstance(cmd.target, ICRSTarget):
            self.mount.Tracking = True
            self.mount.SlewToCoordinatesAsync(
                RightAscension=cmd.target.coords.ra / 15,
                Declination=cmd.target.coords.dec,
            )

        await asyncio.sleep(0.1)
        await _target_acquisition()
