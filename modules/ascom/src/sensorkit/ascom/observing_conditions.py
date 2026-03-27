import asyncio
from typing import Self

from alpaca.observingconditions import ObservingConditions as AscomObservingConditions
from loguru import logger

import sensorkit.api as sk


class ObservingConditionsTelemetryPublisher:
    """Publishes observing conditions telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        observing_conditions: AscomObservingConditions,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.oc = observing_conditions
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
                    self.publish_basic_weather(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Observing conditions telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.oc.Connected)
            await self.binding.publish(
                sk.Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_basic_weather(self):
        try:
            weather = sk.BasicWeather()

            try:
                weather.temperature = await asyncio.to_thread(lambda: self.oc.Temperature)
            except Exception:
                weather.temperature = None

            try:
                weather.humidity = await asyncio.to_thread(lambda: self.oc.Humidity)
            except Exception:
                weather.humidity = None

            try:
                weather.pressure = await asyncio.to_thread(lambda: self.oc.Pressure)
            except Exception:
                weather.pressure = None

            try:
                weather.cloud_cover = await asyncio.to_thread(lambda: self.oc.CloudCover)
            except Exception:
                weather.cloud_cover = None

            try:
                weather.dew_point = await asyncio.to_thread(lambda: self.oc.DewPoint)
            except Exception:
                weather.dew_point = None

            try:
                weather.rain_rate = await asyncio.to_thread(lambda: self.oc.RainRate)
            except Exception:
                weather.rain_rate = None

            try:
                weather.wind_speed = await asyncio.to_thread(lambda: self.oc.WindSpeed)
            except Exception:
                weather.wind_speed = None

            await self.binding.publish(weather)
        except Exception:
            return None


@sk.declare_device
class ObservingConditionsService:

    def __init__(
        self,
        device: AscomObservingConditions,
        status_frequency: float,
    ):
        self.observingconditions = device
        self.status_frequency = status_frequency
        self.telemetry_publisher: ObservingConditionsTelemetryPublisher | None = None

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config=None,
    ) -> Self:
        device_number = config.device_number if config else 0
        status_frequency = config.status_frequency if config else 1.0
        return cls(
            device=AscomObservingConditions(address=device_url, device_number=device_number),
            status_frequency=status_frequency,
        )

    @sk.on_attach
    async def startup(self):
        self.observingconditions.Connected = True
        await sk.device().publish(sk.Connected(is_connected=True))

        self.telemetry_publisher = ObservingConditionsTelemetryPublisher(
            binding=sk.device(),
            observing_conditions=self.observingconditions,
            frequency=self.status_frequency
        )
        await self.telemetry_publisher.start()

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()
        self.observingconditions.Connected = False
        await sk.device().publish(sk.Connected(is_connected=False))

    @sk.command_handler
    async def observing_conditions_connect(self, cmd: sk.Connect):
        self.observingconditions.Connected = True
        await sk.device().publish(sk.Connected(is_connected=True))

    @sk.command_handler
    async def observing_conditions_disconnect(self, cmd: sk.Disconnect):
        self.observingconditions.Connected = False
        await sk.device().publish(sk.Connected(is_connected=False))
