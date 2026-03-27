import asyncio
from typing import Self

from alpaca.rotator import Rotator as AscomRotator
from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected


class RotatorTelemetryPublisher:
    """Publishes rotator telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        rotator: AscomRotator,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.rotator = rotator
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
                    self.publish_position(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Rotator telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.rotator.Connected)
            await self.binding.publish(
                Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_position(self):
        try:
            position = await asyncio.to_thread(getattr, self.rotator, "Position", None)
            if position is not None:
                await self.binding.publish(
                    sk.RotatorPosition(position=float(position))
                )
        except Exception:
            return None


@sk.declare_device
class RotatorService:

    def __init__(
        self,
        device: AscomRotator,
        status_frequency: float,
    ):
        self.rotator = device
        self.status_frequency = status_frequency
        self.telemetry_publisher: RotatorTelemetryPublisher | None = None

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config=None,
    ) -> Self:
        device_number = config.device_number if config else 0
        status_frequency = config.status_frequency if config else 1.0
        return cls(
            device=AscomRotator(address=device_url, device_number=device_number),
            status_frequency=status_frequency,
        )

    @sk.on_attach
    async def startup(self):
        self.rotator.Connected = True
        await sk.device().publish(Connected(is_connected=True))

        # Initialize and start telemetry publisher
        self.telemetry_publisher = RotatorTelemetryPublisher(
            binding=sk.device(),
            rotator=self.rotator,
            frequency=self.status_frequency
        )
        await self.telemetry_publisher.start()

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()
        self.rotator.Connected = False
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def rotator_connect(self, cmd: sk.Connect):
        self.rotator.Connected = True
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def rotator_disconnect(self, cmd: sk.Disconnect):
        self.rotator.Connected = False
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def rotator_move(self, cmd: sk.ChangeRotatorPosition):
        await asyncio.to_thread(self.rotator.MoveMechanical, cmd.position)

    # @rotator.command_handler(RotatorSync)
    # async def rotator_sync(cmd: RotatorSync):
    #     await asyncio.to_thread(self.device.Sync, cmd.position)

    @sk.command_handler
    async def rotator_halt(self, cmd: sk.Stop):
        await asyncio.to_thread(self.rotator.Halt)
