import asyncio
from typing import Self

from alpaca.focuser import Focuser as AscomFocuser
from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import (
    ChangeFocusPosition,
    Connect,
    Connected,
    Disconnect,
    FocusPosition,
    Stop,
)


class FocuserTelemetryPublisher:
    """Publishes focuser telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        focuser: AscomFocuser,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.focuser = focuser
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
                    self.publish_temperature(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Focuser telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.focuser.Connected)
            await self.binding.publish(
                Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_position(self):
        try:
            position = await asyncio.to_thread(getattr, self.focuser, "Position", None)
            if position is not None:
                await self.binding.publish(
                    FocusPosition(position=int(position))
                )
        except Exception:
            return None

    async def publish_temperature(self):
        try:
            temp = await asyncio.to_thread(getattr, self.focuser, "Temperature", None)
            if temp is not None:
                await self.binding.publish(
                    sk.Temperature(temperature=float(temp))
                )
        except Exception:
            return None


@sk.declare_device
class FocuserService:

    def __init__(
        self,
        device: AscomFocuser,
        status_frequency: float,
    ):
        self.focuser = device
        self.status_frequency = status_frequency
        self.telemetry_publisher: FocuserTelemetryPublisher | None = None

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config=None,
    ) -> Self:
        device_number = config.device_number if config else 0
        status_frequency = config.status_frequency if config else 1.0
        return cls(
            device=AscomFocuser(address=device_url, device_number=device_number),
            status_frequency=status_frequency,
        )

    @sk.on_attach
    async def startup(self):
        self.focuser.Connected = True
        await sk.device().publish(Connected(is_connected=True))

        # Initialize and start telemetry publisher
        self.telemetry_publisher = FocuserTelemetryPublisher(
            binding=sk.device(),
            focuser=self.focuser,
            frequency=self.status_frequency
        )
        await self.telemetry_publisher.start()

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()
        self.focuser.Connected = False
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_connect(self, cmd: Connect):
        self.focuser.Connected = True
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def focuser_disconnect(self, cmd: Disconnect):
        self.focuser.Connected = False
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_move(self, cmd: ChangeFocusPosition):
        await asyncio.to_thread(self.focuser.Move, int(cmd.position))

    @sk.command_handler
    async def focuser_halt(self, cmd: Stop):
        await asyncio.to_thread(self.focuser.Halt)
