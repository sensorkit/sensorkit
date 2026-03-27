import asyncio
from typing import Literal

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.pwi4.api import PWI4, RotatorAPI
from sensorkit.pwi4.models import PWI4StatusModel


class RotatorConfig(BaseModel):
    """Configuration specific to rotator devices."""
    device_type: Literal["rotator"] = "rotator"
    entity: str
    status_frequency: float = 1.0

class RotatorTelemetryPublisher:
    def __init__(self, *, binding: sk.DeviceImpl, api: RotatorAPI, frequency: float = 1.0):
        self.binding = binding
        self.api = api
        self.frequency = frequency
        self._task: asyncio.Task | None = None

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()

    async def _run(self):
        backoff = 1.0
        while True:
            try:
                status: PWI4StatusModel = await self.api.status()
                backoff = 1.0
                await asyncio.gather(
                    self.publish_position(status),
                    self.publish_connected(status),
                    self.publish_enabled(status),
                )
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_position(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.RotatorPosition(position=s.rotator.mech_position_degs)
            )
        except Exception:
            return None

    async def publish_connected(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.Connected(is_connected=bool(s.rotator.is_connected))
            )
        except Exception:
            return None

    async def publish_enabled(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.Enabled(is_enabled=bool(s.rotator.is_enabled))
            )
        except Exception:
            return None


@sk.declare_device
class RotatorDevice:

    def __init__(
        self,
        api: PWI4,
        config: RotatorConfig,
    ):
        self.rotator: RotatorAPI = api.rotator
        self.config = config
        self.telemetry: RotatorTelemetryPublisher | None = None

    @sk.on_attach
    async def startup(self):
        await self.rotator.connect()
        await self.rotator.enable()
        self.telemetry = RotatorTelemetryPublisher(
            binding=sk.device(),
            api=self.rotator,
            frequency=self.config.status_frequency
        )
        await self.telemetry.start()

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry:
            await self.telemetry.stop()
        await self.rotator.disable()
        await self.rotator.disconnect()

    @sk.command_handler
    async def move(self, cmd: sk.ChangeRotatorPosition):
        await self.rotator.goto_mech(cmd.position)

    @sk.command_handler
    async def halt(self, cmd: sk.Stop):
        await self.rotator.stop()

    @sk.command_handler
    async def connect(self, cmd: sk.Connect):
        await self.rotator.connect()

    @sk.command_handler
    async def disconnect(self, cmd: sk.Disconnect):
        await self.rotator.disconnect()

    @sk.command_handler
    async def enable(self, cmd: sk.Enable):
        await self.rotator.enable()

    @sk.command_handler
    async def disable(self, cmd: sk.Disable):
        await self.rotator.disable()


