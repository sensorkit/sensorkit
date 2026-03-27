import asyncio
from typing import Literal

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.pwi4.api import PWI4, FocuserAPI
from sensorkit.pwi4.models import PWI4StatusModel


class FocuserConfig(BaseModel):
    device_type: Literal["focuser"] = "focuser"
    entity: str
    status_frequency: float = 1.0
    disable_on_deinit: bool = False

class FocuserTelemetryPublisher:
    def __init__(self, *, binding: sk.DeviceImpl, api: FocuserAPI, frequency: float = 1.0):
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
                    self.publish_focus_position(status),
                    self.publish_connected(status),
                    self.publish_enabled(status),
                )
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception:
                # Keep publishing resilient; back off on failures
                # (logger available via DeviceImpl if needed)
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_focus_position(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.FocusPosition(position=s.focuser.position)
            )
        except Exception:
            return None

    async def publish_connected(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.Connected(is_connected=bool(s.focuser.is_connected))
            )
        except Exception:
            return None

    async def publish_enabled(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.Enabled(is_enabled=bool(s.focuser.is_enabled))
            )
        except Exception:
            return None


@sk.declare_device
class FocuserDevice:

    def __init__(self, api: PWI4, config: FocuserConfig):
        self.focuser: FocuserAPI = api.focuser
        self.config = config
        self.telemetry: FocuserTelemetryPublisher | None = None

    @sk.on_attach
    async def startup(self):
        await self.focuser.connect()
        await self.focuser.enable()
        self.telemetry = FocuserTelemetryPublisher(
            binding=sk.device(),
            api=self.focuser,
            frequency=self.config.status_frequency
        )
        await self.telemetry.start()

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry:
            await self.telemetry.stop()
        if self.config.disable_on_deinit:
            await self.focuser.disable()
        await self.focuser.disconnect()

    @sk.command_handler
    async def halt(self, cmd: sk.Stop):
        await self.focuser.stop()

    @sk.command_handler
    async def move(self, cmd: sk.ChangeFocusPosition):
        await self.focuser.goto(cmd.position)

    @sk.command_handler
    async def connect(self, _: sk.Connect):
        await self.focuser.connect()

    @sk.command_handler
    async def disconnect(self, _: sk.Disconnect):
        await self.focuser.disconnect()

    @sk.command_handler
    async def enable(self, _: sk.Enable):
        await self.focuser.enable()

    @sk.command_handler
    async def disable(self, _: sk.Disable):
        await self.focuser.disable()
