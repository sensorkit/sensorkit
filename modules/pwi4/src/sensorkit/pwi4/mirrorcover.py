import asyncio
from typing import Literal

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.pwi4.api import PWI4, MirrorCoverAPI
from sensorkit.pwi4.models import PWI4StatusModel
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


class Reset(sk.DeviceCommand):
    command_id: Literal["Reset"] = "Reset"

class MirrorCoverConfig(BaseModel):
    """Configuration specific to mirror cover devices."""
    device_type: Literal["mirrorcover"] = "mirrorcover"
    entity: str
    status_frequency: float = 1.0

class MirrorCoverTelemetryPublisher:
    def __init__(self, *, binding: sk.DeviceImpl, api: MirrorCoverAPI, frequency: float = 1.0):
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
                    self.publish_connected(status),
                )
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self, s: PWI4StatusModel):
        try:
            await self.binding.publish(
                sk.Connected(is_connected=bool(s.mirrorcover.is_connected))
            )
        except Exception:
            return None


@sk.declare_device
class MirrorCoverDevice:

    def __init__(
        self,
        api: PWI4,
        config: MirrorCoverConfig,
    ):
        self.cover: MirrorCoverAPI = api.mirrorcover
        self.config = config
        self.telemetry: MirrorCoverTelemetryPublisher | None = None

    @sk.on_attach
    async def init(self):
        await self.cover.connect()
        self.telemetry = MirrorCoverTelemetryPublisher(
            binding=sk.device(),
            api=self.cover,
            frequency=self.config.status_frequency
        )
        await self.telemetry.start()

    @sk.on_detach
    async def deinit(self):
        if self.telemetry:
            await self.telemetry.stop()
        await self.cover.disconnect()

    @sk.command_handler
    async def open(self, cmd: OpenMirrorCover):
        await self.cover.open()

    @sk.command_handler
    async def close(self, cmd: CloseMirrorCover):
        await self.cover.close()

    @sk.command_handler
    async def stop(self, cmd: sk.Stop):
        await self.cover.stop()

    @sk.command_handler
    async def reset(self, cmd: Reset):
        await self.cover.reset()
