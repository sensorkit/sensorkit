from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std.optics import ChangeFocusPosition, FocusPosition


@sk.declare_keyword
class NinaFocuserStatus(BaseModel):
    position: float | None = None
    is_moving: bool = False
    temperature: float | None = None
    max_step: int | None = None
    step_size: float | None = None


@sk.declare_device
class NinaFocuser(NinaDevice):
    """NINA Focuser implementation."""

    config: NinaFocuserConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaFocuserState)
        except Exception:
            self.state = NinaFocuserState()

        await self.focuser_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.focuser_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def focuser_init(self, cmd: sk.Init):
        self.device_name = "Focuser"
        self.focuser_position: float | None = None

        await self.connect("focuser")
        await sk.device().publish(Connected(is_connected=True))
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(0.5)

    @sk.command_handler
    async def focuser_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.disconnect("focuser")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_connect(self, cmd: sk.Connect):
        await self.connect("focuser")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def focuser_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("focuser")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_move(self, cmd: ChangeFocusPosition):
        self.require_connected()
        position = int(cmd.position)
        logger.debug(f"moving focuser to {position}")
        await self.client.get("/equipment/focuser/move", position=position)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("focuser")
                if not info.get("IsMoving", True):
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug(f"moved focuser to {self.focuser_position}")

    @sk.command_handler
    async def focuser_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping focuser")
        await self.client.get("/equipment/focuser/stop-move")
        logger.info("stopped focuser")

    async def status_publish(self):
        while True:
            try:
                info = await self.info("focuser")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    position = info.get("Position")
                    if position is not None:
                        self.focuser_position = float(position)
                        await device.publish(FocusPosition(position=self.focuser_position))

                    fields: dict = {
                        "position": self.focuser_position,
                        "is_moving": info.get("IsMoving", False),
                    }

                    for key, info_key in (
                        ("temperature", "Temperature"),
                        ("max_step", "MaxStep"),
                        ("step_size", "StepSize"),
                    ):
                        val = info.get(info_key)
                        if val is not None:
                            fields[key] = val

                    fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                    # logger.debug(f"NINA focuser status: connected={connected}, {fields_str}")

                    await device.publish(NinaFocuserStatus(**fields))
            except Exception as e:
                logger.exception(f"Error in focuser status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class NinaFocuserConfig(NinaDeviceConfig[NinaFocuser]):
    device_type: Literal["focuser"] = "focuser"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NinaFocuser(self)


class NinaFocuserState(NinaDeviceState):
    device_type: Literal["focuser"] = "focuser"
