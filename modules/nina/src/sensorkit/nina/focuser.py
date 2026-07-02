from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Stop
from sensorkit.std import Connect, Connected, Disconnect
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
    device_name = "Focuser"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaFocuserState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaFocuserState()

        self.focuser_position: float | None = None

        # Initialize the focuser
        await self._initialize()
        self.start_status_loop(self.status_publish())

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.focuser_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.focuser_connect(Connect())
        await self.focuser_connect(Connect())

    @sk.command_handler
    async def focuser_connect(self, cmd: Connect):
        await self.connect("focuser")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def focuser_disconnect(self, cmd: Disconnect):
        await self.disconnect("focuser")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping focuser")
        await self.client.get("/equipment/focuser/stop-move")
        logger.debug("stopped focuser")

    @sk.command_handler
    async def focuser_change(self, cmd: ChangeFocusPosition):
        await self.require_connected()
        position = int(cmd.position)
        logger.debug(f"changing focuser to {position}")

        await self.client.get("/equipment/focuser/move", position=position)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("focuser")
                if not info.get("IsMoving", True):
                    break
                await asyncio.sleep(0.5)

        logger.debug(f"changed focuser to {self.focuser_position}")

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

                    await device.publish(NinaFocuserStatus(**fields))

                    fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                    # logger.debug(
                    #     f"NINA focuser status: connected={connected}, {fields_str}"
                    # )
            except Exception as e:
                logger.exception(f"Error in focuser status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaFocuserConfig(NinaDeviceConfig[NinaFocuser]):
    device_type: Literal["focuser"] = "focuser"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return NinaFocuser(self)


class NinaFocuserState(NinaDeviceState):
    device_type: Literal["focuser"] = "focuser"
