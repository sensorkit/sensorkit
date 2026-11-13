# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.common.aio import AsyncLoop
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std import Connect, Connected, Disconnect, Stop
from sensorkit.std.focus import ChangeFocusPosition, FocusPosition, StandardFocuser


@sk.declare_keyword
class NinaFocuserStatus(BaseModel):
    position: float | None = None
    is_moving: bool = False
    temperature: float | None = None
    max_step: int | None = None
    step_size: float | None = None


@sk.declare_device(type=StandardFocuser)
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
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
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
        info = await self.info("focuser")
        maximum = info.get("MaxStep")

        if cmd.position < 0 or (maximum is not None and cmd.position > maximum):
            raise ValueError(f"Focuser position {cmd.position} lies outside travel limits")

        target = int(cmd.position)
        logger.debug(f"changing focuser to {target}")

        await self.client.get("/equipment/focuser/move", position=target)
        position = None

        try:
            async with asyncio.timeout(self.config.timeout):
                while True:
                    info = await self.info("focuser")
                    position = info.get("Position")

                    if position is None:
                        raise RuntimeError("Focuser returned no position after the move")

                    self.focuser_position = float(position)

                    if info.get("IsMoving") is False and self.focuser_position == target:
                        break

                    await asyncio.sleep(self.config.status_frequency)
        except TimeoutError:
            raise TimeoutError(
                f"Focuser did not stop at {target}; last position {position}"
            ) from None

        await sk.device().publish(FocusPosition(position=self.focuser_position))

        logger.debug(f"changed focuser to {self.focuser_position}")

    async def status_publish(self):
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

            # fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
            # logger.debug(
            #     f"NINA focuser status: connected={connected}, {fields_str}"
            # )


class NinaFocuserConfig(NinaDeviceConfig[NinaFocuser]):
    device_type: Literal["focuser"] = "focuser"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return NinaFocuser(self)


class NinaFocuserState(NinaDeviceState):
    device_type: Literal["focuser"] = "focuser"
