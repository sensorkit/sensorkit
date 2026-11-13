# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.focuser import Focuser
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import Connect, Connected, Disconnect, Stop, Temperature, TemperatureUnit
from sensorkit.std.focus import ChangeFocusPosition, FocusPosition, StandardFocuser


class AlpacaFocuserState(AlpacaDeviceState):
    device_type: Literal["focuser"] = "focuser"


@sk.declare_keyword
class AlpacaFocuserStatus(BaseModel):
    """IFocuserV4 properties."""

    position: float | None = None
    is_moving: bool = False
    absolute: bool = True
    max_step: int = 0
    max_increment: int = 0
    step_size: float | None = None
    temp_comp: bool = False
    temp_comp_available: bool = False
    temperature: float | None = None


@sk.declare_device(type=StandardFocuser)
class AlpacaFocuser(AlpacaDevice):
    """Alpaca Focuser implementation."""

    config: AlpacaFocuserConfig
    device_name = "Focuser"
    state_model = AlpacaFocuserState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

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
        self.focuser = Focuser(self.address, self.config.device_number, self.config.protocol)
        await self.focuser_connect(Connect())

        f = self.focuser

        # Read capabilities
        self._absolute = await self.get(f, "Absolute", True)
        self._max_step = await self.get(f, "MaxStep", 100000)
        self._max_increment = await self.get(f, "MaxIncrement", 100000)
        self._step_size = await self.get(f, "StepSize", None)
        self._temp_comp_available = await self.get(f, "TempCompAvailable", False)

    @sk.command_handler
    async def focuser_connect(self, cmd: Connect):
        await self.connect(self.focuser, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def focuser_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.focuser)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping focuser")
        await self.call(self.focuser, "Halt")
        logger.debug("stopped focuser")

    @sk.command_handler
    async def focuser_change(self, cmd: ChangeFocusPosition):
        await self.require_connected()
        logger.debug(f"changing focus to position {cmd.position}")

        if not 0 <= cmd.position <= self._max_step:
            raise ValueError(f"Focuser position {cmd.position} lies outside 0 to {self._max_step}")

        target = int(cmd.position)

        match self._absolute:
            case True:
                steps = target
            case False:
                current = await self.get(self.focuser, "Position", None)

                if current is None:
                    raise RuntimeError("Cannot move focuser without its current position")

                steps = target - int(current)

                if abs(steps) > self._max_increment:
                    raise ValueError(
                        f"Focuser travel {steps} exceeds the maximum increment "
                        f"{self._max_increment}"
                    )

        await self.call(self.focuser, "Move", steps)
        position = None

        try:
            async with asyncio.timeout(self.config.timeout):
                while True:
                    is_moving = await self.get(self.focuser, "IsMoving", None)
                    position = await self.get(self.focuser, "Position", None)

                    if position is None:
                        raise RuntimeError("Focuser returned no position after the move")

                    self.focuser_position = float(position)

                    if is_moving is False and self.focuser_position == target:
                        break

                    await asyncio.sleep(self.config.status_frequency)
        except TimeoutError:
            raise TimeoutError(
                f"Focuser did not stop at {target}; last position {position}"
            ) from None

        await sk.device().publish(FocusPosition(position=self.focuser_position))

        logger.debug(f"changed focus to position {self.focuser_position}")

    async def status_publish(self):
        f = self.focuser
        connected = await self.get(f, "Connected", False)
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if connected:
            position = await self.get(f, "Position", None)
            if position is not None:
                self.focuser_position = float(position)
                await device.publish(FocusPosition(position=self.focuser_position))

            is_moving = await self.get(f, "IsMoving", False)
            temp_comp = await self.get(f, "TempComp", False)
            temperature = await self.get(f, "Temperature", None)

            properties: dict = {
                "position": self.focuser_position,
                "is_moving": is_moving,
                "absolute": self._absolute,
                "max_step": self._max_step,
                "max_increment": self._max_increment,
                "temp_comp": temp_comp,
            }

            if self._step_size is not None:
                properties["step_size"] = self._step_size
            if self._temp_comp_available:
                properties["temp_comp_available"] = True
            if temperature is not None:
                properties["temperature"] = temperature

            # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
            # logger.debug(
            #     f"Alpaca focuser status: connected={connected}, position={position}, "
            #     f"is_moving={is_moving}, {properties_str}"
            # )

            await device.publish(AlpacaFocuserStatus(**properties))

            if temperature is not None:
                await device.publish(
                    Temperature(temperature=temperature, units=TemperatureUnit.CELSIUS)
                )


class AlpacaFocuserConfig(AlpacaDeviceConfig[AlpacaFocuser]):
    device_type: Literal["focuser"] = "focuser"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AlpacaFocuser(self)
