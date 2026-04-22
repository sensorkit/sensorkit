from __future__ import annotations

import asyncio
from typing import Literal, override

from alpaca.focuser import Focuser
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected
from sensorkit.std.traits import Temperature, TemperatureUnit
from sensorkit.std.optics import ChangeFocusPosition, FocusPosition


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


@sk.declare_device
class AlpacaFocuser(AlpacaDevice):
    """Alpaca Focuser implementation."""

    config: AlpacaFocuserConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaFocuserState)
        except Exception:
            self.state = AlpacaFocuserState()

        self._start_init(self.focuser_init(sk.Init()))

    @sk.on_detach
    async def entity_deinit(self):
        await self._cancel_init()
        await self.focuser_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def focuser_init(self, cmd: sk.Init):
        self.device_name = "Focuser"
        self.focuser = Focuser(
            self.address, self.config.device_number, self.config.protocol
        )
        self.focuser_position: float | None = None

        await self.focuser_connect(sk.Connect())

        f = self.focuser

        # Read capabilities
        self._absolute = await self.get(f, "Absolute", True)
        self._max_step = await self.get(f, "MaxStep", 100000)
        self._max_increment = await self.get(f, "MaxIncrement", 100000)
        self._step_size = await self.get(f, "StepSize", None)
        self._temp_comp_available = await self.get(f, "TempCompAvailable", False)

        self.start_status_loop(self.status_publish())

        # Wait for initial position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def focuser_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.focuser_disconnect(sk.Disconnect())

    @sk.command_handler
    async def focuser_connect(self, cmd: sk.Connect):
        await self.connect(self.focuser, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def focuser_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.focuser)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def focuser_change(self, cmd: ChangeFocusPosition):
        await self.require_connected()
        target = int(cmd.position)
        target = max(0, min(target, self._max_step))

        logger.debug(f"changing to position {target}")

        if self._absolute:
            await self.call(self.focuser, "Move", target)
        else:
            if self.focuser_position is not None:
                delta = target - int(self.focuser_position)
                delta = max(-self._max_increment, min(delta, self._max_increment))
                await self.call(self.focuser, "Move", delta)
            else:
                logger.warning("Cannot change focuser; current position unknown")
                return

        async with asyncio.timeout(self.config.timeout):
            while True:
                is_moving = await self.get(self.focuser, "IsMoving", False)
                if not is_moving:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug(f"changed to position {self.focuser_position}")

    @sk.command_handler
    async def focuser_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping focuser")
        await self.call(self.focuser, "Halt")
        logger.debug("stopped focuser")

    async def status_publish(self):
        while True:
            try:
                f = self.focuser
                connected = await self.get(f, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    position = await self.get(f, "Position", None)
                    if position is not None:
                        self.focuser_position = float(position)
                        await device.publish(
                            FocusPosition(position=self.focuser_position)
                        )

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

                    properties_str = ", ".join(
                        f"{k}={v}" for k, v in properties.items()
                    )
                    # logger.debug(
                    #     f"Alpaca focuser status: connected={connected}, position={position}, "
                    #     f"is_moving={is_moving}, {properties_str}"
                    # )

                    await device.publish(AlpacaFocuserStatus(**properties))

                    if temperature is not None:
                        await device.publish(
                            Temperature(
                                temperature=temperature, units=TemperatureUnit.CELSIUS
                            )
                        )
            except Exception as e:
                logger.exception(f"Error in focuser status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AlpacaFocuserConfig(AlpacaDeviceConfig[AlpacaFocuser]):
    device_type: Literal["focuser"] = "focuser"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return AlpacaFocuser(self)


class AlpacaFocuserState(AlpacaDeviceState):
    device_type: Literal["focuser"] = "focuser"
