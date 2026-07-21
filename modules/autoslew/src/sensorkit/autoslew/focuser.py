# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew focuser (standard ASCOM Alpaca Focuser).

A standard-device wrapper (Autoslew's Focuser exposes no extension actions of its own
— SupportedActions = 0). The ASA focuser extras (``focuser:homefind``, ``afc:*``) live
on the Telescope device and are driven through the shared Telescope backbone.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Literal, override

from alpaca.focuser import Focuser
from alpaca.telescope import Telescope
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.autoslew.device import (
    AutoslewDevice,
    AutoslewDeviceConfig,
    AutoslewDeviceState,
)
from sensorkit.std import Connect, Connected, Disconnect, Home, Stop, Temperature, TemperatureUnit
from sensorkit.std.optics import ChangeFocusPosition, FocusPosition


@sk.declare_keyword
class AutoslewFocuserStatus(BaseModel):
    """IFocuserV3 properties."""

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
class AutoslewFocuser(AutoslewDevice):
    """ASA Autoslew focuser implementation."""

    config: AutoslewFocuserConfig
    device_name = "Focuser"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(AutoslewFocuserState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AutoslewFocuserState()

        self.focuser_position: float | None = None

        await self._initialize()
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.focuser_disconnect(Disconnect())
        with contextlib.suppress(Exception):
            await self.disconnect(self.telescope)
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        self._reconnect = lambda: self.focuser_connect(Connect())
        self.focuser = Focuser(self.address, self.config.device_number, self.config.protocol)
        await self.focuser_connect(Connect())

        f = self.focuser
        self._absolute = await self.get(f, "Absolute", True)
        self._max_step = await self.get(f, "MaxStep", 100000)
        self._max_increment = await self.get(f, "MaxIncrement", 100000)
        self._step_size = await self.get(f, "StepSize", None)
        self._temp_comp_available = await self.get(f, "TempCompAvailable", False)

        # Telescope backbone for ASA focuser extras (focuser:homefind, afc:*).
        self.telescope = Telescope(self.address, 0, self.config.protocol)
        await self.connect(self.telescope, timeout=self.config.timeout)

        # Home once per session (state-gated), like the mount/dome.
        if not self.state.has_been_homed:
            await self.focuser_home(Home())

    @sk.command_handler
    async def focuser_home(self, cmd: Home):
        """Home the focuser via the ASA focuser:homefind Telescope action.

        Called once per session at init (state-gated on has_been_homed, like the
        mount/dome), and also available over the command bus.
        """
        await self.require_connected()
        logger.debug("homing focuser")
        await self.action("focuser:homefind")
        async with asyncio.timeout(self.config.timeout):
            while await self.get(self.focuser, "IsMoving", False):
                await asyncio.sleep(self.config.status_frequency)
        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed focuser")

    # ---- ASA focuser extras (Telescope-device actions via the backbone) ----- #
    # NB: afc:* parameter formats are a working assumption pending ICD confirmation.
    async def afc_get_focus_position(self) -> str:
        return await self.action("afc:getfocuspos")

    async def afc_set_focus_position(self, position: float) -> None:
        await self.action("afc:setfocuspos", repr(position))

    async def afc_get_filter(self) -> str:
        return await self.action("afc:getfilter")

    async def afc_set_filter(self, filter_index: int) -> None:
        await self.action("afc:setfilter", str(filter_index))

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

        target = max(0, min(int(cmd.position), self._max_step))

        if self._absolute:
            await self.call(self.focuser, "Move", target)
        elif self.focuser_position is not None:
            delta = target - int(self.focuser_position)
            delta = max(-self._max_increment, min(delta, self._max_increment))
            await self.call(self.focuser, "Move", delta)
        else:
            logger.warning("Cannot change focuser; current position unknown")
            return

        async with asyncio.timeout(self.config.timeout):
            while await self.get(self.focuser, "IsMoving", False):
                await asyncio.sleep(self.config.status_frequency)

        logger.debug(f"changed focus to position {self.focuser_position}")

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
                        await device.publish(FocusPosition(position=self.focuser_position))

                    temperature = await self.get(f, "Temperature", None)
                    properties: dict = {
                        "position": self.focuser_position,
                        "is_moving": await self.get(f, "IsMoving", False),
                        "absolute": self._absolute,
                        "max_step": self._max_step,
                        "max_increment": self._max_increment,
                        "temp_comp": await self.get(f, "TempComp", False),
                    }
                    if self._step_size is not None:
                        properties["step_size"] = self._step_size
                    if self._temp_comp_available:
                        properties["temp_comp_available"] = True
                    if temperature is not None:
                        properties["temperature"] = temperature

                    await device.publish(AutoslewFocuserStatus(**properties))
                    if temperature is not None:
                        await device.publish(
                            Temperature(temperature=temperature, units=TemperatureUnit.CELSIUS)
                        )
            except Exception as e:
                logger.exception(f"Error in focuser status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AutoslewFocuserConfig(AutoslewDeviceConfig[AutoslewFocuser]):
    device_type: Literal["focuser"] = "focuser"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AutoslewFocuser(self)


class AutoslewFocuserState(AutoslewDeviceState):
    device_type: Literal["focuser"] = "focuser"
    has_been_homed: bool = False
