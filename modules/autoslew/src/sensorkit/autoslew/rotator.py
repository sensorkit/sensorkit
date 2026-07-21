# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew rotator (standard ASCOM Alpaca Rotator).

Standard-device wrapper. Autoslew's Rotator has one own action (``settarget``); the
ASA rotator extras (``rotator:setslewoption``/``dontslewtozero``/``homefind``) live on
the Telescope device and will be wired through the backbone later.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Literal, override

from alpaca.rotator import Rotator
from alpaca.telescope import Telescope
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.autoslew.device import (
    AutoslewDevice,
    AutoslewDeviceConfig,
    AutoslewDeviceState,
)
from sensorkit.std import Connect, Connected, Disconnect, Home, Stop
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition


@sk.declare_keyword
class AutoslewRotatorStatus(BaseModel):
    """IRotatorV3 properties."""

    mechanical_position: float | None = None
    position: float | None = None
    target_position: float | None = None
    is_moving: bool = False
    reverse: bool = False
    can_reverse: bool = False
    step_size: float | None = None


@sk.declare_device
class AutoslewRotator(AutoslewDevice):
    """ASA Autoslew rotator implementation."""

    config: AutoslewRotatorConfig
    device_name = "Rotator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(AutoslewRotatorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AutoslewRotatorState()

        self.rotator_position: float | None = None

        await self._initialize()
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.rotator_disconnect(Disconnect())
        with contextlib.suppress(Exception):
            await self.disconnect(self.telescope)
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        self._reconnect = lambda: self.rotator_connect(Connect())
        self.rotator = Rotator(self.address, self.config.device_number, self.config.protocol)
        await self.rotator_connect(Connect())

        self._can_reverse = await self.get(self.rotator, "CanReverse", False)
        self._step_size = await self.get(self.rotator, "StepSize", None)

        # Telescope backbone for ASA rotator extras (rotator:setslewoption, etc.).
        self.telescope = Telescope(self.address, 0, self.config.protocol)
        await self.connect(self.telescope, timeout=self.config.timeout)
        await self._apply_asa_settings()

        # Home once per session (state-gated), like the mount/dome.
        if not self.state.has_been_homed:
            await self.rotator_home(Home())

    async def _apply_asa_settings(self):
        if self.config.slew_option is not None:
            logger.debug(f"setting rotator slew option to {self.config.slew_option}")
            await self.action("rotator:setslewoption", str(self.config.slew_option))

    @sk.command_handler
    async def rotator_home(self, cmd: Home):
        """Home the rotator via the ASA rotator:homefind Telescope action.

        Called once per session at init (state-gated on has_been_homed, like the
        mount/dome), and also available over the command bus.
        """
        await self.require_connected()
        logger.debug("homing rotator")
        await self.action("rotator:homefind")
        async with asyncio.timeout(self.config.timeout):
            while await self.get(self.rotator, "IsMoving", False):
                await asyncio.sleep(1)
        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed rotator")

    async def dont_slew_to_zero(self) -> None:
        """ASA rotator:dontslewtozero — keep mechanical position on the next slew."""
        await self.action("rotator:dontslewtozero")

    @sk.command_handler
    async def rotator_connect(self, cmd: Connect):
        await self.connect(self.rotator, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def rotator_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.rotator)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def rotator_change(self, cmd: ChangeRotatorPosition):
        await self.require_connected()
        logger.debug(f"changing rotator position to {cmd.position:.2f}°")

        await self.call(self.rotator, "MoveAbsolute", cmd.position)

        async with asyncio.timeout(self.config.timeout):
            while await self.get(self.rotator, "IsMoving", False):
                await asyncio.sleep(1)

        logger.debug(f"changed rotator position to {self.rotator_position}")

    @sk.command_handler
    async def rotator_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping rotator")
        await self.call(self.rotator, "Halt")
        logger.debug("stopped rotator")

    async def status_publish(self):
        while True:
            try:
                r = self.rotator
                connected = await self.get(r, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    mechanical_position = await self.get(r, "MechanicalPosition", None)
                    position = await self.get(r, "Position", None)
                    target_position = await self.get(r, "TargetPosition", None)
                    reverse = await self.get(r, "Reverse", False) if self._can_reverse else False

                    if mechanical_position is not None:
                        self.rotator_position = float(mechanical_position)

                    await device.publish(RotatorPosition(position=self.rotator_position or 0.0))
                    properties: dict = {"is_moving": await self.get(r, "IsMoving", False)}
                    if mechanical_position is not None:
                        properties["mechanical_position"] = float(mechanical_position)
                    if position is not None:
                        properties["position"] = float(position)
                    if target_position is not None:
                        properties["target_position"] = float(target_position)
                    if self._can_reverse:
                        properties["can_reverse"] = True
                        properties["reverse"] = reverse
                    if self._step_size is not None:
                        properties["step_size"] = self._step_size

                    await device.publish(AutoslewRotatorStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in rotator status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AutoslewRotatorConfig(AutoslewDeviceConfig[AutoslewRotator]):
    device_type: Literal["rotator"] = "rotator"
    slew_option: int | None = None  # ASA rotator:setslewoption (0=track, 1=North, 2=SmartNS)
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AutoslewRotator(self)


class AutoslewRotatorState(AutoslewDeviceState):
    device_type: Literal["rotator"] = "rotator"
    has_been_homed: bool = False
