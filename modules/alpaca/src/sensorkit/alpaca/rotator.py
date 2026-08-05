# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.rotator import Rotator
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.std import Connect, Connected, Disconnect, Stop
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition


class AlpacaRotatorState(AlpacaDeviceState):
    device_type: Literal["rotator"] = "rotator"


@sk.declare_keyword
class AlpacaRotatorStatus(BaseModel):
    """IRotatorV4 properties."""

    mechanical_position: float | None = None
    position: float | None = None
    target_position: float | None = None
    is_moving: bool = False
    reverse: bool = False
    can_reverse: bool = False
    step_size: float | None = None


@sk.declare_device
class AlpacaRotator(AlpacaDevice):
    """Alpaca Rotator implementation."""

    config: AlpacaRotatorConfig
    device_name = "Rotator"
    state_model = AlpacaRotatorState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        self.rotator_position: float | None = None

        # Initialize the rotator
        await self._initialize()
        self.start_status_loop(self.status_publish())

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.rotator_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.rotator_connect(Connect())
        self.rotator = Rotator(self.address, self.config.device_number, self.config.protocol)
        await self.rotator_connect(Connect())

        # Read capabilities
        self._can_reverse = await self.get(self.rotator, "CanReverse", False)
        self._step_size = await self.get(self.rotator, "StepSize", None)

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

        target = cmd.position

        await self.call(self.rotator, "MoveAbsolute", target)

        async with asyncio.timeout(self.config.timeout):
            while True:
                is_moving = await self.get(self.rotator, "IsMoving", False)
                if not is_moving:
                    break
                await asyncio.sleep(1)

        logger.debug(f"changed rotator position to {self.rotator_position:.2f}°")

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
                    is_moving = await self.get(r, "IsMoving", False)
                    reverse = await self.get(r, "Reverse", False) if self._can_reverse else False

                    if mechanical_position is not None:
                        self.rotator_position = float(mechanical_position)

                    await device.publish(RotatorPosition(position=self.rotator_position or 0.0))
                    properties: dict = {
                        "is_moving": is_moving,
                    }
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

                    # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                    # logger.debug(
                    #     f"Alpaca rotator status: connected={connected}, position={position}, "
                    #     f"target_position={target_position}, is_moving={is_moving}, "
                    #     f"{properties_str}"
                    # )

                    await device.publish(AlpacaRotatorStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in rotator status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class AlpacaRotatorConfig(AlpacaDeviceConfig[AlpacaRotator]):
    device_type: Literal["rotator"] = "rotator"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AlpacaRotator(self)
