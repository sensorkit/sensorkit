from __future__ import annotations

import asyncio
from typing import Literal, override

from alpaca.rotator import Rotator
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition


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

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(AlpacaRotatorState)
        except Exception:
            self.state = AlpacaRotatorState()

        await self.rotator_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.rotator_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def rotator_init(self, cmd: sk.Init):
        self.device_name = "Rotator"
        self.rotator = Rotator(
            self.address, self.config.device_number, self.config.protocol
        )
        self.rotator_position: float | None = None

        await self.rotator_connect(sk.Connect())

        # Read capabilities
        self._can_reverse = await self.get(self.rotator, "CanReverse", False)
        self._step_size = await self.get(self.rotator, "StepSize", None)

        self.start_status_loop(self.status_publish())

        # Wait for initial position
        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def rotator_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.rotator_disconnect(sk.Disconnect())

    @sk.command_handler
    async def rotator_connect(self, cmd: sk.Connect):
        await self.connect(self.rotator, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def rotator_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.rotator)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def rotator_change(self, cmd: ChangeRotatorPosition):
        self.require_connected()
        target = cmd.position

        logger.debug(f"changing position to {target:.2f}°")

        await self.call(self.rotator, "MoveAbsolute", target)

        async with asyncio.timeout(self.config.timeout):
            while True:
                is_moving = await self.get(self.rotator, "IsMoving", False)
                if not is_moving:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug(f"changed position to {self.rotator_position:.2f}°")

    @sk.command_handler
    async def rotator_stop(self, cmd: sk.Stop):
        self.require_connected()
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
                    reverse = (
                        await self.get(r, "Reverse", False)
                        if self._can_reverse
                        else False
                    )

                    if mechanical_position is not None:
                        self.rotator_position = float(mechanical_position)

                    await device.publish(
                        RotatorPosition(position=self.rotator_position or 0.0)
                    )
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

                    properties_str = ", ".join(
                        f"{k}={v}" for k, v in properties.items()
                    )
                    # logger.debug(
                    #     f"Alpaca rotator status: connected={connected}, position={position}, "
                    #     f"target_position={target_position}, is_moving={is_moving}, "
                    #     f"{properties_str}"
                    # )

                    await device.publish(AlpacaRotatorStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in rotator status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AlpacaRotatorConfig(AlpacaDeviceConfig[AlpacaRotator]):
    device_type: Literal["rotator"] = "rotator"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return AlpacaRotator(self)


class AlpacaRotatorState(AlpacaDeviceState):
    device_type: Literal["rotator"] = "rotator"
