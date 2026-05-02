from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition


@sk.declare_keyword
class NinaRotatorStatus(BaseModel):
    mechanical_position: float | None = None
    position: float | None = None
    is_moving: bool = False
    step_size: float | None = None


@sk.declare_device
class NinaRotator(NinaDevice):
    """NINA Rotator implementation."""

    config: NinaRotatorConfig
    device_name = "Rotator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaRotatorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaRotatorState()

        self.rotator_position: float | None = None

        await self.rotator_init(sk.Init())
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.rotator_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def rotator_init(self, cmd: sk.Init):
        self._reconnect = lambda: self.rotator_connect(sk.Connect())
        await self.rotator_connect(sk.Connect())

    @sk.command_handler
    async def rotator_deinit(self, cmd: sk.Deinit):
        await self.rotator_stop(sk.Stop())
        await self.rotator_disconnect(sk.Disconnect())

    @sk.command_handler
    async def rotator_connect(self, cmd: sk.Connect):
        await self.connect("rotator")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def rotator_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("rotator")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def rotator_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping rotator")
        await self.client.get("/equipment/rotator/stop-move")
        logger.debug("stopped rotator")

    @sk.command_handler
    async def rotator_change(self, cmd: ChangeRotatorPosition):
        await self.require_connected()
        target = cmd.position
        logger.debug(f"changing rotator to position {target:.1f}°")
        await self.client.get(
            "/equipment/rotator/move",
            positionAngle=target,
            waitToFinish=True,
        )
        logger.debug(f"changed rotator to position {self.rotator_position:.1f}°")

    async def status_publish(self):
        while True:
            try:
                info = await self.info("rotator")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    mechanical_position = info.get("MechanicalPosition")
                    position = info.get("Position")

                    if mechanical_position is not None:
                        self.rotator_position = float(mechanical_position)
                        await device.publish(RotatorPosition(position=self.rotator_position))

                    fields: dict = {"is_moving": info.get("IsMoving", False)}
                    if mechanical_position is not None:
                        fields["mechanical_position"] = float(mechanical_position)
                    if position is not None:
                        fields["position"] = float(position)
                    step_size = info.get("StepSize")
                    if step_size is not None:
                        fields["step_size"] = step_size

                    await device.publish(NinaRotatorStatus(**fields))

                    fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                    # logger.debug(f"NINA rotator status: connected={connected}, {fields_str}")
            except Exception as e:
                logger.exception(f"Error in rotator status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaRotatorConfig(NinaDeviceConfig[NinaRotator]):
    device_type: Literal["rotator"] = "rotator"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return NinaRotator(self)


class NinaRotatorState(NinaDeviceState):
    device_type: Literal["rotator"] = "rotator"
