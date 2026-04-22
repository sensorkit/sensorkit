from __future__ import annotations

import asyncio
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, RotatorPosition
from sensorkit.std.instrument import ChangeRotatorPosition
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


@sk.declare_device
class NodePlatformRotator(NodePlatformDevice):
    """Node Platform Rotator implementation."""

    config: NodePlatformRotatorConfig
    device_name = "Rotator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformRotatorState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformRotatorState()

        self.rotator_position: float | None = None
        self.rotator_moving: bool | None = None

        # Start rotator status publishing
        logger.debug("starting node_platform rotator status loop")
        self.start_status_loop(self.status_publish())

        # Wait for initial position
        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def rotator_init(self, cmd: sk.Init):
        self.require_connected()
        if self.config.derotate:
            await self.api.call("v1_enable_derotation_compensation")
        else:
            await self.api.call("v1_disable_derotation_compensation")

    @sk.command_handler
    async def rotator_deinit(self, cmd: sk.Deinit):
        self.require_connected()
        await self.api.call("v1_disable_derotation_compensation")
        await self.rotator_stop(sk.Stop())

    @sk.on_detach
    async def entity_deinit(self):
        logger.debug("stopping node_platform rotator status loop")
        await self.stop_status_loop()

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def rotator_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform rotator")
        await self.api.call("v1_halt_rotator")
        logger.debug("stopped node_platform rotator")

    @sk.command_handler
    async def rotator_move(self, cmd: ChangeRotatorPosition):
        self.require_connected()
        logger.debug(f"moving node_platform rotator to position {cmd.position}°")

        req = osapi.V1GoToRotatorPositionRequest(
            position_degrees=cmd.position,
            target_type=osapi.RotatorTargetType.MECHANICAL,
        )
        await self.api.call("v1_go_to_rotator_position", req)
        await asyncio.sleep(0.1)

        # Wait for the rotator move to complete
        async with asyncio.timeout(self.config.timeout):
            while self.rotator_moving is None or self.rotator_moving:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug(f"moved node_platform rotator to position {cmd.position}°")

    async def status_publish(self):
        while True:
            try:
                status: osapi.V1RotatorStatus = await self.api.call("v1_get_rotator_status")
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                self.device_connected = status.connected
                self.rotator_moving = status.moving

                if status.position is not None:
                    self.rotator_position = float(status.position.mechanical_angle_degrees)

                # logger.debug(
                #     f"NodePlatform rotator status: connected={status.connected}, "
                #     f"derotation={status.is_derotation_enabled}, moving={status.moving}, "
                #     f"position={self.rotator_position}°"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))
                if self.rotator_position is not None:
                    await device.publish(RotatorPosition(position=self.rotator_position))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform rotator status ({e})")
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformRotatorConfig(NodePlatformDeviceConfig[NodePlatformRotator]):
    """Node Platform Rotator configuration."""

    device_type: Literal["rotator"] = "rotator"
    derotate: bool = False
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformRotator(self)


class NodePlatformRotatorState(NodePlatformDeviceState):
    """Node Platform Rotator state."""

    device_type: Literal["rotator"] = "rotator"
