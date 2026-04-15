from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import ourskyai_node_platform_api as osapi

import sensorkit.api as sk
from sensorkit.models.devices import Connected, FocusPosition
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


@sk.declare_device
class NodePlatformFocuser(NodePlatformDevice):
    """Node Platform Focuser implementation."""
    config: NodePlatformFocuserConfig
    device_name = "Focuser"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state and start status publishing."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformFocuserState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformFocuserState()

        self.focuser_position: float | None = None
        self.focuser_moving: bool | None = None

        # Start focuser status publishing
        logger.debug("starting node_platform focuser status loop")
        self.start_status_loop(self.status_publish())

        # Wait for initial position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def focuser_init(self, cmd: sk.Init):
        """Nothing to do."""
        pass

    @sk.command_handler
    async def focuser_deinit(self, cmd: sk.Deinit):
        """Nothing to do."""
        pass

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state and stop status publishing."""
        logger.debug("stopping node_platform focuser status loop")
        await self.stop_status_loop()

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def focuser_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform focuser")
        await self.api.call("v1_halt_focuser")
        logger.debug("stopped node_platform focuser")

    @sk.command_handler
    async def focuser_move(self, cmd: sk.ChangeFocusPosition):
        self.require_connected()
        logger.debug(f"moving node_platform focuser position to {cmd.position}")

        req = osapi.V1GoToFocuserPositionRequest(
            position=cmd.position,
            operation=osapi.FocusOperation.GOTO,
        )
        await self.api.call("v1_go_to_focuser_position", req)
        await asyncio.sleep(0.1)

        # Wait for the focus move to complete
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_moving is None or self.focuser_moving:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug(f"moved node_platform focuser position to {cmd.position}")

    async def status_publish(self):
        while True:
            try:
                status: osapi.V1FocuserStatus = await self.api.call("v1_get_focuser_status")
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                self.device_connected = status.connected
                self.focuser_moving = status.moving

                # Extract Z-axis position (primary focus axis) in microns
                if status.position is not None:
                    self.focuser_position = float(status.position.zaxis_microns)

                logger.debug(
                    f"NodePlatform focuser status: connected={status.connected}, "
                    f"moving={status.moving}, position={self.focuser_position}"
                )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))
                if self.focuser_position is not None:
                    await device.publish(FocusPosition(position=self.focuser_position))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform focuser status ({e})")
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformFocuserConfig(NodePlatformDeviceConfig[NodePlatformFocuser]):
    """Node Platform Focuser configuration."""
    device_type: Literal["focuser"] = "focuser"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformFocuser(self)


class NodePlatformFocuserState(NodePlatformDeviceState):
    """Node Platform Focuser state."""
    device_type: Literal["focuser"] = "focuser"