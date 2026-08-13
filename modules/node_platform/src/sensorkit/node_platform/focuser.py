# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger

import sensorkit.api as sk
from sensorkit.common.aio import AsyncLoop
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)
from sensorkit.std import Connected, FocusPosition, Stop
from sensorkit.std.optics import ChangeFocusPosition


@sk.declare_device
class NodePlatformFocuser(NodePlatformDevice):
    """Node Platform Focuser implementation."""

    config: NodePlatformFocuserConfig
    device_name = "Focuser"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NodePlatformFocuserState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformFocuserState()

        self.focuser_position: float | None = None
        self.focuser_moving: bool | None = None

        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

        # Establish the configured base position — the anchor that per-filter offsets
        # and FocusCorrection are relative to (published as FocusPosition.base_position).
        if (
            self.config.base_position is not None
            and int(self.focuser_position) != int(self.config.base_position)
        ):
            await self.focuser_change(ChangeFocusPosition(position=self.config.base_position))

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self.api.close()
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def focuser_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping focuser")
        await self.api.call("v1_halt_focuser")
        logger.debug("stopped focuser")

    @sk.command_handler
    async def focuser_change(self, cmd: ChangeFocusPosition):
        await self.require_connected()
        logger.debug(f"changed focuser position to {cmd.position}")

        req = osapi.V1GoToFocuserPositionRequest(
            position=cmd.position,
            operation=osapi.FocusOperation.GOTO,
        )
        await self.api.call("v1_go_to_focuser_position", req)
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                status: osapi.V1FocuserStatus = await self.api.call("v1_get_focuser_status")
                if not status.moving:
                    break
                await asyncio.sleep(0.5)

        logger.debug(f"changed focuser position to {cmd.position}")

    async def status_publish(self):
        status: osapi.V1FocuserStatus = await self.api.call("v1_get_focuser_status")
        self.device_connected = status.connected
        self.focuser_moving = status.moving

        # Extract Z-axis position (primary focus axis) in microns
        if status.position is not None:
            self.focuser_position = float(status.position.zaxis_microns)

        # logger.debug(
        #     f"NodePlatform focuser status: connected={status.connected}, "
        #     f"moving={status.moving}, position={self.focuser_position}"
        # )

        device = sk.device()
        await device.publish(Connected(is_connected=status.connected))
        if self.focuser_position is not None:
            await device.publish(
                FocusPosition(
                    base_position=self.config.base_position,
                    current_position=self.focuser_position,
                )
            )


class NodePlatformFocuserConfig(NodePlatformDeviceConfig[NodePlatformFocuser]):
    """Node Platform Focuser configuration."""

    device_type: Literal["focuser"] = "focuser"
    status_frequency: float = 1.0
    timeout: float = 60.0
    # Base focuser position: driven to at init; published as
    # FocusPosition.base_position — the anchor for filter offsets and FocusCorrection.
    base_position: float | None = None

    @override
    def create_device(self):
        return NodePlatformFocuser(self)


class NodePlatformFocuserState(NodePlatformDeviceState):
    """Node Platform Focuser state."""

    device_type: Literal["focuser"] = "focuser"
