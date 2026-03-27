from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import ourskyai_node_platform_api as osapi

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


@sk.declare_device
class NodePlatformM3(NodePlatformDevice):
    """Node Platform M3 (tertiary mirror) implementation."""
    config: NodePlatformM3Config
    device_name = "M3"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state and start status publishing."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformM3State)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformM3State()

        self.m3_port: int | None = None

        # Start M3 status publishing
        logger.debug("starting node_platform m3 status loop")
        self._status_task = asyncio.create_task(self.status_publish())

    @sk.command_handler
    async def m3_init(self, cmd: sk.Init):
        """Nothing to do."""
        pass

    @sk.command_handler
    async def m3_deinit(self, cmd: sk.Deinit):
        """Nothing to do."""
        pass

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state and stop status publishing."""
        logger.debug("stopping node_platform m3 status loop")
        if hasattr(self, "_status_task"):
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def m3_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform m3")
        await self.api.call("v1_halt_optical_tube_m3")
        logger.debug("stopped node_platform m3")

    # @sk.command_handler
    # async def m3_go_to_port(self, cmd: sk.ChangeM3Port):
    #     self.require_connected()
    #     logger.debug(f"moving node_platform m3 to port {cmd.port}")
    #
    #     req = osapi.V1GoToOpticalTubeM3PortRequest(port=cmd.port)
    #     await self.api.call("v1_go_to_optical_tube_m3_port", req)
    #     await asyncio.sleep(0.1)
    #
    #     # Wait for port change to complete
    #     async with asyncio.timeout(self.config.timeout):
    #         while self.m3_port is None or self.m3_port != cmd.port:
    #             await asyncio.sleep(self.config.status_frequency)
    #
    #     logger.debug(f"moved node_platform m3 to port {cmd.port}")

    async def status_publish(self):
        while True:
            try:
                status: osapi.V1OpticalTubeM3Status = await self.api.call(
                    "v1_get_optical_tube_m3_status"
                )
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                self.device_connected = status.connected
                self.m3_port = status.port

                logger.debug(
                    f"NodePlatform M3 status: connected={status.connected}, port={status.port}"
                )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform M3 status ({e})")
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformM3Config(NodePlatformDeviceConfig[NodePlatformM3]):
    """Node Platform M3 configuration."""
    device_type: Literal["m3"] = "m3"
    timeout: float = 30.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformM3(self)


class NodePlatformM3State(NodePlatformDeviceState):
    """Node Platform M3 state."""
    device_type: Literal["m3"] = "m3"