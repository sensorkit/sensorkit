from __future__ import annotations

import asyncio
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger

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
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NodePlatformM3State)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformM3State()

        self.m3_port: int | None = None

        # Initialize the M3
        await self.m3_init(sk.Init())
        self.start_status_loop(self.status_publish())

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.m3_port is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        # Deinitialize the M3
        await self.m3_deinit(sk.Deinit())

        # Clean up
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.api.close()
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def m3_init(self, cmd: sk.Init):
        pass

    @sk.command_handler
    async def m3_deinit(self, cmd: sk.Deinit):
        await self.require_connected()
        await self.m3_stop(sk.Stop())

    @sk.command_handler
    async def m3_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping m3")
        await self.api.call("v1_halt_optical_tube_m3")
        logger.debug("stopped m3")

    # @sk.command_handler
    # async def m3_change(self, cmd: sk.ChangeM3Port):
    #     self.require_connected()
    #     logger.debug(f"changing m3 to port {cmd.port}")
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
    #     logger.debug(f"changed m3 to port {cmd.port}")

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
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformM3Config(NodePlatformDeviceConfig[NodePlatformM3]):
    """Node Platform M3 configuration."""

    device_type: Literal["m3"] = "m3"
    status_frequency: float = 1.0
    timeout: float = 30.0

    @override
    def create_device(self):
        return NodePlatformM3(self)


class NodePlatformM3State(NodePlatformDeviceState):
    """Node Platform M3 state."""

    device_type: Literal["m3"] = "m3"
