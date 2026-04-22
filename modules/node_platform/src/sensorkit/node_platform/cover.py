from __future__ import annotations

import asyncio
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


@sk.declare_device
class NodePlatformCover(NodePlatformDevice):
    """Node Platform Cover implementation."""

    config: NodePlatformCoverConfig
    device_name = "Cover"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformCoverState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformCoverState()

        self.cover_is_open: bool | None = None

        # Start cover status publishing
        logger.debug("starting node_platform cover status loop")
        self.start_status_loop(self.status_publish())

        # Wait for initial status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def cover_init(self, cmd: sk.Init):
        pass

    @sk.command_handler
    async def cover_deinit(self, cmd: sk.Deinit):
        pass

    @sk.on_detach
    async def entity_deinit(self):
        logger.debug("stopping node_platform cover status loop")
        await self.stop_status_loop()

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def cover_open(self, cmd: OpenMirrorCover):
        self.require_connected()
        logger.debug("opening node_platform cover")

        await self.api.call("v1_open_optical_tube_cover")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while not self.cover_is_open:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opened node_platform cover")

    @sk.command_handler
    async def cover_close(self, cmd: CloseMirrorCover):
        self.require_connected()
        logger.debug("closing node_platform cover")

        await self.api.call("v1_close_optical_tube_cover")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.cover_is_open is None or self.cover_is_open:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closed node_platform cover")

    @sk.command_handler
    async def cover_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform cover")
        await self.api.call("v1_halt_optical_tube_cover")
        logger.debug("stopped node_platform cover")

    async def status_publish(self):
        while True:
            try:
                status: osapi.V1OpticalTubeCoverStatus = await self.api.call(
                    "v1_get_optical_tube_cover_status"
                )
            except Exception as e:
                logger.exception(f"Error in status_publish get: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                self.device_connected = status.connected
                self.cover_is_open = status.is_open

                logger.debug(
                    f"NodePlatform cover status: connected={status.connected}, "
                    f"is_open={status.is_open}"
                )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))
                await device.publish(Opened(is_open=status.is_open))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform cover status ({e})")
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformCoverConfig(NodePlatformDeviceConfig[NodePlatformCover]):
    """Node Platform Cover configuration."""

    device_type: Literal["cover"] = "cover"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformCover(self)


class NodePlatformCoverState(NodePlatformDeviceState):
    """Node Platform Cover state."""

    device_type: Literal["cover"] = "cover"
    is_open: bool | None = None
