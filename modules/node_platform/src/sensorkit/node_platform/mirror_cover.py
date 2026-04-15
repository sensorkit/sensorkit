from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import ourskyai_node_platform_api as osapi

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)


@sk.declare_device
class NodePlatformMirrorCover(NodePlatformDevice):
    """Node Platform Mirror Cover implementation."""
    config: NodePlatformMirrorCoverConfig
    device_name = "Mirror Cover"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state and start status publishing."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(NodePlatformMirrorCoverState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformMirrorCoverState()

        self.cover_is_open: bool | None = None

        # Start mirror cover status publishing
        logger.debug("starting node_platform mirror cover status loop")
        self.start_status_loop(self.status_publish())

        # Wait for initial status
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def mirror_cover_init(self, cmd: sk.Init):
        """Nothing to do."""
        pass

    @sk.command_handler
    async def mirror_cover_deinit(self, cmd: sk.Deinit):
        """Nothing to do."""
        pass

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state and stop status publishing."""
        logger.debug("stopping node_platform mirror cover status loop")
        await self.stop_status_loop()

        await sk.device().kv_put_model(self.state)
        await self.api.close()

    @sk.command_handler
    async def mirror_cover_open(self, cmd: sk.Open):
        self.require_connected()
        logger.debug("opening node_platform mirror cover")

        await self.api.call("v1_open_optical_tube_cover")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while not self.cover_is_open:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opened node_platform mirror cover")

    @sk.command_handler
    async def mirror_cover_close(self, cmd: sk.Close):
        self.require_connected()
        logger.debug("closing node_platform mirror cover")

        await self.api.call("v1_close_optical_tube_cover")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while self.cover_is_open is None or self.cover_is_open:
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closed node_platform mirror cover")

    @sk.command_handler
    async def mirror_cover_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping node_platform mirror cover")
        await self.api.call("v1_halt_optical_tube_cover")
        logger.debug("stopped node_platform mirror cover")

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

                # FIXME: mimic shutter_state of enclosure

                logger.debug(
                    f"NodePlatform mirror cover status: connected={status.connected}, "
                    f"is_open={status.is_open}"
                )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))
                await device.publish(Opened(is_open=status.is_open))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform mirror cover status ({e})")
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformMirrorCoverConfig(NodePlatformDeviceConfig[NodePlatformMirrorCover]):
    """Node Platform Mirror Cover configuration."""
    device_type: Literal["mirror_cover"] = "mirror_cover"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NodePlatformMirrorCover(self)


class NodePlatformMirrorCoverState(NodePlatformDeviceState):
    """Node Platform Mirror Cover state."""
    device_type: Literal["mirror_cover"] = "mirror_cover"
    is_open: bool | None = None