from __future__ import annotations

import asyncio
from typing import Literal, override

import ourskyai_node_platform_api as osapi
from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.node_platform.device import (
    NodePlatformDevice,
    NodePlatformDeviceConfig,
    NodePlatformDeviceState,
)
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


@sk.declare_device
class NodePlatformCover(NodePlatformDevice):
    """Node Platform Cover implementation."""

    config: NodePlatformCoverConfig
    device_name = "Cover"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NodePlatformCoverState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NodePlatformCoverState()

        self.start_status_loop(self.status_publish())
        await self.cover_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.cover_deinit(sk.Deinit())
        await self.api.close()
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def cover_init(self, cmd: sk.Init):
        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def cover_deinit(self, cmd: sk.Deinit):
        await self.require_connected()
        await self.cover_stop(sk.Stop())

    @sk.command_handler
    async def cover_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping mirror cover")
        await self.api.call("v1_halt_optical_tube_cover")
        logger.debug("stopped mirror cover")

    @sk.command_handler
    async def cover_open(self, cmd: OpenMirrorCover):
        await self.require_connected()
        logger.debug("opening mirror cover")

        await self.api.call("v1_open_optical_tube_cover")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                status = await self.api.call("v1_get_optical_tube_cover_status")
                if status.is_open:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opened mirror cover")

    @sk.command_handler
    async def cover_close(self, cmd: CloseMirrorCover):
        await self.require_connected()
        logger.debug("closing mirror cover")

        await self.api.call("v1_close_optical_tube_cover")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                status = await self.api.call("v1_get_optical_tube_cover_status")
                if not status.is_open:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closed mirror cover")

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
                is_open = status.is_open

                # logger.debug(
                #     f"NodePlatform cover status: connected={status.connected}, "
                #     f"is_open={is_open}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=status.connected))
                await device.publish(Opened(is_open=is_open))

            except Exception as e:
                logger.warning(f"Failed to update Node Platform cover status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NodePlatformCoverConfig(NodePlatformDeviceConfig[NodePlatformCover]):
    """Node Platform Cover configuration."""

    device_type: Literal["cover"] = "cover"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return NodePlatformCover(self)


class NodePlatformCoverState(NodePlatformDeviceState):
    """Node Platform Cover state."""

    device_type: Literal["cover"] = "cover"
