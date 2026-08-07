# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.common.aio import AsyncLoop
from sensorkit.pwi4.device import PWI4Client, PWI4Device, PWI4DeviceConfig, PWI4DeviceState
from sensorkit.std import Connect, Connected, Disconnect, Opened, Stop
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


@sk.declare_device
class PWI4Cover(PWI4Device):
    """PWI4 cover implementation."""

    config: PWI4CoverConfig
    device_name = "Cover"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(PWI4CoverState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = PWI4CoverState()

        # Initialize the cover
        await self._initialize()
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self.cover_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.cover_connect(Connect())
        await self.cover_connect(Connect())

    async def _deinitialize(self):
        await self.cover_stop(Stop())
        await self.cover_close(CloseMirrorCover())

    @sk.command_handler
    async def cover_connect(self, cmd: Connect):
        logger.debug("connecting to mirror cover")

        await self.client.request("/mirrorcover/connect")

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                if self.client.get_bool(st, "mirrorcover.is_connected"):
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to mirror cover")

    @sk.command_handler
    async def cover_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from mirror cover")

        await self.client.request("/mirrorcover/disconnect")

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                if not self.client.get_bool(st, "mirrorcover.is_connected"):
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from mirror cover")

    @sk.command_handler
    async def cover_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping mirror cover")
        await self.client.request("/mirrorcover/stop")
        logger.debug("stopped mirror cover")

    @sk.command_handler
    async def cover_open(self, cmd: OpenMirrorCover):
        await self.require_connected()
        logger.debug("opening mirror cover")

        await self.client.request("/mirrorcover/open")

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                state_name = self.client.get_str(st, "mirrorcover.overall_state_name", "")
                if state_name.lower() == "open":
                    break
                await asyncio.sleep(1)

        logger.debug("opened mirror cover")

    @sk.command_handler
    async def cover_close(self, cmd: CloseMirrorCover):
        await self.require_connected()
        logger.debug("closing mirror cover")

        await self.client.request("/mirrorcover/close")

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                state_name = self.client.get_str(st, "mirrorcover.overall_state_name", "")
                if state_name.lower() == "closed":
                    break
                await asyncio.sleep(1)

        logger.debug("closed mirror cover")

    async def status_publish(self):
        st = await self.client.status()
        connected = self.client.get_bool(st, "mirrorcover.is_connected")
        self.device_connected = connected

        state_name = self.client.get_str(st, "mirrorcover.overall_state_name", "")
        is_open = state_name.lower() == "open"

        device = sk.device()
        await device.publish(Connected(is_connected=connected))
        await device.publish(Opened(is_open=is_open))


class PWI4CoverConfig(PWI4DeviceConfig[PWI4Cover]):
    device_type: Literal["cover"] = "cover"

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Cover(config=self, client=client)


class PWI4CoverState(PWI4DeviceState):
    device_type: Literal["cover"] = "cover"
