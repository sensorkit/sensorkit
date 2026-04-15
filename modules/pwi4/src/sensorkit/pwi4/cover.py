from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.pwi4.device import PWI4Client, PWI4Device, PWI4DeviceConfig, PWI4DeviceState
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


@sk.declare_device
class PWI4Cover(PWI4Device):
    """PWI4 cover implementation."""

    config: PWI4CoverConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(PWI4CoverState)
        except Exception:
            self.state = PWI4CoverState()

        await self.cover_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.cover_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def cover_init(self, cmd: sk.Init):
        self.device_name = "Cover"

        await self.cover_connect(sk.Connect())
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def cover_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.cover_disconnect(sk.Disconnect())

    @sk.command_handler
    async def cover_connect(self, cmd: sk.Connect):
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
    async def cover_disconnect(self, cmd: sk.Disconnect):
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
    async def cover_open(self, cmd: OpenMirrorCover):
        self.require_connected()
        logger.debug("opening mirror cover")
        await self.client.request("/mirrorcover/open")

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                state_name = self.client.get_str(st, "mirrorcover.overall_state_name", "")
                if state_name.lower() == "open":
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opened mirror cover")

    @sk.command_handler
    async def cover_close(self, cmd: CloseMirrorCover):
        self.require_connected()
        logger.debug("closing mirror cover")
        await self.client.request("/mirrorcover/close")

        async with asyncio.timeout(self.config.timeout):
            while True:
                st = await self.client.status()
                state_name = self.client.get_str(st, "mirrorcover.overall_state_name", "")
                if state_name.lower() == "closed":
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closed mirror cover")

    @sk.command_handler
    async def cover_stop(self, cmd: sk.Stop):
        logger.debug("stopping mirror cover")
        await self.client.request("/mirrorcover/stop")
        logger.debug("stopped mirror cover")

    async def status_publish(self):
        while True:
            try:
                st = await self.client.status()
                connected = self.client.get_bool(st, "mirrorcover.is_connected")
                self.device_connected = connected

                state_name = self.client.get_str(st, "mirrorcover.overall_state_name", "")
                is_open = state_name.lower() == "open"

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(Opened(is_open=is_open))
            except Exception as e:
                logger.exception(f"Error in cover status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class PWI4CoverConfig(PWI4DeviceConfig[PWI4Cover]):
    device_type: Literal["cover"] = "cover"

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Cover(config=self, client=client)


class PWI4CoverState(PWI4DeviceState):
    device_type: Literal["cover"] = "cover"
