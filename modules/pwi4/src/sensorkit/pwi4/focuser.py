from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Enabled
from sensorkit.pwi4.device import PWI4Client, PWI4Device, PWI4DeviceConfig, PWI4DeviceState


@sk.declare_device
class PWI4Focuser(PWI4Device):
    """PWI4 focuser implementation."""

    config: PWI4FocuserConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(PWI4FocuserState)
        except Exception:
            self.state = PWI4FocuserState()

        await self.focuser_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.focuser_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def focuser_init(self, cmd: sk.Init):
        self.device_name = "Focuser"

        await self.focuser_connect(sk.Connect())
        await self.focuser_enable(sk.Enable())

        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def focuser_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.focuser_disable(sk.Disable())
        await self.focuser_disconnect(sk.Disconnect())

    @sk.command_handler
    async def focuser_connect(self, cmd: sk.Connect):
        logger.debug("connecting to focuser")
        await self.client.request("/focuser/connect")

        async with asyncio.timeout(self.config.timeout):
            await self.client.poll(
                lambda s: self.client.get_bool(s, "focuser.is_connected"),
            )

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))
        logger.debug("connected to focuser")

    @sk.command_handler
    async def focuser_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from focuser")
        await self.client.request("/focuser/disconnect")

        async with asyncio.timeout(self.config.timeout):
            await self.client.poll(
                lambda s: not self.client.get_bool(s, "focuser.is_connected"),
            )

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))
        logger.debug("disconnected from focuser")

    @sk.command_handler
    async def focuser_enable(self, cmd: sk.Enable):
        await self.client.request("/focuser/enable")

    @sk.command_handler
    async def focuser_disable(self, cmd: sk.Disable):
        await self.client.request("/focuser/disable")

    @sk.command_handler
    async def focuser_move(self, cmd: sk.ChangeFocusPosition):
        await self.require_connected()
        logger.debug(f"moving focuser to {cmd.position}")
        await self.client.request("/focuser/goto", params={"target": cmd.position})
        await self.client.poll(
            lambda s: not self.client.get_bool(s, "focuser.is_moving"),
        )
        logger.debug(f"moved focuser to {cmd.position}")

    @sk.command_handler
    async def focuser_stop(self, cmd: sk.Stop):
        logger.debug("stopping focuser")
        await self.client.request("/focuser/stop")
        logger.debug("stopped focuser")

    async def status_publish(self):
        while True:
            try:
                st = await self.client.status()
                connected = self.client.get_bool(st, "focuser.is_connected")
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    await device.publish(
                        Enabled(is_enabled=self.client.get_bool(st, "focuser.is_enabled"))
                    )
                    await device.publish(
                        sk.FocusPosition(position=self.client.get_float(st, "focuser.position"))
                    )
            except Exception as e:
                logger.exception(f"Error in focuser status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class PWI4FocuserConfig(PWI4DeviceConfig[PWI4Focuser]):
    device_type: Literal["focuser"] = "focuser"

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Focuser(config=self, client=client)


class PWI4FocuserState(PWI4DeviceState):
    device_type: Literal["focuser"] = "focuser"
