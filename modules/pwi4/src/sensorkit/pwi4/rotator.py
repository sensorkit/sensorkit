from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Enabled
from sensorkit.pwi4.device import PWI4Client, PWI4Device, PWI4DeviceConfig, PWI4DeviceState


@sk.declare_device
class PWI4Rotator(PWI4Device):
    """PWI4 rotator implementation."""

    config: PWI4RotatorConfig

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(PWI4RotatorState)
        except Exception:
            self.state = PWI4RotatorState()

        await self.rotator_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.rotator_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def rotator_init(self, cmd: sk.Init):
        self.device_name = "Rotator"

        await self.rotator_connect(sk.Connect())
        await self.rotator_enable(sk.Enable())

        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.device_connected is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def rotator_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        await self.rotator_disable(sk.Disable())
        await self.rotator_disconnect(sk.Disconnect())

    @sk.command_handler
    async def rotator_connect(self, cmd: sk.Connect):
        logger.debug("connecting to rotator")
        await self.client.request("/rotator/connect")

        async with asyncio.timeout(self.config.timeout):
            await self.client.poll(
                lambda s: self.client.get_bool(s, "rotator.is_connected"),
            )

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))
        logger.debug("connected to rotator")

    @sk.command_handler
    async def rotator_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from rotator")
        await self.client.request("/rotator/disconnect")

        async with asyncio.timeout(self.config.timeout):
            await self.client.poll(
                lambda s: not self.client.get_bool(s, "rotator.is_connected"),
            )

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))
        logger.debug("disconnected from rotator")

    @sk.command_handler
    async def rotator_enable(self, cmd: sk.Enable):
        await self.client.request("/rotator/enable")

    @sk.command_handler
    async def rotator_disable(self, cmd: sk.Disable):
        await self.client.request("/rotator/disable")

    @sk.command_handler
    async def rotator_move(self, cmd: sk.ChangeRotatorPosition):
        await self.require_connected()
        logger.debug(f"moving rotator to {cmd.position}°")
        await self.client.request("/rotator/goto_mech", params={"degs": cmd.position})
        await self.client.poll(
            lambda s: not self.client.get_bool(s, "rotator.is_moving"),
        )
        logger.debug(f"moved rotator to {cmd.position}°")

    @sk.command_handler
    async def rotator_stop(self, cmd: sk.Stop):
        await self.client.request("/rotator/stop")

    async def status_publish(self):
        while True:
            try:
                st = await self.client.status()
                connected = self.client.get_bool(st, "rotator.is_connected")
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    await device.publish(
                        Enabled(is_enabled=self.client.get_bool(st, "rotator.is_enabled"))
                    )
                    await device.publish(
                        sk.RotatorPosition(
                            position=self.client.get_float(st, "rotator.mech_position_degs")
                        )
                    )
            except Exception as e:
                logger.exception(f"Error in rotator status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class PWI4RotatorConfig(PWI4DeviceConfig[PWI4Rotator]):
    device_type: Literal["rotator"] = "rotator"

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Rotator(config=self, client=client)


class PWI4RotatorState(PWI4DeviceState):
    device_type: Literal["rotator"] = "rotator"
