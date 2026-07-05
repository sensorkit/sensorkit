# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Stop
from sensorkit.std import Connect, Connected, Disconnect, Enabled
from sensorkit.pwi4.device import PWI4Client, PWI4Device, PWI4DeviceConfig, PWI4DeviceState
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition


@sk.declare_device
class PWI4Rotator(PWI4Device):
    """PWI4 rotator implementation."""

    config: PWI4RotatorConfig
    device_name = "Rotator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(PWI4RotatorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            self.state = PWI4RotatorState()
            logger.warning(f"No saved state for {device.entity}")

        self.rotator_position: float | None = None

        # Initialize the rotator
        await self._initialize()
        self.start_status_loop(self.status_publish())

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.rotator_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.rotator_connect(Connect())
        await self.rotator_connect(Connect())
        await self.rotator_enable(sk.Enable())

    async def _deinitialize(self):
        await self.rotator_stop(Stop())
        await self.rotator_disable(sk.Disable())

    @sk.command_handler
    async def rotator_connect(self, cmd: Connect):
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
    async def rotator_disconnect(self, cmd: Disconnect):
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
        await self.require_connected()
        logger.debug("enabling rotator")
        await self.client.request("/rotator/enable")
        logger.debug("enabled rotator")

    @sk.command_handler
    async def rotator_disable(self, cmd: sk.Disable):
        await self.require_connected()
        logger.debug("disabling rotator")
        await self.client.request("/rotator/disable")
        logger.debug("disabled rotator")

    @sk.command_handler
    async def rotator_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping rotator")
        await self.client.request("/rotator/stop")
        logger.debug("stopped rotator")

    @sk.command_handler
    async def rotator_change(self, cmd: ChangeRotatorPosition):
        await self.require_connected()
        logger.debug(f"changing rotator to position {cmd.position}°")

        await self.client.request("/rotator/goto_mech", params={"degs": cmd.position})
        await self.client.poll(
            lambda s: not self.client.get_bool(s, "rotator.is_moving"),
        )

        logger.debug(f"changed rotator to position {cmd.position}°")

    async def status_publish(self):
        while True:
            try:
                st = await self.client.status()
                connected = self.client.get_bool(st, "rotator.is_connected")
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if st is not None:
                    self.rotator_position = self.client.get_float(st, "rotator.mech_position_degs")

                if connected:
                    await device.publish(
                        Enabled(is_enabled=self.client.get_bool(st, "rotator.is_enabled"))
                    )
                    await device.publish(RotatorPosition(position=self.rotator_position))

                    # logger.debug(
                    #     f"PWI4 rotator status: connected={connected}, "
                    #     f"position={self.rotator_position}"
                    # )
            except Exception as e:
                logger.exception(f"Error in rotator status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class PWI4RotatorConfig(PWI4DeviceConfig[PWI4Rotator]):
    device_type: Literal["rotator"] = "rotator"

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Rotator(config=self, client=client)


class PWI4RotatorState(PWI4DeviceState):
    device_type: Literal["rotator"] = "rotator"
