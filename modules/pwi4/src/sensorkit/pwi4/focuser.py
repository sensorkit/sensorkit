# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.common.aio import AsyncLoop
from sensorkit.pwi4.device import PWI4Client, PWI4Device, PWI4DeviceConfig, PWI4DeviceState
from sensorkit.std import Connect, Connected, Disable, Disconnect, Enable, Enabled, Stop
from sensorkit.std.optics import ChangeFocusPosition, FocusPosition


@sk.declare_device
class PWI4Focuser(PWI4Device):
    """PWI4 focuser implementation."""

    config: PWI4FocuserConfig
    device_name = "Focuser"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(PWI4FocuserState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            self.state = PWI4FocuserState()
            logger.warning(f"No saved state for {device.entity}")

        self.focuser_position: float | None = None

        # Initialize the focuser
        await self._initialize()
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

        # Establish the configured base position — the anchor that per-filter offsets
        # and FocusCorrection are relative to (published as FocusPosition.base_position).
        if (
            self.config.base_position is not None
            and int(self.focuser_position) != int(self.config.base_position)
        ):
            await self.focuser_change(ChangeFocusPosition(position=self.config.base_position))

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self.focuser_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.focuser_connect(Connect())
        await self.focuser_connect(Connect())
        await self.focuser_enable(Enable())

    async def _deinitialize(self):
        await self.focuser_stop(Stop())
        await self.focuser_disable(Disable())

    @sk.command_handler
    async def focuser_connect(self, cmd: Connect):
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
    async def focuser_disconnect(self, cmd: Disconnect):
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
    async def focuser_enable(self, cmd: Enable):
        await self.require_connected()
        logger.debug("enabling focuser")
        await self.client.request("/focuser/enable")
        logger.debug("enabled focuser")

    @sk.command_handler
    async def focuser_disable(self, cmd: Disable):
        await self.require_connected()
        logger.debug("disabling focuser")
        await self.client.request("/focuser/disable")
        logger.debug("disabled focuser")

    @sk.command_handler
    async def focuser_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping focuser")
        await self.client.request("/focuser/stop")
        logger.debug("stopped focuser")

    @sk.command_handler
    async def focuser_change(self, cmd: ChangeFocusPosition):
        await self.require_connected()
        logger.debug(f"changing focuser to position {cmd.position}")

        await self.client.request("/focuser/goto", params={"target": cmd.position})
        await self.client.poll(
            lambda s: not self.client.get_bool(s, "focuser.is_moving"),
        )

        logger.debug(f"changed focuser to position {cmd.position}")

    async def status_publish(self):
        st = await self.client.status()
        connected = self.client.get_bool(st, "focuser.is_connected")
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if st is not None:
            self.focuser_position = self.client.get_float(st, "focuser.position")

        if connected:
            await device.publish(
                Enabled(is_enabled=self.client.get_bool(st, "focuser.is_enabled"))
            )
            await device.publish(
                FocusPosition(
                    base_position=self.config.base_position,
                    current_position=self.focuser_position,
                )
            )


class PWI4FocuserConfig(PWI4DeviceConfig[PWI4Focuser]):
    device_type: Literal["focuser"] = "focuser"
    # Base focuser position: driven to at init; published as
    # FocusPosition.base_position — the anchor for filter offsets and FocusCorrection.
    base_position: float | None = None

    @override
    def create_device(self, client: PWI4Client):
        return PWI4Focuser(config=self, client=client)


class PWI4FocuserState(PWI4DeviceState):
    device_type: Literal["focuser"] = "focuser"
