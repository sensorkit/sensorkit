# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew tertiary / Nasmyth selector — bespoke, backbone-only.

ASA calls this the "Tertiary" (hence the file name; cf. node_platform's "M3").
There is no ASCOM device for it and no SK archetype — control is the Telescope
actions ``selectnasmythport`` / ``getcurrentnasmythport`` / ``tertiarystatus``, so
this bare device rides the shared Telescope backbone and publishes the current
port. A port-select command is deferred until SensorKit defines one (the
node_platform M3 change command is commented out for the same reason).
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Literal, override

from alpaca.telescope import Telescope
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.autoslew.device import (
    AutoslewDevice,
    AutoslewDeviceConfig,
    AutoslewDeviceState,
    _num,
)
from sensorkit.std import Connect, Connected, Disconnect, Stop


@sk.declare_keyword
class AutoslewTertiaryStatus(BaseModel):
    """Tertiary / Nasmyth selector status."""

    port: int | None = None


@sk.declare_device
class AutoslewTertiary(AutoslewDevice):
    """ASA Autoslew tertiary (Nasmyth) selector, via the Telescope backbone."""

    config: AutoslewTertiaryConfig
    device_name = "Tertiary"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(AutoslewTertiaryState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AutoslewTertiaryState()

        self._port: int | None = None

        await self._initialize()
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.tertiary_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        self._reconnect = lambda: self.tertiary_connect(Connect())
        self.telescope = Telescope(self.address, 0, self.config.protocol)
        await self.tertiary_connect(Connect())

    @sk.command_handler
    async def tertiary_connect(self, cmd: Connect):
        await self.connect(self.telescope, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def tertiary_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.telescope)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def tertiary_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stop tertiary (no continuous motion to halt)")

    async def _publish_status(self):
        connected = await self.get(self.telescope, "Connected", False)
        self.device_connected = connected

        device = sk.device()
        await device.publish(Connected(is_connected=connected))

        if connected:
            with contextlib.suppress(Exception):
                self._port = int(_num(await self.action("getcurrentnasmythport")))
            await device.publish(AutoslewTertiaryStatus(port=self._port))

    async def status_publish(self):
        while True:
            try:
                await self._publish_status()
            except Exception as e:
                logger.exception(f"Error in tertiary status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AutoslewTertiaryConfig(AutoslewDeviceConfig[AutoslewTertiary]):
    device_type: Literal["tertiary"] = "tertiary"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AutoslewTertiary(self)


class AutoslewTertiaryState(AutoslewDeviceState):
    device_type: Literal["tertiary"] = "tertiary"
