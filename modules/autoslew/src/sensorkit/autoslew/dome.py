# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew enclosure shutter (StandardEnclosure) — backbone-only.

Autoslew exposes NO ASCOM Dome device; shutter control is the Telescope actions
``dome:openshutter``/``dome:closeshutter`` plus the ``IsDomeInScopePosition``
CommandBool. So this device has no alpyca device of its own — it connects the
shared Telescope backbone and drives those verbs through it. Most deployments run
the enclosure as its own module; this covers the "Autoslew drives its own shutter"
case. There is no shutter-open readback, so open/closed is tracked in memory.
"""

from __future__ import annotations

import asyncio
from typing import Literal, override

from alpaca.telescope import Telescope
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.alpaca.device import AlpacaDeviceConfig, AlpacaDeviceState
from sensorkit.autoslew.device import AutoslewDevice
from sensorkit.std import Connect, Connected, Disconnect, Opened
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure


@sk.declare_keyword
class AutoslewDomeStatus(BaseModel):
    """Autoslew dome status (limited: shutter has no state readback)."""

    in_scope_position: bool = False


class AutoslewDomeState(AlpacaDeviceState):
    device_type: Literal["dome"] = "dome"


@sk.declare_device
class AutoslewDome(AutoslewDevice):
    """ASA Autoslew enclosure shutter, driven via the Telescope backbone."""

    config: AutoslewDomeConfig
    device_name = "Dome"
    state_model = AutoslewDomeState

    @sk.on_attach
    async def entity_init(self):
        await self.restore_state()

        self._is_open: bool | None = None

        await self._initialize()
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.dome_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # No ASCOM Dome device: the shutter rides the shared Telescope backbone.
        self._reconnect = lambda: self.dome_connect(Connect())
        self.telescope = Telescope(self.address, 0, self.config.protocol)
        await self.dome_connect(Connect())

    @sk.command_handler
    async def dome_connect(self, cmd: Connect):
        await self.connect(self.telescope, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def dome_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.telescope)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        await self.require_connected()
        logger.debug("opening shutter")
        await self.action("dome:openshutter")
        self._is_open = True
        await sk.device().publish(Opened(is_open=True))

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        await self.require_connected()
        logger.debug("closing shutter")
        await self.action("dome:closeshutter")
        self._is_open = False
        await sk.device().publish(Opened(is_open=False))

    async def status_publish(self):
        while True:
            try:
                connected = await self.get(self.telescope, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    in_position = await self.command_bool("IsDomeInScopePosition")
                    await device.publish(AutoslewDomeStatus(in_scope_position=in_position))
                    if self._is_open is not None:
                        await device.publish(Opened(is_open=self._is_open))
            except Exception as e:
                logger.exception(f"Error in dome status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AutoslewDomeConfig(AlpacaDeviceConfig[AutoslewDome]):
    device_type: Literal["dome"] = "dome"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AutoslewDome(self)
