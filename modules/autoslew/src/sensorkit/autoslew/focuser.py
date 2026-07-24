# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew focuser (standard ASCOM Alpaca Focuser).

A standard-device wrapper (Autoslew's Focuser exposes no extension actions of its own
— SupportedActions = 0). The ASA focuser extras (``focuser:homefind``, ``afc:*``) live
on the Telescope device and are driven through the shared Telescope backbone.

Inherits `entity_init`/`focuser_connect`/`focuser_disconnect`/`focuser_stop`/
`focuser_change`/`status_publish` from `sensorkit.alpaca`'s `AlpacaFocuser`
unchanged. The `state_model` classvar swaps in `AutoslewFocuserState` (its own
`has_been_homed` field) at restore time; `_initialize` is overridden to add the
backbone + home, and `entity_deinit` to also drop the backbone connection.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import override

from alpaca.telescope import Telescope
from loguru import logger

import sensorkit.api as sk
from sensorkit.alpaca.focuser import AlpacaFocuser, AlpacaFocuserConfig, AlpacaFocuserState
from sensorkit.autoslew.device import AutoslewMixin
from sensorkit.std import Home


class AutoslewFocuserState(AlpacaFocuserState):
    has_been_homed: bool = False


@sk.declare_device
class AutoslewFocuser(AutoslewMixin, AlpacaFocuser):
    """ASA Autoslew focuser implementation."""

    config: AutoslewFocuserConfig
    device_name = "Focuser"
    state_model = AutoslewFocuserState

    @sk.on_detach
    async def entity_deinit(self):
        await super().entity_deinit()
        with contextlib.suppress(Exception):
            await self.disconnect(self.telescope)

    @override
    async def _initialize(self):
        await super()._initialize()

        # Telescope backbone for ASA focuser extras (focuser:homefind, afc:*).
        self.telescope = Telescope(self.address, 0, self.config.protocol)
        await self.connect(self.telescope, timeout=self.config.timeout)

        # Home once per session (state-gated), like the mount/dome.
        if not self.state.has_been_homed:
            await self.focuser_home(Home())

    @sk.command_handler
    async def focuser_home(self, cmd: Home):
        """Home the focuser via the ASA focuser:homefind Telescope action.

        Called once per session at init (state-gated on has_been_homed, like the
        mount/dome), and also available over the command bus.
        """
        await self.require_connected()
        logger.debug("homing focuser")
        await self.action("focuser:homefind")
        async with asyncio.timeout(self.config.timeout):
            while await self.get(self.focuser, "IsMoving", False):
                await asyncio.sleep(self.config.status_frequency)
        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed focuser")

    # ---- ASA focuser extras (Telescope-device actions via the backbone) ----- #
    # NB: afc:* parameter formats are a working assumption pending ICD confirmation.
    async def afc_get_focus_position(self) -> str:
        return await self.action("afc:getfocuspos")

    async def afc_set_focus_position(self, position: float) -> None:
        await self.action("afc:setfocuspos", repr(position))

    async def afc_get_filter(self) -> str:
        return await self.action("afc:getfilter")

    async def afc_set_filter(self, filter_index: int) -> None:
        await self.action("afc:setfilter", str(filter_index))


class AutoslewFocuserConfig(AlpacaFocuserConfig):
    @override
    def create_device(self):
        return AutoslewFocuser(self)
