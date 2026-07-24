# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew rotator (standard ASCOM Alpaca Rotator).

Standard-device wrapper. Autoslew's Rotator has one own action (``settarget``); the
ASA rotator extras (``rotator:setslewoption``/``dontslewtozero``/``homefind``) live on
the Telescope device, driven through the shared backbone.

Inherits `entity_init`/`rotator_connect`/`rotator_disconnect`/`rotator_change`/
`rotator_stop`/`status_publish` from `sensorkit.alpaca`'s `AlpacaRotator`
unchanged. The `state_model` classvar swaps in `AutoslewRotatorState` (its own
`has_been_homed` field) at restore time; `_initialize` is overridden to add the
backbone, ASA settings, and one-time home, and `entity_deinit` to also drop the
backbone connection.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import override

from alpaca.telescope import Telescope
from loguru import logger

import sensorkit.api as sk
from sensorkit.alpaca.rotator import AlpacaRotator, AlpacaRotatorConfig, AlpacaRotatorState
from sensorkit.autoslew.device import AutoslewMixin
from sensorkit.std import Home


class AutoslewRotatorState(AlpacaRotatorState):
    has_been_homed: bool = False


@sk.declare_device
class AutoslewRotator(AutoslewMixin, AlpacaRotator):
    """ASA Autoslew rotator implementation."""

    config: AutoslewRotatorConfig
    device_name = "Rotator"
    state_model = AutoslewRotatorState

    @sk.on_detach
    async def entity_deinit(self):
        await super().entity_deinit()
        with contextlib.suppress(Exception):
            await self.disconnect(self.telescope)

    @override
    async def _initialize(self):
        await super()._initialize()

        # Telescope backbone for ASA rotator extras (rotator:setslewoption, etc.).
        self.telescope = Telescope(self.address, 0, self.config.protocol)
        await self.connect(self.telescope, timeout=self.config.timeout)
        await self._apply_asa_settings()

        # Home once per session (state-gated), like the mount/dome.
        if not self.state.has_been_homed:
            await self.rotator_home(Home())

    async def _apply_asa_settings(self):
        if self.config.slew_option is not None:
            logger.debug(f"setting rotator slew option to {self.config.slew_option}")
            await self.action("rotator:setslewoption", str(self.config.slew_option))

    @sk.command_handler
    async def rotator_home(self, cmd: Home):
        """Home the rotator via the ASA rotator:homefind Telescope action.

        Called once per session at init (state-gated on has_been_homed, like the
        mount/dome), and also available over the command bus.
        """
        await self.require_connected()
        logger.debug("homing rotator")
        await self.action("rotator:homefind")
        async with asyncio.timeout(self.config.timeout):
            while await self.get(self.rotator, "IsMoving", False):
                await asyncio.sleep(1)
        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)
        logger.debug("homed rotator")

    async def dont_slew_to_zero(self) -> None:
        """ASA rotator:dontslewtozero — keep mechanical position on the next slew."""
        await self.action("rotator:dontslewtozero")


class AutoslewRotatorConfig(AlpacaRotatorConfig):
    slew_option: int | None = None  # ASA rotator:setslewoption (0=track, 1=North, 2=SmartNS)

    @override
    def create_device(self):
        return AutoslewRotator(self)
