# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew rotator (standard ASCOM Alpaca Rotator).

Standard-device wrapper. Autoslew's Rotator has one own action (``settarget``); the
ASA rotator extras (``rotator:setslewoption``/``dontslewtozero``/``homefind``) live on
the Telescope device, driven through the shared backbone.

Inherits `rotator_connect`/`rotator_disconnect`/`rotator_change`/`rotator_stop`/
`status_publish` from `sensorkit.alpaca`'s `AlpacaRotator` unchanged.
`entity_init`/`entity_deinit` are overridden only to restore/persist
`AutoslewRotatorState` (its own `has_been_homed` field) instead of the base
`AlpacaRotatorState`; `_initialize` is overridden to add the backbone, ASA
settings, and one-time home.
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
from sensorkit.std import Disconnect, Home


@sk.declare_device
class AutoslewRotator(AutoslewMixin, AlpacaRotator):
    """ASA Autoslew rotator implementation."""

    config: AutoslewRotatorConfig
    device_name = "Rotator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(AutoslewRotatorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AutoslewRotatorState()

        self.rotator_position: float | None = None

        await self._initialize()
        self.start_status_loop(self.status_publish())

        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.rotator_disconnect(Disconnect())
        with contextlib.suppress(Exception):
            await self.disconnect(self.telescope)
        await sk.device().kv_put_model(self.state)

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


class AutoslewRotatorState(AlpacaRotatorState):
    has_been_homed: bool = False
