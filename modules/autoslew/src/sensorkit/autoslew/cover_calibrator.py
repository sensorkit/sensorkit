# SPDX-License-Identifier: Apache-2.0
"""ASA Autoslew mirror cover (standard ASCOM Alpaca CoverCalibrator).

Autoslew exposes a CoverCalibrator device but only the *cover* half is implemented
(``Brightness``/``MaxBrightness`` return NotImplemented) — so this is effectively a
mirror cover (``StandardMirrorCover``). ASA also exposes cover control via the
Telescope actions ``telescope:opencover``/``closecover``; we prefer the real device.
"""

from __future__ import annotations

import asyncio
from typing import Literal, override

from alpaca.covercalibrator import CoverCalibrator
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.autoslew.device import (
    AutoslewDevice,
    AutoslewDeviceConfig,
    AutoslewDeviceState,
)
from sensorkit.std import Connect, Connected, Disconnect, Opened, Stop
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover

# CoverStatus enum (ICoverCalibratorV2)
_COVER_CLOSED = 1
_COVER_OPEN = 3
_COVER_UNKNOWN = 4

_COVER_NAMES = {0: "NotPresent", 1: "Closed", 2: "Moving", 3: "Open", 4: "Unknown", 5: "Error"}


@sk.declare_keyword
class AutoslewCoverStatus(BaseModel):
    """Cover state (the calibrator half is not implemented on this build)."""

    cover_state: str = "Unknown"
    cover_moving: bool = False


@sk.declare_device
class AutoslewCoverCalibrator(AutoslewDevice):
    """ASA Autoslew mirror cover implementation."""

    config: AutoslewCoverCalibratorConfig
    device_name = "CoverCalibrator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(AutoslewCoverCalibratorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AutoslewCoverCalibratorState()

        await self._initialize()
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.cover_calibrator_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        self._reconnect = lambda: self.cover_calibrator_connect(Connect())
        self.cover_calibrator = CoverCalibrator(
            self.address, self.config.device_number, self.config.protocol
        )
        await self.cover_calibrator_connect(Connect())

    @sk.command_handler
    async def cover_calibrator_connect(self, cmd: Connect):
        await self.connect(self.cover_calibrator, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def cover_calibrator_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.cover_calibrator)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def cover_calibrator_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping cover")
        await self.call(self.cover_calibrator, "HaltCover")
        logger.debug("stopped cover")

    @sk.command_handler
    async def cover_calibrator_open(self, cmd: OpenMirrorCover):
        await self.require_connected()
        logger.debug("opening cover")
        await self.call(self.cover_calibrator, "OpenCover")
        async with asyncio.timeout(self.config.timeout):
            while (
                await self.get(self.cover_calibrator, "CoverState", _COVER_UNKNOWN) != _COVER_OPEN
            ):
                await asyncio.sleep(self.config.status_frequency)
        logger.debug("opened cover")

    @sk.command_handler
    async def cover_calibrator_close(self, cmd: CloseMirrorCover):
        await self.require_connected()
        logger.debug("closing cover")
        await self.call(self.cover_calibrator, "CloseCover")
        async with asyncio.timeout(self.config.timeout):
            while (
                await self.get(self.cover_calibrator, "CoverState", _COVER_UNKNOWN)
                != _COVER_CLOSED
            ):
                await asyncio.sleep(self.config.status_frequency)
        logger.debug("closed cover")

    async def status_publish(self):
        while True:
            try:
                c = self.cover_calibrator
                connected = await self.get(c, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    cover_state = await self.get(c, "CoverState", _COVER_UNKNOWN)
                    await device.publish(Opened(is_open=cover_state == _COVER_OPEN))
                    await device.publish(
                        AutoslewCoverStatus(
                            cover_state=_COVER_NAMES.get(cover_state, "Unknown"),
                            cover_moving=await self.get(c, "CoverMoving", False),
                        )
                    )
            except Exception as e:
                logger.exception(f"Error in cover status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AutoslewCoverCalibratorConfig(AutoslewDeviceConfig[AutoslewCoverCalibrator]):
    device_type: Literal["cover_calibrator"] = "cover_calibrator"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AutoslewCoverCalibrator(self)


class AutoslewCoverCalibratorState(AutoslewDeviceState):
    device_type: Literal["cover_calibrator"] = "cover_calibrator"
