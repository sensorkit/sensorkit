from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.covercalibrator import CoverCalibrator
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.models.devices import Connected, Opened
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover

# CoverStatus enum (ICoverCalibratorV2)
_COVER_NOT_PRESENT = 0
_COVER_CLOSED = 1
_COVER_MOVING = 2
_COVER_OPEN = 3
_COVER_UNKNOWN = 4
_COVER_ERROR = 5

_COVER_NAMES = {
    0: "NotPresent",
    1: "Closed",
    2: "Moving",
    3: "Open",
    4: "Unknown",
    5: "Error",
}

# CalibratorStatus enum
_CAL_NOT_PRESENT = 0
_CAL_OFF = 1
_CAL_NOT_READY = 2
_CAL_READY = 3
_CAL_UNKNOWN = 4
_CAL_ERROR = 5

_CAL_NAMES = {
    0: "NotPresent",
    1: "Off",
    2: "NotReady",
    3: "Ready",
    4: "Unknown",
    5: "Error",
}


@sk.declare_keyword
class AlpacaCoverCalibratorStatus(BaseModel):
    """ICoverCalibratorV2 properties."""

    cover_state: str = "Unknown"
    cover_moving: bool = False
    calibrator_state: str = "NotPresent"
    calibrator_changing: bool = False
    brightness: int | None = None
    max_brightness: int | None = None


@sk.declare_device
class AlpacaCoverCalibrator(AlpacaDevice):
    """Alpaca CoverCalibrator implementation."""

    config: AlpacaCoverCalibratorConfig
    device_name = "CoverCalibrator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(AlpacaCoverCalibratorState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AlpacaCoverCalibratorState()

        # Initialize the cover calibrator
        await self.cover_calibrator_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.cover_calibrator_disconnect(sk.Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def cover_calibrator_init(self, cmd: sk.Init):
        # Connect to the hardware
        self._reconnect = lambda: self.cover_calibrator_connect(sk.Connect())
        self.cover_calibrator = CoverCalibrator(
            self.address, self.config.device_number, self.config.protocol
        )
        await self.cover_calibrator_connect(sk.Connect())

        # Read capabilities
        self._max_brightness = await self.get(self.cover_calibrator, "MaxBrightness", None)

    @sk.command_handler
    async def cover_calibrator_deinit(self, cmd: sk.Deinit):
        await self.cover_calibrator_stop(sk.Stop())
        await self.cover_calibrator_close(CloseMirrorCover())

    @sk.command_handler
    async def cover_calibrator_connect(self, cmd: sk.Connect):
        await self.connect(self.cover_calibrator, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def cover_calibrator_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect(self.cover_calibrator)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def cover_calibrator_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping cover calibrator")
        await self.call(self.cover_calibrator, "HaltCover")
        logger.debug("stopped cover calibrator")

    @sk.command_handler
    async def cover_calibrator_open(self, cmd: OpenMirrorCover):
        await self.require_connected()
        logger.debug("opening cover calibrator")

        await self.call(self.cover_calibrator, "OpenCover")

        async with asyncio.timeout(self.config.timeout):
            while True:
                cover_state = await self.get(self.cover_calibrator, "CoverState", _COVER_UNKNOWN)
                if cover_state == _COVER_OPEN:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("opened cover calibrator")

    @sk.command_handler
    async def cover_calibrator_close(self, cmd: CloseMirrorCover):
        await self.require_connected()
        logger.debug("closing cover calibrator")

        await self.call(self.cover_calibrator, "CloseCover")

        async with asyncio.timeout(self.config.timeout):
            while True:
                cover_state = await self.get(self.cover_calibrator, "CoverState", _COVER_UNKNOWN)
                if cover_state == _COVER_CLOSED:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("closed cover calibrator")

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
                    cover_moving = await self.get(c, "CoverMoving", False)
                    calibrator_state = await self.get(c, "CalibratorState", _CAL_NOT_PRESENT)
                    calibrator_changing = await self.get(c, "CalibratorChanging", False)
                    brightness = await self.get(c, "Brightness", None)

                    is_open = cover_state == _COVER_OPEN

                    # logger.debug(
                    #     f"Alpaca cover calibrator status: connected={connected}, cover_state={cover_state}, "
                    #     f"cover_moving={cover_moving}, calibrator_state={calibrator_state}, "
                    #     f"calibrator_changing={calibrator_changing}, brightness={brightness}"
                    # )

                    await device.publish(Opened(is_open=is_open))
                    await device.publish(
                        AlpacaCoverCalibratorStatus(
                            cover_state=_COVER_NAMES.get(cover_state, "Unknown"),
                            cover_moving=cover_moving,
                            calibrator_state=_CAL_NAMES.get(calibrator_state, "Unknown"),
                            calibrator_changing=calibrator_changing,
                            brightness=brightness,
                            max_brightness=self._max_brightness,
                        )
                    )
            except Exception as e:
                logger.exception(f"Error in cover calibrator status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class AlpacaCoverCalibratorConfig(AlpacaDeviceConfig[AlpacaCoverCalibrator]):
    device_type: Literal["cover_calibrator"] = "cover_calibrator"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AlpacaCoverCalibrator(self)


class AlpacaCoverCalibratorState(AlpacaDeviceState):
    device_type: Literal["cover_calibrator"] = "cover_calibrator"
