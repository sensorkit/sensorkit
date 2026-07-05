# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.dome import Dome
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.astro.common import AltAzPointing
from sensorkit.models.devices import Home, MoveToPark, Opened, Stop
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.std.enclosure import CloseEnclosure, MoveEnclosure, OpenEnclosure

_SHUTTER_OPEN = 0
_SHUTTER_CLOSED = 1
_SHUTTER_OPENING = 2
_SHUTTER_CLOSING = 3
_SHUTTER_ERROR = 4

_SHUTTER_NAMES = {
    0: "open",
    1: "closed",
    2: "opening",
    3: "closing",
    4: "error",
}


@sk.declare_keyword
class AlpacaDomeStatus(BaseModel):
    """IDomeV3 properties."""

    # State
    shutter_status: str = "unknown"
    slewing: bool = False
    at_home: bool = False
    at_park: bool = False
    slaved: bool = False

    # Pointing
    azimuth: float | None = None
    altitude: float | None = None

    # Capabilities
    can_find_home: bool = False
    can_park: bool = False
    can_set_altitude: bool = False
    can_set_azimuth: bool = False
    can_set_park: bool = False
    can_set_shutter: bool = False
    can_slave: bool = False
    can_sync_azimuth: bool = False


@sk.declare_device
class AlpacaDome(AlpacaDevice):
    """Alpaca Dome implementation."""

    config: AlpacaDomeConfig
    device_name = "Dome"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(AlpacaDomeState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AlpacaDomeState()

        # Initialize the dome
        await self._initialize()
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.dome_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.dome_connect(Connect())
        self.dome = Dome(self.address, self.config.device_number, self.config.protocol)
        await self.dome_connect(Connect())

        d = self.dome

        # Read capabilities
        self._can_set_azimuth = await self.get(d, "CanSetAzimuth", False)
        self._can_set_altitude = await self.get(d, "CanSetAltitude", False)
        self._can_set_shutter = await self.get(d, "CanSetShutter", False)
        self._can_find_home = await self.get(d, "CanFindHome", False)
        self._can_park = await self.get(d, "CanPark", False)
        self._can_set_park = await self.get(d, "CanSetPark", False)
        self._can_slave = await self.get(d, "CanSlave", False)
        self._can_sync_azimuth = await self.get(d, "CanSyncAzimuth", False)

        # Home, as needed
        if not self.state.has_been_homed:
            await self.dome_home(Home())

    @sk.command_handler
    async def dome_connect(self, cmd: Connect):
        await self.connect(self.dome, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def dome_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.dome)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def dome_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping enclosure")
        await self.call(self.dome, "AbortSlew")
        logger.debug("stopped enclosure")

    @sk.command_handler
    async def dome_home(self, cmd: Home):
        await self.require_connected()
        if not self._can_find_home:
            logger.warning("Cannot find home")
            return
        logger.debug("homing enclosure")

        await self.call(self.dome, "FindHome")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                at_home = await self.get(self.dome, "AtHome", False)
                slewing = await self.get(self.dome, "Slewing", True)
                if at_home and not slewing:
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed enclosure")

    @sk.command_handler
    async def dome_park(self, cmd: MoveToPark):
        await self.require_connected()
        if not self._can_park:
            logger.warning("Cannot park")
            return
        logger.debug("parking enclosure")

        await self.call(self.dome, "Park")

        async with asyncio.timeout(self.config.timeout):
            while True:
                at_park = await self.get(self.dome, "AtPark", False)
                if at_park:
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("parked enclosure")

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        await self.require_connected()
        if not self._can_set_shutter:
            logger.warning("Cannot set shutter")
            return
        logger.debug("opening enclosure")

        await self.call(self.dome, "OpenShutter")

        async with asyncio.timeout(self.config.timeout):
            while True:
                status = await self.get(self.dome, "ShutterStatus", None)
                if status == _SHUTTER_OPEN:
                    break
                await asyncio.sleep(1)

        logger.debug("opened enclosure")

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        await self.require_connected()
        if not self._can_set_shutter:
            logger.warning("Cannot set shutter")
            return
        logger.debug("closing enclosure")

        await self.call(self.dome, "CloseShutter")

        async with asyncio.timeout(self.config.timeout):
            while True:
                status = await self.get(self.dome, "ShutterStatus", None)
                if status == _SHUTTER_CLOSED:
                    break
                await asyncio.sleep(1)

        logger.debug("closed enclosure")

    @sk.command_handler
    async def dome_move(self, cmd: MoveEnclosure):
        await self.require_connected()
        if not self._can_set_azimuth:
            logger.warning("Cannot set azimuth")
            return

        logger.debug(
            f"moving to altitude={cmd.target_altitude}°, azimuth {cmd.target_azimuth:.1f}°"
        )

        await self.call(self.dome, "SlewToAzimuth", cmd.target_azimuth)

        async with asyncio.timeout(self.config.timeout):
            while True:
                slewing = await self.get(self.dome, "Slewing", True)
                if not slewing:
                    break
                await asyncio.sleep(self.config.status_frequency)

        if cmd.target_altitude is not None and self._can_set_altitude:
            await self.call(self.dome, "SlewToAltitude", cmd.target_altitude)
            async with asyncio.timeout(self.config.timeout):
                while True:
                    slewing = await self.get(self.dome, "Slewing", True)
                    if not slewing:
                        break
                    await asyncio.sleep(1)

        logger.debug(
            f"moved to altitude={cmd.target_altitude}°, azimuth {cmd.target_azimuth:.1f}°"
        )

    async def status_publish(self):
        while True:
            try:
                d = self.dome
                connected = await self.get(d, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    shutter_status = await self.get(d, "ShutterStatus", None)
                    slewing = await self.get(d, "Slewing", False)
                    at_home = await self.get(d, "AtHome", False)
                    at_park = await self.get(d, "AtPark", False)

                    shutter_name = (
                        _SHUTTER_NAMES.get(shutter_status, "unknown")
                        if shutter_status is not None
                        else "unknown"
                    )
                    is_open = shutter_status == _SHUTTER_OPEN

                    await device.publish(Opened(is_open=is_open))

                    # Full IDomeV3 status — only include properties the dome supports
                    properties: dict = {
                        "shutter_status": shutter_name,
                        "slewing": slewing,
                        "at_home": at_home,
                        "at_park": at_park,
                    }

                    azimuth = None
                    altitude = None

                    if self._can_set_azimuth:
                        properties["can_set_azimuth"] = True
                        azimuth = await self.get(d, "Azimuth", None)
                        if azimuth is not None:
                            properties["azimuth"] = azimuth

                    if self._can_set_altitude:
                        properties["can_set_altitude"] = True
                        altitude = await self.get(d, "Altitude", None)
                        if altitude is not None:
                            properties["altitude"] = altitude

                    if azimuth is not None and altitude is not None:
                        await device.publish(AltAzPointing(
                            azimuth_degrees=azimuth,
                            altitude_degrees=altitude,
                        ))
                    elif azimuth is not None:
                        await device.publish(AltAzPointing(
                            azimuth_degrees=azimuth,
                            altitude_degrees=0.0,
                        ))

                    if self._can_slave:
                        properties["can_slave"] = True
                        properties["slaved"] = await self.get(d, "Slaved", False)

                    if self._can_find_home:
                        properties["can_find_home"] = True
                    if self._can_park:
                        properties["can_park"] = True
                    if self._can_set_park:
                        properties["can_set_park"] = True
                    if self._can_set_shutter:
                        properties["can_set_shutter"] = True
                    if self._can_sync_azimuth:
                        properties["can_sync_azimuth"] = True

                    # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                    # logger.debug(
                    #     f"Alpaca dome status: connected={connected}, shutter_status={shutter_status}, "
                    #     f"slewing={slewing}, {properties_str}"
                    # )

                    await device.publish(AlpacaDomeStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in dome status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class AlpacaDomeConfig(AlpacaDeviceConfig[AlpacaDome]):
    device_type: Literal["dome"] = "dome"
    status_frequency: float = 1.0
    timeout: float = 300.0

    @override
    def create_device(self):
        return AlpacaDome(self)


class AlpacaDomeState(AlpacaDeviceState):
    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
