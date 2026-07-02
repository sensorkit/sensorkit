from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Home, MoveToPark, Opened, Stop
from sensorkit.std import Connect, Connected, Disconnect
from sensorkit.astro.common import AltAzPointing
from sensorkit.nina.device import NinaDevice, NinaDeviceConfig, NinaDeviceState
from sensorkit.std.enclosure import CloseEnclosure, MoveEnclosure, OpenEnclosure


@sk.declare_keyword
class NinaDomeStatus(BaseModel):
    shutter_status: str = "unknown"
    slewing: bool = False
    at_home: bool = False
    at_park: bool = False
    azimuth: float | None = None
    following: bool = False


@sk.declare_device
class NinaDome(NinaDevice):
    """NINA Dome implementation."""

    config: NinaDomeConfig
    device_name = "Dome"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaDomeState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaDomeState()

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
        await self.dome_connect(Connect())

        # Home, as needed
        if not self.state.has_been_homed:
            await self.dome_home(Home())

    @sk.command_handler
    async def dome_connect(self, cmd: Connect):
        await self.connect("dome")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def dome_disconnect(self, cmd: Disconnect):
        await self.disconnect("dome")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def dome_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping enclosure")
        await self.client.get("/equipment/dome/stop")
        logger.debug("stopped enclosure")

    @sk.command_handler
    async def dome_home(self, cmd: Home):
        await self.require_connected()
        logger.debug("homing enclosure")

        await self.client.get("/equipment/dome/home")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if info.get("AtHome", False):
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

        logger.debug("homed enclosure")

    @sk.command_handler
    async def dome_park(self, cmd: MoveToPark):
        await self.require_connected()
        logger.debug("parking enclosure")

        await self.client.get("/equipment/dome/park")

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if info.get("AtPark", False):
                    break
                await asyncio.sleep(self.config.status_frequency)

        logger.debug("parked enclosure")

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        await self.require_connected()
        logger.debug("opening enclosure")

        await self.client.get("/equipment/dome/open")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if info.get("ShutterStatus") == "ShutterOpen":
                    break
                await asyncio.sleep(1)

        logger.debug("opened enclosure")

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        await self.require_connected()
        logger.debug("closing enclosure")

        await self.client.get("/equipment/dome/close")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if info.get("ShutterStatus") == "ShutterClosed":
                    break
                await asyncio.sleep(1)

        logger.debug("closed enclosure")

    @sk.command_handler
    async def dome_move(self, cmd: MoveEnclosure):
        await self.require_connected()
        logger.debug(f"moving enclosure to azimuth {cmd.target_azimuth:.1f}°")

        await self.client.get(
            "/equipment/dome/slew",
            azimuth=cmd.target_azimuth,
            waitToFinish=True,
        )

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if not info.get("Slewing", False):
                    break
                await asyncio.sleep(1)

        logger.debug(f"moved enclosure to azimuth {cmd.target_azimuth:.1f}°")

    async def status_publish(self):
        while True:
            try:
                info = await self.info("dome")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    shutter_status = info.get("ShutterStatus", "unknown")
                    is_open = shutter_status == "ShutterOpen"

                    await device.publish(Opened(is_open=is_open))

                    fields: dict = {
                        "shutter_status": shutter_status,
                        "slewing": info.get("Slewing", False),
                        "at_home": info.get("AtHome", False),
                        "at_park": info.get("AtPark", False),
                    }

                    azimuth = info.get("Azimuth")
                    if azimuth is not None:
                        fields["azimuth"] = azimuth
                        await device.publish(AltAzPointing(
                            azimuth_degrees=azimuth,
                            altitude_degrees=0.0,
                        ))

                    following = info.get("Following")
                    if following is not None:
                        fields["following"] = following

                    await device.publish(NinaDomeStatus(**fields))

                    fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                    # logger.debug(f"NINA dome status: connected={connected}, {fields_str}")
            except Exception as e:
                logger.exception(f"Error in dome status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaDomeConfig(NinaDeviceConfig[NinaDome]):
    device_type: Literal["dome"] = "dome"
    status_frequency: float = 1.0
    timeout: float = 300.0

    @override
    def create_device(self):
        return NinaDome(self)


class NinaDomeState(NinaDeviceState):
    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
