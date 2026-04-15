from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
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

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()
        try:
            self.state = await device.kv_get_model(NinaDomeState)
        except Exception:
            self.state = NinaDomeState()

        await self.dome_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await self.dome_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def dome_init(self, cmd: sk.Init):
        self.device_name = "Dome"
        await self.connect("dome")
        await sk.device().publish(Connected(is_connected=True))
        self.start_status_loop(self.status_publish())

        if self.config.needs_homed and not self.state.has_been_homed:
            await self.dome_home(sk.Home())

    @sk.command_handler
    async def dome_deinit(self, cmd: sk.Deinit):
        await self.stop_status_loop()
        if not self.device_connected:
            return
        try:
            await self.client.get("/equipment/dome/park")
            await self.disconnect("dome")
            await sk.device().publish(Connected(is_connected=False))
        except Exception as e:
            logger.warning(f"Error during dome deinit: {e}")

    @sk.command_handler
    async def dome_connect(self, cmd: sk.Connect):
        await self.connect("dome")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def dome_disconnect(self, cmd: sk.Disconnect):
        await self.disconnect("dome")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def dome_home(self, cmd: sk.Home):
        self.require_connected()
        logger.debug("homing dome")
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
        logger.debug("homed dome")

    @sk.command_handler
    async def dome_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping dome")
        await self.client.get("/equipment/dome/stop")
        logger.debug("stopped dome")

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        self.require_connected()
        logger.debug("opening dome")
        await self.client.get("/equipment/dome/open")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if info.get("ShutterStatus") == "ShutterOpen":
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.state.shutter_state = "open"
        await sk.device().kv_put_model(self.state)
        logger.debug("opened dome")

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        self.require_connected()
        logger.debug("closing dome")
        await self.client.get("/equipment/dome/close")
        await asyncio.sleep(0.1)

        async with asyncio.timeout(self.config.timeout):
            while True:
                info = await self.info("dome")
                if info.get("ShutterStatus") == "ShutterClosed":
                    break
                await asyncio.sleep(self.config.status_frequency)

        self.state.shutter_state = "closed"
        await sk.device().kv_put_model(self.state)
        logger.debug("closed dome")

    @sk.command_handler
    async def dome_move(self, cmd: MoveEnclosure):
        self.require_connected()
        logger.debug(f"slewing dome to azimuth {cmd.target_azimuth:.1f}°")
        await self.client.get(
            "/equipment/dome/slew",
            azimuth=cmd.target_azimuth,
            waitToFinish=True,
        )
        logger.debug("slewed dome")

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
                    is_open = shutter_status in ("Open", "Opening")

                    if shutter_status == "Open":
                        self.state.shutter_state = "open"
                    elif shutter_status == "Closed":
                        self.state.shutter_state = "closed"

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

                    following = info.get("Following")
                    if following is not None:
                        fields["following"] = following

                    fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                    logger.debug(f"NINA dome status: connected={connected}, {fields_str}")

                    await device.publish(NinaDomeStatus(**fields))
            except Exception as e:
                logger.exception(f"Error in dome status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class NinaDomeConfig(NinaDeviceConfig[NinaDome]):
    device_type: Literal["dome"] = "dome"
    needs_homed: bool = False
    timeout: float = 300.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return NinaDome(self)


class NinaDomeState(NinaDeviceState):
    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
    shutter_state: str = "unknown"
