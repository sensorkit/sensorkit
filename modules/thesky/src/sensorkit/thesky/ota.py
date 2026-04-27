from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover
from sensorkit.thesky.device import (
    OTACommandInProgressError,
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device
class TheSkyOTA(TheSkyDevice):
    """TheSky OTA implementation."""

    config: TheSkyOTAConfig
    device_name = "OTA"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(TheSkyOTAState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyOTAState()

        await self.ota_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.ota_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def ota_init(self, cmd: sk.Init):
        self._reconnect = lambda: self.ota_connect(sk.Connect())
        await self.ota_connect(sk.Connect())

    @sk.command_handler
    async def ota_deinit(self, cmd: sk.Deinit):
        await self.ota_close(CloseMirrorCover())
        await self.ota_disconnect(sk.Disconnect())

    @sk.command_handler
    async def ota_connect(self, cmd: sk.Connect):
        logger.debug("connecting to thesky ota")

        await self.execute(
            """
            OpticalTubeAssembly.Connect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.isConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to thesky ota")

    @sk.command_handler
    async def ota_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from thesky ota")

        await self.execute(
            """
            OpticalTubeAssembly.Disconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.isConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from thesky ota")

    @sk.command_handler
    async def ota_open(self, cmd: OpenMirrorCover):
        await self.require_connected()
        logger.debug("opening thesky ota mirror cover")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""OpticalTubeAssembly.startOpenMirrorCover();""")
                    break
                except OTACommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.mirrorCoverState;""", "1")

        logger.debug("opened thesky ota mirror cover")

    @sk.command_handler
    async def ota_close(self, cmd: CloseMirrorCover):
        await self.require_connected()
        logger.debug("closing thesky ota mirror cover")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""OpticalTubeAssembly.startCloseMirrorCover();""")
                    break
                except OTACommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.mirrorCoverState;""", "0")

        logger.debug("closed thesky ota mirror cover")

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    Out = [
                        OpticalTubeAssembly.isConnected,
                        OpticalTubeAssembly.mirrorCoverState
                    ];
                    """
                )
            except Exception as e:
                logger.exception(f"Error in status_publish execute: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                connected, cover_num = [float(x) for x in resp.split(",")]
                cover_str = {0: "unknown", 1: "open", 2: "closed"}.get(int(cover_num), "unknown")

                connected = bool(connected)
                self.device_connected = connected

                is_open = int(cover_num) in (0, 1)

                # logger.debug(f"TheSky OTA status: connected={connected}, cover={cover_str}")

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(Opened(is_open=is_open))

            except Exception as e:
                logger.warning(f"Failed to update TheSky OTA status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyOTAConfig(TheSkyDeviceConfig[TheSkyOTA]):
    """TheSky OTA configuration."""

    device_type: Literal["ota"] = "ota"
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return TheSkyOTA(self)


class TheSkyOTAState(TheSkyDeviceState):
    """TheSky OTA state."""

    device_type: Literal["ota"] = "ota"
