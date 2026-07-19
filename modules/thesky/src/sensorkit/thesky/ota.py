# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.std import Connect, Connected, Disconnect, Opened
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

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyOTAState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyOTAState()

        # Initialize the OTA
        await self._initialize()
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.ota_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.ota_connect(Connect())
        await self.ota_connect(Connect())

    @sk.command_handler
    async def ota_connect(self, cmd: Connect):
        logger.debug("connecting to ota")

        await self.execute(
            """
            OpticalTubeAssembly.Connect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.isConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to ota")

    @sk.command_handler
    async def ota_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from ota")

        await self.execute(
            """
            OpticalTubeAssembly.Disconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.isConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from ota")

    @sk.command_handler
    async def ota_open(self, cmd: OpenMirrorCover):
        await self.require_connected()
        logger.debug("opening ota mirror cover")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""OpticalTubeAssembly.startOpenMirrorCover();""")
                    break
                except OTACommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.mirrorCoverState;""", "1")

        logger.debug("opened ota mirror cover")

    @sk.command_handler
    async def ota_close(self, cmd: CloseMirrorCover):
        await self.require_connected()
        logger.debug("closing ota mirror cover")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""OpticalTubeAssembly.startCloseMirrorCover();""")
                    break
                except OTACommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.mirrorCoverState;""", "0")

        logger.debug("closed ota mirror cover")

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
