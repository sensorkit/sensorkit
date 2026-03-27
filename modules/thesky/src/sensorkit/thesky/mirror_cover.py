from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected, Opened
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device
class TheSkyMirrorCover(TheSkyDevice):
    """TheSky MirrorCover implementation."""
    config: TheSkyMirrorCoverConfig
    device_name = "Mirror Cover"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(TheSkyMirrorCoverState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyMirrorCoverState()

        # Initialize the mirror cover
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.mirror_cover_init(sk.Init())

    @sk.command_handler
    async def mirror_cover_init(self, cmd: sk.Init):
        """Connect to the hardware, start publishing status."""
        # Connect to the hardware
        await self.mirror_cover_connect(sk.Connect())

        # Start mirror cover status publishing
        # TODO: Use service context ThreadGroup.
        logger.debug("starting thesky mirror cover status loop")
        self._status_task = asyncio.create_task(self.status_publish())

    @sk.command_handler
    async def mirror_cover_deinit(self, cmd: sk.Deinit):
        """Stop publishing status, disconnect from the hardware."""
        # Stop mirror cover status publishing
        logger.debug("stopping thesky mirror cover status loop")
        if hasattr(self, "_status_task"):
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        # Disconnect from the hardware
        await self.mirror_cover_disconnect(sk.Disconnect())

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state."""
        await sk.device().kv_put_model(self.state)

        # De-initialize the mirror cover
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.mirror_cover_deinit(sk.Deinit())

    @sk.command_handler
    async def mirror_cover_connect(self, cmd: sk.Connect):
        logger.debug("connecting to thesky mirror cover")
        await self.execute(
            """
            OpticalTubeAssembly.Connect();
            """
        )

        # Wait for the mirror cover to connect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.isConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to thesky mirror cover")

    @sk.command_handler
    async def mirror_cover_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from thesky mirror cover")
        await self.execute(
            """
            OpticalTubeAssembly.Disconnect();
            """
        )

        # Wait for the mirror cover to disconnect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.isConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from thesky mirror cover")

    @sk.command_handler
    async def mirror_cover_open(self, cmd: sk.Open):
        self.require_connected()
        logger.debug(f"opening thesky mirror cover")
        if self.state.cover_status in ["opening", "open"]:
            return

        await self.execute(
            """
            OpticalTubeAssembly.startOpenMirrorCover();
            """
        )

        # Wait for the mirror cover to open
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.mirrorCoverState;""", "1")
        logger.debug("opened thesky mirror cover")

    @sk.command_handler
    async def mirror_cover_close(self, cmd: sk.Close):
        self.require_connected()
        logger.debug(f"closing thesky mirror cover")
        if self.state.cover_status in ["closing", "closed"]:
            return

        await self.execute(
            """
            OpticalTubeAssembly.startCloseMirrorCover();
            """
        )

        # Wait for the mirror cover to close
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""OpticalTubeAssembly.mirrorCoverState;""", "0")
        logger.debug("closed thesky mirror cover")

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
                connected, cover_num = [float(x) for x in resp.split(',')]
                cover_str = {0: "unknown", 1: "open", 2: "closed"}.get(int(cover_num), "unknown")

                connected = bool(connected)
                self.device_connected = connected

                if self.state.cover_status is None:
                    self.state.cover_status = "unknown"
                elif cover_str in ("open", "closed"):
                    self.state.cover_status = cover_str
                await sk.device().kv_put_model(self.state)

                # logger.debug(
                #     f"TheSky mirror cover status: connected={connected}, cover={self.state.cover_status}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(
                    Opened(is_open=int(cover_num) in (0, 1))
                )

            except Exception as e:
                logger.warning(f"Failed to update TheSky mirror cover status ({e})")
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyMirrorCoverConfig(TheSkyDeviceConfig[TheSkyMirrorCover]):
    """TheSky MirrorCover configuration."""
    device_type: Literal["mirror_cover"] = "mirror_cover"
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return TheSkyMirrorCover(self)


class TheSkyMirrorCoverState(TheSkyDeviceState):
    """TheSky MirrorCover state."""
    device_type: Literal["mirror_cover"] = "mirror_cover"
    cover_status: str = "unknown"