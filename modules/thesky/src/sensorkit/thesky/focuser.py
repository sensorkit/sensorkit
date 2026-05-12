from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.std.optics import ChangeFocusPosition, FocusPosition
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device
class TheSkyFocuser(TheSkyDevice):
    """TheSky Focuser implementation."""

    config: TheSkyFocuserConfig
    device_name = "Focuser"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyFocuserState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyFocuserState()

        self.focuser_position: float | None = None

        # Initialize the focuser
        await self.focuser_init(sk.Init())
        self.start_status_loop(self.status_publish())

        # Ensure we have a position
        async with asyncio.timeout(self.config.timeout):
            while self.focuser_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.on_detach
    async def entity_deinit(self):
        # Deinitialize the focuser
        await self.focuser_deinit(sk.Deinit())

        # Clean up, disconnect
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.focuser_disconnect(sk.Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def focuser_init(self, cmd: sk.Init):
        # Connect to the hardware
        self._reconnect = lambda: self.focuser_connect(sk.Connect())
        await self.focuser_connect(sk.Connect())

    @sk.command_handler
    async def focuser_deinit(self, cmd: sk.Deinit):
        pass

    @sk.command_handler
    async def focuser_connect(self, cmd: sk.Connect):
        logger.debug("connecting to Focuser")

        await self.execute(
            """
            ccdsoftCamera.Asynchronous = 1;
            ccdsoftCamera.focConnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.focIsConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to Focuser")

    @sk.command_handler
    async def focuser_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from Focuser")

        await self.execute(
            """
            ccdsoftCamera.focDisconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.focIsConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from Focuser")

    @sk.command_handler
    async def focuser_change(self, cmd: ChangeFocusPosition):
        # NOTE: the TheSky focuser simulator does not allow setting limits (for testing). As of this release, we respect
        # limits set via config, but a future release ought to respect both config and TheSky limits.
        await self.require_connected()
        logger.debug(f"changing focuser position to {cmd.position}")

        if not (self.config.limit_min <= cmd.position <= self.config.limit_max):
            logger.error(f"Moving focus position ({cmd.position}) abandoned due to limits")
            raise RuntimeError(f"Focuser position ({cmd.position}) outside limits")

        relative_position = cmd.position - self.focuser_position

        if relative_position < 0:
            await self.execute(
                f"""
                ccdsoftCamera.focMoveIn({relative_position});
                """
            )
        elif relative_position > 0:
            await self.execute(
                f"""
                ccdsoftCamera.focMoveOut({relative_position});
                """
            )
        else:
            return

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.focPosition;""", f"{cmd.position}")

        logger.debug(f"changed focuser position to {cmd.position}")

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    Out = [
                        ccdsoftCamera.focIsConnected,
                        ccdsoftCamera.focPosition
                    ];
                    """
                )
            except Exception as e:
                logger.exception(f"Error in status_publish execute: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                connected, position = [float(x) for x in resp.split(",")]

                connected = bool(connected)
                self.device_connected = connected

                self.focuser_position = float(position)

                # logger.debug(
                #     f"TheSky focuser status: connected={connected}, position={self.focuser_position}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(FocusPosition(position=self.focuser_position))

            except Exception as e:
                logger.warning(f"Failed to update TheSky focuser status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyFocuserConfig(TheSkyDeviceConfig[TheSkyFocuser]):
    """TheSky Focuser configuration."""

    device_type: Literal["focuser"] = "focuser"
    limit_min: float = float("-inf")
    limit_max: float = float("inf")
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return TheSkyFocuser(self)


class TheSkyFocuserState(TheSkyDeviceState):
    """TheSky Focuser state."""

    device_type: Literal["focuser"] = "focuser"
