from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger

import sensorkit.api as sk
from sensorkit.models.devices import Connected
from sensorkit.std.instrument import ChangeRotatorPosition, RotatorPosition
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device
class TheSkyRotator(TheSkyDevice):
    """TheSky Rotator implementation."""

    config: TheSkyRotatorConfig
    device_name = "Rotator"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(TheSkyRotatorState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyRotatorState()

        self.rotator_position = None

        # Initialize the rotator
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.rotator_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await sk.device().kv_put_model(self.state)

        # De-initialize the rotator
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.rotator_deinit(sk.Deinit())

    @sk.command_handler
    async def rotator_init(self, cmd: sk.Init):
        # Connect to the hardware
        await self.rotator_connect(sk.Connect())

        # Start rotator status publishing
        # TODO: Use service context ThreadGroup.
        logger.debug("starting thesky rotator status loop")
        self.start_status_loop(self.status_publish())

        # Ensure an initial rotator_position is received
        async with asyncio.timeout(self.config.timeout):
            while self.rotator_position is None:
                await asyncio.sleep(self.config.status_frequency)

    @sk.command_handler
    async def rotator_deinit(self, cmd: sk.Deinit):
        # Connect to the hardware
        await self.rotator_connect(sk.Connect())

        # Stop rotator status publishing
        logger.debug("stopping thesky rotator status loop")
        await self.stop_status_loop()

        # Disconnect from the hardware
        await self.rotator_disconnect(sk.Disconnect())

    @sk.command_handler
    async def rotator_connect(self, cmd: sk.Connect):
        logger.debug("connecting to thesky rotator")
        await self.execute(
            """
            ccdsoftCamera.Asynchronous = 1;
            ccdsoftCamera.rotatorConnect();
            """
        )

        # Wait for the rotator to connect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.rotatorIsConnected();""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to thesky rotator")

    @sk.command_handler
    async def rotator_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from thesky rotator")
        await self.execute(
            """
            ccdsoftCamera.rotatorDisconnect();
            """
        )

        # Wait for the rotator to disconnect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.rotatorIsConnected();""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from thesky rotator")

    @sk.command_handler
    async def rotator_move(self, cmd: ChangeRotatorPosition):
        self.require_connected()
        logger.debug(f"moving thesky rotator position to {cmd.position}")
        if not (
            self.config.limit_min <= self.rotator_position + cmd.position <= self.config.limit_max
        ):
            logger.error(f"setting rotator position ({cmd.position}) abandoned due to limits")
            raise RuntimeError(f"Rotator position ({cmd.position}) outside limits")

        await self.execute(
            f"""
            ccdsoftCamera.rotatorGotoPositionAngle({cmd.position});
            """
        )

        # Wait for the rotator move to complete
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.rotatorPositionAngle;""", f"{cmd.position}")
        logger.debug(f"moved thesky rotator position to {cmd.position}")

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    Out = [
                        ccdsoftCamera.rotatorIsConnected(),
                        ccdsoftCamera.rotatorPositionAngle()
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

                self.rotator_position = float(position)

                # logger.debug(
                #     f"TheSky rotator status: connected={connected}, position={self.rotator_position}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(RotatorPosition(position=self.rotator_position))

            except Exception as e:
                logger.warning(f"Failed to update TheSky rotator status ({e})")
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyRotatorConfig(TheSkyDeviceConfig[TheSkyRotator]):
    """TheSky Rotator configuration."""

    device_type: Literal["rotator"] = "rotator"
    limit_max: float
    limit_min: float
    timeout: float = 60.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return TheSkyRotator(self)


class TheSkyRotatorState(TheSkyDeviceState):
    """TheSky Rotator state."""

    device_type: Literal["rotator"] = "rotator"
