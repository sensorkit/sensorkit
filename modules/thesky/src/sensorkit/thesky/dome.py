from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import AltAzPointing, Connected, Opened
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.thesky.device import (
    CommandFailedError,
    DomeCommandInProgressError,
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
    send_thesky_script,
)


@sk.declare_keyword
class IsTracking(BaseModel):
    is_tracking: bool = False


@sk.declare_device
class TheSkyDome(TheSkyDevice):
    """TheSky Dome implementation."""

    config: TheSkyDomeConfig
    device_name = "Dome"
    _home_task: asyncio.Task | None = None

    # NOTE: the current implementation assumes that the dome is slaved to the mount (or that a clamshell style dome is
    # being used). A future version will support a dome that can rotate independently of the mount, but such
    # functionality would also be required at the core SensorKit level. Additional note: the dome must be unparked
    # before a GotoAzEl command but not before a FindHome command.

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(TheSkyDomeState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyDomeState()

        # Initialize the dome
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.dome_init(sk.Init())

    @sk.on_detach
    async def entity_deinit(self):
        await sk.device().kv_put_model(self.state)

        # De-initialize the dome
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.dome_deinit(sk.Deinit())

    @sk.command_handler
    async def dome_init(self, cmd: sk.Init):
        # Connect to the hardware
        await self.dome_connect(sk.Connect())

        # Start dome status publishing
        # TODO: Use service context ThreadGroup.
        logger.debug("starting thesky dome status loop")
        self.start_status_loop(self.status_publish())

        # Home as needed
        if not self.state.has_been_homed:
            self._home_task = asyncio.create_task(self.dome_home(sk.Home()))

    @sk.command_handler
    async def dome_deinit(self, cmd: sk.Deinit):
        # Connect to the hardware
        await self.dome_connect(sk.Connect())

        if self._home_task is not None:
            self._home_task.cancel()
            try:
                await self._home_task
            except asyncio.CancelledError:
                pass

        # Stop all current dome motion
        await self.dome_stop(sk.Stop())

        # Close the dome
        await self.dome_close(CloseEnclosure())

        # Send the dome to its park position
        await self.dome_park(sk.MoveToPark())

        # Stop dome status publishing
        logger.debug("stopping thesky dome status loop")
        await self.stop_status_loop()

        # Disconnect from the hardware
        await self.dome_disconnect(sk.Disconnect())

    @sk.command_handler
    async def dome_connect(self, cmd: sk.Connect):
        logger.debug("connecting to thesky dome")
        await self.execute(
            """
            sky6Dome.Connect();
            """
        )

        # Wait for the dome to connect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to thesky dome")

    @sk.command_handler
    async def dome_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from thesky dome")
        await self.execute(
            """
            sky6Dome.Disconnect();
            """
        )

        # Wait for the dome to disconnect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from thesky dome")

    @sk.command_handler
    async def dome_park(self, cmd: sk.MoveToPark):
        self.require_connected()
        logger.debug("parking thesky dome")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""sky6Dome.Park();""")
                    break
                except DomeCommandInProgressError:
                    await asyncio.sleep(0.5)

        # Wait for the dome to finish parking
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsParkComplete;""", "1")
        logger.debug("parked thesky dome")

    async def dome_unpark(self):
        # This is unique to TheSky. It requires you to unpark the dome before issuing any
        # other motion command.
        self.require_connected()
        logger.debug("unparking thesky dome")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute(
                        """
                        sky6Dome.Unpark();
                        """
                    )
                    break
                except DomeCommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsUnparkComplete;""", "1")
        logger.debug("unparked thesky dome")

    @sk.command_handler
    async def dome_home(self, cmd: sk.Home):
        self.require_connected()
        await self.dome_unpark()
        logger.debug("homing thesky dome")

        try:
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute("""sky6Dome.FindHome();""")
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            # Wait for the dome to finish homing
            async with asyncio.timeout(self.config.timeout):
                await self.poll("sky6Dome.IsFindHomeComplete;", "1")
        except CommandFailedError:
            logger.warning("Unable to home dome")
            return

        logger.debug("homed thesky dome")

        # Persist to state
        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def dome_stop(self, cmd: sk.Stop):
        self.require_connected()
        logger.debug("stopping thesky dome")

        # Send Abort on a separate TCP connection, bypassing the script lock.
        # This ensures we can stop the dome even while another command (e.g.
        # OpenSlit/CloseSlit) is in flight and holding the lock.
        await send_thesky_script(self.config.host, self.config.port, b"sky6Dome.Abort();")

        logger.debug("stopped thesky dome")

    async def _retry_with_reconnect(self, action, max_retries=2):
        """Try action; on CommandFailedError, reconnect to TheSky and retry."""
        for attempt in range(max_retries + 1):
            try:
                await action()
                return
            except CommandFailedError:
                if attempt == max_retries:
                    raise
                logger.warning(
                    f"dome command failed (attempt {attempt + 1}/{max_retries + 1}), "
                    f"reconnecting and retrying"
                )
                try:
                    await self.dome_disconnect(sk.Disconnect())
                except Exception:
                    pass
                await self.dome_connect(sk.Connect())

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        # NOTE: the SensorKit agent will send an Init task any time the demand state changes, which is any of service
        # up/down, target program, and/or context. In the case of the dome, that means opening it. Since opening is a
        # non-blocking operation in TheSky and may take several minutes, depending on the dome, we first check whether
        # it is currently opening or is already opened (via state) and return if so. While commanding an 'OpenSlit' on
        # an already opened dome ought to return instantly in TheSky, their dome simulator executes a non-negligible
        # time-consuming operation. And finally, slit status is persisted to state to account for domes with no position
        # recording (and so rely on TheSky recording state transitions).
        self.require_connected()
        await self.dome_unpark()

        logger.debug("opening thesky dome")

        async def _do_open():
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute("""sky6Dome.OpenSlit();""")
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            # Wait for the dome to open
            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6Dome.IsOpenComplete;""", "1")

        await self._retry_with_reconnect(_do_open)
        logger.debug("opened thesky dome")

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        # NOTE: see note in dome_open
        self.require_connected()
        await self.dome_unpark()
        logger.debug("closing thesky dome")

        async def _do_close():
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute("""sky6Dome.CloseSlit();""")
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            # Wait for the dome to close
            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6Dome.IsCloseComplete;""", "1")

        await self._retry_with_reconnect(_do_close)
        logger.debug("closed thesky dome")

    async def status_publish(self):
        while True:
            await self._is_mount_home_complete().wait()
            try:
                resp = await self.execute(
                    """
                    var Out;
                    sky6Dome.GetAzEl();
                    Out = [
                        sky6Dome.IsConnected,
                        sky6Dome.slitState(),
                        sky6Dome.dAz,
                        sky6Dome.IsGotoComplete
                    ];
                    """
                )
            except Exception as e:
                logger.exception(f"Error in status_publish execute: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                # 0=unknown, 1=open, 2=closed
                connected, slit_num, az, is_tracking = [float(x) for x in resp.split(",")]
                slit_str = {0: "unknown", 1: "open", 2: "closed"}.get(int(slit_num), "unknown")

                connected = bool(connected)
                self.device_connected = connected

                # logger.debug(
                #     f"TheSky dome status: connected={connected}, slit={slit_str}, az={az}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(Opened(is_open=int(slit_num) in (0, 1)))
                await device.publish(AltAzPointing(altitude_degrees=0, azimuth_degrees=az))
                await device.publish(IsTracking(is_tracking=bool(is_tracking)))

            except Exception as e:
                logger.warning(f"Failed to update TheSky dome status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyDomeConfig(TheSkyDeviceConfig[TheSkyDome]):
    """TheSky Dome configuration."""

    device_type: Literal["dome"] = "dome"
    timeout: float = 300.0
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return TheSkyDome(self)


class TheSkyDomeState(TheSkyDeviceState):
    """TheSky Dome state."""

    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
