from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.models.devices import AltAzPointing, Connected, Opened
from sensorkit.std.enclosure import CloseEnclosure, MoveEnclosure, OpenEnclosure
from sensorkit.thesky.device import (
    CommandFailedError,
    DomeCommandInProgressError,
    ProcessAbortedError,
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

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyDomeState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyDomeState()

        # Initialize the dome
        await self.dome_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        # Deinitialize the dome
        await self.dome_deinit(sk.Deinit())

        # Clean up, disconnect
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.dome_disconnect(sk.Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def dome_init(self, cmd: sk.Init):
        # Connect to the hardware
        self._reconnect = lambda: self.dome_connect(sk.Connect())
        await self.dome_connect(sk.Connect())

        # Home, as needed
        if not self.state.has_been_homed:
            self._home_task = asyncio.create_task(self.dome_home(sk.Home()))

    @sk.command_handler
    async def dome_deinit(self, cmd: sk.Deinit):
        await self.dome_stop(sk.Stop())

        if self._home_task is not None:
            self._home_task.cancel()
            try:
                await self._home_task
            except asyncio.CancelledError:
                pass

        await self.dome_close(CloseEnclosure())
        await self.dome_park(sk.MoveToPark())

    @sk.command_handler
    async def dome_connect(self, cmd: sk.Connect):
        logger.debug("connecting to Dome")

        await self.execute(
            """
            sky6Dome.Connect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsConnected;""", "1")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to Dome")

    @sk.command_handler
    async def dome_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from Dome")

        await self.execute(
            """
            sky6Dome.Disconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsConnected;""", "0")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from Dome")

    @sk.command_handler
    async def dome_stop(self, cmd: sk.Stop):
        await self.require_connected()
        logger.debug("stopping dome")

        # Send Abort on a separate TCP connection, bypassing the script lock.
        # This ensures we can stop the dome even while another command (e.g.
        # OpenSlit/CloseSlit) is in flight and holding the lock.
        await send_thesky_script(self.config.host, self.config.port, b"sky6Dome.Abort();")

        logger.debug("stopped dome")

    @sk.command_handler
    async def dome_home(self, cmd: sk.Home):
        await self.require_connected()
        await self.dome_unpark()
        logger.debug("homing dome")

        try:
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute("""sky6Dome.FindHome();""")
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            async with asyncio.timeout(self.config.timeout):
                await self.poll("sky6Dome.IsFindHomeComplete;", "1")
        except CommandFailedError:
            logger.warning("Unable to home dome")
            return

        logger.debug("homed dome")

        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def dome_park(self, cmd: sk.MoveToPark):
        await self.require_connected()
        logger.debug("parking dome")

        async with asyncio.timeout(self.config.timeout):
            while True:
                try:
                    await self.execute("""sky6Dome.Park();""")
                    break
                except DomeCommandInProgressError:
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""sky6Dome.IsParkComplete;""", "1")

        logger.debug("parked dome")

    async def dome_unpark(self):
        # This is unique to TheSky. It requires you to unpark the dome before issuing any
        # other motion command.
        await self.require_connected()
        logger.debug("unparking dome")

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

        logger.debug("unparked dome")

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
                    f"Dome command failed (attempt {attempt + 1}/{max_retries + 1}), "
                    f"reconnecting and retrying"
                )
                try:
                    await self.dome_disconnect(sk.Disconnect())
                except Exception:
                    pass
                await self.dome_connect(sk.Connect())

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        await self.require_connected()
        await self.dome_unpark()

        logger.debug("opening dome")

        async def _do_open():
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute("""sky6Dome.OpenSlit();""")
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6Dome.IsOpenComplete;""", "1")

        await self._retry_with_reconnect(_do_open)

        logger.debug("opened dome")

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        await self.require_connected()
        await self.dome_unpark()
        logger.debug("closing dome")

        async def _do_close():
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute("""sky6Dome.CloseSlit();""")
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6Dome.IsCloseComplete;""", "1")

        await self._retry_with_reconnect(_do_close)

        logger.debug("closed dome")

    @sk.command_handler
    async def dome_move(self, cmd: MoveEnclosure):
        await self.require_connected()
        await self.dome_unpark()

        # GotoAzEl requires both az and el; if no target altitude was provided,
        # hold the current dome altitude.
        if cmd.target_altitude is None:
            resp = await self.execute(
                """
                sky6Dome.GetAzEl();
                Out = sky6Dome.dEl;
                """
            )
            target_el = float(resp)
        else:
            target_el = float(cmd.target_altitude)
        target_az = float(cmd.target_azimuth)

        logger.debug(f"moving dome to azimuth={target_az:.1f}°, elevation={target_el}°")

        async def _do_move():
            async with asyncio.timeout(self.config.timeout):
                while True:
                    try:
                        await self.execute(
                            f"""sky6Dome.GotoAzEl({target_az}, {target_el});"""
                        )
                        break
                    except DomeCommandInProgressError:
                        await asyncio.sleep(0.5)

            async with asyncio.timeout(self.config.timeout):
                await self.poll("""sky6Dome.IsGotoComplete;""", "1")

        await self._retry_with_reconnect(_do_move)

        logger.debug(f"moved dome to azimuth={target_az:.1f}°, elevation={target_el}°")

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    sky6Dome.GetAzEl();
                    Out = [
                        sky6Dome.IsConnected,
                        sky6Dome.slitState(),
                        sky6Dome.dEl,
                        sky6Dome.dAz,
                        sky6Dome.IsGotoComplete
                    ];
                    """
                )
            except ProcessAbortedError:
                # TheSky returns 212 transiently after an `abort`; dome is fine
                pass
            except Exception as e:
                logger.exception(f"Error in status_publish execute: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                # 0=unknown, 1=open, 2=closed
                connected, slit_num, alt, az, is_tracking = [float(x) for x in resp.split(",")]
                slit_str = {0: "unknown", 1: "open", 2: "closed"}.get(int(slit_num), "unknown")

                connected = bool(connected)
                self.device_connected = connected

                # logger.debug(
                #     f"TheSky dome status: connected={connected}, slit={slit_str}, alt={alt}, az={az}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(Opened(is_open=int(slit_num) in (0, 1)))
                await device.publish(AltAzPointing(altitude_degrees=alt, azimuth_degrees=az))
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
    status_frequency: float = 1.0
    timeout: float = 300.0

    @override
    def create_device(self):
        return TheSkyDome(self)


class TheSkyDomeState(TheSkyDeviceState):
    """TheSky Dome state."""

    device_type: Literal["dome"] = "dome"
    has_been_homed: bool = False
