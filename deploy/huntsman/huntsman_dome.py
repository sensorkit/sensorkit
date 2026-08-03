"""SensorKit plugin for the Huntsman dome.

Combines two subsystems in a single device:
  - Dome rotation via TheSkyX JavaScript TCP commands
  - Shutter control via the Musca microcontroller over Bluetooth (BT) serial

The Musca firmware has a watchdog timer: if Keep_dome_open is not received
within the timeout period, the shutter auto-closes. This module sends that
heartbeat once per minute while the shutter is open.

The Bluetooth serial link can drop unpredictably. When it does, the service
publishes Connected(is_connected=False) so the agent can shut down the
controller (via a conditional constraint on HuntsmanDome.Connected). A grace
period delays the False publish to allow a separate BT watchdog to power-cycle.

Lifecycle:
  - on_attach   — restore state, open the TheSkyX and Musca links, start the
                  status loop. NO hardware motion: the service starting (or a
                  container restarting in daylight) must never move the dome.
  - Init        — first hardware motion: unpark, then home if never homed.
  - Deinit      — return to a safe state: close the shutter, park the dome.
  - on_detach   — Deinit, then tear down both links and persist state.

Structure follows the standard SensorKit dome module pattern
(see thesky/dome.py): standalone class with per-device
script lock and standard execute/poll helpers, plus a `_MuscaShutter`
helper class encapsulating the BT side.

To run standalone:
    SENSORKIT_IMPORTS=huntsman_dome sensorkit service run huntsman_dome_service huntsman_dome.py
"""

from __future__ import annotations

import asyncio
import contextlib

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing
from sensorkit.std import (
    Connect,
    Connected,
    Deinit,
    Disconnect,
    Home,
    Init,
    MoveToPark,
    Opened,
    Stop,
)
from sensorkit.std.enclosure import (
    CloseEnclosure,
    MoveEnclosure,
    OpenEnclosure,
    StandardEnclosure,
)

# IsTracking is published on the dome's stream and consumed by mount.py via
# `dome.monitor(IsTracking)`. We MUST use the same class instance the mount
# imports, hence importing it rather than redeclaring.
from sensorkit.thesky.dome import IsTracking

# Error classes from the TheSky transport. We share these so callers
# (and our own retry logic) can use the same except clauses other modules use.
from sensorkit.thesky.device import (
    CommandFailedError,
    DeviceConnectionError,
    DomeCommandInProgressError,
    ProcessAbortedError,
    ScriptBusyError,
    TheSkyError,
    parse_thesky_response,
    send_thesky_script,
)


# ── Musca protocol constants ────────────────────────────────────────

OPEN_CMD = "Shutter_open"
CLOSE_CMD = "Shutter_close"
KEEP_OPEN_CMD = "Keep_dome_open"
STATUS_CMD = "Status_update"

# Status fields the Musca sends (everything else is skipped)
_MUSCA_STATUS_KEYS = {"Shutter", "Door", "Battery", "Solar_A", "Switch"}

# Fields that carry state we track. "Switch" is reported but ignored, so it
# must not count toward "the status snapshot is complete".
_MUSCA_STATE_KEYS = _MUSCA_STATUS_KEYS - {"Switch"}


# ── TheSky script transport ─────────────────────────────────────────

# Bound each TheSky round trip well below thesky_timeout: an unresponsive TheSky must
# not hold the script lock and stall every other caller, including the status loop.
_SCRIPT_TIMEOUT = 10.0


# ── Keywords ─────────────────────────────────────────────────────────


@sk.declare_keyword
class HuntsmanDomeStatus(BaseModel):
    """Extended dome telemetry combining TheSkyX and Musca data."""

    shutter: str = "unknown"
    door: str = "unknown"
    battery_voltage: float | None = None
    solar_current: float | None = None
    bt_connected: bool = False
    dome_az: float | None = None


# ── Config & State ───────────────────────────────────────────────────


class HuntsmanDomeConfig(BaseModel):
    # TheSky
    thesky_host: str = "localhost"
    thesky_port: int = 3040
    thesky_timeout: float = 300.0
    # Bound on an on-demand reconnect (see require_connected). Deliberately much
    # shorter than thesky_timeout so a dropped link can't stall a Stop for the
    # full motion timeout.
    thesky_connect_timeout: float = 30.0
    status_frequency: float = 5.0

    bt_disconnect_grace_period: float = 30.0  # delay before publishing Connected=False

    # IsTracking debounce: TheSky's IsGotoComplete occasionally flickers True for
    # a single sample mid-slew. Require this many consecutive True samples from
    # the status loop before publishing IsTracking(is_tracking=True). One False
    # immediately resets the counter so motion-resume is reflected without lag.
    tracking_stable_count: int = 3

    # Musca shutter (Bluetooth serial)
    serial_port: str = "/dev/rfcomm0"
    heartbeat_interval: float = 60.0
    shutter_timeout: float = 100.0
    min_battery_voltage: float = 12.0
    reconnect_delay: float = 5.0
    max_reconnect_delay: float = 60.0
    read_timeout: float = 3.0


# Unified-config section. Lets the dome's settings live under a top-level
# `huntsman_dome:` key in sensorkit.yaml (parsed by `sensorkit config load` /
# `sensorkit go -l`) instead of a separate `sensorkit kv load`.
#
# Because this is a loose file rather than an installed package, the section
# only registers when this module is imported *by name* — run with
# SENSORKIT_IMPORTS=huntsman_dome from /opt/sk. service_path=__name__ makes the
# parser auto-launch the entrypoint under `sensorkit go`, so do NOT also add a
# python_path `services:` entry for it: that loads this file a second time
# under a different module identity and re-runs declare_config_section, which
# raises "already registered".
sk.declare_config_section(
    "huntsman_dome",
    HuntsmanDomeConfig,
    entity_mapper=lambda raw: raw.pop("id", "huntsman_dome_service"),
    service_path=__name__,
)


class HuntsmanDomeState(BaseModel):
    has_been_homed: bool = False


# ── Musca serial transport (async line-based) ────────────────────────


class MuscaTransport:
    """Async line-based serial transport for the Musca protocol."""

    BAUD_RATE = 9600

    def __init__(self, config: HuntsmanDomeConfig):
        self.config = config
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._reconnect_delay = config.reconnect_delay

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self):
        # Imported lazily: every SensorKit process imports this module to register its
        # config section, but only the dome service ever opens the serial port.
        import serial_asyncio

        try:
            self._reader, self._writer = await serial_asyncio.open_serial_connection(
                url=self.config.serial_port,
                baudrate=self.BAUD_RATE,
            )
            self._connected = True
            self._reconnect_delay = self.config.reconnect_delay
            logger.info(f"Musca serial connected on {self.config.serial_port}")
        except Exception as e:
            self._connected = False
            raise ConnectionError(
                f"Failed to connect to Musca on {self.config.serial_port}: {e}"
            ) from e

    async def disconnect(self):
        self._connected = False
        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()

    async def reconnect(self):
        """Disconnect and reconnect with exponential backoff."""
        await self.disconnect()
        while True:
            logger.info(f"Musca reconnecting in {self._reconnect_delay:.0f}s...")
            await asyncio.sleep(self._reconnect_delay)
            try:
                async with asyncio.timeout(10.0):
                    await self.connect()
                return
            except (ConnectionError, TimeoutError):
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self.config.max_reconnect_delay
                )

    async def send(self, cmd: str) -> bool:
        if not self._connected or self._writer is None:
            return False
        try:
            self._writer.write(f"{cmd}\n".encode())
            await self._writer.drain()
            return True
        except Exception as e:
            logger.warning(f"Musca send failed: {e}")
            self._connected = False
            return False

    async def read_message(self) -> tuple[str, str] | None:
        """Read one status Key:Value line. Skips headers and empty lines."""
        if not self._connected or self._reader is None:
            return None
        try:
            line = await asyncio.wait_for(
                self._reader.readline(), timeout=self.config.read_timeout
            )
            if not line:
                logger.warning("Musca serial returned empty read")
                self._connected = False
                return None
            decoded = line.decode(errors="replace").strip()
            if not decoded:
                return None
            parts = decoded.split(":", 1)
            if len(parts) == 2 and parts[0].strip() in _MUSCA_STATUS_KEYS:
                return parts[0].strip(), parts[1].strip()
            # Skip non-status lines (Status: header, errors, etc.)
            return None
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.warning(f"Musca read failed: {e}")
            self._connected = False
        return None


# ── Musca shutter helper ─────────────────────────────────────────────


class _MuscaShutter:
    """Encapsulates the Musca side: transport, reader loop, heartbeat,
    shutter state, and BT-drop grace period.

    The HuntsmanDome class delegates all shutter-related operations here so
    its own command_handlers stay parallel to other dome modules.
    """

    def __init__(self, config: HuntsmanDomeConfig):
        self.config = config
        self.transport = MuscaTransport(config)
        self.shutter: str = "unknown"
        self.door: str = "unknown"
        self.battery_voltage: float | None = None
        self.solar_current: float | None = None
        self._keep_open = False
        self._bt_dropped_at: float | None = None
        self._reader_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        # Serializes open()/close() so their _keep_open writes and shutter commands
        # cannot interleave across await points. close() clears _keep_open *before*
        # taking this lock, so a pending close never leaves the heartbeat alive
        # behind an in-flight open.
        self._op_lock = asyncio.Lock()

    # ── public state ──

    @property
    def bt_connected(self) -> bool:
        return self.transport.connected

    @property
    def in_grace_period(self) -> bool:
        """True if BT is currently down but the grace period hasn't expired."""
        if self.transport.connected or self._bt_dropped_at is None:
            return False
        elapsed = asyncio.get_running_loop().time() - self._bt_dropped_at
        return elapsed < self.config.bt_disconnect_grace_period

    @property
    def connected_for_constraint(self) -> bool:
        """bt_connected OR within the post-drop grace period."""
        return self.bt_connected or self.in_grace_period

    @property
    def is_open(self) -> bool:
        return self.shutter in ("Open", "Opening")

    # ── lifecycle ──

    async def start(self):
        """Connect to Musca (non-fatal on failure) and start background tasks.

        Read-only: this asks for a status snapshot but never actuates the
        shutter, so it is safe to call from on_attach.
        """
        try:
            await self.transport.connect()
            await self.transport.send(STATUS_CMD)
            await self._poll_until_status_complete(timeout=10.0)
        except Exception as e:
            logger.warning(f"Musca initial connection failed: {e} — background loop will retry")

        self._reader_task = asyncio.create_task(self._reader_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Cancel background tasks and disconnect transport."""
        for task in (self._reader_task, self._heartbeat_task):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._reader_task = None
        self._heartbeat_task = None
        await self.transport.disconnect()

    async def request_fresh_status(self):
        """Ask Musca to publish a status snapshot (the reader loop will absorb it)."""
        if self.transport.connected:
            await self.transport.send(STATUS_CMD)

    # ── shutter operations ──

    async def open(self):
        async with self._op_lock:
            if not self.transport.connected:
                raise RuntimeError("Musca Bluetooth not connected — cannot open shutter")

            if self.shutter == "Open":
                self._keep_open = True
                return

            voltage = self.battery_voltage or 0
            if voltage < self.config.min_battery_voltage:
                raise RuntimeError(
                    f"Battery too low ({voltage:.1f}V < {self.config.min_battery_voltage:.1f}V)"
                )

            logger.info("Opening Musca shutter")
            await self.transport.send(OPEN_CMD)
            self._keep_open = True

            # Resend if shutter hasn't started moving after 5 seconds
            await asyncio.sleep(5.0)
            if self.shutter not in ("Open", "Opening"):
                logger.warning("Shutter didn't respond to first open command, resending")
                await self.transport.send(OPEN_CMD)

            await self._wait_for_shutter("Open")
            logger.info("Musca shutter opened")

    async def close(self):
        # Clear the heartbeat flag before contending for the lock: if an open() is
        # still in flight, its watchdog heartbeat must stop now so the Musca auto-closes
        # as a fallback even while our explicit close waits behind the open.
        self._keep_open = False

        async with self._op_lock:
            if self.shutter == "Closed":
                return

            if not self.transport.connected:
                logger.warning("BT disconnected — Musca watchdog should auto-close")
                return

            logger.info("Closing Musca shutter")
            await self.transport.send(CLOSE_CMD)
            await self._wait_for_shutter("Closed")
            logger.info("Musca shutter closed")

    # ── helpers ──

    def _apply_status(self, msg: tuple[str, str]):
        key, value = msg
        match key:
            case "Shutter":
                self.shutter = value
            case "Door":
                self.door = value
            case "Battery":
                try:
                    self.battery_voltage = float(value)
                except ValueError:
                    pass
            case "Solar_A":
                try:
                    self.solar_current = float(value)
                except ValueError:
                    pass
            case "Switch":
                pass

    async def _poll_until_status_complete(self, timeout: float = 10.0):
        """Read messages until all tracked status fields have been seen (or timeout)."""
        fields_seen: set[str] = set()
        async with asyncio.timeout(timeout):
            while not _MUSCA_STATE_KEYS <= fields_seen:
                msg = await self.transport.read_message()
                if msg:
                    self._apply_status(msg)
                    fields_seen.add(msg[0])
                else:
                    await asyncio.sleep(0.5)

    async def _wait_for_shutter(self, target: str):
        async with asyncio.timeout(self.config.shutter_timeout):
            while self.shutter != target:
                await asyncio.sleep(0.5)

    # ── background loops ──

    async def _publish_fast(self, dome_az: float | None = None):
        """Fast-path publish of HuntsmanDomeStatus (for BT-drop notification)."""
        try:
            await sk.device().publish(
                HuntsmanDomeStatus(
                    shutter=self.shutter,
                    door=self.door,
                    battery_voltage=self.battery_voltage,
                    solar_current=self.solar_current,
                    bt_connected=self.bt_connected,
                    dome_az=dome_az,
                )
            )
        except Exception as e:
            logger.warning(f"Fast publish failed: {e}")

    async def _reader_loop(self):
        """Continuously read Musca status lines and handle BT reconnection."""
        while True:
            if not self.transport.connected:
                if self._bt_dropped_at is None:
                    self._bt_dropped_at = asyncio.get_running_loop().time()
                    logger.warning("Musca BT dropped — grace period started")
                    # Immediate publish so the watchdog can react quickly.
                    await self._publish_fast()

                try:
                    await self.transport.reconnect()
                    await self.transport.send(STATUS_CMD)
                    self._bt_dropped_at = None
                    logger.info("Musca reconnected, status re-requested")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    continue

            msg = await self.transport.read_message()
            if msg:
                self._apply_status(msg)
            elif self.transport.connected:
                # Connected but no data — avoid busy loop
                await asyncio.sleep(0.5)

    async def _heartbeat_loop(self):
        """Send Keep_dome_open at regular intervals while shutter is open."""
        while True:
            await asyncio.sleep(self.config.heartbeat_interval)
            if self._keep_open and self.transport.connected:
                ok = await self.transport.send(KEEP_OPEN_CMD)
                if ok:
                    logger.debug("Musca heartbeat sent")
                else:
                    logger.warning("Musca heartbeat failed — link may be down")


# ── Dome ─────────────────────────────────────────────────────────────


@sk.declare_device(type=StandardEnclosure)
class HuntsmanDome:
    """Huntsman dome — TheSky-driven rotation + Musca-driven shutter."""

    def __init__(self, config: HuntsmanDomeConfig):
        self.config = config
        self.state = HuntsmanDomeState()
        self._shutter = _MuscaShutter(config)
        self._script_lock = asyncio.Lock()
        self._thesky_connected = False
        self._dome_az: float | None = None
        # IsGotoComplete (raw, this iteration) and IsTracking (debounced).
        self._is_goto_complete = False
        self._is_tracking_count = 0
        self._is_tracking = False
        # True once Init has unparked (and homed) the dome. The lock serializes the
        # Init sequence: an Init and an OpenEnclosure can arrive together, and both
        # reach it — the command handler directly, the open via _ensure_initialized.
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._status_task: asyncio.Task | None = None

    # ── TheSky helpers ──

    async def execute(self, script: str):
        """Run a TheSky script, serialized behind this device's script lock."""
        async with self._script_lock:
            response = await send_thesky_script(
                self.config.thesky_host,
                self.config.thesky_port,
                script.encode(),
                timeout=_SCRIPT_TIMEOUT,
            )
            return parse_thesky_response(response)

    async def poll(
        self, script: str, expected: str, delay: float = 0.1, interval: float = 1.0
    ):
        """Poll TheSky until *script* returns *expected*.

        Tolerant of transient errors (DomeCommandInProgressError, ProcessAbortedError,
        CommandFailedError) — keeps retrying. Bounded by the caller's asyncio.timeout.
        """
        await asyncio.sleep(delay)
        while True:
            try:
                resp = await self.execute(script)
                if resp.strip() == expected:
                    return
            except (DomeCommandInProgressError, ProcessAbortedError, CommandFailedError) as e:
                logger.debug(f"TheSky poll transient error, retrying: {e}")
            except TimeoutError as e:
                # send_thesky_script timed out — TheSky didn't respond. Retry.
                logger.debug(f"TheSky poll timeout, retrying: {e}")
            except TheSkyError as e:
                logger.debug(f"TheSky poll transient error, retrying: {e}")
            await asyncio.sleep(interval)

    async def require_connected(self):
        """Verify TheSky's dome is connected, attempting a reconnect if not.

        Mirrors TheSkyDevice.require_connected: we connect at attach, so a
        dropped link should be re-established on demand rather than failing
        the command outright.
        """
        if self._thesky_connected:
            return

        logger.warning("TheSky dome not connected, attempting reconnect")
        try:
            async with asyncio.timeout(self.config.thesky_connect_timeout):
                await self.dome_connect(Connect())
        except Exception as e:
            raise DeviceConnectionError(
                message=f"TheSky dome reconnect failed: {e}", code=-1
            ) from e

    async def _retry_with_reconnect(self, action, max_retries: int = 2):
        """Try *action*; on CommandFailedError, reconnect TheSky and retry."""
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
                    await self.dome_disconnect(Disconnect())
                except Exception:
                    pass
                await self.dome_connect(Connect())

    # ── Status loop plumbing ──

    def start_status_loop(self):
        """Start the status publishing task, cancelling any existing one."""
        if self._status_task is not None and not self._status_task.done():
            self._status_task.cancel()
        self._status_task = asyncio.create_task(self.status_publish())

    async def stop_status_loop(self):
        """Cancel the status publishing task."""
        if self._status_task is not None:
            self._status_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._status_task
            self._status_task = None

    # ── Lifecycle ──

    @sk.on_attach
    async def entity_init(self):
        """Open both links and start telemetry. Deliberately moves no hardware.

        The dome service is restarted by Docker (`restart: unless-stopped`) and
        may come up at any time of day, so attaching must never rotate the dome
        or actuate the shutter. That belongs to Init (`dome_init`).
        """
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(HuntsmanDomeState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = HuntsmanDomeState()

        # Connect the TheSky link. This is a link-level connect, not motion.
        # It has to happen here rather than in Init: the agent holds a
        # conditional constraint on HuntsmanDome.Connected, so the service must
        # publish Connected(True) as soon as it is up or the constraint fires.
        try:
            await self.dome_connect(Connect())
        except Exception as e:
            logger.warning(
                f"TheSky dome connect failed at attach: {e} — will retry on demand"
            )

        # Bring up Musca (non-fatal — reader loop will retry on its own). Only
        # reads status; the shutter is not touched.
        await self._shutter.start()

        # Start status loop.
        self.start_status_loop()

    @sk.on_detach
    async def entity_deinit(self):
        # Leave the hardware safe (shutter closed, dome parked) before tearing
        # the links down.
        await self.dome_deinit(Deinit())

        await self.stop_status_loop()
        await self._shutter.stop()

        try:
            await self.dome_disconnect(Disconnect())
        except Exception as e:
            logger.warning(f"Could not disconnect from TheSky dome: {e}")

        await sk.device().kv_put_model(self.state)

    # ── Command handlers ──

    @sk.command_handler
    async def dome_init(self, cmd: Init):
        """Bring the dome to an operational state — the first hardware motion.

        Unparking is required before TheSky accepts any motion command and
        before dome slaving can drive the rotation. An explicit Init always
        unparks, even if the dome was already initialized.
        """
        async with self._init_lock:
            await self._run_init()

    @sk.command_handler
    async def dome_deinit(self, cmd: Deinit):
        """Return the dome to a safe idle state: shutter closed, dome parked.

        Leaves the TheSky/Musca links and the status loop up so telemetry keeps
        flowing and a later Init can bring the dome back without reattaching.
        """
        # Shutter first: it runs off the Musca link, so it can still be closed
        # even if TheSky is unreachable, and closed is the safe state.
        try:
            await self.dome_close(CloseEnclosure())
        except Exception as e:
            logger.warning(f"Could not close shutter during deinit: {e}")

        try:
            await self.dome_park(MoveToPark())
        except Exception as e:
            logger.warning(f"Could not park dome during deinit: {e}")

    async def _ensure_initialized(self):
        """Run Init on demand, once.

        `SensorControl` sends Init ahead of OpenEnclosure, but the two run
        concurrently under the `concurrent_dome_init_open` policy, and an operator
        can open the dome directly. So a completed Init is treated as a
        precondition of opening rather than something the ordering guarantees.
        """
        async with self._init_lock:
            if not self._initialized:
                await self._run_init()

    async def _run_init(self):
        """Unpark and, if never homed, home. Callers must hold `_init_lock`."""
        await self.require_connected()
        await self._dome_unpark()

        # Home, as needed.
        if not self.state.has_been_homed:
            await self.dome_home(Home())

        self._initialized = True

    @sk.command_handler
    async def dome_connect(self, cmd: Connect):
        logger.debug("connecting to thesky dome")
        await self.execute("sky6Dome.Connect();")
        async with asyncio.timeout(self.config.thesky_timeout):
            await self.poll("sky6Dome.IsConnected;", "1")
        self._thesky_connected = True
        await sk.device().publish(Connected(is_connected=True))
        logger.debug("connected to thesky dome")

    @sk.command_handler
    async def dome_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from thesky dome")
        await self.execute("sky6Dome.Disconnect();")
        async with asyncio.timeout(self.config.thesky_timeout):
            await self.poll("sky6Dome.IsConnected;", "0")
        self._thesky_connected = False
        await sk.device().publish(Connected(is_connected=False))
        logger.debug("disconnected from thesky dome")

    @sk.command_handler
    async def dome_stop(self, cmd: Stop):
        await self.require_connected()
        logger.debug("stopping thesky dome")
        # Abort on a separate connection, bypassing the script lock, so we can
        # still stop while another command is holding the lock.
        await send_thesky_script(
            self.config.thesky_host,
            self.config.thesky_port,
            b"sky6Dome.Abort();",
            timeout=_SCRIPT_TIMEOUT,
        )
        logger.debug("stopped thesky dome")

    @sk.command_handler
    async def dome_home(self, cmd: Home):
        await self.require_connected()
        await self._dome_unpark()
        logger.debug("homing thesky dome")

        try:
            async with asyncio.timeout(self.config.thesky_timeout):
                while True:
                    try:
                        await self.execute("sky6Dome.FindHome();")
                        break
                    except (DomeCommandInProgressError, ScriptBusyError):
                        await asyncio.sleep(0.5)

            async with asyncio.timeout(self.config.thesky_timeout):
                await self.poll("sky6Dome.IsFindHomeComplete;", "1")
        except CommandFailedError:
            logger.warning("Unable to home dome")
            return
        except TimeoutError:
            # `poll` retries through TheSky errors, so a dome that refuses to
            # home surfaces as the outer timeout rather than CommandFailedError.
            # Homing is best-effort — don't fail Init (and with it, the open).
            logger.warning(
                f"Dome did not report homed within {self.config.thesky_timeout:.0f}s"
            )
            return

        logger.debug("homed thesky dome")
        self.state.has_been_homed = True
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def dome_park(self, cmd: MoveToPark):
        await self.require_connected()
        logger.debug("parking thesky dome")

        async with asyncio.timeout(self.config.thesky_timeout):
            while True:
                try:
                    await self.execute("sky6Dome.Park();")
                    break
                except (DomeCommandInProgressError, ScriptBusyError):
                    await asyncio.sleep(0.5)

        async with asyncio.timeout(self.config.thesky_timeout):
            await self.poll("sky6Dome.IsParkComplete;", "1")

        # Parked: the next motion needs a fresh unpark, so make Init run again.
        self._initialized = False

        logger.debug("parked thesky dome")

    async def _dome_unpark(self):
        """Unpark the dome (required before any motion command in TheSky)."""
        await self.require_connected()
        logger.debug("unparking thesky dome")

        async with asyncio.timeout(self.config.thesky_timeout):
            while True:
                try:
                    await self.execute("sky6Dome.Unpark();")
                    break
                except (DomeCommandInProgressError, ScriptBusyError):
                    await asyncio.sleep(0.5)
                except CommandFailedError:
                    # Already unparked — not an error
                    logger.debug("dome unpark returned command failed; check TheSky")
                    return

        async with asyncio.timeout(self.config.thesky_timeout):
            await self.poll("sky6Dome.IsUnparkComplete;", "1")
        logger.debug("unparked thesky dome")

    @sk.command_handler
    async def dome_open(self, cmd: OpenEnclosure):
        # An open can arrive before Init has finished, so make sure the rotation
        # side is ready (unparked, homed) before the shutter moves.
        await self._ensure_initialized()
        await self._shutter.open()
        await sk.device().publish(Opened(is_open=True))

    @sk.command_handler
    async def dome_close(self, cmd: CloseEnclosure):
        await self._shutter.close()
        await sk.device().publish(Opened(is_open=False))

    @sk.command_handler
    async def dome_move(self, cmd: MoveEnclosure):
        """Slew the dome to a target azimuth (and optional altitude).

        Mirrors thesky/dome.py:dome_move semantics — uses GotoAzEl with the
        current dome elevation if target_altitude is not provided.
        """
        await self.require_connected()
        await self._dome_unpark()

        if cmd.target_altitude is None:
            # Read the current dome elevation, tolerating a transient busy
            # TheSky script channel ("Another script is running!", code 0) the
            # same way the GotoAzEl loop below does.
            async with asyncio.timeout(self.config.thesky_timeout):
                while True:
                    try:
                        resp = await self.execute(
                            """
                            var Out;
                            sky6Dome.GetAzEl();
                            Out = sky6Dome.dEl;
                            """
                        )
                        target_el = float(resp)
                        break
                    except (DomeCommandInProgressError, ScriptBusyError):
                        await asyncio.sleep(0.5)
                    except TheSkyError as e:
                        # Busy shared script channel — retry. Re-raise anything
                        # else (e.g. CommandFailedError) for the caller.
                        if e.code != 0:
                            raise
                        logger.debug(f"GetAzEl transient busy channel, retrying: {e}")
                        await asyncio.sleep(0.5)
        else:
            target_el = float(cmd.target_altitude)
        target_az = float(cmd.target_azimuth)

        logger.debug(f"moving thesky dome to azimuth={target_az:.1f}°, elevation={target_el}°")

        async def _do_move():
            async with asyncio.timeout(self.config.thesky_timeout):
                while True:
                    try:
                        await self.execute(
                            f"sky6Dome.GotoAzEl({target_az}, {target_el});"
                        )
                        break
                    except (DomeCommandInProgressError, ScriptBusyError):
                        await asyncio.sleep(0.5)
                    except TheSkyError as e:
                        # Busy shared script channel ("Another script is
                        # running!", code 0) — retry. Re-raise anything else
                        # (e.g. CommandFailedError) for _retry_with_reconnect.
                        if e.code != 0:
                            raise
                        logger.debug(f"GotoAzEl transient busy channel, retrying: {e}")
                        await asyncio.sleep(0.5)

            async with asyncio.timeout(self.config.thesky_timeout):
                await self.poll("sky6Dome.IsGotoComplete;", "1")

        await self._retry_with_reconnect(_do_move)

        logger.debug(f"moved thesky dome to azimuth={target_az:.1f}°, elevation={target_el}°")

    # ── Status publishing ──

    async def status_publish(self):
        """Periodically query TheSky for dome state, refresh Musca, publish everything.

        Tolerant of TheSky's transient ProcessAborted errors (mount holding the
        script channel during long ops, etc.) so this loop keeps producing events
        even while the mount is busy.
        """
        while True:
            # Query TheSky.
            try:
                resp = await self.execute(
                    """
                    var Out;
                    sky6Dome.GetAzEl();
                    Out = [
                        sky6Dome.IsConnected,
                        sky6Dome.dAz,
                        sky6Dome.IsGotoComplete
                    ];
                    """
                )
                connected_num, az, goto_complete = [float(x) for x in resp.split(",")]
                self._thesky_connected = bool(connected_num)
                self._dome_az = az
                self._is_goto_complete = bool(goto_complete)

                # Debounce IsTracking: require N consecutive True samples to
                # filter out TheSky's transient mid-slew IsGotoComplete=True
                # flickers. One False resets immediately so motion-resume is
                # reflected without lag.
                if self._is_goto_complete:
                    self._is_tracking_count += 1
                else:
                    self._is_tracking_count = 0
                self._is_tracking = (
                    self._is_tracking_count >= self.config.tracking_stable_count
                )

            except ProcessAbortedError:
                # Transient — TheSky was mid-abort or a script was just cancelled.
                pass
            except (DomeCommandInProgressError, CommandFailedError) as e:
                logger.debug(f"status_publish transient TheSky error: {e}")
            except TimeoutError as e:
                # send_thesky_script timed out — TheSky was unresponsive this
                # cycle. Lock is released; we'll retry on the next iteration.
                logger.debug(f"status_publish TheSky timeout: {e}")
            except TheSkyError as e:
                # "Another script is running!" and other code-0 conditions —
                # TheSky's shared script channel is briefly busy. Log the bare
                # message; keep the warning for genuinely unexpected errors.
                if "Another script is running" in str(e):
                    logger.debug(str(e))
                else:
                    logger.warning(f"TheSky dome status query failed: {e}")
            except Exception as e:
                logger.warning(f"TheSky dome status query failed: {e}")

            # Ask Musca for a fresh status (reader loop will absorb the reply).
            try:
                await self._shutter.request_fresh_status()
            except Exception as e:
                logger.warning(f"Musca status request failed: {e}")

            # Compose and publish.
            try:
                await self._publish_all()
            except Exception as e:
                logger.warning(f"publish failed: {e}")

            logger.debug(
                f"dome status: az={self._dome_az} "
                f"is_goto_complete={self._is_goto_complete} "
                f"is_tracking={self._is_tracking} "
                f"(stable_count={self._is_tracking_count}/{self.config.tracking_stable_count}) "
                f"shutter={self._shutter.shutter} bt_connected={self._shutter.bt_connected} "
                f"thesky_connected={self._thesky_connected}"
            )

            await asyncio.sleep(self.config.status_frequency)

    async def _publish_all(self):
        """Publish all keywords from current state."""
        device = sk.device()

        # Connected = TheSky_connected AND (bt_connected OR in grace period)
        both_connected = self._thesky_connected and self._shutter.connected_for_constraint
        await device.publish(Connected(is_connected=both_connected))

        # Opened (Musca-side)
        await device.publish(Opened(is_open=self._shutter.is_open))

        # AltAzPointing (TheSky-side). This dome is azimuth-only — sky6Dome.dEl
        # reads a dead -100 sentinel — so altitude is reported as 0.
        if self._dome_az is not None:
            await device.publish(
                AltAzPointing(altitude_degrees=0, azimuth_degrees=self._dome_az)
            )

        # IsTracking — TheSky's IsGotoComplete: the dome has caught up.
        await device.publish(IsTracking(is_tracking=self._is_tracking))

        # Custom composite status (consumed by huntsman_dome_watchdog.py).
        await device.publish(
            HuntsmanDomeStatus(
                shutter=self._shutter.shutter,
                door=self._shutter.door,
                battery_voltage=self._shutter.battery_voltage,
                solar_current=self._shutter.solar_current,
                bt_connected=self._shutter.bt_connected,
                dome_az=self._dome_az,
            )
        )


# ── Service entrypoint ───────────────────────────────────────────────


@sk.service_entrypoint(version=sk.VERSION)
async def service(svc: sk.Service):
    await svc.register()

    config = await svc.context.kv_get_model(HuntsmanDomeConfig)

    dome = HuntsmanDome(config)
    svc.include(dome, name="HuntsmanDome")

    await svc.run()
