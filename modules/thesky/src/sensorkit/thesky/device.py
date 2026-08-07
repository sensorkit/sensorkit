# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import ClassVar, Literal

from loguru import logger
from pydantic import BaseModel

SCRIPT_HEADER = b"/* Java Script */\n/* Socket Start Packet */\n"
SCRIPT_FOOTER = b"\n/* Socket End Packet */"

# Sentinel for `execute(timeout=...)`, distinguishing "use the device's configured
# timeout" (the default) from an explicit None, which means do not bound the call.
_CONFIG_TIMEOUT = object()

# Shared lock registry keyed by (host, port) - ensures all devices talking to the
# same TheSky server serialize their commands (scripts) when needed
_script_locks: dict[tuple[str, int], asyncio.Lock] = {}


def _get_script_lock(host: str, port: int) -> asyncio.Lock:
    """Get or create a shared lock for a TheSky server connection."""

    key = (host, port)
    try:
        lock = _script_locks[key]
        lock._get_loop()  # Raises RuntimeError if bound to a different loop
    except (KeyError, RuntimeError):
        _script_locks[key] = asyncio.Lock()
    return _script_locks[key]


async def send_thesky_script(
    host: str, port: int | str, command: bytes | memoryview, timeout: float | None = None
):
    async with asyncio.timeout(timeout):
        # Connect to the TheSky endpoint.
        reader, writer = await asyncio.open_connection(host, port, limit=4096)

        try:
            # Write our command packet.
            writer.write(SCRIPT_HEADER + command + SCRIPT_FOOTER)
            await writer.drain()

            # Read the response.
            response = await reader.read()
        finally:
            # Always release the socket, even if the read failed or timed out.
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

        return response


def parse_thesky_response(response: bytes):
    parts = response.split(b". Error = ")

    match parts:
        case (left, right):
            code = int(float(right))
            sep = left.rfind(b"|")

            if sep >= 0:
                if code == 0:
                    value = left[:sep].decode(errors="ignore")

                    # TheSky reports a busy script channel as a code-0 *value* rather
                    # than an error code. Raise it so callers treat it as the transient,
                    # retryable condition it is, instead of parsing the sentinel as data.
                    if "Another script is running" in value:
                        raise TheSkyError(message="Another script is running!", code=0)

                    return value
                else:
                    raise TheSkyError(message=left[sep + 1 :].decode(errors="ignore"), code=code)
        case (left, middle, _):
            sep = middle.find(b"|")
            code = int(float(middle[:sep]))
            raise TheSkyError(
                message=left.removeprefix(b"TypeError: ").decode(errors="ignore"),
                code=code,
            )

    raise TheSkyError(message="Could not parse response", code=-1)


class TheSkyDevice:
    """Generic TheSky device."""

    device_name: ClassVar[str] = "Device"

    def __init__(self, config: TheSkyDeviceConfig):
        self.config = config
        self.device_connected: bool | None = None
        self._reconnect: Callable[[], Coroutine] | None = None

    async def require_connected(self):
        """Verify the device is connected, attempting to reconnect if not."""

        if self.device_connected:
            return
        if self._reconnect is not None:
            logger.warning(f"{self.device_name} not connected, attempting reconnect")
            try:
                async with asyncio.timeout(self.config.timeout):
                    await self._reconnect()
            except Exception as e:
                raise DeviceConnectionError(
                    message=f"{self.device_name} reconnect failed: {e}", code=-1
                ) from e
        else:
            raise DeviceConnectionError(message=f"{self.device_name} not connected", code=-1)

    async def execute(self, script: str, timeout=_CONFIG_TIMEOUT):
        """Execute a TheSky script, serialized behind the shared script lock."""

        lock = _get_script_lock(self.config.host, self.config.port)

        async with lock:
            response = await send_thesky_script(
                self.config.host,
                self.config.port,
                script.encode(),
                timeout=self.config.timeout if timeout is _CONFIG_TIMEOUT else timeout,
            )
            return parse_thesky_response(response)

    async def poll(self, script: str, expected: str, delay: float = 0.1, interval: float = 1.0):
        """Poll TheSky for script completion.

        Parameters
        ----------
        script : str
            The JavaScript to execute.
        expected : str
            The result upon completion.
        delay : float
            A fudge factor [sec] to avoid command in progress errors. May remove this after demonstrated stability.
        interval : float
            How often to poll [sec].
        """

        await asyncio.sleep(delay)
        while True:
            try:
                resp = await self.execute(script)
                if resp.strip() == expected:
                    return
            except ScriptBusyError:
                logger.debug("Another script is running on TheSky, retrying poll")
            await asyncio.sleep(interval)


class TheSkyDeviceConfig[T: TheSkyDevice = TheSkyDevice](BaseModel):
    """Generic TheSky device configuration."""

    device_type: Literal[None]
    host: str
    port: int = 3040
    timeout: float = 60.0

    def create_device(self) -> T:
        return TheSkyDevice(self)


class TheSkyDeviceState(BaseModel):
    """Generic TheSky device state."""

    device_type: Literal[None]


class TheSkyError(Exception):
    """Base exception for TheSky device errors."""

    subtypes: ClassVar[dict[int, type[TheSkyError]]] = {}
    code: int = 0

    def __new__(cls, code, message):
        return super().__new__(cls.subtypes.get(code, cls), message)

    def __init__(self, message: str, *, code: int | None = None):
        if type(self) is TheSkyError and code is not None:
            self.code = code
        super().__init__(message)

    def __init_subclass__(cls):
        cls.subtypes[cls.code] = cls


class DeviceConnectionError(TheSkyError):
    code = -1


class ScriptBusyError(TheSkyError):
    code = 0


class FilterWheelCommandInProgressError(TheSkyError):
    code = 113


class FocuserCommandInProgressError(TheSkyError):
    code = 117


class OTACommandInProgressError(TheSkyError):
    code = 118


class CameraCommandInProgressError(TheSkyError):
    code = 120


class MountCommandInProgressError(TheSkyError):
    code = 121


class DomeCommandInProgressError(TheSkyError):
    code = 123


class RotatorCommandInProgressError(TheSkyError):
    code = 124


class CommandFailedError(TheSkyError):
    code = 206


class ProcessAbortedError(TheSkyError):
    code = 212


class LimitsExceededError(TheSkyError):
    code = 218


class CommandConflictError(TheSkyError):
    code = 219


class CommandNotSupportedError(TheSkyError):
    code = 228


class ObjectNotFoundError(TheSkyError):
    code = 250


class UnknownCommandError(TheSkyError):
    code = 303


class BadWeatherError(TheSkyError):
    code = 737


class TargetLostError(TheSkyError):
    code = 7501
