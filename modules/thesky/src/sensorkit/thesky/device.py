from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import ClassVar, Literal

from loguru import logger
from pydantic import BaseModel

SCRIPT_HEADER = b"/* Java Script */\n/* Socket Start Packet */\n"
SCRIPT_FOOTER = b"\n/* Socket End Packet */"

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


async def send_thesky_script(host: str, port: int | str, command: bytes | memoryview):
    # Connect to the TheSky endpoint.
    reader, writer = await asyncio.open_connection(host, port, limit=4096)

    # Write our command packet.
    writer.write(SCRIPT_HEADER + command + SCRIPT_FOOTER)
    await writer.drain()

    # Read the response and close the socket.
    response = await reader.read()
    writer.close()
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
                    return left[:sep].decode(errors="ignore")
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


@dataclass
class TheSkyDevice:
    """Generic TheSky device."""

    config: TheSkyDeviceConfig

    device_connected: bool | None = field(default=None, init=False)
    device_name: ClassVar[str] = "Device"
    _status_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _reconnect: Callable[[], Coroutine] | None = field(default=None, init=False, repr=False)

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

    async def execute(self, script: str):
        """Execute a TheSky script, serialized behind the shared script lock."""

        lock = _get_script_lock(self.config.host, self.config.port)

        async with lock:
            response = await send_thesky_script(
                self.config.host, self.config.port, script.encode()
            )
            return parse_thesky_response(response)

    def start_status_loop(self, coro):
        """Start a background status publishing task, cancelling any existing one."""

        if self._status_task is not None and not self._status_task.done():
            self._status_task.cancel()
        self._status_task = asyncio.create_task(coro)

    async def stop_status_loop(self):
        """Cancel the background status publishing task."""

        if self._status_task is not None:
            self._status_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await self._status_task

            self._status_task = None

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
            resp = await self.execute(script)
            if resp.strip() == expected:
                return
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

    def __init__(self, message: str, *, code: int = None):
        if type(self) is TheSkyError and code is not None:
            self.code = code
        super().__init__(message)

    def __init_subclass__(cls):
        cls.subtypes[cls.code] = cls


class DeviceConnectionError(TheSkyError):
    code = -1


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


class ObjectNotFoundError(TheSkyError):
    code = 250


class UnknownCommandError(TheSkyError):
    code = 303


class BadWeatherError(TheSkyError):
    code = 737


class TargetLostError(TheSkyError):
    code = 7501
