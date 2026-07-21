# SPDX-License-Identifier: Apache-2.0
"""Base device for ASA Autoslew over ASCOM Alpaca.

Autoslew's remote surface is plain ASCOM Alpaca. Every ASA-specific feature
(satellite tracking, refraction, pointing model, dome/cover/nasmyth control) rides
the standard ASCOM extension mechanisms — ``Action`` / ``CommandString`` /
``CommandBool`` — on the single **Telescope** device. So the Telescope client is a
shared *backbone* every Autoslew device leans on for its ASA verbs, in addition to
its own typed Alpaca device (Focuser/Rotator/CoverCalibrator) where one exists.

The structure mirrors the ``alpaca`` module's ``AlpacaDevice`` (the import-linter
keeps each module in its own layer, so we cannot import it — this is a near-verbatim
copy plus the ASA-extension helpers). The alpyca SDK is synchronous; every call is
dispatched via ``asyncio.to_thread`` so the event loop never blocks.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

from alpaca.device import Device
from loguru import logger
from pydantic import BaseModel


# --------------------------------------------------------------------------- #
# Parsing helpers for ASA extension payloads
#
# Autoslew returns numbers as strings, sometimes with a COMMA decimal separator
# and/or a trailing '#'. Several JSON payloads also use MISSPELLED VB property
# names ("RigthAscension") or a lowercase "status". These two helpers tolerate
# all of that. (This VM emits dot-decimals; comma handling is defensive.)
# --------------------------------------------------------------------------- #
def _num(s: Any) -> float:
    """Parse an Autoslew numeric string: tolerate comma decimals and trailing '#'."""
    if isinstance(s, (int, float)):
        return float(s)
    return float(str(s).strip().rstrip("#").strip().replace(",", "."))


def _pick(d: dict, *keys: str, default: Any = None) -> Any:
    """First present key (case-insensitive) from a dict — tolerates ICD misspellings."""
    lower = {k.lower(): v for k, v in d.items()}
    for k in keys:
        if k.lower() in lower:
            return lower[k.lower()]
    return default


@dataclass
class AutoslewDevice:
    """Generic Autoslew (ASCOM Alpaca) device."""

    config: AutoslewDeviceConfig
    device_connected: bool | None = field(default=None, init=False)
    device_name: ClassVar[str] = "Device"
    # The Telescope backbone: set by each device's *_init, used for every ASA
    # Action/CommandString/CommandBool. On the mount it is also the primary device.
    telescope: Any = field(default=None, init=False, repr=False)
    _status_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _reconnect: Callable[[], Coroutine] | None = field(default=None, init=False, repr=False)

    @property
    def address(self) -> str:
        return f"{self.config.host}:{self.config.port}"

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
                raise DeviceConnectionError(f"{self.device_name} reconnect failed: {e}") from e
        else:
            raise DeviceConnectionError(f"{self.device_name} not connected")

    # ---- alpyca SDK wrappers (sync -> async) ------------------------------ #
    async def call(self, device: Device, attr: str, *args, **kwargs) -> Any:
        """Call an alpyca method (or read a property) off the event loop."""

        method = getattr(device, attr)
        if callable(method):
            return await asyncio.to_thread(method, *args, **kwargs)
        return method

    async def get(self, device: Device, attr: str, default: Any = None) -> Any:
        from alpaca.exceptions import NotImplementedException

        def _read():
            try:
                return getattr(device, attr)
            except NotImplementedException:
                return default
            except Exception:
                return default

        return await asyncio.to_thread(_read)

    async def put(self, device: Device, attr: str, value: Any) -> None:
        def _write():
            setattr(device, attr, value)

        await asyncio.to_thread(_write)

    async def connect(self, device: Device, timeout: float = 60.0):
        logger.debug(f"connecting to {self.device_name}")
        try:
            await asyncio.to_thread(device.Connect)
            async with asyncio.timeout(timeout):
                while await self.get(device, "Connecting", False):
                    await asyncio.sleep(0.5)
        except Exception:
            # Some Autoslew devices (e.g. the CoverCalibrator) don't implement the
            # Platform-7 Connect() method; fall back to the legacy Connected property.
            logger.debug(f"{self.device_name}: Connect() unavailable, using legacy Connected")
            await self.put(device, "Connected", True)

        connected = await self.get(device, "Connected", False)
        if not connected:
            raise DeviceConnectionError(f"Failed to connect to {self.device_name}")

        self.device_connected = True
        logger.debug(f"connected to {self.device_name}")

    async def disconnect(self, device: Device, timeout: float = 30.0):
        logger.debug(f"disconnecting from {self.device_name}")
        try:
            await asyncio.to_thread(device.Disconnect)
            async with asyncio.timeout(timeout):
                while await self.get(device, "Connecting", False):
                    await asyncio.sleep(0.5)
        except Exception:
            await self.put(device, "Connected", False)

        self.device_connected = False
        logger.debug(f"disconnected from {self.device_name}")

    # ---- ASA extension backbone (all ride the Telescope device) ----------- #
    async def action(self, name: str, params: str = "") -> str:
        """PUT .../telescope/N/action — returns a string always."""
        return await self.call(self.telescope, "Action", name, params)

    async def command_string(self, cmd: str, raw: bool = True) -> str:
        return await self.call(self.telescope, "CommandString", cmd, raw)

    async def command_bool(self, cmd: str, raw: bool = True) -> bool:
        return bool(await self.call(self.telescope, "CommandBool", cmd, raw))

    async def action_json(self, name: str, params: str = "") -> dict:
        """An Action whose string return value is a JSON object."""
        return json.loads(await self.action(name, params))

    # ---- status loop plumbing --------------------------------------------- #
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


class AutoslewDeviceConfig[T: AutoslewDevice = AutoslewDevice](BaseModel):
    """Base configuration for an Autoslew device."""

    device_type: Literal[None] = None
    host: str = "localhost"
    port: int = 11111  # Autoslew's Alpaca server is always 11111
    device_number: int = 0
    protocol: str = "http"
    timeout: float = 60.0
    status_frequency: float = 1.0  # aux devices; the mount adds slow/fast below

    def create_device(self) -> T:
        return AutoslewDevice(self)


class AutoslewDeviceState(BaseModel):
    """Generic Autoslew device state."""

    device_type: Literal[None] = None


class AutoslewError(Exception):
    """Base exception for Autoslew device errors."""


class DeviceConnectionError(AutoslewError):
    """Device is not connected."""


class DeviceTimeoutError(AutoslewError):
    """Device operation timed out."""
