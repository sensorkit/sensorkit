from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any, Literal

from loguru import logger
from pydantic import BaseModel

from alpaca.device import Device


@dataclass
class AlpacaDevice:
    """Generic Alpaca device."""

    config: AlpacaDeviceConfig
    device_connected: bool | None = field(default=None, init=False)
    device_name: str = "Device"
    _status_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _init_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _init_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)

    @property
    def address(self) -> str:
        return f"{self.config.host}:{self.config.port}"

    async def require_connected(self, timeout: float | None = None):
        """Wait for device init to complete and verify still connected."""
        if not self.device_connected:
            if not self._init_event.is_set():
                t = timeout if timeout is not None else self.config.timeout
                async with asyncio.timeout(t):
                    await self._init_event.wait()
            if not self.device_connected:
                raise DeviceConnectionError(f"{self.device_name} not connected")

    def _start_init(self, coro):
        """Run device init as a background task."""

        self._init_task = asyncio.create_task(coro)
        self._init_task.add_done_callback(self._on_init_done)

    def _on_init_done(self, task: asyncio.Task):
        if task.cancelled():
            return
        if exc := task.exception():
            logger.opt(exception=exc).error(f"{self.device_name} init failed")
        else:
            self._init_event.set()

    async def _cancel_init(self):
        """Cancel a running init task, if any."""

        if self._init_task is not None and not self._init_task.done():
            self._init_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._init_task

    async def call(self, device: Device, attr: str, *args, **kwargs) -> Any:
        """Async wrapper around the Alpyca SDK.

        The SDK is synchronous, so every call is dispatched
        via ``asyncio.to_thread`` to avoid blocking the event loop.
        """

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
        await asyncio.to_thread(device.Connect)

        async with asyncio.timeout(timeout):
            while True:
                connecting = await self.get(device, "Connecting", False)
                if not connecting:
                    break
                await asyncio.sleep(0.5)

        connected = await self.get(device, "Connected", False)
        if not connected:
            raise DeviceConnectionError(f"Failed to connect to {self.device_name}")

        self.device_connected = True
        logger.debug(f"connected to {self.device_name}")

    async def disconnect(self, device: Device, timeout: float = 30.0):
        logger.debug(f"disconnecting from {self.device_name}")
        await asyncio.to_thread(device.Disconnect)

        async with asyncio.timeout(timeout):
            while True:
                connecting = await self.get(device, "Connecting", False)
                if not connecting:
                    break
                await asyncio.sleep(0.5)

        self.device_connected = False
        logger.debug(f"disconnected from {self.device_name}")

    def start_status_loop(self, coro):
        """Start a background status publishing task, cancelling any existing one."""

        if self._status_task is not None and not self._status_task.done():
            self._status_task.cancel()
        self._status_task = asyncio.create_task(coro)

    async def stop_status_loop(self):
        """Cancel the background status publishing task."""

        if self._status_task is not None:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
            self._status_task = None


class AlpacaDeviceConfig[T: AlpacaDevice = AlpacaDevice](BaseModel):
    """Base configuration for an Alpaca device."""

    device_type: Literal[None] = None
    host: str = "localhost"
    port: int = 11111
    device_number: int = 0
    protocol: str = "http"
    timeout: float = 60.0
    status_frequency: float = 1.0

    def create_device(self) -> T:
        return AlpacaDevice(self)


class AlpacaDeviceState(BaseModel):
    """Generic Alpaca device state."""

    device_type: Literal[None] = None


class AlpacaError(Exception):
    """Base exception for Alpaca device errors."""


class DeviceConnectionError(AlpacaError):
    """Device is not connected."""


class DeviceTimeoutError(AlpacaError):
    """Device operation timed out."""
