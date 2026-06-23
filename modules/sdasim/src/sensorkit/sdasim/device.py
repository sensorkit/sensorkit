from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import ClassVar, Literal

from loguru import logger
from pydantic import BaseModel


@dataclass
class SdasimDevice:
    """Generic sdasim simulated device.

    Provides the shared lifecycle scaffolding (connection tracking, reconnect,
    background status loop) used by every sdasim device, mirroring the pattern
    of the other SensorKit hardware modules. Unlike those modules there is no
    remote client to talk to -- the "hardware" is the in-process sdasim renderer.
    """

    config: SdasimDeviceConfig
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
                raise DeviceConnectionError(f"{self.device_name} reconnect failed: {e}") from e
        else:
            raise DeviceConnectionError(f"{self.device_name} not connected")

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


class SdasimDeviceConfig[T: SdasimDevice = SdasimDevice](BaseModel):
    """Generic sdasim device configuration."""

    device_type: Literal[None] = None
    timeout: float = 60.0
    status_frequency: float = 1.0

    def create_device(self) -> T:
        return SdasimDevice(self)


class SdasimDeviceState(BaseModel):
    """Base persistent state for an sdasim device."""

    device_type: Literal[None] = None


class SdasimError(Exception):
    """Base exception for sdasim device errors."""


class DeviceConnectionError(SdasimError):
    """Device is not connected."""
