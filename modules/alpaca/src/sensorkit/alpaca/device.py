# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar, Literal

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.device import Device


class AlpacaDeviceState(BaseModel):
    """Generic Alpaca device state."""

    device_type: Literal[None] = None


class AlpacaDevice:
    """Generic Alpaca device."""

    device_name: ClassVar[str] = "Device"
    state_model: ClassVar[type[AlpacaDeviceState]] = AlpacaDeviceState

    def __init__(self, config: AlpacaDeviceConfig):
        self.config = config
        self.device_connected: bool | None = None
        self.state: AlpacaDeviceState = self.state_model()
        self._reconnect: Callable[[], Coroutine] | None = None

    @property
    def address(self) -> str:
        return f"{self.config.host}:{self.config.port}"

    async def restore_state(self):
        """Restore persisted device state from the KV store.

        Falls back to a fresh `state_model` instance if nothing is saved.
        """

        device = sk.device()
        try:
            self.state = await device.kv_get_model(self.state_model)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = self.state_model()

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

    async def call(self, device: Device, attr: str, *args, **kwargs) -> Any:
        """Async wrapper around the Alpyca SDK.

        The SDK is synchronous, so every call is dispatched
        via `asyncio.to_thread` to avoid blocking the event loop.
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


class AlpacaError(Exception):
    """Base exception for Alpaca device errors."""


class DeviceConnectionError(AlpacaError):
    """Device is not connected."""


class DeviceTimeoutError(AlpacaError):
    """Device operation timed out."""
