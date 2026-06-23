"""Base device behaviour tests for the sdasim module."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from sensorkit.sdasim.device import (
    DeviceConnectionError,
    SdasimDevice,
    SdasimDeviceConfig,
)


def make_device() -> SdasimDevice:
    return SdasimDevice(SdasimDeviceConfig(timeout=0.5))


class TestRequireConnected:
    @pytest.mark.asyncio
    async def test_returns_when_connected(self):
        device = make_device()
        device.device_connected = True
        await device.require_connected()  # no raise

    @pytest.mark.asyncio
    async def test_raises_without_reconnect(self):
        device = make_device()
        with pytest.raises(DeviceConnectionError):
            await device.require_connected()

    @pytest.mark.asyncio
    async def test_reconnects(self):
        device = make_device()
        reconnect = AsyncMock()

        async def _do_reconnect():
            await reconnect()
            device.device_connected = True

        device._reconnect = _do_reconnect
        await device.require_connected()
        reconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reconnect_failure_wrapped(self):
        device = make_device()
        device._reconnect = AsyncMock(side_effect=RuntimeError("boom"))
        with pytest.raises(DeviceConnectionError):
            await device.require_connected()


class TestStatusLoop:
    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        device = make_device()
        ran = asyncio.Event()

        async def loop():
            ran.set()
            await asyncio.sleep(3600)

        device.start_status_loop(loop())
        await asyncio.wait_for(ran.wait(), timeout=1.0)
        assert device._status_task is not None

        await device.stop_status_loop()
        assert device._status_task is None
