# SPDX-License-Identifier: Apache-2.0
"""Tests for the Alpaca base device class."""

import asyncio

import pytest

from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
    DeviceConnectionError,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice


class TestAlpacaDeviceConfig:
    def test_defaults(self):
        config = AlpacaDeviceConfig(host="localhost")
        assert config.host == "localhost"
        assert config.port == 11111
        assert config.device_number == 0
        assert config.protocol == "http"
        assert config.timeout == 60.0
        assert config.status_frequency == 1.0

    def test_custom_values(self):
        config = AlpacaDeviceConfig(
            host="192.168.1.100",
            port=7654,
            device_number=1,
            protocol="https",
            timeout=120.0,
            status_frequency=2.0,
        )
        assert config.host == "192.168.1.100"
        assert config.port == 7654
        assert config.device_number == 1
        assert config.protocol == "https"

    def test_create_device(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaDevice)


class TestAlpacaDeviceState:
    def test_serialization(self):
        state = AlpacaDeviceState()
        data = state.model_dump()
        assert data["device_type"] is None

        restored = AlpacaDeviceState.model_validate(data)
        assert restored.device_type is None


class TestAlpacaDevice:
    def test_address_property(self):
        config = AlpacaDeviceConfig(host="192.168.1.100", port=7654)
        device = AlpacaDevice(config)
        assert device.address == "192.168.1.100:7654"

    @pytest.mark.asyncio
    async def test_require_connected_raises(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)
        device.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await device.require_connected()

    @pytest.mark.asyncio
    async def test_require_connected_none_raises(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)
        # device_connected defaults to None

        with pytest.raises(DeviceConnectionError):
            await device.require_connected()

    @pytest.mark.asyncio
    async def test_require_connected_ok(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)
        device.device_connected = True
        await device.require_connected()  # Should not raise

    @pytest.mark.asyncio
    async def test_get_returns_default_on_not_implemented(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)

        from alpaca.exceptions import NotImplementedException

        sdk_device = FakeAlpacaSDKDevice(SomeProperty=NotImplementedException("not impl"))

        result = await device.get(sdk_device, "SomeProperty", "default_val")
        assert result == "default_val"

    @pytest.mark.asyncio
    async def test_get_returns_value(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)

        sdk_device = FakeAlpacaSDKDevice(Temperature=25.5)

        result = await device.get(sdk_device, "Temperature", None)
        assert result == 25.5

    @pytest.mark.asyncio
    async def test_put_sets_value(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)

        sdk_device = FakeAlpacaSDKDevice()
        await device.put(sdk_device, "BinX", 2)
        assert sdk_device.BinX == 2

    @pytest.mark.asyncio
    async def test_start_stop_status_loop(self):
        config = AlpacaDeviceConfig(host="localhost")
        device = AlpacaDevice(config)

        called = asyncio.Event()

        async def dummy_loop():
            called.set()
            await asyncio.sleep(100)

        device.start_status_loop(dummy_loop())
        await asyncio.wait_for(called.wait(), timeout=1.0)
        assert device._status_task is not None

        await device.stop_status_loop()
        assert device._status_task is None
