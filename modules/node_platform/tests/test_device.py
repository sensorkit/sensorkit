"""Tests for Node Platform base device."""

import pytest

from sensorkit.node_platform.device import (
    DeviceConnectionError,
    NodePlatformDevice,
    NodePlatformDeviceConfig,
)


class TestRequireConnected:
    @pytest.mark.asyncio
    async def test_returns_when_connected(self):
        config = NodePlatformDeviceConfig(host="localhost")
        device = NodePlatformDevice(config)
        device.device_connected = True
        await device.require_connected()  # Should not raise

    @pytest.mark.asyncio
    async def test_raises_when_disconnected(self):
        config = NodePlatformDeviceConfig(host="localhost")
        device = NodePlatformDevice(config)
        device.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await device.require_connected()

    @pytest.mark.asyncio
    async def test_raises_when_none(self):
        config = NodePlatformDeviceConfig(host="localhost")
        device = NodePlatformDevice(config)
        # device_connected defaults to None

        with pytest.raises(DeviceConnectionError):
            await device.require_connected()
