# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform base device."""

import pytest

from .fakes import RecordingSDK
from sensorkit.node_platform.device import (
    DeviceConnectionError,
    NodePlatformAPI,
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


class TestApiRequestTimeout:
    """NodePlatformAPI.call() should bound each request with _request_timeout."""

    @pytest.mark.asyncio
    async def test_injects_request_timeout(self):
        api = NodePlatformAPI(host="localhost", request_timeout=12.5)
        api._sdk = RecordingSDK()

        await api.call("v1_get_safety_status")

        ((_args, kwargs),) = api._sdk.find_calls("v1_get_safety_status")
        assert kwargs["_request_timeout"] == 12.5

    @pytest.mark.asyncio
    async def test_not_injected_when_unset(self):
        api = NodePlatformAPI(host="localhost")  # request_timeout defaults to None
        api._sdk = RecordingSDK()

        await api.call("v1_get_safety_status")

        ((_args, kwargs),) = api._sdk.find_calls("v1_get_safety_status")
        assert "_request_timeout" not in kwargs

    @pytest.mark.asyncio
    async def test_explicit_value_not_overridden(self):
        api = NodePlatformAPI(host="localhost", request_timeout=12.5)
        api._sdk = RecordingSDK()

        await api.call("v1_get_safety_status", _request_timeout=3.0)

        ((_args, kwargs),) = api._sdk.find_calls("v1_get_safety_status")
        assert kwargs["_request_timeout"] == 3.0
