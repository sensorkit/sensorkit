# SPDX-License-Identifier: Apache-2.0
"""Tests for the NINA base device class."""

import asyncio

import pytest

from sensorkit.nina.device import (
    DeviceConnectionError,
    NinaAPIError,
    NinaClient,
    NinaDevice,
    NinaDeviceConfig,
    NinaDeviceState,
)


class TestNinaDeviceConfig:
    def test_defaults(self):
        config = NinaDeviceConfig(host="localhost")
        assert config.host == "localhost"
        assert config.port == 1888
        assert config.timeout == 30.0
        assert config.status_frequency == 1.0
        assert config.env_file == ".env"

    def test_custom_values(self):
        config = NinaDeviceConfig(
            host="192.168.1.100",
            port=9999,
            timeout=120.0,
            env_file="/opt/sk/.env.nina",
        )
        assert config.host == "192.168.1.100"
        assert config.port == 9999
        assert config.env_file == "/opt/sk/.env.nina"

    def test_create_device(self):
        config = NinaDeviceConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, NinaDevice)


class TestNinaDeviceState:
    def test_serialization(self):
        state = NinaDeviceState()
        data = state.model_dump()
        assert data["device_type"] is None


class TestNinaDevice:
    @pytest.mark.asyncio
    async def test_require_connected_raises(self):
        config = NinaDeviceConfig(host="localhost")
        device = NinaDevice(config)
        device.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await device.require_connected()

    @pytest.mark.asyncio
    async def test_require_connected_none_raises(self):
        config = NinaDeviceConfig(host="localhost")
        device = NinaDevice(config)

        with pytest.raises(DeviceConnectionError):
            await device.require_connected()

    @pytest.mark.asyncio
    async def test_require_connected_ok(self):
        config = NinaDeviceConfig(host="localhost")
        device = NinaDevice(config)
        device.device_connected = True
        await device.require_connected()

    def test_client_property(self):
        config = NinaDeviceConfig(host="myhost", port=2000)
        device = NinaDevice(config)
        client = device.client
        assert isinstance(client, NinaClient)
        assert client.base_url == "http://myhost:2000/v2/api"
        # Same client returned on second access
        assert device.client is client

    @pytest.mark.asyncio
    async def test_start_stop_status_loop(self):
        config = NinaDeviceConfig(host="localhost")
        device = NinaDevice(config)

        called = asyncio.Event()

        async def dummy_loop():
            called.set()
            await asyncio.sleep(100)

        device.start_status_loop(dummy_loop())
        await asyncio.wait_for(called.wait(), timeout=1.0)
        assert device._status_task is not None

        await device.stop_status_loop()
        assert device._status_task is None


class TestNinaClient:
    def test_base_url(self):
        client = NinaClient(host="192.168.1.50", port=1888)
        assert client.base_url == "http://192.168.1.50:1888/v2/api"

    def test_no_auth_headers(self):
        client = NinaClient()
        headers = client._sign_request("http://example.com")
        assert headers == {}

    def test_auth_headers_with_token(self):
        client = NinaClient()
        client._token = "test_token"
        client._session_key = "test_key"
        headers = client._sign_request("http://example.com/test")
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_token"
        assert "X-Signature" in headers


class TestNinaAPIError:
    def test_error_with_status(self):
        err = NinaAPIError("something failed", status_code=500)
        assert str(err) == "something failed"
        assert err.status_code == 500
