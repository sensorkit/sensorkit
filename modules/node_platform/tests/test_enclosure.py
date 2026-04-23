"""Tests for Node Platform enclosure device."""

import asyncio

import ourskyai_node_platform_api as osapi
import pytest
from unittest.mock import MagicMock

from conftest import MockNodePlatformAPI, make_enclosure_status
from sensorkit.node_platform.enclosure import (
    NodePlatformEnclosure,
    NodePlatformEnclosureConfig,
    NodePlatformEnclosureState,
)
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
import sensorkit.api as sk


@pytest.fixture
def api():
    return MockNodePlatformAPI()


@pytest.fixture
def enclosure(api):
    config = NodePlatformEnclosureConfig(
        device_type="dome",
        host="localhost",
        port=9080,
        operation_mode="manual",
    )
    enc = NodePlatformEnclosure(config)
    enc._api = api
    enc.state = NodePlatformEnclosureState()
    enc.device_connected = True
    return enc


class TestEnclosureConfig:
    def test_defaults(self):
        config = NodePlatformEnclosureConfig(device_type="dome", host="localhost")
        assert config.device_type == "dome"
        assert config.timeout == 120.0
        assert config.operation_mode == "assisted"

    def test_create_device(self):
        config = NodePlatformEnclosureConfig(device_type="dome", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformEnclosure)


class TestEnclosureInit:
    @pytest.mark.asyncio
    async def test_init_homes_if_needed(self, enclosure, api):
        enclosure.state.has_been_homed = False

        # Mock status to show homing complete
        api.set_response("v1_home_enclosure_shutters", None)
        enclosure.shutter_state = osapi.EnclosureShutterState.CLOSED

        await enclosure.enclosure_init(sk.Init())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_sync_enclosure_rotator_with_mount")) == 1
        assert len(api.find_calls("v1_sync_enclosure_window_with_mount")) == 1

    @pytest.mark.asyncio
    async def test_init_skips_home_if_already_homed(self, enclosure, api):
        enclosure.state.has_been_homed = True

        await enclosure.enclosure_init(sk.Init())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_deinit_stops(self, enclosure, api):
        await enclosure.enclosure_deinit(sk.Deinit())

        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_halt_enclosure_window")) == 1


class TestEnclosureOpenClose:
    @pytest.mark.asyncio
    async def test_open_in_manual_mode(self, enclosure, api):
        enclosure.config = NodePlatformEnclosureConfig(
            device_type="dome", host="localhost", operation_mode="manual",
        )
        enclosure.shutter_state = osapi.EnclosureShutterState.OPENED

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_open_enclosure_shutters")) == 1

    @pytest.mark.asyncio
    async def test_open_skipped_in_assisted_mode(self, enclosure, api):
        enclosure.config = NodePlatformEnclosureConfig(
            device_type="dome", host="localhost", operation_mode="assisted",
        )

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_open_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_close_in_manual_mode(self, enclosure, api):
        enclosure.config = NodePlatformEnclosureConfig(
            device_type="dome", host="localhost", operation_mode="manual",
        )
        enclosure.shutter_state = osapi.EnclosureShutterState.CLOSED

        await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_close_enclosure_shutters")) == 1

    @pytest.mark.asyncio
    async def test_close_skipped_in_assisted_mode(self, enclosure, api):
        enclosure.config = NodePlatformEnclosureConfig(
            device_type="dome", host="localhost", operation_mode="assisted",
        )

        await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_close_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_open_requires_connected(self, enclosure):
        enclosure.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await enclosure.enclosure_open(OpenEnclosure())

    @pytest.mark.asyncio
    async def test_stop(self, enclosure, api):
        await enclosure.enclosure_stop(sk.Stop())

        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_halt_enclosure_window")) == 1
