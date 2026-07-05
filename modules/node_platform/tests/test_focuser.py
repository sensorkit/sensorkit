# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform focuser device."""

import pytest
from conftest import MockNodePlatformAPI

from sensorkit.models.devices import Stop
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.focuser import (
    NodePlatformFocuser,
    NodePlatformFocuserConfig,
    NodePlatformFocuserState,
)
from sensorkit.std.optics import ChangeFocusPosition


@pytest.fixture
def api():
    return MockNodePlatformAPI()


@pytest.fixture
def focuser(api):
    config = NodePlatformFocuserConfig(device_type="focuser", host="localhost")
    f = NodePlatformFocuser(config)
    f._api = api
    f.state = NodePlatformFocuserState()
    f.device_connected = True
    f.focuser_position = 15000.0
    f.focuser_moving = False
    return f


class TestFocuserConfig:
    def test_defaults(self):
        config = NodePlatformFocuserConfig(device_type="focuser", host="localhost")
        assert config.device_type == "focuser"
        assert config.timeout == 60.0

    def test_create_device(self):
        config = NodePlatformFocuserConfig(device_type="focuser", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformFocuser)


class TestFocuserCommands:
    @pytest.mark.asyncio
    async def test_move(self, focuser, api):
        cmd = ChangeFocusPosition(position=20000)
        await focuser.focuser_move(cmd)

        calls = api.find_calls("v1_go_to_focuser_position")
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_stop(self, focuser, api):
        await focuser.focuser_stop(Stop())

        assert len(api.find_calls("v1_halt_focuser")) == 1

    @pytest.mark.asyncio
    async def test_move_requires_connected(self, focuser):
        focuser.device_connected = False
        cmd = ChangeFocusPosition(position=10000)

        with pytest.raises(DeviceConnectionError):
            await focuser.focuser_move(cmd)
