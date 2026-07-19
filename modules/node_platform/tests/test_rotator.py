# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform rotator device."""

import pytest
from conftest import MockNodePlatformAPI, make_rotator_status

from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.rotator import (
    NodePlatformRotator,
    NodePlatformRotatorConfig,
    NodePlatformRotatorState,
)
from sensorkit.std import Stop
from sensorkit.std.instrument import ChangeRotatorPosition


@pytest.fixture
def api():
    return MockNodePlatformAPI()


@pytest.fixture
def rotator(api):
    config = NodePlatformRotatorConfig(device_type="rotator", host="localhost", derotate=False)
    r = NodePlatformRotator(config)
    r._api = api
    r.state = NodePlatformRotatorState()
    r.device_connected = True
    r.rotator_position = 90.0
    r.rotator_moving = False
    # rotator_change() polls v1_get_rotator_status until the move settles.
    api.set_response("v1_get_rotator_status", lambda *a, **k: make_rotator_status(moving=False))
    return r


class TestRotatorConfig:
    def test_defaults(self):
        config = NodePlatformRotatorConfig(device_type="rotator", host="localhost")
        assert config.device_type == "rotator"
        assert config.derotate is False
        assert config.timeout == 60.0

    def test_create_device(self):
        config = NodePlatformRotatorConfig(device_type="rotator", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformRotator)


class TestRotatorInit:
    @pytest.mark.asyncio
    async def test_init_enables_derotation(self, rotator, api):
        rotator.config = NodePlatformRotatorConfig(
            device_type="rotator", host="localhost", derotate=True,
        )
        await rotator._initialize()

        assert len(api.find_calls("v1_enable_derotation_compensation")) == 1

    @pytest.mark.asyncio
    async def test_init_disables_derotation(self, rotator, api):
        await rotator._initialize()

        assert len(api.find_calls("v1_disable_derotation_compensation")) == 1

    @pytest.mark.asyncio
    async def test_deinit_disables_and_stops(self, rotator, api):
        await rotator._deinitialize()

        assert len(api.find_calls("v1_disable_derotation_compensation")) == 1
        assert len(api.find_calls("v1_halt_rotator")) == 1


class TestRotatorCommands:
    @pytest.mark.asyncio
    async def test_move(self, rotator, api):
        rotator.rotator_moving = False
        cmd = ChangeRotatorPosition(position=135.0)
        await rotator.rotator_change(cmd)

        calls = api.find_calls("v1_go_to_rotator_position")
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_stop(self, rotator, api):
        await rotator.rotator_stop(Stop())

        assert len(api.find_calls("v1_halt_rotator")) == 1

    @pytest.mark.asyncio
    async def test_move_requires_connected(self, rotator):
        rotator.device_connected = False
        cmd = ChangeRotatorPosition(position=45.0)

        with pytest.raises(DeviceConnectionError):
            await rotator.rotator_change(cmd)
