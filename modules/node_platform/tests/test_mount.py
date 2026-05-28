"""Tests for Node Platform mount device."""

from unittest.mock import MagicMock

import pytest
from conftest import MockNodePlatformAPI

from sensorkit.models.devices import Deinit, FollowTarget, Home, Init, MoveToPark, Stop
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.mount import (
    NodePlatformMount,
    NodePlatformMountConfig,
    NodePlatformMountState,
)


@pytest.fixture
def api():
    return MockNodePlatformAPI()


@pytest.fixture
def mount(api):
    config = NodePlatformMountConfig(
        device_type="mount",
        host="localhost",
        status_frequency_slow=1.0,
        status_frequency_fast=0.1,
    )
    m = NodePlatformMount(config)
    m._api = api
    m.state = NodePlatformMountState()
    m.device_connected = True
    m.mount_slewing = False
    m.mount_tracking = False
    m._fast_status_task = None
    return m


class TestMountConfig:
    def test_defaults(self):
        config = NodePlatformMountConfig(device_type="mount", host="localhost")
        assert config.device_type == "mount"
        assert config.timeout == 300.0
        assert config.status_frequency_slow == 1.0
        assert config.status_frequency_fast == 0.1

    def test_create_device(self):
        config = NodePlatformMountConfig(device_type="mount", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformMount)


class TestMountInit:
    @pytest.mark.asyncio
    async def test_init_homes_and_sets_up_ota(self, mount, api):
        mount.state.has_been_homed = False
        mount.mount_slewing = False

        await mount.mount_init(Init())

        assert len(api.find_calls("v1_mount_go_to_home")) == 1

    @pytest.mark.asyncio
    async def test_init_skips_home_if_already_homed(self, mount, api):
        mount.state.has_been_homed = True

        await mount.mount_init(Init())

        assert len(api.find_calls("v1_mount_go_to_home")) == 0

    @pytest.mark.asyncio
    async def test_deinit_stops_and_parks(self, mount, api):
        mount.mount_slewing = False

        await mount.mount_deinit(Deinit())

        assert len(api.find_calls("v1_halt_mount")) == 1
        assert len(api.find_calls("v1_park_mount")) == 1


class TestMountCommands:
    @pytest.mark.asyncio
    async def test_home(self, mount, api):
        mount.mount_slewing = False

        await mount.mount_home(Home())

        assert len(api.find_calls("v1_mount_go_to_home")) == 1
        assert mount.state.has_been_homed is True

    @pytest.mark.asyncio
    async def test_park(self, mount, api):
        mount.mount_slewing = False

        await mount.mount_park(MoveToPark())

        assert len(api.find_calls("v1_park_mount")) == 1

    @pytest.mark.asyncio
    async def test_stop(self, mount, api):
        mount.mount_slewing = False

        await mount.mount_stop(Stop())

        assert len(api.find_calls("v1_halt_mount")) == 1

    @pytest.mark.asyncio
    async def test_home_requires_connected(self, mount):
        mount.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await mount.mount_home(Home())

    @pytest.mark.asyncio
    async def test_follow_target_requires_connected(self, mount):
        mount.device_connected = False

        cmd = MagicMock()
        with pytest.raises(DeviceConnectionError):
            await mount.mount_follow_target(cmd)


class TestMountFollowTarget:
    @pytest.mark.asyncio
    async def test_follow_tle(self, mount, api):
        from sensorkit.astro.common import TLE
        from sensorkit.astro.target import TLETarget

        mount.mount_tracking = True

        tle = TLE(
            line0="0 ISS (ZARYA)",
            line1="1 25544U 98067A   21275.52628565  .00001453  00000-0  35296-4 0  9991",
            line2="2 25544  51.6447 218.1320 0001432  95.8011  15.1138 15.48920210306114",
        )
        cmd = FollowTarget(target=TLETarget(tle=tle))
        await mount.mount_follow_target(cmd)

        calls = api.find_calls("v1_mount_follow_tle")
        assert len(calls) == 1


class TestMountFastStatus:
    @pytest.mark.asyncio
    async def test_start_fast_status(self, mount):
        assert mount._fast_status_task is None
        mount._start_fast_status()
        assert mount._fast_status_task is not None
        mount._stop_fast_status()

    @pytest.mark.asyncio
    async def test_stop_fast_status(self, mount):
        mount._start_fast_status()
        mount._stop_fast_status()
        assert mount._fast_status_task is None

    @pytest.mark.asyncio
    async def test_fast_active_property(self, mount):
        assert mount._fast_status_active is False
        mount._start_fast_status()
        assert mount._fast_status_active is True
        mount._stop_fast_status()
        assert mount._fast_status_active is False
