# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform mount device."""

from dataclasses import replace

import pytest
import pytest_asyncio

from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.common.aio import AsyncLoop
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.mount import (
    NodePlatformMount,
    NodePlatformMountConfig,
    NodePlatformMountState,
)
from sensorkit.std import Deinit, FollowTarget, Home, Init, MoveToPark, Stop

from .fakes import (
    FakeNodePlatformAPI,
    MountStatus,
    make_mount_status,
)


def install_mount(api: FakeNodePlatformAPI) -> MountStatus:
    """Install a mount simulation whose motion commands drive its own status.

    The driver's settle wait polls v2_get_mount_status for slew onset and then for the flags to
    reach their commanded values, so a command has to actually move the simulated mount for it
    to return.
    """
    status = make_mount_status()

    def read(*a, **k):
        # A commanded slew shows up as motion on the next poll and has settled by the one after,
        # which is what the driver's onset-then-settle wait expects to see.
        if status.is_slewing:
            status.is_slewing = False
            return replace(status, is_slewing=True)

        return status

    def slew(*, tracking: bool):
        def command(*a, **k):
            status.is_slewing = True
            status.is_tracking = tracking

        return command

    def go_to_coordinates(req, *a, **k):
        # RA/Dec go-tos leave the mount tracking; alt/az go-tos park it at a fixed pointing.
        slew(tracking=req.ra is not None)()

    def halt(*a, **k):
        status.is_slewing = False
        status.is_tracking = False

    api.set_response("v2_get_mount_status", read)
    api.set_response("v1_mount_go_to_home", slew(tracking=False))
    api.set_response("v1_park_mount", slew(tracking=False))
    api.set_response("v1_mount_follow_tle", slew(tracking=True))
    api.set_response("v1_start_mount_track_path", slew(tracking=True))
    api.set_response("v1_go_to_mount_coordinates", go_to_coordinates)
    api.set_response("v1_halt_mount", halt)

    return status


@pytest.fixture
def api():
    api = FakeNodePlatformAPI()
    install_mount(api)
    return api


@pytest_asyncio.fixture
async def mount(api):
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
    m._geodetic = None
    m.status_loop = AsyncLoop(m.status_publish, interval=config.status_frequency_slow)
    m.fast_loop = AsyncLoop(m.status_publish_fast, interval=config.status_frequency_fast)

    yield m

    await m.status_loop.stop()
    await m.fast_loop.stop()


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

        with pytest.raises(DeviceConnectionError):
            await mount.mount_follow_target(
                FollowTarget(target=AltAzTarget(coords=Horizontal(az=180.0, alt=45.0)))
            )


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
