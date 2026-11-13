# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform focuser device."""

import pytest

from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.focuser import (
    NodePlatformFocuser,
    NodePlatformFocuserConfig,
    NodePlatformFocuserState,
)
from sensorkit.std import ChangeFocusPosition, FocusPosition, Stop

from .fakes import FakeNodePlatformAPI, make_focuser_status


@pytest.fixture
def api():
    return FakeNodePlatformAPI()


@pytest.fixture
def focuser(api):
    config = NodePlatformFocuserConfig(
        device_type="focuser", host="localhost", timeout=1.0, status_frequency=0.001
    )
    f = NodePlatformFocuser(config)
    f._api = api
    f.state = NodePlatformFocuserState()
    f.device_connected = True
    f.focuser_position = 15000.0
    f.focuser_moving = False
    # focuser_change() polls v1_get_focuser_status until the move settles.
    api.set_response("v1_get_focuser_status", lambda *a, **k: make_focuser_status(moving=False))
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
    async def test_move(self, focuser, api, recorder):
        api.set_response("v1_get_focuser_status", make_focuser_status(position=20000))
        published = await recorder()
        cmd = ChangeFocusPosition(position=20000.75)
        await focuser.focuser_change(cmd)

        calls = api.find_calls("v1_go_to_focuser_position")
        assert len(calls) == 1
        assert calls[0][1][0].position == 20000
        assert (await published.wait_for(FocusPosition)).position == 20000.0

    @pytest.mark.asyncio
    async def test_stop(self, focuser, api):
        await focuser.focuser_stop(Stop())

        assert len(api.find_calls("v1_halt_focuser")) == 1

    @pytest.mark.asyncio
    async def test_move_requires_connected(self, focuser):
        focuser.device_connected = False
        cmd = ChangeFocusPosition(position=10000)

        with pytest.raises(DeviceConnectionError):
            await focuser.focuser_change(cmd)


class FocuserStatusSequence:
    """Return successive hardware statuses and repeat the final status."""

    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.reads = 0

    def __call__(self):
        self.reads += 1
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]


@pytest.mark.asyncio
async def test_move_waits_for_delayed_start_and_settling(focuser, api):
    statuses = FocuserStatusSequence(
        make_focuser_status(position=15000, moving=False),
        make_focuser_status(position=15000, moving=True),
        make_focuser_status(position=20000, moving=True),
        make_focuser_status(position=20000, moving=False),
    )
    api.set_response("v1_get_focuser_status", statuses)

    await focuser.focuser_change(ChangeFocusPosition(position=20000))

    assert statuses.reads == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("position", "moving"),
    [(19000, False), (20000, True), (20000, None)],
    ids=["stopped-short", "moving-at-target", "unknown-motion"],
)
async def test_move_times_out_without_verified_completion(focuser, api, position, moving):
    focuser.config.timeout = 0.03
    api.set_response(
        "v1_get_focuser_status", make_focuser_status(position=position, moving=moving)
    )

    with pytest.raises(TimeoutError, match="did not stop at 20000"):
        await focuser.focuser_change(ChangeFocusPosition(position=20000))


@pytest.mark.asyncio
async def test_move_requires_position(focuser, api):
    status = make_focuser_status()
    status.position = None
    api.set_response("v1_get_focuser_status", status)

    with pytest.raises(RuntimeError, match="no position after the move"):
        await focuser.focuser_change(ChangeFocusPosition(position=20000))
