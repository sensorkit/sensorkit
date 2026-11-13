# SPDX-License-Identifier: Apache-2.0
"""Test PWI4 focuser device."""

import pytest

from sensorkit.pwi4.device import DeviceConnectionError
from sensorkit.pwi4.focuser import PWI4Focuser, PWI4FocuserConfig
from sensorkit.std import ChangeFocusPosition, FocusPosition, Stop

from .fakes import FakePWI4Client


class TestPWI4FocuserConfig:
    def test_defaults(self):
        config = PWI4FocuserConfig()
        assert config.device_type == "focuser"
        assert config.status_frequency == 1.0
        assert config.timeout == 60.0

    def test_create_device(self):
        config = PWI4FocuserConfig()
        client = FakePWI4Client()
        device = config.create_device(client)
        assert isinstance(device, PWI4Focuser)


class TestPWI4Focuser:
    @pytest.mark.asyncio
    async def test_init_connects_and_enables(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)

        await focuser._initialize()

        connect_reqs = client.find_requests("/focuser/connect")
        enable_reqs = client.find_requests("/focuser/enable")
        assert len(connect_reqs) == 1
        assert len(enable_reqs) == 1

    @pytest.mark.asyncio
    async def test_move(self, recorder):
        client = FakePWI4Client(status_overrides={"focuser.position": "20000.0"})
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True
        published = await recorder()

        await focuser.focuser_change(ChangeFocusPosition(position=20000))

        reqs = client.find_requests("/focuser/goto")
        assert len(reqs) == 1
        assert reqs[0][1] == {"target": 20000}
        assert (await published.wait_for(FocusPosition)).position == 20000.0

    @pytest.mark.asyncio
    async def test_move_requires_connected(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await focuser.focuser_change(ChangeFocusPosition(position=20000))

    @pytest.mark.asyncio
    async def test_stop(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        await focuser.focuser_stop(Stop())

        reqs = client.find_requests("/focuser/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_deinit_stops_and_disables(self):
        client = FakePWI4Client()
        config = PWI4FocuserConfig()
        focuser = PWI4Focuser(config=config, client=client)
        focuser.device_connected = True

        await focuser._deinitialize()

        # focuser_deinit stops and disables; disconnect happens on detach (entity_deinit).
        stop_reqs = client.find_requests("/focuser/stop")
        disable_reqs = client.find_requests("/focuser/disable")
        assert len(stop_reqs) == 1
        assert len(disable_reqs) == 1

    @pytest.mark.asyncio
    async def test_status_publish(self, recorder):
        client = FakePWI4Client()
        config = PWI4FocuserConfig(status_frequency=0.05)
        focuser = PWI4Focuser(config=config, client=client)
        published = await recorder()

        await focuser.status_publish()

        assert (await published.wait_for(FocusPosition)).position == 15000.0
        assert focuser.device_connected is True
        assert published.keys() >= {"Connected", "Enabled", "FocusPosition"}


class FocuserPWI4Client(FakePWI4Client):
    """Serve successive position and motion values from the external controller."""

    def __init__(self, statuses):
        super().__init__()
        self.statuses = list(statuses)
        self.status_reads = 0

    async def status(self):
        self.status_reads += 1
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return await super().status() | status


@pytest.fixture
def moving_focuser():
    def build(*statuses):
        client = FocuserPWI4Client(statuses)
        config = PWI4FocuserConfig(timeout=1.0, status_frequency=0.001)
        device = PWI4Focuser(config=config, client=client)
        device.device_connected = True
        return device, client

    return build


@pytest.mark.asyncio
async def test_move_waits_for_delayed_start_and_settling(moving_focuser):
    focuser, client = moving_focuser(
        {"focuser.position": "15000", "focuser.is_moving": "false"},
        {"focuser.position": "15000", "focuser.is_moving": "true"},
        {"focuser.position": "20000", "focuser.is_moving": "true"},
        {"focuser.position": "20000", "focuser.is_moving": "false"},
    )

    await focuser.focuser_change(ChangeFocusPosition(position=20000))

    assert client.status_reads == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("difference", [-1.0, -0.5, 0.0, 0.5, 1.0])
async def test_move_accepts_absolute_tolerance_boundary(moving_focuser, recorder, difference):
    position = 20000.25 + difference
    focuser, _ = moving_focuser({"focuser.position": str(position)})
    published = await recorder()

    await focuser.focuser_change(ChangeFocusPosition(position=20000.25))

    assert (await published.wait_for(FocusPosition)).position == position


@pytest.mark.asyncio
@pytest.mark.parametrize("difference", [-1.01, 1.01])
async def test_move_rejects_position_outside_tolerance(moving_focuser, difference):
    focuser, _ = moving_focuser({"focuser.position": str(20000.0 + difference)})
    focuser.config.timeout = 0.03

    with pytest.raises(TimeoutError, match="did not stop within"):
        await focuser.focuser_change(ChangeFocusPosition(position=20000))


@pytest.mark.asyncio
async def test_move_tolerance_does_not_grow_with_target(moving_focuser):
    focuser, _ = moving_focuser({"focuser.position": "1000000000002"})
    focuser.config.timeout = 0.03

    with pytest.raises(TimeoutError, match="did not stop within"):
        await focuser.focuser_change(ChangeFocusPosition(position=1000000000000))


@pytest.mark.asyncio
@pytest.mark.parametrize("position", [None, "invalid", "NaN"])
async def test_move_requires_valid_position_at_zero(moving_focuser, position):
    focuser, _ = moving_focuser({"focuser.position": position})

    with pytest.raises(RuntimeError, match="no (valid|finite) position"):
        await focuser.focuser_change(ChangeFocusPosition(position=0))


@pytest.mark.asyncio
@pytest.mark.parametrize("moving", [None, "unknown"])
async def test_move_requires_valid_motion_state(moving_focuser, moving):
    focuser, _ = moving_focuser({"focuser.position": "20000", "focuser.is_moving": moving})

    with pytest.raises(RuntimeError, match="no valid motion state"):
        await focuser.focuser_change(ChangeFocusPosition(position=20000))
