# SPDX-License-Identifier: Apache-2.0
import pytest

from .fakes import FakeNinaClient
from sensorkit.nina.focuser import NinaFocuserConfig, NinaFocuserState
from sensorkit.std import Connect, Disconnect, Stop
from sensorkit.std.optics import ChangeFocusPosition


@pytest.fixture
def client():
    return FakeNinaClient(
        info_response={
            "Connected": True,
            "Position": 5000,
            "IsMoving": False,
            "Temperature": 20.0,
            "MaxStep": 50000,
            "StepSize": 1.0,
        }
    )


@pytest.fixture
def focuser(client):
    config = NinaFocuserConfig(device_type="focuser")
    f = config.create_device()
    f._client = client
    f.state = NinaFocuserState()
    f.device_connected = True
    f.focuser_position = 5000
    return f


class TestFocuserConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, focuser):
        focuser.device_connected = None
        await focuser.focuser_connect(Connect())
        assert focuser.device_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self, client, focuser):
        await focuser.focuser_disconnect(Disconnect())
        assert focuser.device_connected is False


class TestFocuserCommands:
    @pytest.mark.asyncio
    async def test_move(self, client, focuser):
        await focuser.focuser_change(ChangeFocusPosition(position=6000))
        reqs = client.find_requests("/equipment/focuser/move")
        assert len(reqs) == 1
        assert reqs[0][1]["position"] == 6000

    @pytest.mark.asyncio
    async def test_stop(self, client, focuser):
        await focuser.focuser_stop(Stop())
        reqs = client.find_requests("/equipment/focuser/stop-move")
        assert len(reqs) == 1
