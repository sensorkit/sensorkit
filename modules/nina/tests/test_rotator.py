import pytest
from conftest import MockNinaClient

import sensorkit.api as sk
from sensorkit.nina.rotator import NinaRotatorConfig, NinaRotatorState
from sensorkit.std.instrument import ChangeRotatorPosition


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "MechanicalPosition": 0.0,
        "Position": 0.0,
        "IsMoving": False,
        "StepSize": 1.0,
    })


@pytest.fixture
def rotator(client):
    config = NinaRotatorConfig(device_type="rotator")
    r = config.create_device()
    r._client = client
    r.state = NinaRotatorState()
    r.device_connected = True
    r.rotator_position = 0.0
    return r


class TestRotatorConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, rotator):
        rotator.device_connected = None
        await rotator.rotator_connect(sk.Connect())
        assert rotator.device_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self, client, rotator):
        await rotator.rotator_disconnect(sk.Disconnect())
        assert rotator.device_connected is False


class TestRotatorCommands:
    @pytest.mark.asyncio
    async def test_move(self, client, rotator):
        await rotator.rotator_move(ChangeRotatorPosition(position=45.0))
        reqs = client.find_requests("/equipment/rotator/move")
        assert len(reqs) == 1
        assert reqs[0][1]["positionAngle"] == 45.0

    @pytest.mark.asyncio
    async def test_stop(self, client, rotator):
        await rotator.rotator_stop(sk.Stop())
        reqs = client.find_requests("/equipment/rotator/stop-move")
        assert len(reqs) == 1
