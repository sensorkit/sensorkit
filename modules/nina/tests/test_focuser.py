# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.nina.focuser import NinaFocuserConfig, NinaFocuserState
from sensorkit.std import ChangeFocusPosition, Connect, Disconnect, FocusPosition, Stop

from .fakes import FakeNinaClient


class FocuserNinaClient(FakeNinaClient):
    """Serve a sequence of hardware readings after the move request."""

    def __init__(self):
        super().__init__(
            info_response={
                "Connected": True,
                "Position": 5000,
                "IsMoving": False,
                "Temperature": 20.0,
                "MaxStep": 50000,
                "StepSize": 1.0,
            }
        )
        self.statuses = [{"Position": 6000, "IsMoving": False}]
        self.move_requested = False
        self.status_reads = 0

    async def get(self, path, **params):
        info = await super().get(path, **params)

        match path:
            case "/equipment/focuser/move":
                self.move_requested = True
            case "/equipment/focuser/info" if self.move_requested:
                self.status_reads += 1
                status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
                return info | status

        return info


@pytest.fixture
def client():
    return FocuserNinaClient()


@pytest.fixture
def focuser(client):
    config = NinaFocuserConfig(device_type="focuser", timeout=1.0, status_frequency=0.001)
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
    async def test_move(self, client, focuser, recorder):
        published = await recorder()
        await focuser.focuser_change(ChangeFocusPosition(position=6000.75))
        reqs = client.find_requests("/equipment/focuser/move")
        assert len(reqs) == 1
        assert reqs[0][1]["position"] == 6000
        assert (await published.wait_for(FocusPosition)).position == 6000.0

    @pytest.mark.asyncio
    async def test_move_waits_for_delayed_start_and_settling(self, client, focuser):
        client.statuses = [
            {"Position": 5000, "IsMoving": False},
            {"Position": 5000, "IsMoving": True},
            {"Position": 6000, "IsMoving": True},
            {"Position": 6000, "IsMoving": False},
        ]

        await focuser.focuser_change(ChangeFocusPosition(position=6000))

        assert client.status_reads == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [
            {"Position": 5500, "IsMoving": False},
            {"Position": 6000, "IsMoving": True},
            {"Position": 6000, "IsMoving": None},
        ],
        ids=["stopped-short", "moving-at-target", "unknown-motion"],
    )
    async def test_move_times_out_without_verified_completion(self, client, focuser, status):
        client.statuses = [status]
        focuser.config.timeout = 0.03

        with pytest.raises(TimeoutError, match="did not stop at 6000"):
            await focuser.focuser_change(ChangeFocusPosition(position=6000))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("position", [None, "invalid"])
    async def test_move_requires_valid_position(self, client, focuser, position):
        client.statuses = [{"Position": position, "IsMoving": False}]

        with pytest.raises((RuntimeError, ValueError)):
            await focuser.focuser_change(ChangeFocusPosition(position=6000))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("position", [-1, 50001])
    async def test_move_rejects_travel_limits(self, client, focuser, position):
        with pytest.raises(ValueError, match="outside travel limits"):
            await focuser.focuser_change(ChangeFocusPosition(position=position))

        assert client.find_requests("/equipment/focuser/move") == []

    @pytest.mark.asyncio
    async def test_stop(self, client, focuser):
        await focuser.focuser_stop(Stop())
        reqs = client.find_requests("/equipment/focuser/stop-move")
        assert len(reqs) == 1
