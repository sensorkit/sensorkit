import pytest
from conftest import MockNinaClient

import sensorkit.api as sk
from sensorkit.models.devices import Deinit, Init
from sensorkit.nina.filter_wheel import NinaFilterWheelConfig, NinaFilterWheelState
from sensorkit.std import Connect, Disconnect
from sensorkit.std.optics import SetFilter


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "Position": 0,
        "SelectedFilter": {"Name": "Red", "Position": 0},
        "Filters": [
            {"Name": "Red", "Position": 0},
            {"Name": "Green", "Position": 1},
            {"Name": "Blue", "Position": 2},
        ],
    })


@pytest.fixture
def filter_wheel(client):
    config = NinaFilterWheelConfig(device_type="filter_wheel")
    fw = config.create_device()
    fw._client = client
    fw.state = NinaFilterWheelState()
    fw.device_connected = True
    fw.filter_position = 0
    fw._filter_names = ["Red", "Green", "Blue"]
    fw._name_to_id = {"Red": 0, "Green": 1, "Blue": 2}
    return fw


class TestFilterWheelConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, filter_wheel):
        filter_wheel.device_connected = None
        await filter_wheel.filter_wheel_connect(Connect())
        assert filter_wheel.device_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self, client, filter_wheel):
        await filter_wheel.filter_wheel_disconnect(Disconnect())
        assert filter_wheel.device_connected is False


class TestFilterWheelCommands:
    @pytest.mark.asyncio
    async def test_set_filter_by_name(self, client, filter_wheel):
        # Mock returns SelectedFilter with matching Id after change
        client.set_info(SelectedFilter={"Name": "Green", "Id": 1, "Position": 1})
        await filter_wheel.filter_wheel_set(SetFilter(filter="Green"))
        reqs = client.find_requests("/equipment/filterwheel/change-filter")
        assert len(reqs) == 1
        assert reqs[0][1]["filterId"] == 1

    @pytest.mark.asyncio
    async def test_set_filter_by_index(self, client, filter_wheel):
        client.set_info(SelectedFilter={"Name": "Blue", "Id": 2, "Position": 2})
        await filter_wheel.filter_wheel_set(SetFilter(filter=2))
        reqs = client.find_requests("/equipment/filterwheel/change-filter")
        assert len(reqs) == 1
        assert reqs[0][1]["filterId"] == 2

    @pytest.mark.asyncio
    async def test_set_filter_invalid_name(self, client, filter_wheel):
        """Invalid filter name logs error and returns without issuing a request."""
        await filter_wheel.filter_wheel_set(SetFilter(filter="InvalidFilter"))
        reqs = client.find_requests("/equipment/filterwheel/change-filter")
        assert len(reqs) == 0
