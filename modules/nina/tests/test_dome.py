# SPDX-License-Identifier: Apache-2.0
import pytest
from conftest import MockNinaClient

from sensorkit.nina.dome import NinaDomeConfig, NinaDomeState
from sensorkit.std import Connect, Disconnect, Home, Stop
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "Slewing": False,
        "AtHome": True,
        "AtPark": False,
        "ShutterStatus": "ShutterClosed",
        "Azimuth": 180.0,
    })


@pytest.fixture
def dome(client):
    config = NinaDomeConfig(device_type="dome")
    d = config.create_device()
    d._client = client
    d.state = NinaDomeState()
    d.device_connected = True
    return d


class TestDomeConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, dome):
        dome.device_connected = None
        await dome.dome_connect(Connect())
        assert dome.device_connected is True

    @pytest.mark.asyncio
    async def test_disconnect(self, client, dome):
        await dome.dome_disconnect(Disconnect())
        assert dome.device_connected is False


class TestDomeCommands:
    @pytest.mark.asyncio
    async def test_home(self, client, dome):
        await dome.dome_home(Home())
        reqs = client.find_requests("/equipment/dome/home")
        assert len(reqs) == 1
        assert dome.state.has_been_homed is True

    @pytest.mark.asyncio
    async def test_stop(self, client, dome):
        await dome.dome_stop(Stop())
        reqs = client.find_requests("/equipment/dome/stop")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_open(self, client, dome):
        client.set_info(ShutterStatus="ShutterOpen")
        await dome.dome_open(OpenEnclosure())
        reqs = client.find_requests("/equipment/dome/open")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_close(self, client, dome):
        client.set_info(ShutterStatus="ShutterClosed")
        await dome.dome_close(CloseEnclosure())
        reqs = client.find_requests("/equipment/dome/close")
        assert len(reqs) == 1
