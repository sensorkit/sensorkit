# SPDX-License-Identifier: Apache-2.0
import pytest
from conftest import MockNinaClient

from sensorkit.nina.mount import NinaMountConfig, NinaMountState
from sensorkit.std import Connect, Disconnect, Home, MoveToPark, Stop


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "Slewing": False,
        "Tracking": False,
        "AtHome": True,
        "AtPark": False,
        "SiteLatitude": 32.0,
        "SiteLongitude": -110.0,
        "SiteElevation": 700.0,
        "RightAscension": 12.0,
        "Declination": 30.0,
        "Altitude": 45.0,
        "Azimuth": 180.0,
        "RightAscensionRate": 0.0,
        "DeclinationRate": 0.0,
    })


@pytest.fixture
def mount(client):
    config = NinaMountConfig(device_type="mount")
    m = config.create_device()
    m._client = client
    m.state = NinaMountState()
    m.device_connected = True
    m._site_lat = 32.0
    m._site_lon = -110.0
    m._site_elev = 700.0
    m._tracking = None
    m._slewing = None
    m._fast_status_task = None
    return m


class TestMountConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, mount):
        mount.device_connected = None
        await mount.mount_connect(Connect())
        assert mount.device_connected is True
        reqs = client.find_requests("/equipment/mount/connect")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_disconnect(self, client, mount):
        await mount.mount_disconnect(Disconnect())
        assert mount.device_connected is False
        reqs = client.find_requests("/equipment/mount/disconnect")
        assert len(reqs) == 1


class TestMountHome:
    @pytest.mark.asyncio
    async def test_home(self, client, mount):
        await mount.mount_home(Home())
        reqs = client.find_requests("/equipment/mount/home")
        assert len(reqs) == 1
        assert mount.state.has_been_homed is True


class TestMountStop:
    @pytest.mark.asyncio
    async def test_stop(self, client, mount):
        await mount.mount_stop(Stop())
        reqs = client.find_requests("/equipment/mount/slew/stop")
        assert len(reqs) == 1
        reqs = client.find_requests("/equipment/mount/tracking")
        assert any(p.get("enabled") is False for _, p in reqs)


class TestMountPark:
    @pytest.mark.asyncio
    async def test_park(self, client, mount):
        client.set_info(AtPark=True)
        await mount.mount_park(MoveToPark())
        reqs = client.find_requests("/equipment/mount/park")
        assert len(reqs) == 1
