# SPDX-License-Identifier: Apache-2.0
import pytest
from conftest import MockNinaClient

from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.nina.mount import NinaMountConfig, NinaMountState
from sensorkit.std import FollowTarget


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "Slewing": False,
        "Tracking": False,
        "AtHome": False,
        "AtPark": False,
        "RightAscension": 12.0,
        "Declination": 30.0,
        "Altitude": 60.0,
        "Azimuth": 200.0,
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
    m._fast_status_task = None
    m._site_lat = 32.0
    m._site_lon = -110.0
    m._site_elev = 700.0
    return m


@pytest.mark.asyncio
async def test_mount_follow_altaz(client, mount):
    target = AltAzTarget(coords=Horizontal(alt=60.0, az=200.0))
    await mount.mount_follow_target(FollowTarget(target=target))

    slew_reqs = client.find_requests("/equipment/mount/slew-altaz")
    assert len(slew_reqs) == 1
    assert slew_reqs[0][1]["altitude"] == 60.0
    assert slew_reqs[0][1]["azimuth"] == 200.0
