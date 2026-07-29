# SPDX-License-Identifier: Apache-2.0
import pytest

from .fakes import FakeNinaClient
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.nina.mount import NinaMountConfig, NinaMountState
from sensorkit.std import FollowTarget


@pytest.fixture
def client():
    return FakeNinaClient(
        info_response={
            "Connected": True,
            "Slewing": False,
            "Tracking": True,
            "AtHome": False,
            "AtPark": False,
            "RightAscension": 6.0,
            "Declination": 20.0,
            "Altitude": 45.0,
            "Azimuth": 180.0,
            "RightAscensionRate": 0.0,
            "DeclinationRate": 0.0,
        }
    )


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


@pytest.mark.asyncio
async def test_mount_follow_icrs(client, mount):
    target = ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0))
    await mount.mount_follow_target(FollowTarget(target=target))

    # Should enable tracking
    tracking_reqs = client.find_requests("/equipment/mount/tracking")
    assert any(p.get("enabled") is True for _, p in tracking_reqs)

    # Should slew to RA/Dec (ra in hours = 90/15 = 6.0)
    slew_reqs = client.find_requests("/equipment/mount/slew-radec")
    assert len(slew_reqs) == 1
    assert slew_reqs[0][1]["ra"] == 6.0
    assert slew_reqs[0][1]["dec"] == 20.0
