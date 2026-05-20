import pytest

import sensorkit.api as sk
from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.nina.mount import NinaMount, NinaMountConfig, NinaMountState

from conftest import MockNinaClient


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "Slewing": False,
        "Tracking": True,
        "AtHome": False,
        "AtPark": False,
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
    return m


ISS_TLE = TLE(
    line0="0 ISS (ZARYA)",
    line1="1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002",
    line2="2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000",
)


@pytest.mark.xfail(reason="TLE tracking via NINA is best-effort; adapt may fail without live propagation")
@pytest.mark.asyncio
async def test_mount_follow_tle(client, mount):
    target = TLETarget(tle=ISS_TLE)
    await mount.mount_follow_target(sk.FollowTarget(target=target))

    # Should have slewed to adapted ICRS position
    slew_reqs = client.find_requests("/equipment/mount/slew-radec")
    assert len(slew_reqs) == 1
