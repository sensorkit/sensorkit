# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.std import FollowTarget

from .fakes import FakeNinaClient


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
