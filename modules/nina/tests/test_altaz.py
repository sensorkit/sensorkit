# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.std import FollowTarget

from .fakes import FakeNinaClient


@pytest.fixture
def client():
    return FakeNinaClient(
        info_response={
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
        }
    )


@pytest.mark.asyncio
async def test_mount_follow_altaz(client, mount):
    target = AltAzTarget(coords=Horizontal(alt=60.0, az=200.0))
    await mount.mount_follow_target(FollowTarget(target=target))

    slew_reqs = client.find_requests("/equipment/mount/slew-altaz")
    assert len(slew_reqs) == 1
    assert slew_reqs[0][1]["altitude"] == 60.0
    assert slew_reqs[0][1]["azimuth"] == 200.0
