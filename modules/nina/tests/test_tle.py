# SPDX-License-Identifier: Apache-2.0
import astropy.units as u
import pytest
from astropy.coordinates import ICRS, SkyCoord

from sensorkit.astro.common import TLE, ReferenceFrame
from sensorkit.astro.target import EphemerisTarget, TLETarget
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
            "RightAscension": 12.0,
            "Declination": 30.0,
            "Altitude": 45.0,
            "Azimuth": 180.0,
            "RightAscensionRate": 0.0,
            "DeclinationRate": 0.0,
        }
    )


# Use a near-geostationary orbit with zero modeled drag for this fixed test TLE.
GEO_TLE = TLE(
    line0="0 TEST GEO",
    line1="1 99999U 24001A   24100.50000000  .00000000  00000-0  00000-0 0  9994",
    line2="2 99999   0.0500 250.0000 0001000  90.0000 270.0000  1.00270000    19",
)


@pytest.mark.asyncio
async def test_mount_follow_tle_slews_to_current_position(client, mount):
    target = TLETarget(tle=GEO_TLE)

    await mount.mount_follow_target(FollowTarget(target=target))

    eph = await target.adapt((EphemerisTarget, ReferenceFrame.ICRF), observer=mount._geodetic)
    expected = SkyCoord(ra=eph.points[0].ra * u.deg, dec=eph.points[0].dec * u.deg, frame=ICRS())

    ((_, params),) = client.find_requests("/equipment/mount/slew-radec")
    actual = SkyCoord(ra=params["ra"] * 15.0 * u.deg, dec=params["dec"] * u.deg, frame=ICRS())

    # Allow for motion between the slew request and the separate ephemeris calculation.
    assert expected.separation(actual).arcmin < 1.0
