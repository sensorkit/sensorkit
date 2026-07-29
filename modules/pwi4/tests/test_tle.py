# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with TLETarget."""

import pytest

from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.std import FollowTarget

ISS_TLE = TLE(
    line0="ISS (ZARYA)",
    line1="1 25544U 98067A   26100.50000000  .00016717  00000-0  10270-3 0  9002",
    line2="2 25544  51.6400 200.0000 0007000 300.0000  60.0000 15.50000000100000",
)


class TestTLETarget:
    @pytest.mark.asyncio
    async def test_follow_tle(self, client, mount):
        await mount.mount_follow_target(FollowTarget(target=TLETarget(tle=ISS_TLE)))

        reqs = client.find_requests("/mount/follow_tle")
        assert len(reqs) == 1
        # PWI4 numbers the three TLE lines from one, so line0 lands in "line1".
        assert reqs[0][1] == {
            "line1": ISS_TLE.line0,
            "line2": ISS_TLE.line1,
            "line3": ISS_TLE.line2,
        }

    @pytest.mark.asyncio
    async def test_tle_follow_is_not_sidereal(self, client, mount):
        await mount.mount_follow_target(FollowTarget(target=TLETarget(tle=ISS_TLE)))

        # The mount tracks the satellite's own rate, not the sidereal rate.
        assert mount._sidereal is False
