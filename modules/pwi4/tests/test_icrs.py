# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with ICRSTarget (RA/Dec J2000 goto)."""

import pytest

from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.std import FollowTarget


class TestICRSTarget:
    @pytest.mark.asyncio
    async def test_goto_ra_dec_j2000(self, client, mount):
        await mount.mount_follow_target(
            FollowTarget(target=ICRSTarget(coords=Equatorial(ra=187.5, dec=45.0)))
        )

        reqs = client.find_requests("/mount/goto_ra_dec_j2000")
        assert len(reqs) == 1
        # PWI4 takes RA in hours, so the target's degrees are divided by 15.
        assert reqs[0][1] == {"ra_hours": 12.5, "dec_degs": 45.0}

    @pytest.mark.asyncio
    async def test_follow_enables_both_axes(self, client, mount):
        await mount.mount_follow_target(
            FollowTarget(target=ICRSTarget(coords=Equatorial(ra=187.5, dec=45.0)))
        )

        enables = client.find_requests("/mount/enable")
        assert sorted(params["axis"] for _, params in enables) == [0, 1]

    @pytest.mark.asyncio
    async def test_follow_marks_tracking_sidereal(self, client, mount):
        await mount.mount_follow_target(
            FollowTarget(target=ICRSTarget(coords=Equatorial(ra=187.5, dec=45.0)))
        )

        # An ICRS goto leaves the mount tracking at the sidereal rate.
        assert mount._sidereal is True
