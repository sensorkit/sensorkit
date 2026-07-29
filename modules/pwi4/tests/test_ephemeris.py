# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with EphemerisTarget (radecpath)."""

import pytest

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import EphemerisTarget
from sensorkit.std import FollowTarget

JDS = [2460400.5, 2460400.501, 2460400.502]
POINTS = [
    Equatorial(ra=180.0, dec=45.0),
    Equatorial(ra=180.1, dec=45.01),
    Equatorial(ra=180.2, dec=45.02),
]


def ephemeris_target() -> EphemerisTarget:
    return EphemerisTarget(frame=ReferenceFrame.ICRF, jds=JDS, points=POINTS)


class TestEphemerisTarget:
    @pytest.mark.asyncio
    async def test_radecpath_sequence(self, client, mount):
        await mount.mount_follow_target(FollowTarget(target=ephemeris_target()))

        assert len(client.find_requests("/mount/radecpath/new")) == 1
        assert len(client.find_requests("/mount/radecpath/add_point")) == len(JDS)
        assert len(client.find_requests("/mount/radecpath/apply")) == 1

    @pytest.mark.asyncio
    async def test_points_converted_to_hours(self, client, mount):
        await mount.mount_follow_target(FollowTarget(target=ephemeris_target()))

        added = [params for _, params in client.find_requests("/mount/radecpath/add_point")]

        assert [p["jd"] for p in added] == JDS
        assert [p["dec_j2000_degs"] for p in added] == [c.dec for c in POINTS]
        assert [p["ra_j2000_hours"] for p in added] == pytest.approx([c.ra / 15 for c in POINTS])
