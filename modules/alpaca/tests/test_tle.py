# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca telescope TLE FollowTarget."""

from datetime import UTC, datetime, timedelta

import astropy.units as u
import pytest
from astropy.coordinates import angular_separation

from sensorkit.astro.common import TLE, ReferenceFrame
from sensorkit.astro.target import TLETarget
from sensorkit.std import FollowTarget

# Use a near-geostationary orbit with zero modeled drag for this fixed test TLE.
GEO_TLE = TLE(
    line0="0 TEST GEO",
    line1="1 99999U 24001A   24100.50000000  .00000000  00000-0  00000-0 0  9994",
    line2="2 99999   0.0500 250.0000 0001000  90.0000 270.0000  1.00270000    19",
)


@pytest.mark.asyncio
async def test_telescope_follow_tle(telescope):
    """TLE follow slews to astrometric ICRS coordinates and sets tracking offsets."""
    target = TLETarget(tle=GEO_TLE)

    before = datetime.now(UTC)
    await telescope.telescope_follow_target(FollowTarget(target=target))
    after = datetime.now(UTC)

    (ra_hours, dec_deg), _ = telescope.telescope.calls("SlewToCoordinatesAsync")[-1]

    # Compare against the track over the whole call, since the slew time is not known.
    track = await target.to_ephemeris_target(
        start_time=before - timedelta(seconds=1),
        duration=after - before + timedelta(seconds=2),
        step=timedelta(milliseconds=50),
        frame=ReferenceFrame.ICRF,
        observer=telescope._geodetic,
    )
    separations = angular_separation(
        ra_hours * u.hourangle,
        dec_deg * u.deg,
        [p.ra for p in track.points] * u.deg,
        [p.dec for p in track.points] * u.deg,
    )
    assert separations.min().to_value(u.arcsec) < 2.0

    # Near-geostationary motion requires about one RA second per sidereal second.
    assert telescope.telescope.RightAscensionRate == pytest.approx(1.0, rel=0.003)
    assert abs(telescope.telescope.DeclinationRate) < 1.0
