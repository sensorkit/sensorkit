# SPDX-License-Identifier: Apache-2.0
"""Autoslew telescope ICRS FollowTarget — must convert ICRS -> JNow before slewing."""

import pytest
from astropy.time import Time

from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.autoslew.telescope import icrs_to_jnow
from sensorkit.std import FollowTarget


@pytest.mark.asyncio
async def test_follow_icrs_converts_to_jnow_and_tracks(telescope):
    await telescope.telescope_follow_target(
        FollowTarget(target=ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0)))
    )

    # Tracking is enabled for a sidereal/ICRS target.
    assert telescope._tracking is True

    # A slew was issued, and in JNow coordinates — not the raw ICRS pass-through.
    slews = [c for c in telescope.telescope.calls if c[0] == "SlewToCoordinatesAsync"]
    assert slews, "expected a SlewToCoordinatesAsync call"
    ra_hours, dec_deg = slews[-1][1]

    exp_ra, exp_dec = icrs_to_jnow(90.0, 20.0, Time.now(), telescope._location)
    assert ra_hours == pytest.approx(exp_ra / 15.0, abs=1e-2)
    assert dec_deg == pytest.approx(exp_dec, abs=1e-2)

    # The conversion actually moved the coordinates (precession is ~arcmin-scale).
    assert abs(ra_hours - 90.0 / 15.0) > 1e-3
