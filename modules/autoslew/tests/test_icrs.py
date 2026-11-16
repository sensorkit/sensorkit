# SPDX-License-Identifier: Apache-2.0
"""Tests for ICRS slews on a Topocentric Autoslew mount."""

import astropy.units as u
import pytest
from astropy.coordinates import ICRS, TETE, SkyCoord
from astropy.time import Time

from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.std import FollowTarget


@pytest.mark.asyncio
async def test_follow_icrs_converts_to_jnow_and_tracks(telescope):
    await telescope.telescope_follow_target(
        FollowTarget(target=ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0)))
    )

    # Tracking is enabled for a sidereal/ICRS target.
    assert telescope._tracking is True

    # A slew was issued, and in JNow coordinates — not the raw ICRS pass-through.
    slews = telescope.telescope.calls("SlewToCoordinatesAsync")
    assert slews, "expected a SlewToCoordinatesAsync call"
    ra_hours, dec_deg = slews[-1][0]

    jnow = SkyCoord(ra=90.0 * u.deg, dec=20.0 * u.deg, frame=ICRS()).transform_to(
        TETE(obstime=Time.now(), location=telescope._location)
    )
    assert ra_hours == pytest.approx(jnow.ra.hour, abs=1e-2)
    assert dec_deg == pytest.approx(jnow.dec.deg, abs=1e-2)

    # The conversion actually moved the coordinates (precession is ~arcmin-scale).
    assert abs(ra_hours - 90.0 / 15.0) > 1e-3
