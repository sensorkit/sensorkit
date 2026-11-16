# SPDX-License-Identifier: Apache-2.0
"""Tests for Autoslew ephemeris slews and constant tracking offsets."""

import astropy.units as u
import pytest
from astropy.time import Time

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import EphemerisTarget
from sensorkit.autoslew.telescope import icrs_to_jnow
from sensorkit.std import FollowTarget


@pytest.mark.asyncio
async def test_follow_ephemeris_reduces_to_position_and_rate(telescope):
    jd0 = 2461237.0
    eph = EphemerisTarget(
        frame=ReferenceFrame.ICRF,
        jds=[jd0, jd0 + 1.0 / 86400.0],  # samples one second apart
        points=[Equatorial(ra=90.0, dec=20.0), Equatorial(ra=90.01, dec=20.005)],
    )

    await telescope.telescope_follow_target(FollowTarget(target=eph))

    assert telescope._tracking is True

    # The slew uses the first sample converted from ICRS to JNow.
    slews = telescope.telescope.calls("SlewToCoordinatesAsync")
    assert slews
    ra_hours, _dec = slews[-1][0]
    exp_ra, _ = icrs_to_jnow(90.0, 20.0, Time.now(), telescope._location)
    assert ra_hours == pytest.approx(exp_ra / 15.0, abs=1e-2)

    # Check RA in seconds of RA per sidereal second and Dec in arcseconds per SI second.
    # The tolerance allows for precision loss when subtracting Julian dates.
    sidereal_rate = 360.0 / u.sday.to(u.s)
    props = telescope.telescope._properties
    assert props["RightAscensionRate"] == pytest.approx(0.01 / sidereal_rate, rel=1e-3)
    assert props["DeclinationRate"] == pytest.approx(18.0, rel=1e-3)
