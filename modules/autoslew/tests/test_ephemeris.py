# SPDX-License-Identifier: Apache-2.0
"""Tests for Autoslew ephemeris slews and constant tracking offsets."""

import astropy.units as u
import pytest
from astropy.coordinates import ICRS, TETE, SkyCoord
from astropy.time import Time

from sensorkit.alpaca.telescope import transform_radec_rates
from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import EphemerisTarget
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
    jnow = SkyCoord(ra=90.0 * u.deg, dec=20.0 * u.deg, frame=ICRS()).transform_to(
        TETE(obstime=Time.now(), location=telescope._location)
    )
    assert ra_hours == pytest.approx(jnow.ra.hour, abs=1e-2)

    # Convert the reference ICRS rates to the mount's Topocentric frame.
    # Check ASCOM units with a tolerance for precision loss in Julian-date subtraction.
    sidereal_rate = 360.0 / u.sday.to(u.s)
    ra_rate, dec_rate = transform_radec_rates(
        90.0,
        20.0,
        0.01,
        0.005,
        lambda _: ICRS(),
        lambda t: TETE(obstime=t, location=telescope._location),
    )
    props = telescope.telescope._properties
    assert props["RightAscensionRate"] == pytest.approx(ra_rate / sidereal_rate, rel=1e-3)
    assert props["DeclinationRate"] == pytest.approx(dec_rate * 3600.0, rel=1e-3)
