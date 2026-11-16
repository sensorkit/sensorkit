# SPDX-License-Identifier: Apache-2.0
"""Tests for publishing Topocentric Autoslew pointing as ICRF."""

import astropy.units as u
import pytest
from astropy.coordinates import ICRS, TETE, SkyCoord
from astropy.time import Time

from sensorkit.astro.common import RADecPointing, ReferenceFrame


@pytest.mark.asyncio
async def test_status_publishes_icrf_converted_from_jnow(telescope, recorder):
    published = await recorder()
    telescope.telescope._properties["RightAscension"] = 6.0  # JNow hours
    telescope.telescope._properties["Declination"] = 20.0  # JNow deg

    await telescope._publish_telescope_status()

    r = await published.wait_for(RADecPointing)

    # Pointing carries an explicit ICRF frame after coordinate conversion.
    assert r.reference_frame == ReferenceFrame.ICRF
    jnow = SkyCoord(
        ra=6.0 * u.hourangle,
        dec=20.0 * u.deg,
        frame=TETE(obstime=Time.now(), location=telescope._location),
    )
    icrs = jnow.transform_to(ICRS())
    assert r.right_ascension_hours == pytest.approx(icrs.ra.hour, abs=1e-2)
    assert r.declination_degrees == pytest.approx(icrs.dec.deg, abs=1e-2)

    # The conversion actually moved the coordinates off the raw JNow value.
    assert abs(r.right_ascension_hours - 6.0) > 1e-3
