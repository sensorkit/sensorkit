# SPDX-License-Identifier: Apache-2.0
"""Autoslew telescope status — JNow reads must be published as ICRF."""

import pytest
from astropy.time import Time

from sensorkit.astro.common import RADecPointing, ReferenceFrame
from sensorkit.autoslew.telescope import jnow_to_icrs


@pytest.mark.asyncio
async def test_status_publishes_icrf_converted_from_jnow(telescope, recorder):
    published = await recorder()
    telescope.telescope._properties["RightAscension"] = 6.0  # JNow hours
    telescope.telescope._properties["Declination"] = 20.0  # JNow deg

    await telescope._publish_telescope_status()

    r = await published.wait_for(RADecPointing)

    # Explicit ICRF, converted from the JNow reads (never the implicit default).
    assert r.reference_frame == ReferenceFrame.ICRF
    exp_ra, exp_dec = jnow_to_icrs(6.0 * 15.0, 20.0, Time.now(), telescope._location)
    assert r.right_ascension_hours == pytest.approx(exp_ra / 15.0, abs=1e-2)
    assert r.declination_degrees == pytest.approx(exp_dec, abs=1e-2)

    # The conversion actually moved the coordinates off the raw JNow value.
    assert abs(r.right_ascension_hours - 6.0) > 1e-3
