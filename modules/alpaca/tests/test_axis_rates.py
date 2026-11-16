# SPDX-License-Identifier: Apache-2.0
"""Tests for ICRS angular rates and their altitude/azimuth projection.

The altitude/azimuth calculation includes Earth's rotation.
A sidereal RA offset with zero Dec motion approximates a target fixed
in altitude and azimuth.
"""

import astropy.units as u
import pytest
from astropy.coordinates import EarthLocation

from sensorkit.alpaca.telescope import (
    AlpacaTelescopeConfig,
    AlpacaTelescopeState,
    ra_rate_from_ascom,
    radec_rates_to_altaz_rates,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import AxisRates

_SIDEREAL_RATE_DEG_S = 360.0 / u.sday.to(u.s)
# One second of RA per sidereal second gives a sidereal angular offset rate.
_SIDEREAL_RA_RATE = 1.0

_SITE_LAT, _SITE_LON, _SITE_ELEV = 20.82028, -156.27944, 1000.0


@pytest.fixture
def telescope():
    config = AlpacaTelescopeConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    t = config.create_device()
    t.state = AlpacaTelescopeState()
    t.device_name = "Telescope"
    # A southeast, rising pointing (matches a real near-GEO rate-track frame).
    t.telescope = FakeAlpacaSDKDevice(
        Connected=True,
        RightAscension=16.337573641588563,  # hours
        Declination=-5.1555390419601235,
        Altitude=40.768356804697376,
        Azimuth=117.0292647944468,
        RightAscensionRate=0.0,
        DeclinationRate=0.0,
    )
    t.device_connected = True
    t._can_set_right_ascension_rate = True
    t._can_set_declination_rate = True
    t._tracking = True
    t._equatorial_system = None
    t._location = EarthLocation(
        lat=_SITE_LAT * u.deg, lon=_SITE_LON * u.deg, height=_SITE_ELEV * u.m
    )
    t.status_loop = AsyncLoop(t.status_publish, interval=config.status_frequency)
    t.fast_loop = AsyncLoop(t._publish_telescope_status, interval=config.status_frequency_fast)

    return t


@pytest.mark.asyncio
async def test_near_geo_rate_track_altaz_rates_near_zero(telescope, recorder):
    """A sidereal RA offset with zero Dec offset yields near-zero alt/az rates."""
    published = await recorder()
    telescope.telescope.RightAscensionRate = _SIDEREAL_RA_RATE
    telescope.telescope.DeclinationRate = 0.0

    await telescope._publish_telescope_status()
    rates = await published.wait_for(AxisRates)

    # Inertial RA/Dec rates pass through unchanged (used by sdasim, FITS *_RATE).
    assert rates.right_ascension.velocity == pytest.approx(_SIDEREAL_RATE_DEG_S, rel=1e-6)
    assert rates.declination.velocity == pytest.approx(0.0, abs=1e-12)
    # Alt/az slew vanishes for a geostationary-like target.
    assert rates.altitude.velocity == pytest.approx(0.0, abs=1e-3)
    assert rates.azimuth.velocity == pytest.approx(0.0, abs=1e-3)


@pytest.mark.asyncio
async def test_altaz_rates_fed_inertial_offset(telescope, recorder, monkeypatch):
    """Published alt/az rates include ICRS target motion and Earth's rotation.

    The observation time is fixed for both coordinate calculations.
    """
    from astropy.time import Time

    published = await recorder()
    fixed = Time("2026-06-21T05:56:53.826", scale="utc")

    class _FixedTime:
        @staticmethod
        def now():
            return fixed

    monkeypatch.setattr("sensorkit.alpaca.telescope.Time", _FixedTime)

    ra_rate_ascom = 2.0  # seconds of RA per sidereal second, twice the sidereal rate
    dec_rate_arcsec = 30.0  # DeclinationRate is arcsec/s
    telescope.telescope.RightAscensionRate = ra_rate_ascom
    telescope.telescope.DeclinationRate = dec_rate_arcsec

    await telescope._publish_telescope_status()
    rates = await published.wait_for(AxisRates)

    ra_offset = ra_rate_from_ascom(ra_rate_ascom)
    dec_offset = dec_rate_arcsec / 3600.0

    def altaz_for(ra_rate_deg_s):
        return radec_rates_to_altaz_rates(
            ra_hr=telescope.telescope.RightAscension,
            dec_deg=telescope.telescope.Declination,
            ra_rate_deg_per_sec=ra_rate_deg_s,
            dec_rate_deg_per_sec=dec_offset,
            location=telescope._location,
            time=fixed,
        )

    exp_alt, exp_az = altaz_for(ra_offset)  # fed the inertial offset directly
    old_alt, old_az = altaz_for(ra_offset - _SIDEREAL_RATE_DEG_S)  # removed hour-angle form

    assert rates.altitude.velocity == pytest.approx(exp_alt, rel=1e-9, abs=1e-12)
    assert rates.azimuth.velocity == pytest.approx(exp_az, rel=1e-9, abs=1e-12)
    # Guard against regression to the removed (offset - sidereal) hour-angle form.
    assert abs(rates.azimuth.velocity - old_az) > 1e-4
    # Inertial RA/Dec axis rates still pass through as the offsets.
    assert rates.right_ascension.velocity == pytest.approx(ra_offset, rel=1e-9)
    assert rates.declination.velocity == pytest.approx(dec_offset, rel=1e-9)
