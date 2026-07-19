# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca telescope Ephemeris FollowTarget."""

import astropy.units as u
import pytest
from astropy.coordinates import EarthLocation
from astropy.time import Time
from conftest import MockAlpacaSDKDevice

from sensorkit.alpaca.telescope import (
    AlpacaTelescopeConfig,
    AlpacaTelescopeState,
)
from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial, Geodetic
from sensorkit.astro.target import EphemerisTarget
from sensorkit.std import FollowTarget

_UTC_TO_SIDEREAL = 1.00273791


@pytest.fixture
def telescope():
    config = AlpacaTelescopeConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    t = config.create_device()
    t.state = AlpacaTelescopeState()
    t.device_name = "Telescope"
    mock = MockAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        Slewing=False,
        Tracking=False,
        AtHome=False,
        AtPark=False,
        RightAscension=0.0,
        Declination=0.0,
        Altitude=45.0,
        Azimuth=180.0,
        RightAscensionRate=0.0,
        DeclinationRate=0.0,
        SiteLatitude=-31.0,
        SiteLongitude=149.0,
        SiteElevation=1100.0,
        CanSlew=True,
        CanSlewAsync=True,
        CanSlewAltAz=True,
        CanSlewAltAzAsync=True,
        CanPark=True,
        CanUnpark=True,
        CanFindHome=True,
        CanSetTracking=True,
        CanSetPark=False,
        CanPulseGuide=False,
        CanSetRightAscensionRate=True,
        CanSetDeclinationRate=True,
        CanSetGuideRates=False,
        CanSetPierSide=False,
        CanSync=False,
        CanSyncAltAz=False,
        TrackingRates=[],
        SideOfPier=-1,
        SiderealTime=0.0,
    )
    t.telescope = mock
    t.device_connected = True
    t._can_slew = True
    t._can_slew_async = True
    t._can_slew_altaz = True
    t._can_slew_altaz_async = True
    t._can_park = True
    t._can_unpark = True
    t._can_find_home = True
    t._can_set_tracking = True
    t._can_set_park = False
    t._can_pulse_guide = False
    t._can_set_right_ascension_rate = True
    t._can_set_declination_rate = True
    t._can_set_guide_rates = False
    t._can_set_pier_side = False
    t._can_sync = False
    t._can_sync_altaz = False
    t._can_move_axis = [False, False, False]
    t._tracking = False
    t._slewing = False
    t._fast_status_task = None
    t._site_lat = -31.0
    t._site_lon = 149.0
    t._site_elev = 1100.0
    t._tracking_rates = []
    t._aperture_diameter = None
    t._aperture_area = None
    t._focal_length = None
    t._equatorial_system = None
    t._alignment_mode = None
    t._does_refraction = None
    t._location = EarthLocation(lat=-31.0 * u.deg, lon=149.0 * u.deg, height=1100.0 * u.m)
    t._geodetic = Geodetic(lon=149.0, lat=-31.0, elev=1100.0)
    return t


@pytest.mark.asyncio
async def test_telescope_follow_ephemeris(telescope):
    """An ICRF EphemerisTarget collapses to a slew plus constant ASCOM offset rate.

    A linear path moving 0.02 deg in RA and 0.01 deg in Dec per 2 s sample should
    produce a RightAscensionRate of 0.01 deg/s (sidereal-scaled, seconds of RA per
    sidereal second) and a DeclinationRate of 18 arcsec/s.
    """
    step_days = 2.0 / 86400.0
    now_jd = Time.now().jd
    # Samples straddling "now" so the nearest interior sample has a forward
    # neighbour to difference against.
    ks = [-2, -1, 0, 1, 2, 3]
    jds = [now_jd + k * step_days for k in ks]
    points = [Equatorial(ra=100.0 + 0.02 * k, dec=20.0 + 0.01 * k) for k in ks]

    target = EphemerisTarget(frame=ReferenceFrame.ICRF, jds=jds, points=points)

    await telescope.telescope_follow_target(FollowTarget(target=target))

    # Tracking must be enabled to apply equatorial offset rates.
    assert telescope._tracking is True

    expected_ra_rate = 0.02 / 2.0 / 15.0 * 3600.0 * _UTC_TO_SIDEREAL  # ~2.4066
    expected_dec_rate = 0.01 / 2.0 * 3600.0  # 18.0 arcsec/s

    # rel=1e-3 absorbs float64 precision loss from differencing ~2.46e6-magnitude
    # JDs; it still resolves the 0.27% sidereal scaling factor on the RA rate.
    assert telescope.telescope.RightAscensionRate == pytest.approx(expected_ra_rate, rel=1e-3)
    assert telescope.telescope.DeclinationRate == pytest.approx(expected_dec_rate, rel=1e-3)
