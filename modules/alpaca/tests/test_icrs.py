# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca telescope ICRS FollowTarget."""

import astropy.units as u
import pytest
import pytest_asyncio
from astropy.coordinates import EarthLocation

from sensorkit.alpaca.telescope import (
    AlpacaTelescopeConfig,
    AlpacaTelescopeState,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice
from sensorkit.astro.coords import Geodetic
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import FollowTarget


@pytest_asyncio.fixture
async def telescope():
    config = AlpacaTelescopeConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    t = config.create_device()
    t.state = AlpacaTelescopeState()
    t.device_name = "Telescope"
    device = FakeAlpacaSDKDevice(
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
    t.telescope = device
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
    t.status_loop = AsyncLoop(t.status_publish, interval=config.status_frequency)
    t.fast_loop = AsyncLoop(t._publish_telescope_status, interval=config.status_frequency_fast)

    yield t

    await t.status_loop.stop()
    await t.fast_loop.stop()


@pytest.mark.asyncio
async def test_telescope_follow_icrs(telescope):
    from sensorkit.astro.coords import Equatorial
    from sensorkit.astro.target import ICRSTarget

    # The fake's SlewToCoordinatesAsync is a no-op, Slewing is already False,
    # so the wait loop exits immediately.
    await telescope.telescope_follow_target(
        FollowTarget(target=ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0)))
    )

    # Tracking should be enabled for ICRS targets
    assert telescope._tracking is True
