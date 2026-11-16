# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca telescope ICRS FollowTarget."""

import astropy.units as u
import pytest
import pytest_asyncio
from alpaca.telescope import EquatorialCoordinateType, PierSide
from astropy.coordinates import EarthLocation, angular_separation
from skyfield import api as skyfield

from sensorkit.alpaca.telescope import (
    AlpacaTelescopeConfig,
    AlpacaTelescopeState,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice
from sensorkit.astro.common import RADecPointing
from sensorkit.astro.coords import Equatorial, Geodetic
from sensorkit.astro.target import ICRSTarget
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
        SideOfPier=PierSide.pierUnknown,
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

    # The fake reports slew onset on the next read, then a settled mount.
    await telescope.telescope_follow_target(
        FollowTarget(target=ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0)))
    )

    # Tracking should be enabled for ICRS targets
    assert telescope._tracking is True


_STAR = ICRSTarget(coords=Equatorial(ra=90.0, dec=20.0))


@pytest.mark.asyncio
async def test_topocentric_mount_slews_to_apparent_place(telescope):
    """Topocentric mounts receive apparent RA/Dec for the current date and site."""
    telescope._equatorial_system = EquatorialCoordinateType.equTopocentric

    await telescope.telescope_follow_target(FollowTarget(target=_STAR))

    (((ra_hours, dec_deg), _),) = telescope.telescope.calls("SlewToCoordinatesAsync")

    ts = skyfield.load.timescale()
    site = skyfield.load("de421.bsp")["earth"] + skyfield.wgs84.latlon(-31.0, 149.0, 1100.0)
    star = skyfield.Star(ra_hours=_STAR.coords.ra / 15.0, dec_degrees=_STAR.coords.dec)
    ra, dec, _ = site.at(ts.now()).observe(star).apparent().radec(epoch="date")

    sep = angular_separation(
        ra_hours * u.hourangle, dec_deg * u.deg, ra.hours * u.hourangle, dec.degrees * u.deg
    )
    assert sep.to_value(u.arcsec) < 1.0


@pytest.mark.parametrize(
    ("system", "converted"),
    [
        (EquatorialCoordinateType.equOther, False),
        (EquatorialCoordinateType.equTopocentric, True),
        (EquatorialCoordinateType.equJ2000, False),
        (EquatorialCoordinateType.equJ2050, True),
        (EquatorialCoordinateType.equB1950, True),
    ],
)
@pytest.mark.asyncio
async def test_mount_system_round_trips_to_icrs(telescope, recorder, system, converted):
    """Slew coordinates use the mount's frame and published pointing uses ICRS."""
    telescope._equatorial_system = system

    await telescope.telescope_follow_target(FollowTarget(target=_STAR))
    await telescope.fast_loop.stop()

    (((ra_hours, dec_deg), _),) = telescope.telescope.calls("SlewToCoordinatesAsync")
    slewed = angular_separation(
        ra_hours * u.hourangle, dec_deg * u.deg, _STAR.coords.ra * u.deg, _STAR.coords.dec * u.deg
    )
    assert (slewed.to_value(u.arcmin) > 1.0) == converted

    telescope.telescope._properties.update(RightAscension=ra_hours, Declination=dec_deg)
    published = await recorder()
    await telescope._publish_telescope_status()
    pointing = await published.wait_for(RADecPointing)

    reported = angular_separation(
        pointing.right_ascension_hours * u.hourangle,
        pointing.declination_degrees * u.deg,
        _STAR.coords.ra * u.deg,
        _STAR.coords.dec * u.deg,
    )
    assert reported.to_value(u.arcsec) < 0.01
