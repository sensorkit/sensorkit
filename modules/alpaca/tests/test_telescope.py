"""Tests for Alpaca telescope device (lifecycle only)."""

import pytest

from conftest import MockAlpacaSDKDevice
from sensorkit.alpaca.telescope import (
    AlpacaTelescope,
    AlpacaTelescopeConfig,
    AlpacaTelescopeState,
)


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

    import astropy.units as u
    from astropy.coordinates import EarthLocation

    t._location = EarthLocation(lat=-31.0 * u.deg, lon=149.0 * u.deg, height=1100.0 * u.m)
    return t


@pytest.mark.asyncio
async def test_telescope_connect(telescope):
    import sensorkit.api as sk

    telescope.device_connected = False
    telescope.telescope._properties["Connected"] = False
    await telescope.telescope_connect(sk.Connect())
    assert telescope.device_connected is True


@pytest.mark.asyncio
async def test_telescope_disconnect(telescope):
    import sensorkit.api as sk

    await telescope.telescope_disconnect(sk.Disconnect())
    assert telescope.device_connected is False


@pytest.mark.asyncio
async def test_telescope_home(telescope):
    import sensorkit.api as sk

    telescope.telescope._properties["Slewing"] = False
    telescope._slewing = False
    await telescope.telescope_home(sk.Home())
    assert telescope.state.has_been_homed is True


@pytest.mark.asyncio
async def test_telescope_park(telescope):
    import sensorkit.api as sk

    telescope.telescope._properties["AtPark"] = True
    await telescope.telescope_park(sk.MoveToPark())


@pytest.mark.asyncio
async def test_telescope_stop(telescope):
    import sensorkit.api as sk

    await telescope.telescope_stop(sk.Stop())
    assert telescope._tracking is False
