# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the Autoslew suite."""

from __future__ import annotations

import astropy.units as u
import pytest
import pytest_asyncio
from astropy.coordinates import EarthLocation

from sensorkit.common.aio import AsyncLoop

from .fakes import FakeAutoslewSDKDevice


@pytest.fixture(autouse=True)
def _autouse_device_context(device_impl):
    """All tests in this suite may access an active `DeviceImpl` via `sk.device()`."""


@pytest_asyncio.fixture
async def telescope():
    """A connected AutoslewTelescope wired to a fake, as entity_init would leave it."""
    from sensorkit.astro.coords import Geodetic
    from sensorkit.autoslew.telescope import AutoslewTelescopeConfig, AutoslewTelescopeState

    config = AutoslewTelescopeConfig(
        host="localhost",
        timeout=5.0,
        min_altitude_degrees=20.0,
        status_frequency=0.05,
        status_frequency_fast=0.01,
    )
    t = config.create_device()
    t.state = AutoslewTelescopeState()
    t.device_name = "Telescope"
    t.telescope = FakeAutoslewSDKDevice(
        Connected=True,
        Slewing=False,
        Tracking=False,
        AtHome=False,
        AtPark=False,
        RightAscension=6.0,  # JNow hours
        Declination=20.0,  # JNow deg
        Altitude=45.0,
        Azimuth=180.0,
        SiteLatitude=20.7,
        SiteLongitude=156.25,
        SiteElevation=3040.0,
    )
    t.device_connected = True
    t._tracking = False
    t._slewing = False
    t._sidereal = False
    t._icrf_rate = (0.0, 0.0)
    t._tle_target = None
    t._can_slew = t._can_slew_async = True
    t._can_slew_altaz = t._can_slew_altaz_async = True
    t._can_park = t._can_unpark = t._can_find_home = True
    t._site_lat, t._site_lon, t._site_elev = 20.7, 156.25, 3040.0
    t._location = EarthLocation(lat=20.7 * u.deg, lon=156.25 * u.deg, height=3040.0 * u.m)
    t._geodetic = Geodetic(lon=156.25, lat=20.7, elev=3040.0)
    t.status_loop = AsyncLoop(t.status_publish, interval=config.status_frequency)
    t.fast_loop = AsyncLoop(t._publish_telescope_status, interval=config.status_frequency_fast)

    yield t

    await t.status_loop.stop()
    await t.fast_loop.stop()
