# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures and mocks for Autoslew device tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import astropy.units as u
import pytest
from astropy.coordinates import EarthLocation


class MockAutoslewSDKDevice:
    """Mock for the alpyca Telescope, including the ASA extension verbs.

    Standard ASCOM properties live in a dict (get via ``__getattr__``, set via
    ``__setattr__``). Unknown methods (``SlewToCoordinatesAsync``, ``AbortSlew``,
    ...) are recording no-ops. The three ASA mechanisms return realistic values:
    ``CommandBool("MotStat")`` reports the motors on, and ``CommandString(
    "getSatStatus")`` reports the tracking bit — so the mount's motors-first and
    sat-acquire waits resolve immediately. Every call is appended to ``calls``.
    """

    def __init__(
        self,
        sat_tracking: bool = True,
        motors_on: bool = True,
        action_returns: dict | None = None,
        **properties,
    ):
        super().__setattr__("_properties", {"Connected": False, "Connecting": False, **properties})
        super().__setattr__("_calls", [])
        super().__setattr__("_motors_on", motors_on)
        super().__setattr__("_sat_bit", 1 if sat_tracking else 0)
        super().__setattr__("_action_returns", action_returns or {})

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        props = super().__getattribute__("_properties")
        if name in props:
            return props[name]

        def _method(*args, **kwargs):
            self._calls.append((name, args, kwargs))
            return None

        return _method

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self._properties[name] = value

    @property
    def calls(self):
        return self._calls

    def actions(self):
        """ASA Action names issued, in order."""
        return [args[0] for (m, args, _kw) in self._calls if m == "Action"]

    def Connect(self):
        self._properties["Connected"] = True
        self._properties["Connecting"] = False

    def Disconnect(self):
        self._properties["Connected"] = False
        self._properties["Connecting"] = False

    def Action(self, name, params=""):
        self._calls.append(("Action", (name, params), {}))
        return self._action_returns.get(name, "")

    def CommandBool(self, cmd, raw=True):
        self._calls.append(("CommandBool", (cmd, raw), {}))
        return self._motors_on if cmd == "MotStat" else False

    def CommandString(self, cmd, raw=True):
        self._calls.append(("CommandString", (cmd, raw), {}))
        if cmd == "getSatStatus":
            return f'{{"status": {self._sat_bit}, "TrackErrAx1": 0.0, "TrackErrAx2": 0.0}}'
        return ""


@pytest.fixture(autouse=True)
def _mock_sk_device():
    """Mock sk.device() so device commands can publish/kv_put without a real service."""
    mock_device = MagicMock()
    mock_device.publish = AsyncMock()
    mock_device.kv_put_model = AsyncMock()
    mock_device.kv_get_model = AsyncMock(side_effect=Exception("no saved state"))
    mock_device.entity = "test_entity"

    with patch("sensorkit.api.device", return_value=mock_device):
        yield mock_device


@pytest.fixture
def telescope():
    """A connected AutoslewTelescope wired to a mock, as entity_init would leave it.

    The settle wait and fast-status background loop are stubbed out so the follow
    tests exercise the dispatch logic (JNow conversion, sat:* staging) directly.
    """
    from sensorkit.astro.coords import Geodetic
    from sensorkit.autoslew.telescope import AutoslewTelescopeConfig, AutoslewTelescopeState

    config = AutoslewTelescopeConfig(host="localhost", timeout=5.0, min_altitude_degrees=20.0)
    t = config.create_device()
    t.state = AutoslewTelescopeState()
    t.device_name = "Telescope"
    t.telescope = MockAutoslewSDKDevice(
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
    t._fast_status_task = None
    t._can_slew = t._can_slew_async = True
    t._can_slew_altaz = t._can_slew_altaz_async = True
    t._can_park = t._can_unpark = t._can_find_home = True
    t._site_lat, t._site_lon, t._site_elev = 20.7, 156.25, 3040.0
    t._location = EarthLocation(lat=20.7 * u.deg, lon=156.25 * u.deg, height=3040.0 * u.m)
    t._geodetic = Geodetic(lon=156.25, lat=20.7, elev=3040.0)
    # Keep unit tests fast + deterministic: no real settle poll or background loop.
    t._wait_for_telescope = AsyncMock()
    t._start_fast_status = lambda: None
    return t
