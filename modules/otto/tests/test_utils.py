# SPDX-License-Identifier: Apache-2.0
"""Tests for Otto utility functions."""

from types import SimpleNamespace

import pytest

from sensorkit.otto.program import OttoState
from sensorkit.otto.utils import (
    ListType,
    ObjectListManager,
    _satellite,
    _time_at,
    calculate_satellite_position,
    classify_orbit,
    fetch_tles,
)

LEO_LINE1 = "1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002"
LEO_LINE2 = "2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000"


def line2_with_mean_motion(mean_motion: float) -> str:
    """A valid TLE line 2 with the given mean motion (columns 53-63)."""
    return LEO_LINE2[:52] + f"{mean_motion:11.8f}" + LEO_LINE2[63:]


class TestClassifyOrbit:
    def test_leo(self):
        assert classify_orbit(LEO_LINE2) == "LEO"

    def test_meo(self):
        assert classify_orbit(line2_with_mean_motion(2.0056)) == "MEO"  # GPS

    def test_geo(self):
        assert classify_orbit(line2_with_mean_motion(1.0027)) == "GEO"

    def test_heo(self):
        assert classify_orbit(line2_with_mean_motion(1.2)) == "HEO"

    def test_leo_meo_boundary(self):
        assert classify_orbit(line2_with_mean_motion(11.26)) == "LEO"
        assert classify_orbit(line2_with_mean_motion(11.25)) == "MEO"

    def test_unparseable_is_other(self):
        assert classify_orbit("") == "OTHER"
        assert classify_orbit("2 25544 garbage") == "OTHER"


class TestCalculateSatellitePosition:
    # Same TLE re-stamped with a mid-2026 epoch so propagation stays sane
    FRESH_LINE1 = LEO_LINE1[:18] + "26191.50000000" + LEO_LINE1[32:]
    TLES = {"25544": {"line0": "0 25544", "line1": FRESH_LINE1, "line2": LEO_LINE2}}

    def test_returns_position(self):
        result = calculate_satellite_position(
            tles=self.TLES, object="25544", latitude=33.0, longitude=-117.0, elevation=100.0
        )
        assert result is not None
        altitude, azimuth, rising, hour_angle = result
        assert -90.0 <= altitude <= 90.0
        assert 0.0 <= azimuth < 360.0
        assert rising in (True, False)
        assert -12.0 <= hour_angle < 12.0

    def test_missing_object_returns_none(self):
        result = calculate_satellite_position(
            tles={}, object="25544", latitude=33.0, longitude=-117.0, elevation=100.0
        )
        assert result is None

    def test_caches_shared(self):
        assert _satellite("0 25544", self.FRESH_LINE1, LEO_LINE2) is _satellite(
            "0 25544", self.FRESH_LINE1, LEO_LINE2
        )
        assert _time_at(1_780_000_000) is _time_at(1_780_000_000)


class TestFetchTLEs:
    """fetch_tles filtering of the full Spacebook catalog."""

    GEO_LINE1 = "1 19548U" + LEO_LINE1[8:]
    GEO_LINE2 = "2 19548" + line2_with_mean_motion(1.0027)[7:]
    CATALOG = "\n".join([LEO_LINE1, LEO_LINE2, GEO_LINE1, GEO_LINE2])

    @pytest.fixture
    def spacebook(self, monkeypatch):
        """Serve a canned Spacebook catalog response to fetch_tles."""

        def set_response(text, status_code=200):
            response = SimpleNamespace(text=text, status_code=status_code)

            class FakeClient:
                def __init__(self, **kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *exc):
                    return False

                async def get(self, url):
                    return response

            monkeypatch.setattr("sensorkit.otto.utils.httpx.AsyncClient", FakeClient)

        return set_response

    @pytest.mark.asyncio
    async def test_filter_by_objects(self, spacebook):
        spacebook(self.CATALOG)
        tles, status = await fetch_tles(objects=["25544"])
        assert status == 200
        assert set(tles) == {"25544"}

    @pytest.mark.asyncio
    async def test_filter_by_orbits(self, spacebook):
        spacebook(self.CATALOG)
        tles, status = await fetch_tles(objects=[], orbits=["GEO"])
        assert status == 200
        assert set(tles) == {"19548"}

    @pytest.mark.asyncio
    async def test_objects_and_orbits_union(self, spacebook):
        spacebook(self.CATALOG)
        tles, _ = await fetch_tles(objects=["25544"], orbits=["GEO"])
        assert set(tles) == {"25544", "19548"}

    @pytest.mark.asyncio
    async def test_error_status_returns_empty(self, spacebook):
        spacebook("", status_code=503)
        tles, status = await fetch_tles(objects=["25544"], orbits=["GEO"])
        assert tles == {}
        assert status == 503


class TestObjectListManager:
    @pytest.fixture
    def state(self):
        return OttoState(
            whitelist=["25544", "42738", "39120"],
            graylist=[],
            blacklist=[],
        )

    @pytest.fixture
    def manager(self, state):
        save_callback = pytest.importorskip("unittest.mock").AsyncMock()
        return ObjectListManager(state, save_callback), save_callback

    @pytest.mark.asyncio
    async def test_move_whitelist_to_graylist(self, state, manager):
        mgr, save = manager
        result = await mgr.move_object("25544", ListType.WHITELIST, ListType.GRAYLIST)
        assert result is True
        assert "25544" not in state.whitelist
        assert "25544" in state.graylist
        save.assert_awaited()

    @pytest.mark.asyncio
    async def test_move_whitelist_to_blacklist(self, state, manager):
        mgr, save = manager
        result = await mgr.move_object("42738", ListType.WHITELIST, ListType.BLACKLIST)
        assert result is True
        assert "42738" not in state.whitelist
        assert "42738" in state.blacklist

    @pytest.mark.asyncio
    async def test_move_graylist_to_whitelist(self, state, manager):
        mgr, _ = manager
        # First move to graylist
        await mgr.move_object("25544", ListType.WHITELIST, ListType.GRAYLIST)
        assert "25544" in state.graylist

        # Then back to whitelist
        await mgr.move_object("25544", ListType.GRAYLIST, ListType.WHITELIST)
        assert "25544" in state.whitelist
        assert "25544" not in state.graylist

    @pytest.mark.asyncio
    async def test_move_nonexistent_object_raises(self, state, manager):
        mgr, _ = manager
        with pytest.raises(ValueError):
            await mgr.move_object("99999", ListType.WHITELIST, ListType.GRAYLIST)


class TestListType:
    def test_values(self):
        assert ListType.WHITELIST.value == "whitelist"
        assert ListType.GRAYLIST.value == "graylist"
        assert ListType.BLACKLIST.value == "blacklist"
