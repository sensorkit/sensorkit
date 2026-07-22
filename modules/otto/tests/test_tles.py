# SPDX-License-Identifier: Apache-2.0
"""Tests for the TLE target source."""

from types import SimpleNamespace

import pytest

from sensorkit.otto import tles
from sensorkit.otto.tles import _satellite
from sensorkit.otto.utils import time_at

LEO_LINE1 = "1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002"
LEO_LINE2 = "2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000"


def line2_with_mean_motion(mean_motion: float) -> str:
    """A valid TLE line 2 with the given mean motion (columns 53-63)."""
    return LEO_LINE2[:52] + f"{mean_motion:11.8f}" + LEO_LINE2[63:]


class TestClassifyOrbit:
    def test_leo(self):
        assert tles.classify_orbit(LEO_LINE2) == "LEO"

    def test_meo(self):
        assert tles.classify_orbit(line2_with_mean_motion(2.0056)) == "MEO"  # GPS

    def test_geo(self):
        assert tles.classify_orbit(line2_with_mean_motion(1.0027)) == "GEO"

    def test_heo(self):
        assert tles.classify_orbit(line2_with_mean_motion(1.2)) == "HEO"

    def test_leo_meo_boundary(self):
        assert tles.classify_orbit(line2_with_mean_motion(11.26)) == "LEO"
        assert tles.classify_orbit(line2_with_mean_motion(11.25)) == "MEO"

    def test_unparseable_is_other(self):
        assert tles.classify_orbit("") == "OTHER"
        assert tles.classify_orbit("2 25544 garbage") == "OTHER"


class TestPosition:
    # Same TLE re-stamped with a mid-2026 epoch so propagation stays sane
    FRESH_LINE1 = LEO_LINE1[:18] + "26191.50000000" + LEO_LINE1[32:]
    TLE_DATA = {"line0": "0 25544", "line1": FRESH_LINE1, "line2": LEO_LINE2}

    def test_returns_position(self):
        result = tles.position(
            tle_data=self.TLE_DATA, latitude=33.0, longitude=-117.0, altitude_km=0.1
        )
        assert result is not None
        altitude, azimuth, rising, hour_angle = result
        assert -90.0 <= altitude <= 90.0
        assert 0.0 <= azimuth < 360.0
        assert rising in (True, False)
        assert -12.0 <= hour_angle < 12.0

    def test_broken_entry_returns_none(self):
        result = tles.position(
            tle_data={"line0": "0 1"}, latitude=33.0, longitude=-117.0, altitude_km=0.1
        )
        assert result is None

    def test_caches_shared(self):
        assert _satellite("0 25544", self.FRESH_LINE1, LEO_LINE2) is _satellite(
            "0 25544", self.FRESH_LINE1, LEO_LINE2
        )
        assert time_at(1_780_000_000) is time_at(1_780_000_000)


class TestFetch:
    """tles.fetch filtering of the full Spacebook catalog."""

    GEO_LINE1 = "1 19548U" + LEO_LINE1[8:]
    GEO_LINE2 = "2 19548" + line2_with_mean_motion(1.0027)[7:]
    CATALOG = "\n".join([LEO_LINE1, LEO_LINE2, GEO_LINE1, GEO_LINE2])

    @pytest.fixture
    def spacebook(self, monkeypatch):
        """Serve a canned Spacebook catalog response to tles.fetch."""

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

            monkeypatch.setattr("sensorkit.otto.tles.httpx.AsyncClient", FakeClient)

        return set_response

    @pytest.mark.asyncio
    async def test_filter_by_objects(self, spacebook):
        spacebook(self.CATALOG)
        fetched, status = await tles.fetch(objects=["25544"])
        assert status == 200
        assert set(fetched) == {"25544"}

    @pytest.mark.asyncio
    async def test_filter_by_orbits(self, spacebook):
        spacebook(self.CATALOG)
        fetched, status = await tles.fetch(objects=[], orbits=["GEO"])
        assert status == 200
        assert set(fetched) == {"19548"}

    @pytest.mark.asyncio
    async def test_objects_and_orbits_union(self, spacebook):
        spacebook(self.CATALOG)
        fetched, _ = await tles.fetch(objects=["25544"], orbits=["GEO"])
        assert set(fetched) == {"25544", "19548"}

    @pytest.mark.asyncio
    async def test_error_status_returns_empty(self, spacebook):
        spacebook("", status_code=503)
        fetched, status = await tles.fetch(objects=["25544"], orbits=["GEO"])
        assert fetched == {}
        assert status == 503
