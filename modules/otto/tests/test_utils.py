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


class TestHorizonsClassification:
    def test_is_horizons(self):
        from sensorkit.otto.utils import is_horizons

        assert is_horizons("horizons:433 Eros")
        assert not is_horizons("25544")
        assert not is_horizons("horizon:433")  # singular is not the scheme

    def test_horizons_query_strips_prefix(self):
        from sensorkit.otto.utils import horizons_query

        assert horizons_query("horizons:433 Eros") == "433 Eros"
        assert horizons_query("horizons:  -170  ") == "-170"


class TestCalculateHorizonsPosition:
    """calculate_horizons_position + build_horizons_target with the core client mocked."""

    @pytest.fixture
    def horizons_client(self, monkeypatch):
        """Patch the core Horizons client's resolve/ephemeris in otto.utils."""
        from sensorkit.astro.horizons import HorizonsResolution, HorizonsSample

        state = {}

        def configure(*, resolved=True, is_sun=False, samples=None):
            state["resolution"] = HorizonsResolution(
                resolved=resolved,
                command="433;",
                name="433 Eros (A898 PA)",
                kind="small",
                is_sun=is_sun,
            )
            state["samples"] = samples

        async def fake_resolve(query):
            return state["resolution"]

        async def fake_ephemeris(**kwargs):
            return state["samples"]

        monkeypatch.setattr("sensorkit.astro.horizons.resolve", fake_resolve)
        monkeypatch.setattr("sensorkit.astro.horizons.ephemeris", fake_ephemeris)
        return configure, HorizonsSample

    @pytest.mark.asyncio
    async def test_returns_altaz_and_rising(self, horizons_client):
        from sensorkit.otto.utils import calculate_horizons_position

        configure, Sample = horizons_client
        # Two samples 60s apart; second higher in the sky (contrived RA/Dec near zenith).
        configure(
            samples=[
                Sample(
                    jd=2461243.5,
                    utc="",
                    ra=45.0,
                    dec=33.0,
                    ra_rate_arcsec_hr=10.0,
                    dec_rate_arcsec_hr=0.0,
                ),
                Sample(jd=2461243.5 + 60 / 86400.0, utc="", ra=45.1, dec=33.1),
            ]
        )
        result = await calculate_horizons_position(
            "433 Eros", latitude=33.0, longitude=-117.0, elevation=100.0
        )
        assert result is not None
        altitude, azimuth, rising, rate = result
        assert -90.0 <= altitude <= 90.0
        assert 0.0 <= azimuth < 360.0
        assert rising in (True, False)
        assert rate == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_unresolved_returns_none(self, horizons_client):
        from sensorkit.otto.utils import calculate_horizons_position

        configure, _ = horizons_client
        configure(resolved=False)
        result = await calculate_horizons_position(
            "zzz", latitude=33.0, longitude=-117.0, elevation=100.0
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_sun_refused(self, horizons_client):
        from sensorkit.otto.utils import calculate_horizons_position

        configure, _ = horizons_client
        configure(is_sun=True, samples=[])
        result = await calculate_horizons_position(
            "sun", latitude=33.0, longitude=-117.0, elevation=100.0
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_build_target(self, horizons_client):
        from sensorkit.astro.common import ReferenceFrame
        from sensorkit.otto.utils import build_horizons_target

        configure, Sample = horizons_client
        configure(
            samples=[
                Sample(jd=2461243.5, utc="", ra=45.0, dec=33.0),
                Sample(jd=2461243.6, utc="", ra=45.1, dec=33.1),
            ]
        )
        built = await build_horizons_target(
            "433 Eros",
            latitude=33.0,
            longitude=-117.0,
            elevation=100.0,
            duration_seconds=120.0,
        )
        assert built is not None
        target, name = built
        assert target.target_type == "ephemeris"
        assert target.frame == ReferenceFrame.ICRF
        assert len(target.jds) == 2
        assert name == "433 Eros (A898 PA)"
