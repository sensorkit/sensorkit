# SPDX-License-Identifier: Apache-2.0
"""Tests for the JPL Horizons client (core/src/sensorkit/astro/horizons.py).

Network access is mocked: `_horizons_get` is monkeypatched to return canned text
captured from real Horizons responses (trimmed), so parsing is exercised without
touching JPL.
"""

from datetime import UTC, datetime

import pytest

from sensorkit.astro import horizons
from sensorkit.astro.common import ReferenceFrame

# A single-object small-body resolve (asteroid 433 Eros), OBJ_DATA header.
SMALL_RESOLVE = """\
*******************************************************************************
JPL/HORIZONS                        Eros                2026-Jul-22 00:00:00
Rec #:     433 (+COV) Soln.date: 2021-Apr-13_11:04:44   # obs: 9130 (1893-2021)
Target body name: 433 Eros (A898 PA)            {source: JPL#659}
*******************************************************************************
"""

# A "Multiple major-bodies match" disambiguation table.
MAJOR_MATCHES = """\
 Multiple major-bodies match string "mars*"

  ID#      Name                               Designation  IAU/aliases/other
  -------  ---------------------------------- -----------  -------------------
        4  Mars Barycenter
      499  Mars
      401  Phobos
      402  Deimos
 Number of matches =   4.
"""

# A "Matching small-bodies" list.
SMALL_MATCHES = """\
 Matching small-bodies:

    Record #  Epoch-yr  Primary Desig  >MATCH NAME<
    --------  --------  -------------  -------------------------
         433            A898 PA         Eros (433)
        4433            1932 HA         Eros II (4433)
 (2 matches. To SELECT, enter record # (integer), followed by semi-colon.)
"""

# An Observer-Table CSV ephemeris (QUANTITIES 1,3,9,20). Columns:
# Date, (solar/lunar flags, two cols), RA, DEC, dRA*cosD, dDEC/dt, APmag, S-brt,
# range(AU), range-rate.
EPHEMERIS = """\
*******************************************************************************
Target body name: 433 Eros (A898 PA)            {source: JPL#659}
*******************************************************************************
$$SOE
 2026-Jul-22 00:00:00.000, , ,  45.1234567,  12.3456789,  15.5,  -8.2,  11.20,  6.5,  1.23456789012, -12.3,
 2026-Jul-22 00:30:00.000, , ,  45.2000000,  12.4000000,  15.6,  -8.1,  11.20,  6.5,  1.23400000000, -12.2,
$$EOE
*******************************************************************************
"""

NO_MATCH = """\
No matches found.
"""


@pytest.fixture(autouse=True)
def _clear_cache():
    horizons._cache.clear()
    yield
    horizons._cache.clear()


def _patch_get(monkeypatch, text):
    async def fake_get(params):
        return text

    monkeypatch.setattr(horizons, "_horizons_get", fake_get)


class TestResolve:
    @pytest.mark.asyncio
    async def test_single_small_body(self, monkeypatch):
        _patch_get(monkeypatch, SMALL_RESOLVE)
        res = await horizons.resolve("433 Eros")
        assert res.resolved
        assert res.name == "433 Eros (A898 PA)"
        assert res.kind == "small"
        assert res.command.endswith(";")  # locked to the small-body record
        assert not res.is_sun

    @pytest.mark.asyncio
    async def test_major_body_candidates(self, monkeypatch):
        _patch_get(monkeypatch, MAJOR_MATCHES)
        res = await horizons.resolve("mars")
        assert not res.resolved
        names = {c.name for c in res.candidates}
        assert "Mars" in names
        assert any(c.command == "499" for c in res.candidates)

    @pytest.mark.asyncio
    async def test_small_body_candidates(self, monkeypatch):
        _patch_get(monkeypatch, SMALL_MATCHES)
        res = await horizons.resolve("eros")
        assert not res.resolved
        assert res.candidates  # at least one parsed
        assert all(c.kind == "small" for c in res.candidates)

    @pytest.mark.asyncio
    async def test_no_match(self, monkeypatch):
        _patch_get(monkeypatch, NO_MATCH)
        res = await horizons.resolve("zzzznotathing")
        assert not res.resolved
        assert res.candidates == []

    @pytest.mark.asyncio
    async def test_sun_flagged(self, monkeypatch):
        _patch_get(monkeypatch, SMALL_RESOLVE.replace("Eros", "Sun"))
        res = await horizons.resolve("sun")
        assert res.is_sun

    @pytest.mark.asyncio
    async def test_cached(self, monkeypatch):
        calls = 0

        async def fake_get(params):
            nonlocal calls
            calls += 1
            return SMALL_RESOLVE

        monkeypatch.setattr(horizons, "_horizons_get", fake_get)
        await horizons.resolve("433 Eros")
        first = calls
        await horizons.resolve("433 Eros")
        assert calls == first  # second call served from cache


class TestEphemeris:
    @pytest.mark.asyncio
    async def test_parses_samples(self, monkeypatch):
        _patch_get(monkeypatch, EPHEMERIS)
        now = datetime(2026, 7, 22, tzinfo=UTC)
        samples = await horizons.ephemeris(
            "433;", lon=-117.0, lat=33.0, alt_km=0.1, start=now, stop=now, intervals=1
        )
        assert len(samples) == 2
        assert samples[0].ra == pytest.approx(45.1234567)
        assert samples[0].dec == pytest.approx(12.3456789)
        assert samples[0].ra_rate_arcsec_hr == pytest.approx(15.5)
        assert samples[0].magnitude == pytest.approx(11.20)
        assert samples[0].range_au == pytest.approx(1.23456789012)
        assert samples[0].utc == "2026-07-22T00:00:00Z"

    @pytest.mark.asyncio
    async def test_no_ephemeris_raises(self, monkeypatch):
        _patch_get(monkeypatch, MAJOR_MATCHES)  # no $$SOE block
        now = datetime(2026, 7, 22, tzinfo=UTC)
        with pytest.raises(horizons.HorizonsError):
            await horizons.ephemeris(
                "mars", lon=0.0, lat=0.0, alt_km=0.0, start=now, stop=now, intervals=1
            )

    def test_total_rate(self):
        s = horizons.HorizonsSample(
            jd=0.0, utc="", ra=0.0, dec=0.0, ra_rate_arcsec_hr=3.0, dec_rate_arcsec_hr=4.0
        )
        assert s.total_rate_arcsec_hr == pytest.approx(5.0)


class TestSamplesToTarget:
    def test_builds_icrf_ephemeris_target(self):
        samples = [
            horizons.HorizonsSample(jd=2460000.5, utc="", ra=45.0, dec=12.0),
            horizons.HorizonsSample(jd=2460000.6, utc="", ra=45.1, dec=12.1),
        ]
        target = horizons.samples_to_ephemeris_target(samples)
        assert target.target_type == "ephemeris"
        assert target.frame == ReferenceFrame.ICRF
        assert target.jds == [2460000.5, 2460000.6]
        assert target.points[0].ra == 45.0
        assert target.points[1].dec == 12.1


class TestWindowSizing:
    def test_estimate_collect_seconds(self):
        # 3 frames × 10s + 2s/frame overhead + 30s settle = 66s
        assert horizons.estimate_collect_seconds(10.0, 3) == pytest.approx(66.0)

    def test_adaptive_intervals_bounds(self):
        # Zero rate → min intervals; huge rate → capped at max.
        assert horizons.adaptive_intervals(60.0, 0.0) == 4
        assert horizons.adaptive_intervals(3600.0, 1_000_000.0) == 240

    def test_adaptive_intervals_scales_with_rate(self):
        slow = horizons.adaptive_intervals(3600.0, 10.0)
        fast = horizons.adaptive_intervals(3600.0, 1000.0)
        assert fast > slow

    def test_collect_window_brackets_now(self):
        now = datetime(2026, 7, 22, 12, 0, 0, tzinfo=UTC)
        win = horizons.collect_window(10.0, 3, rate_arcsec_per_hr=100.0, now=now)
        assert win.start < now < win.stop
        assert win.intervals >= 4
