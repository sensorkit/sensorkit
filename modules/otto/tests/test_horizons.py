# SPDX-License-Identifier: Apache-2.0
"""Tests for the JPL Horizons client.

The fixtures below are trimmed verbatim captures of real Horizons responses.
"""

import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from sensorkit.otto.horizons import (
    MAX_INTERVALS,
    MIN_INTERVALS,
    VISIBILITY_STEP_MINUTES,
    HorizonsSample,
    adaptive_intervals,
    candidates,
    current_rate_arcsec_hr,
    fetch_ephemeris,
    fmt_time,
    parse_ephemeris,
    position,
    resolve,
    visibility_intervals,
)
from sensorkit.otto.utils import to_jd

SMALL_BODY = """\
API VERSION: 1.2
API SOURCE: NASA/JPL Horizons API

*******************************************************************************
JPL/HORIZONS                 433 Eros (A898 PA)            2026-Jul-21 11:26:11
Rec #:     433 (+COV) Soln.date: 2021-May-24_17:55:05   # obs: 9130 (1893-2021)
"""

MAJOR_BODY = """\
*******************************************************************************
 Revised: April 12, 2021               Jupiter                              599

 PHYSICAL DATA (revised 2025-Jan-30):
"""

# Horizons has no fixed date format in this banner, and omits the trailing id.
MAJOR_BODY_ISO_DATE = """\
*******************************************************************************
Revised: 2019-Jan-02                    1 Ceres
MAJOR BODY VERSION          Solution date: 2017-Oct-19 16:09
"""

AMBIGUOUS = """\
*******************************************************************************
 Multiple major-bodies match string "JUPITER*"

  ID#      Name                               Designation  IAU/aliases/other
  -------  ---------------------------------- -----------  -------------------
        5  Jupiter Barycenter
      599  Jupiter

   Number of matches =  2. Use ID# to make unique selection.
*******************************************************************************
"""

NO_MATCH = """\
*******************************************************************************
JPL/DASTCOM            Small-body Index Search Results     2026-Jul-21 11:26:33

 Comet AND asteroid index search:

   NAME = Jupiterr;

 Matching small-bodies:
    No matches found.
"""

EPHEMERIS = """\
 Date__(UT)__HR:MN:SC.fff, , , R.A._(ICRF), DEC_(ICRF),  dRA*cosD, d(DEC)/dt, \
Azi_(a-app), Elev_(a-app),
****************************************************************************
$$SOE
 2026-Jul-21 12:00:00.000,*, ,   188.07090,  -15.40657,  99.55942,  -28.4600,\
   50.208334,   -54.140732,
 2026-Jul-21 12:10:00.000,*, ,   188.07568,  -15.40789,  99.53200,  -28.4713,\
   53.434867,   -52.686916,
 2026-Jul-21 12:20:00.000,*, ,   188.08046,  -15.40921,  99.50258,  -28.4823,\
   56.477048,   -51.172738,
 2026-Jul-21 12:30:00.000,*, ,   188.08524,  -15.41053,  99.47121,  -28.4929,\
   59.350368,   -49.605751,
$$EOE
"""


def _samples(elevations=(10.0, 20.0, 30.0), start=None, step_minutes=15):
    """Ephemeris samples with a controllable elevation trend."""
    start = start or datetime.now(UTC)
    return [
        HorizonsSample(
            jd=to_jd(start + timedelta(minutes=step_minutes * i)),
            ra=180.0,
            dec=-15.0,
            ra_rate=100.0,
            dec_rate=-30.0,
            azimuth=90.0 + i,
            elevation=elevation,
        )
        for i, elevation in enumerate(elevations)
    ]


class TestResolve:
    @pytest.mark.asyncio
    async def test_resolves_small_body(self):
        with patch("sensorkit.otto.horizons._horizons_get", AsyncMock(return_value=SMALL_BODY)):
            assert await resolve("433") == ("433", "433 Eros (A898 PA)")

    @pytest.mark.asyncio
    async def test_resolves_major_body(self):
        with patch("sensorkit.otto.horizons._horizons_get", AsyncMock(return_value=MAJOR_BODY)):
            assert await resolve("599") == ("599", "Jupiter")

    @pytest.mark.asyncio
    async def test_resolves_major_body_with_an_iso_date_banner(self):
        """The banner date format varies and the trailing id may be absent."""
        with patch(
            "sensorkit.otto.horizons._horizons_get",
            AsyncMock(return_value=MAJOR_BODY_ISO_DATE),
        ):
            assert await resolve("Ceres") == ("Ceres", "1 Ceres")

    @pytest.mark.asyncio
    async def test_passes_the_name_through_verbatim(self):
        """Horizons does its own major-then-small-body search; we never rewrite the query."""
        get = AsyncMock(return_value=SMALL_BODY)
        with patch("sensorkit.otto.horizons._horizons_get", get):
            await resolve("  Eros;  ")

        get.assert_awaited_once()
        assert get.await_args.args[0]["COMMAND"] == "'Eros;'"

    @pytest.mark.asyncio
    async def test_ambiguous_name_is_unresolved(self):
        with patch("sensorkit.otto.horizons._horizons_get", AsyncMock(return_value=AMBIGUOUS)):
            assert await resolve("Jupiter") is None

    @pytest.mark.asyncio
    async def test_unknown_name_is_unresolved(self):
        with patch("sensorkit.otto.horizons._horizons_get", AsyncMock(return_value=NO_MATCH)):
            assert await resolve("Jupiterr") is None

    @pytest.mark.asyncio
    async def test_transport_failure_is_unresolved(self):
        with patch(
            "sensorkit.otto.horizons._horizons_get",
            AsyncMock(side_effect=RuntimeError("Horizons HTTP 503")),
        ):
            assert await resolve("433") is None


class TestCandidates:
    """What an unresolvable name offers the operator to fix their config with."""

    def test_ambiguous_response_lists_candidate_ids(self):
        """The operator needs the ID# to disambiguate, so each row must carry it."""
        # Column rules and banner separators are not candidates
        assert candidates(AMBIGUOUS) == ["5 (Jupiter Barycenter)", "599 (Jupiter)"]

    def test_no_match_response_has_nothing_to_offer(self):
        """A typo has no candidates, which is how resolve reports it as unknown
        rather than ambiguous."""
        assert candidates(NO_MATCH) == []


class TestParseEphemeris:
    def test_parses_all_columns(self):
        samples = parse_ephemeris(EPHEMERIS)

        assert len(samples) == 4
        first = samples[0]
        assert first.ra == pytest.approx(188.07090)
        assert first.dec == pytest.approx(-15.40657)
        assert first.ra_rate == pytest.approx(99.55942)
        assert first.dec_rate == pytest.approx(-28.4600)
        assert first.azimuth == pytest.approx(50.208334)
        assert first.elevation == pytest.approx(-54.140732)
        assert first.jd == pytest.approx(to_jd(datetime(2026, 7, 21, 12, 0, tzinfo=UTC)))

    def test_samples_are_ordered(self):
        samples = parse_ephemeris(EPHEMERIS)
        assert [s.jd for s in samples] == sorted(s.jd for s in samples)

    def test_no_block_yields_nothing(self):
        assert parse_ephemeris(NO_MATCH) == []


class TestFetchEphemeris:
    @pytest.mark.asyncio
    async def test_requests_the_site_and_window(self):
        get = AsyncMock(return_value=EPHEMERIS)
        with patch("sensorkit.otto.horizons._horizons_get", get):
            samples = await fetch_ephemeris(
                command="433;",
                latitude=42.3,
                longitude=-83.0,
                altitude_km=0.2,
                start=datetime(2026, 7, 21, 12, 0, tzinfo=UTC),
                stop=datetime(2026, 7, 21, 12, 30, tzinfo=UTC),
                intervals=3,
            )

        params = get.await_args.args[0]
        # Horizons GEODETIC site coordinates are East longitude first
        assert params["SITE_COORD"] == "'-83.0,42.3,0.2'"
        assert params["START_TIME"] == "'2026-07-21 12:00:00'"
        assert params["STEP_SIZE"] == "3"
        assert len(samples) == 4

    @pytest.mark.asyncio
    async def test_missing_ephemeris_block_raises(self):
        with (
            patch("sensorkit.otto.horizons._horizons_get", AsyncMock(return_value=AMBIGUOUS)),
            pytest.raises(RuntimeError, match="no ephemeris"),
        ):
            await fetch_ephemeris(
                command="Jupiter",
                latitude=42.3,
                longitude=-83.0,
                altitude_km=0.2,
                start=datetime(2026, 7, 21, 12, 0, tzinfo=UTC),
                stop=datetime(2026, 7, 21, 12, 30, tzinfo=UTC),
                intervals=3,
            )

    def test_formats_time_in_utc(self):
        local = datetime(2026, 7, 21, 12, 0, tzinfo=UTC).astimezone()
        assert fmt_time(local) == "2026-07-21 12:00:00"


class TestAdaptiveIntervals:
    def test_fast_movers_are_sampled_more_densely(self):
        slow = adaptive_intervals(600, rate_arcsec_hr=10)
        fast = adaptive_intervals(600, rate_arcsec_hr=3600)
        assert fast > slow

    def test_clamped_to_bounds(self):
        assert adaptive_intervals(600, rate_arcsec_hr=0.0001) == MIN_INTERVALS
        assert adaptive_intervals(86400, rate_arcsec_hr=100000) == MAX_INTERVALS

    def test_unknown_rate_falls_back_to_minimum(self):
        assert adaptive_intervals(600, rate_arcsec_hr=None) == MIN_INTERVALS


class TestPosition:
    def test_interpolates_between_the_bracketing_samples(self):
        """The cache is coarse, so reading the preceding sample verbatim would
        leave az/el up to a full step of Earth rotation stale."""
        now = datetime.now(UTC)
        # now sits 1 minute into a 15-minute step from 10° to 20°
        samples = _samples(start=now - timedelta(minutes=1))

        result = position(samples, longitude=-117.0, now=now)

        assert result is not None
        altitude, azimuth, rising, hour_angle = result
        assert altitude == pytest.approx(10.0 + (1 / 15) * 10.0, abs=1e-6)
        assert azimuth == pytest.approx(90.0 + (1 / 15) * 1.0, abs=1e-6)
        assert -12.0 <= hour_angle < 12.0

    def test_lands_exactly_on_a_sample_at_its_timestamp(self):
        now = datetime.now(UTC)
        samples = _samples(start=now)
        altitude, azimuth, _, _ = position(samples, longitude=-117.0, now=now)

        assert altitude == pytest.approx(10.0, abs=1e-6)
        assert azimuth == pytest.approx(90.0, abs=1e-6)

    def test_interpolation_is_monotonic_across_a_step(self):
        now = datetime.now(UTC)
        samples = _samples(start=now)
        altitudes = [
            position(samples, longitude=0.0, now=now + timedelta(minutes=m))[0]
            for m in range(0, 16)
        ]
        assert altitudes == sorted(altitudes)
        assert altitudes[-1] == pytest.approx(20.0, abs=1e-6)

    def test_azimuth_interpolation_takes_the_short_way_around_north(self):
        """A pair straddling 0/360 must not swing the long way through 180°."""
        now = datetime.now(UTC)
        base = _samples(elevations=(10.0, 20.0), start=now)
        samples = [replace(base[0], azimuth=359.0), replace(base[1], azimuth=1.0)]

        # Halfway through the step: due north, not due south
        _, azimuth, _, _ = position(samples, longitude=0.0, now=now + timedelta(minutes=7.5))
        # Measured as a distance from north, since the midpoint sits exactly on
        # the 0/360 seam and legitimately lands on either side of it
        assert min(azimuth, 360.0 - azimuth) == pytest.approx(0.0, abs=1e-3)

    def test_rising_when_elevation_increases(self):
        now = datetime.now(UTC)
        samples = _samples((10.0, 20.0), start=now - timedelta(minutes=1))
        _, _, rising, _ = position(samples, longitude=0.0, now=now)
        assert rising is True

    def test_falling_when_elevation_decreases(self):
        now = datetime.now(UTC)
        samples = _samples((20.0, 10.0), start=now - timedelta(minutes=1))
        _, _, rising, _ = position(samples, longitude=0.0, now=now)
        assert rising is False

    def test_none_when_cache_does_not_cover_now(self):
        now = datetime.now(UTC)
        stale = _samples(start=now - timedelta(days=2))
        assert position(stale, longitude=0.0, now=now) is None

        future = _samples(start=now + timedelta(days=2))
        assert position(future, longitude=0.0, now=now) is None

    def test_none_without_samples(self):
        assert position([], longitude=0.0) is None

    def test_hour_angle_flips_sign_across_the_meridian(self):
        """East of the meridian is negative, west positive — the scan ordering depends on it."""
        now = datetime.now(UTC)
        samples = _samples(start=now - timedelta(minutes=1))

        _, _, _, ha = position(samples, longitude=0.0, now=now)
        # Shifting the observer 90° east advances local sidereal time by 6 hours
        _, _, _, ha_east = position(samples, longitude=90.0, now=now)

        assert ((ha_east - ha + 12.0) % 24.0) - 12.0 == pytest.approx(6.0, abs=1e-6)


class TestCurrentRate:
    def test_combines_both_axes(self):
        now = datetime.now(UTC)
        samples = _samples(start=now - timedelta(minutes=1))
        assert current_rate_arcsec_hr(samples, now) == pytest.approx(math.hypot(100.0, -30.0))

    def test_none_without_samples(self):
        assert current_rate_arcsec_hr([]) is None


class TestVisibilityIntervals:
    """Sizes the coarse visibility cache at a fixed spacing."""

    def test_holds_the_step_as_the_window_grows(self):
        """Fixed spacing, not a fixed count — so accuracy stays constant
        however long a refresh interval the caller configures."""
        for hours in (2, 7, 25):
            intervals = visibility_intervals(hours * 3600)
            spacing_minutes = hours * 60 / intervals
            assert spacing_minutes == pytest.approx(VISIBILITY_STEP_MINUTES, abs=1.0)

    def test_default_window_spacing(self):
        # 6h refresh interval + 1h slack, at 15-minute steps
        assert visibility_intervals(7 * 3600) == 28

    def test_never_returns_zero(self):
        """Horizons rejects a zero STEP_SIZE, so a short window still gets one."""
        assert visibility_intervals(60) == 1
        assert visibility_intervals(0) == 1
