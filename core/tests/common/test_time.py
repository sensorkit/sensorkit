# SPDX-License-Identifier: Apache-2.0
from datetime import UTC, datetime, timedelta

import pytest

from sensorkit.common.time import (
    TimeRangeError,
    TimeRangeParseError,
    clear_timezone_cache,
    get_tzinfo,
    parse_spec,
    parse_time_delta,
    parse_time_of_day,
    parse_time_range,
)


def test_get_tzinfo():
    # Default: no name and no offset -> local tz
    tz = get_tzinfo(None)
    from dateutil.tz import tzlocal as _tzlocal

    assert isinstance(tz, _tzlocal)

    # Named timezone: UTC must be discoverable
    tz_utc = get_tzinfo("UTC")
    assert tz_utc.tzname(datetime.now(UTC)) == "UTC"

    # Named + explicit offset should produce a fixed-offset tz
    # For UTC with offset 0, utcoffset should be zero
    fixed = get_tzinfo("UTC", offset=0)
    assert fixed.utcoffset(datetime.now(UTC)) == timedelta(0)

    # Clearing cache should not break lookups
    clear_timezone_cache()
    tz_utc2 = get_tzinfo("UTC")
    assert tz_utc2.tzname(datetime.now(UTC)) == "UTC"

    with pytest.raises(TimeRangeError):
        get_tzinfo("__NO_SUCH_TZ__")


def test_parse_time_of_day():
    # Window: from 05:00Z on Jan 1, 2025 to 05:00Z on Jan 2, 2025
    range_min = datetime(2025, 1, 1, 5, 0, tzinfo=UTC)
    range_max = range_min + timedelta(days=1)

    # Inside the window on the same day
    t = parse_time_of_day("05:30Z", range_min, range_max)
    assert t == datetime(2025, 1, 1, 5, 30, tzinfo=UTC)

    # Earlier than range_min -> rolls over to the next day, still within window
    t2 = parse_time_of_day("04:00Z", range_min, range_max)
    assert t2 == datetime(2025, 1, 2, 4, 0, tzinfo=UTC)


def test_parse_time_of_day_raises_if_invalid():
    range_min = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    range_max = range_min + timedelta(hours=1)

    with pytest.raises(TimeRangeParseError):
        # Date component is not allowed.
        parse_time_of_day("2025-01-01T12:30Z", range_min, range_max)

    with pytest.raises(TimeRangeError):
        # Time is outside range.
        parse_time_of_day("14:00Z", range_min, range_max)


def test_parse_time_delta():
    assert parse_time_delta("01:15") == timedelta(hours=1, minutes=15)

    with pytest.raises(TimeRangeParseError):
        parse_time_delta("2025-01-01T00:00:00")


def test_parse_spec():
    now = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)

    # Direct time spec path
    dt = parse_spec("06:00Z", now, now + timedelta(days=1))
    assert dt == datetime(2025, 1, 1, 6, 0, tzinfo=UTC)

    # Unknown symbol raises
    with pytest.raises(TimeRangeParseError):
        parse_spec("NOSYM", now, now + timedelta(days=1))


def test_parse_spec_symbol_handlers():
    # Verify that the symbol path is exercised and latest_match flag propagates
    calls: list[tuple[datetime, datetime, bool]] = []

    def handler(rmin: datetime, rmax: datetime, latest: bool) -> datetime:
        calls.append((rmin, rmax, latest))
        # Return a time near the end of the window
        return rmax - timedelta(minutes=5)

    start = datetime(2025, 2, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(days=1)
    out = parse_spec("SUN", start, end, symbol_handlers={"SUN": handler}, latest_match=True)

    assert out == end - timedelta(minutes=5)
    assert len(calls) == 1
    assert calls[0] == (start, end, True)


def test_parse_time_range():
    with pytest.raises(ValueError):
        # time_ref must be tz-aware
        parse_time_range("04:30Z", "06:00Z", time_ref=datetime(2025, 1, 1, 5, 0))

    # Choose a reference time such that end is on the same day
    tref = datetime(2025, 1, 1, 5, 0, tzinfo=UTC)
    start_dt, end_dt = parse_time_range("04:30Z", "06:00Z", time_ref=tref)

    assert start_dt == datetime(2025, 1, 1, 4, 30, tzinfo=UTC)
    assert end_dt == datetime(2025, 1, 1, 6, 0, tzinfo=UTC)
