# SPDX-License-Identifier: Apache-2.0
"""Time range parsing utilities supporting symbolic and absolute time specifications."""

import functools
import re
import zoneinfo
from datetime import UTC, datetime, timedelta
from typing import Callable

from dateutil.parser import parse as parse_datetime
from dateutil.tz import tzlocal, tzoffset, tzutc

_pattern = re.compile(
    r"(?P<symbol>[A-Za-z_]+)\s*(?:(?P<op>[+-])\s*(?P<deltaspec>.*))?|(?P<timespec>.+)"
)
_ref_dt = datetime(year=2000, month=1, day=1, tzinfo=UTC)
_ref_date = _ref_dt.date()

type SymbolParseHandler = Callable[[datetime, datetime, bool], datetime]


@functools.cache
def _timezones():
    now = datetime.now(UTC)
    return {
        info.tzname(now): info
        for info in (
            zoneinfo.ZoneInfo(zone) for zone in zoneinfo.available_timezones()
        )
    }


def clear_timezone_cache():
    """Invalidate the cached timezone name-to-ZoneInfo mapping so it is rebuilt on next access."""
    _timezones.cache_clear()


def get_tzinfo(name: str | None, offset: int = None):
    """Resolve a timezone name and optional UTC offset (in seconds) to a `tzinfo` object.

    Returns the local timezone when both arguments are absent. Raises `TimeRangeError` for
    unrecognized timezone names.
    """
    if not name and offset is None:
        return tzlocal()

    if name:
        tz = _timezones().get(name)

        if not tz:
            raise TimeRangeError(f"unknown timezone: {name}")
    else:
        tz = tzutc()

    if offset is not None:
        offset = int(tz.utcoffset(datetime.now()).total_seconds()) - offset
        return tzoffset(f"UTC{offset // 36:+d}", offset)

    return tz


def parse_time_of_day(spec: str, range_min: datetime, range_max: datetime):
    """Parse a time-of-day string into the first matching `datetime` within `[range_min, range_max]`.

    Raises `TimeRangeParseError` if `spec` contains a date component, or `TimeRangeError` if
    the resolved time falls outside the allowed range.
    """
    # Parse the input spec.
    dt = parse_datetime(spec, default=_ref_dt, tzinfos=get_tzinfo)

    # Make sure the spec did not contain a date component.
    if dt.date() != _ref_date:
        raise TimeRangeParseError("input must be time of day, not date")

    # Find the first time in the allowed range that matches the parsed time.
    if range_min.tzinfo != dt.tzinfo:
        # Make sure timezones match before combining.
        range_min = range_min.astimezone(dt.tzinfo)

    dt = datetime.combine(range_min.date(), dt.timetz())

    if dt < range_min:
        dt += timedelta(days=1)

    if dt > range_max:
        raise TimeRangeError(f"input is outside allowed range ({dt} > {range_max})")

    return dt


def parse_time_delta(spec: str):
    """Parse a time-of-day string as a `timedelta` relative to midnight of the reference date."""
    # Use a default reference datetime to extract the delta.
    delta_dt = parse_datetime(spec, default=_ref_dt)

    if delta_dt.date() != _ref_date:
        raise TimeRangeParseError("delta must be time of day, not date")

    return delta_dt - _ref_dt


def parse_spec(
    spec: str,
    range_min: datetime,
    range_max: datetime,
    symbol_handlers: dict[str, SymbolParseHandler] = None,
    latest_match: bool = False,
) -> datetime:
    """Resolve a time spec string to a `datetime` within a one-day window.

    The spec may be an absolute time-of-day string or a symbol (optionally followed by a
    `+`/`-` delta). Symbol resolution is delegated to `symbol_handlers`.
    """
    assert range_max - range_min <= timedelta(days=1)

    # Parse the input spec.
    match = _pattern.match(spec)

    if not match:
        raise TimeRangeParseError("input not recognized")

    if timespec := match["timespec"]:
        return parse_time_of_day(timespec.upper(), range_min, range_max)
    else:
        symbol_handlers = symbol_handlers or {}
        handler_func = symbol_handlers.get(match["symbol"])

        if not handler_func:
            raise TimeRangeParseError(f"symbol '{match['symbol']}' not recognized")

        if deltaspec := match["deltaspec"]:
            delta = parse_time_delta(deltaspec.lower()) * (-1 if match["op"] == "-" else 1)
        else:
            delta = timedelta(0)

        # Use the matching handler to evaluate the range into a datetime.
        dt = handler_func(
            range_min - delta,
            range_max - delta,
            latest_match,
        )

        if dt is None:
            raise TimeRangeError(
                f"could not evaluate '{match['symbol']}'"
                f" between {range_min - delta} and {range_max - delta}"
            )

        return dt + delta


def parse_time_range(
    start_spec: str,
    end_spec: str,
    time_ref: datetime = None,
    symbol_handlers: dict[str, SymbolParseHandler] = None,
):
    """Parse a start and end spec into a `(start_dt, end_dt)` tuple relative to `time_ref`.

    The end time is the earliest match within one day after `time_ref`; the start time is the
    latest match within one day before the resolved end time. `time_ref` defaults to the current
    UTC time if not provided.
    """
    if time_ref:
        if time_ref.tzinfo is None:
            raise ValueError("time_ref must have a timezone")
    else:
        time_ref = datetime.now(UTC)

    # End time is the earliest spec match within one day following the input reference time.
    end_dt = parse_spec(
        end_spec,
        time_ref,
        time_ref + timedelta(days=1),
        symbol_handlers=symbol_handlers,
    )

    # Start time is the latest spec match within one day preceding the end time.
    start_dt = parse_spec(
        start_spec,
        end_dt - timedelta(days=1),
        end_dt,
        symbol_handlers=symbol_handlers,
        latest_match=True,
    )

    return start_dt, end_dt


class TimeRangeError(Exception):
    """Raised when a resolved time is outside the permitted range or cannot be evaluated."""


class TimeRangeParseError(TimeRangeError):
    """Raised when a time spec string cannot be parsed or is structurally invalid."""
