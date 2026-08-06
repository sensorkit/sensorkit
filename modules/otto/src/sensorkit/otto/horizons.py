# SPDX-License-Identifier: Apache-2.0
"""JPL Horizons targets — object resolution, ephemerides, and visibility.

Otto uses Horizons for targets that have no TLE: asteroids, comets, planets,
and deep-space hardware. Two calls are needed, both against
https://ssd.jpl.nasa.gov/api/horizons.api with EPHEM_TYPE=OBSERVER:

  resolve()          name -> COMMAND, once per object, cached
  fetch_ephemeris()  COMMAND -> sampled RA/Dec (ICRF), rates, and az/el

`QUANTITIES='1,3,4'` returns everything Otto needs from a single request:
astrometric RA/Dec (ICRF/J2000), sky-motion rates (dRA*cosD and d(DEC)/dt, both
arcsec/hr), and apparent azimuth/elevation. With `ANG_FORMAT=DEG` and
`CSV_FORMAT=YES` the rows between $$SOE/$$EOE split on commas:

    Date, presence-flag, (blank), RA, DEC, dRA*cosD, d(DEC)/dt, Azi, Elev

Naming: the configured string is passed to Horizons verbatim. Horizons searches
major bodies first and falls back to its small-body index on its own, so `433`,
`Apophis`, `2026 AB`, and `Ceres` all resolve as typed. Beware that the
major-body search wildcards, so a bare name can match something unintended
(`Eros` resolves to Kerberos, a moon of Pluto); append `;` to force the
small-body record (`Eros;`) or use the catalog number. Every resolution is
logged so a surprise is visible in the log.

Each target source acquires its data its own way — here a per-name `resolve`
plus periodic ephemeris refreshes — then speaks the shared vocabulary the
program dispatches on: `position` reports (altitude, azimuth, rising,
hour_angle) straight from the cached ephemeris (Horizons computes az/el for the
observing site), and `make_target` builds the collect target, an ICRF
`EphemerisTarget` sampled densely enough for the mount to interpolate.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from loguru import logger
from pydantic import BaseModel, Field

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import EphemerisTarget
from sensorkit.otto.utils import (
    dither_offset,
    hour_angle,
    interpolate_angle,
    normalize_degrees,
    to_jd,
)

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
USER_AGENT = "sensorkit-otto (JPL Horizons client)"

# 1=astrometric RA/Dec (ICRF), 3=RA/Dec sky-motion rates, 4=apparent az/el.
QUANTITIES = "1,3,4"

# Ephemeris sampling: target on-sky motion between samples, and the bounds on
# the resulting sample count. Horizons takes a bare integer STEP_SIZE as the
# number of equal sub-intervals, so samples = intervals + 1.
MAX_ARCSEC_PER_STEP = 5.0
MIN_INTERVALS = 4
MAX_INTERVALS = 240

# Sample spacing of the cached visibility ephemeris, which is much coarser than
# a target ephemeris because `position` interpolates between its samples. The
# residual error grows with the square of the spacing — measured against
# minute-by-minute truth, roughly 0.05 degrees at 15 minutes and 0.7 degrees at
# an hour — so this still sets how sharply an `altitude_min` decision lands.
# Holding the spacing fixed (rather than the sample count) keeps that accuracy
# constant however long a refresh interval the caller asks for.
VISIBILITY_STEP_MINUTES = 15


@dataclass(frozen=True)
class HorizonsSample:
    """One row of an Observer-Table ephemeris."""

    jd: float  # UTC Julian Day, matching SensorKit's obstime.jd convention
    ra: float  # degrees, ICRF
    dec: float  # degrees, ICRF
    ra_rate: float  # dRA*cosD, arcsec/hr (on-sky, includes the cos(dec) term)
    dec_rate: float  # d(DEC)/dt, arcsec/hr
    azimuth: float  # degrees, apparent
    elevation: float  # degrees, apparent


class HorizonsObject(BaseModel):
    """A resolved JPL Horizons object and its cached visibility ephemeris."""

    command: str  # what Horizons resolved the configured name to
    name: str  # the resolved object name, as Horizons reports it
    samples: list[HorizonsSample] = Field(default_factory=list)


class HorizonsCache(BaseModel):
    """Cached Horizons resolutions and ephemerides, keyed by configured name."""

    objects: dict[str, HorizonsObject] = Field(default_factory=dict)
    dt: datetime


def _quote(value: str) -> str:
    """Wrap a Horizons parameter value in the single quotes the API expects."""
    return "'" + value.strip().strip("'") + "'"


async def _horizons_get(params: dict[str, str], timeout: float) -> str:
    """Issue a Horizons request and return the raw text result."""
    async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": USER_AGENT}) as client:
        response = await client.get(HORIZONS_URL, params={"format": "text", **params})
    if response.status_code != 200:
        # Horizons returns a short error body for malformed queries.
        raise RuntimeError(f"Horizons HTTP {response.status_code}: {response.text[:300]}")
    return response.text


# === Object resolution ========================================================

# The resolved name appears in the banner: small bodies on the "JPL/HORIZONS
# <name> <timestamp>" line, major bodies on the "Revised: <date> <name> <id>"
# line, and either once an ephemeris has been generated.
_NAME_EPHEM_RE = re.compile(r"Target body name:\s*(.+?)\s*(?:\{|$)", re.MULTILINE)
_NAME_SMALL_RE = re.compile(
    r"^JPL/HORIZONS\s+(.+?)\s+\d{4}-[A-Za-z]{3}-\d{2}\s+\d{2}:\d{2}:\d{2}", re.MULTILINE
)
# The major-body banner is "Revised: <date>  <name>  [<id>]", with the date in
# no fixed format ("April 12, 2021" and "2019-Jan-02" both occur), the id
# sometimes absent, and the label sometimes spelled "Revised :" (spacecraft -93)
# — so anchor on the column gaps rather than the date shape.
_NAME_MAJOR_RE = re.compile(r"^\s*Revised\s*:\s*\S.*?\s{2,}(\S.*?)(?:\s{2,}|\s*$)", re.MULTILINE)
# A listing row leads with its identifier, negative for spacecraft; every other
# line in a listing starts with something other than an integer.
_CANDIDATE_ROW_RE = re.compile(r"^\s*(-?\d+)\s+\S")


def _extract_name(text: str) -> str | None:
    for pattern in (_NAME_EPHEM_RE, _NAME_SMALL_RE, _NAME_MAJOR_RE):
        if match := pattern.search(text):
            return match.group(1).strip()
    return None


def _is_single_resolve(text: str) -> bool:
    return (
        _extract_name(text) is not None
        and "Matching small-bodies" not in text
        and "Multiple major-bodies" not in text
        and "No matches found" not in text
        and "Cannot interpret" not in text
    )


def candidates(text: str, limit: int = 20) -> list[str]:
    """Names an ambiguous match can be narrowed to, ready to paste into config.

    Each listed name resolves on its own: an ID# for a major body, a record
    number for a small body. A response carrying no listing yields an empty
    list, which is how `resolve` tells "ambiguous" from "unknown".

    Args:
        text: A Horizons response.
        limit: Maximum number of names to return.
    """
    out: list[str] = []
    suffix: str | None = None
    for line in text.splitlines():
        # Record numbers overlap major-body IDs (501 is both Io and asteroid
        # Urhixidur), and only select the small-body record with a trailing ";".
        if "Multiple major-bodies" in line:
            suffix = ""
        elif "Matching small-bodies" in line:
            suffix = ";"
        elif suffix is not None and (row := _CANDIDATE_ROW_RE.match(line)):
            out.append(row.group(1) + suffix)
    return out[:limit]


async def resolve(name: str, timeout: float = 30.0) -> tuple[str, str] | None:
    """Resolve a configured object name to a Horizons COMMAND.

    The name is sent verbatim; Horizons searches major bodies first and falls
    back to its small-body index, so catalog numbers, designations, and names
    all work as typed. Append `;` to force the small-body record.

    Args:
        name: Object name, designation, or catalog number from config.
        timeout: Request timeout in seconds.

    Returns:
        Tuple of (command, resolved_name), or None if the name is ambiguous or
        unknown — in which case the reason is logged with any candidates.
    """
    command = name.strip()
    try:
        text = await _horizons_get(
            {"COMMAND": _quote(command), "OBJ_DATA": "YES", "MAKE_EPHEM": "NO"}, timeout
        )
    except Exception as e:
        logger.warning(f"Horizons lookup for {name!r} failed: {e}")
        return None

    if not _is_single_resolve(text):
        if matches := candidates(text):
            logger.warning(
                f"Horizons object {name!r} is ambiguous; use one of: {', '.join(matches)}"
            )
        else:
            logger.warning(f"Horizons has no match for {name!r}")
        return None

    resolved = _extract_name(text)
    # Major-body headers can carry a trailing center reference
    # ("... (spacecraft) / Sun"); keep only the object itself.
    resolved = re.split(r"\s+/\s+", resolved)[0].strip()
    logger.info(f"Horizons resolved {name!r} -> {resolved} (command={command!r})")
    return command, resolved


# === Ephemeris ================================================================

_DATE_FORMATS = ("%Y-%b-%d %H:%M:%S.%f", "%Y-%b-%d %H:%M:%S", "%Y-%b-%d %H:%M")


def _parse_date(value: str) -> datetime:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"unparseable Horizons date: {value!r}")


def _to_float(value: str) -> float | None:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def fmt_time(dt: datetime) -> str:
    """Format a datetime as Horizons' expected 'YYYY-MM-DD HH:MM:SS' in UTC."""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def adaptive_intervals(window_seconds: float, rate_arcsec_hr: float | None = None) -> int:
    """Sub-interval count keeping on-sky motion per step under MAX_ARCSEC_PER_STEP.

    A fast NEO is sampled finely enough for the mount to interpolate accurately;
    a slow outer-solar-system body is not needlessly over-sampled.
    """
    rate_arcsec_sec = abs(rate_arcsec_hr or 0.0) / 3600.0
    if rate_arcsec_sec > 0:
        step_seconds = MAX_ARCSEC_PER_STEP / rate_arcsec_sec
    else:
        step_seconds = window_seconds / MIN_INTERVALS
    intervals = math.ceil(window_seconds / max(1.0, step_seconds))
    return max(MIN_INTERVALS, min(MAX_INTERVALS, intervals))


def visibility_intervals(window_seconds: float) -> int:
    """Sub-interval count for a visibility ephemeris spanning `window_seconds`.

    The sibling of `adaptive_intervals`: that one sizes the dense ephemeris a
    task tracks, this one the coarse cache behind altitude and hour-angle
    checks, at a fixed VISIBILITY_STEP_MINUTES spacing.
    """
    return max(1, int(window_seconds // (VISIBILITY_STEP_MINUTES * 60)))


def parse_ephemeris(text: str) -> list[HorizonsSample]:
    """Parse the CSV rows between $$SOE/$$EOE into samples."""
    block = re.search(r"\$\$SOE\s*(.*?)\s*\$\$EOE", text, re.DOTALL)
    if not block:
        return []

    samples: list[HorizonsSample] = []
    for line in block.group(1).splitlines():
        if not line.strip():
            continue
        fields = [f.strip() for f in line.split(",")]
        # RA is the first parseable float after the date and presence-flag columns.
        start = next(
            (i for i in range(1, len(fields)) if _to_float(fields[i]) is not None), None
        )
        if start is None or start + 5 >= len(fields):
            continue
        try:
            timestamp = _parse_date(fields[0])
        except ValueError:
            continue
        values = [_to_float(f) for f in fields[start : start + 6]]
        if any(v is None for v in values):
            continue
        ra, dec, ra_rate, dec_rate, azimuth, elevation = values
        samples.append(
            HorizonsSample(
                jd=to_jd(timestamp),
                ra=ra,
                dec=dec,
                ra_rate=ra_rate,
                dec_rate=dec_rate,
                azimuth=azimuth,
                elevation=elevation,
            )
        )
    return samples


async def fetch_ephemeris(
    command: str,
    latitude: float,
    longitude: float,
    altitude_km: float,
    start: datetime,
    stop: datetime,
    intervals: int,
    timeout: float = 60.0,
) -> list[HorizonsSample]:
    """Fetch a sampled Observer-Table ephemeris for a resolved object.

    Args:
        command: Resolved Horizons COMMAND (see `resolve`).
        latitude: Observer latitude in degrees.
        longitude: Observer East longitude in degrees.
        altitude_km: Observer altitude in kilometers.
        start: Window start (UTC).
        stop: Window stop (UTC).
        intervals: Number of equal sub-intervals; samples = intervals + 1.
        timeout: Request timeout in seconds.

    Returns:
        The parsed samples, oldest first. Empty if Horizons produced no
        ephemeris; raises on transport or HTTP failure.
    """
    text = await _horizons_get(
        {
            "COMMAND": _quote(command),
            "EPHEM_TYPE": "OBSERVER",
            "CENTER": "'coord@399'",
            "COORD_TYPE": "GEODETIC",
            "SITE_COORD": _quote(f"{longitude},{latitude},{altitude_km}"),
            "START_TIME": _quote(fmt_time(start)),
            "STOP_TIME": _quote(fmt_time(stop)),
            "STEP_SIZE": str(intervals),  # bare integer = number of intervals
            "QUANTITIES": _quote(QUANTITIES),
            "ANG_FORMAT": "DEG",
            "CSV_FORMAT": "YES",
            "OBJ_DATA": "NO",
            "MAKE_EPHEM": "YES",
        },
        timeout,
    )

    if "$$SOE" not in text:
        raise RuntimeError(f"Horizons generated no ephemeris: {text[:300].strip()}")
    return parse_ephemeris(text)


# === Visibility and targeting =================================================


def _bracket(samples: list[HorizonsSample], jd: float) -> tuple[int, int]:
    """Indices of the sample at or before `jd` and the one after it, clamped."""
    index = 0
    for i, sample in enumerate(samples):
        if sample.jd > jd:
            break
        index = i
    index = min(index, len(samples) - 2) if len(samples) > 1 else 0
    return index, min(index + 1, len(samples) - 1)


def position(
    samples: list[HorizonsSample],
    longitude: float,
    now: datetime | None = None,
) -> tuple[float, float, bool, float] | None:
    """Current position of a Horizons object relative to an observer.

    The az/el come straight from the cached ephemeris (Horizons computes them
    for the observing site), so this mirrors the other sources' `position`
    contract without any local astrometry.

    Args:
        samples: Cached ephemeris covering the current time.
        longitude: Observer East longitude in degrees.
        now: Evaluation time, defaulting to the current UTC time.

    Returns:
        Tuple of (altitude, azimuth, rising, hour_angle) or None if the cached
        ephemeris does not cover `now`.
        - altitude: Apparent elevation in degrees
        - azimuth: Apparent azimuth in degrees
        - rising: True if the elevation is increasing
        - hour_angle: Hours; negative = east of meridian, positive = west
    """
    if not samples:
        return None

    now = now or datetime.now(UTC)
    jd = to_jd(now)
    if jd < samples[0].jd or jd > samples[-1].jd:
        return None

    lo, hi = _bracket(samples, jd)
    current, following = samples[lo], samples[hi]

    # Interpolate between the bracketing samples. The cache is sampled coarsely
    # to keep it small, so reading the preceding sample verbatim would leave
    # az/el up to a full step of Earth rotation stale — a couple of degrees of
    # altitude near the horizon, exactly where the altitude floor decides.
    span = following.jd - current.jd
    fraction = (jd - current.jd) / span if span else 0.0

    return (
        current.elevation + fraction * (following.elevation - current.elevation),
        interpolate_angle(current.azimuth, following.azimuth, fraction),
        following.elevation > current.elevation,
        hour_angle(interpolate_angle(current.ra, following.ra, fraction), longitude, now),
    )


def current_rate_arcsec_hr(
    samples: list[HorizonsSample], now: datetime | None = None
) -> float | None:
    """Total on-sky motion rate at `now` in arcsec/hr, from a cached ephemeris.

    Used to size the sampling of the denser ephemeris fetched at task creation.
    """
    if not samples:
        return None
    lo, _ = _bracket(samples, to_jd(now or datetime.now(UTC)))
    sample = samples[lo]
    return math.hypot(sample.ra_rate, sample.dec_rate)


async def make_target(
    entry: HorizonsObject,
    *,
    latitude: float,
    longitude: float,
    altitude_km: float,
    start: datetime,
    stop: datetime,
    dither_arcsec: float = 0.0,
) -> EphemerisTarget | None:
    """Fetch a dense ephemeris over [start, stop] and build the collect target.

    Sample density follows the object's current sky motion (read from its
    cached coarse ephemeris), so a fast NEO is sampled finely and a slow body
    is not over-sampled. Dithering shifts every sample by one shared offset,
    keeping the object tracked off-center rather than smeared.

    Returns None if Horizons is unreachable or returns fewer than the two
    samples a mount needs to finite-difference a rate; the caller retries the
    object on a later pass.
    """
    window_seconds = (stop - start).total_seconds()
    try:
        samples = await fetch_ephemeris(
            command=entry.command,
            latitude=latitude,
            longitude=longitude,
            altitude_km=altitude_km,
            start=start,
            stop=stop,
            intervals=adaptive_intervals(
                window_seconds, current_rate_arcsec_hr(entry.samples, start)
            ),
        )
    except Exception as e:
        logger.warning(f"Horizons ephemeris for {entry.name!r} failed: {e}")
        return None

    if len(samples) < 2:
        logger.warning(f"Horizons returned {len(samples)} sample(s) for {entry.name!r}, skipping")
        return None

    delta_ra = delta_dec = 0.0
    if dither_arcsec > 0:
        delta_ra, delta_dec = dither_offset(samples[0].dec, dither_arcsec)

    logger.debug(f"{entry.name}: {len(samples)} ephemeris samples over {window_seconds:.0f}s")
    return EphemerisTarget(
        frame=ReferenceFrame.ICRF,
        jds=[s.jd for s in samples],
        points=[
            Equatorial(ra=normalize_degrees(s.ra + delta_ra), dec=s.dec + delta_dec)
            for s in samples
        ],
    )
