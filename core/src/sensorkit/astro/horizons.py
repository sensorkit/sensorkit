# SPDX-License-Identifier: Apache-2.0
"""JPL Horizons client — object lookup + Observer-Table ephemeris.

Queries https://ssd.jpl.nasa.gov/api/horizons.api for objects (asteroids, comets,
spacecraft, planets, moons) that do not have a Two-Line Element set, and parses the
service's text output into clean Python values. A resolved object's ephemeris maps
directly onto :class:`~sensorkit.astro.target.EphemerisTarget` for tasking.

The lookup/ephemeris query construction and the text parsers are ported from
SensorView's Horizons proxy (``api/routers/horizons.py``); that implementation is
battle-tested against Horizons' output format.

Two calls are exposed:
  resolve(query)                 -> HorizonsResolution   (resolve / disambiguate)
  ephemeris(command, lon, ...)   -> list[HorizonsSample] (sampled RA/Dec in ICRF)

Both speak to EPHEM_TYPE=OBSERVER with QUANTITIES='1,3,9,20' (astrometric RA/Dec,
sky-motion rates, magnitude, range), ANG_FORMAT=DEG, CSV_FORMAT=YES so the rows
between $$SOE/$$EOE are trivially splittable.

``httpx`` is imported lazily inside the request path so that importing this module
does not require it — it is declared under the module extras that actually query
Horizons (e.g. the ``otto`` extra), not as a hard core dependency.
"""

from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import EphemerisTarget

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
USER_AGENT = "SensorKit (JPL Horizons client)"
TIMEOUT = 30.0

# QUANTITIES: 1=astrometric RA/Dec (ICRF/J2000), 3=RA/Dec sky-motion rates
# (dRA*cosD, d(DEC)/dt in arcsec/hr), 9=visual mag + surface brightness,
# 20=observer range (AU) + range-rate.
QUANTITIES = "1,3,9,20"

# Small in-memory TTL cache. Lookups change rarely; ephemerides are time-relative
# so cache only briefly to coalesce duplicate requests.
_LOOKUP_TTL = 3600.0
_cache: dict[str, tuple[float, Any]] = {}


class HorizonsError(RuntimeError):
    """Raised when a Horizons request fails or its output cannot be parsed."""


@dataclass(frozen=True, slots=True)
class HorizonsCandidate:
    """One entry in an ambiguous-object candidate list."""

    command: str
    name: str
    kind: str  # "small" | "major"


@dataclass(frozen=True, slots=True)
class HorizonsResolution:
    """Result of resolving a Horizons query.

    When ``resolved`` is True, ``command`` is the locked Horizons COMMAND to pass to
    :func:`ephemeris` and ``name`` is the object's display name. When False,
    ``candidates`` holds the disambiguation choices (may be empty for no match).
    """

    resolved: bool
    command: str | None = None
    name: str | None = None
    kind: str | None = None  # "small" | "major"
    is_sun: bool = False
    candidates: list[HorizonsCandidate] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class HorizonsSample:
    """A single Observer-Table ephemeris sample (astrometric RA/Dec in ICRF)."""

    jd: float
    utc: str
    ra: float  # degrees
    dec: float  # degrees
    ra_rate_arcsec_hr: float | None = None
    dec_rate_arcsec_hr: float | None = None
    magnitude: float | None = None
    range_au: float | None = None

    @property
    def total_rate_arcsec_hr(self) -> float:
        """Combined on-sky motion rate (quadrature of the RA*cosD and Dec rates)."""
        r = self.ra_rate_arcsec_hr or 0.0
        d = self.dec_rate_arcsec_hr or 0.0
        return math.hypot(r, d)


def _cache_get(key: str) -> Any | None:
    hit = _cache.get(key)
    if hit and hit[0] > time.monotonic():
        return hit[1]
    return None


def _cache_put(key: str, value: Any, ttl: float) -> None:
    _cache[key] = (time.monotonic() + ttl, value)


async def _horizons_get(params: dict[str, str]) -> str:
    """Issue a Horizons request and return the raw text result."""
    import httpx  # lazy: not a hard core dependency

    q = {"format": "text", **params}
    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}
        ) as client:
            resp = await client.get(HORIZONS_URL, params=q)
    except httpx.RequestError as e:
        raise HorizonsError(f"Horizons unreachable: {e}") from e
    if resp.status_code != 200:
        # Horizons returns a short error body for bad queries.
        raise HorizonsError(f"Horizons HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.text


def _quote(value: str) -> str:
    """Wrap a Horizons parameter value in the single quotes the API expects."""
    return "'" + value.strip().strip("'") + "'"


# === Lookup parsing ===========================================================

# Name appears as "Target body name:" once an ephemeris is generated; in a bare
# OBJ_DATA header it sits in the banner instead — small bodies on the
# "JPL/HORIZONS <name> <timestamp>" line, major bodies on the "Revised: <date>
# <name> <id>" line.
_NAME_EPHEM_RE = re.compile(r"Target body name:\s*(.+?)\s*(?:\{|$)", re.MULTILINE)
_NAME_SMALL_RE = re.compile(
    r"^JPL/HORIZONS\s+(.+?)\s+\d{4}-[A-Za-z]{3}-\d{2}\s+\d{2}:\d{2}:\d{2}", re.MULTILINE
)
_NAME_MAJOR_RE = re.compile(r"^\s*Revised:.*?\d{4}\s{2,}(.+?)\s{2,}", re.MULTILINE)
_MAJOR_ROW_RE = re.compile(r"^\s*(-?\d+)\s+(.+?)\s*$")


def _extract_name(text: str) -> str | None:
    for rx in (_NAME_EPHEM_RE, _NAME_SMALL_RE, _NAME_MAJOR_RE):
        m = rx.search(text)
        if m:
            return m.group(1).strip()
    return None


def _parse_major_matches(text: str) -> list[HorizonsCandidate]:
    """Parse a 'Multiple major-bodies match' table into candidates."""
    out: list[HorizonsCandidate] = []
    lines = text.splitlines()
    started = False
    for line in lines:
        if re.match(r"\s*ID#", line):
            started = True
            continue
        if not started:
            continue
        if set(line.strip()) <= {"-", " "} and line.strip():
            continue  # dashed separator
        if not line.strip():
            if out:
                break  # blank line after rows ends the table
            continue
        m = _MAJOR_ROW_RE.match(line)
        if not m:
            continue
        ident, rest = m.group(1), m.group(2)
        # Name is the first column of `rest` (up to 2+ spaces).
        name = re.split(r"\s{2,}", rest.strip())[0].strip()
        out.append(HorizonsCandidate(command=ident, name=name, kind="major"))
    return out


def _parse_small_matches(text: str) -> list[HorizonsCandidate]:
    """Best-effort parse of a 'Matching small-bodies' list into candidates."""
    out: list[HorizonsCandidate] = []
    lines = text.splitlines()
    started = False
    for line in lines:
        if "Matching small-bodies" in line or re.search(r"Record #|Primary Desig", line):
            started = True
            continue
        if not started:
            continue
        if set(line.strip()) <= {"-", " "} and line.strip():
            continue
        if not line.strip():
            if out:
                break
            continue
        # Rows look like:  "<rec#>  <epoch>  <desig>  <Name (year)>"
        toks = line.split()
        if not toks or not re.match(r"^-?\d", toks[0]):
            continue
        name = re.split(r"\s{2,}", line.strip())[-1].strip()
        # Prefer a numeric designation token for re-query; else the first name word.
        desig = next((t for t in toks if re.match(r"^\d+$", t)), None)
        display = f"{desig} {name}".strip() if desig else (name or toks[0])
        cmd = f"{desig};" if desig else f"{name.split()[0]};"
        out.append(HorizonsCandidate(command=cmd, name=display, kind="small"))
    return out[:40]


def _small_variants(q: str) -> list[str]:
    """Small-body COMMAND forms to try, in order. Horizons rejects 'name';' when
    the name has a number+word ('433 Eros;' → DES search → no match), so we also
    try the bare numeric token ('433;') and the bare name token ('Eros;')."""
    q = q.strip()
    out = [f"{q};"]
    toks = q.split()
    num = next((t for t in toks if re.fullmatch(r"\d+", t)), None)
    if num and f"{num};" not in out:
        out.append(f"{num};")
    alpha = next((t for t in toks if re.fullmatch(r"[A-Za-z][\w/+-]*", t)), None)
    if alpha and f"{alpha};" not in out:
        out.append(f"{alpha};")
    # Prefix/wildcard fallback so a partial name ("Melp" → 18 Melpomene) still
    # finds candidates. Only for plain alphabetic queries; Horizons does this
    # via a trailing '*' on the small-body name search.
    if re.fullmatch(r"[A-Za-z][A-Za-z ]*", q):
        wc = f"{q}*;"
        if wc not in out:
            out.append(wc)
    return out[:4]


def _is_single_resolve(text: str) -> bool:
    return (
        _extract_name(text) is not None
        and "Matching small-bodies" not in text
        and "Multiple major-bodies" not in text
        and "No matches found" not in text
        and "Cannot interpret" not in text
    )


async def resolve(query: str) -> HorizonsResolution:
    """Resolve a Horizons object by name, designation, or id.

    Returns a :class:`HorizonsResolution`: either a single resolved object (with a
    locked ``command`` for :func:`ephemeris`) or a list of candidates to choose from.
    """
    key = f"lookup:{query.strip().lower()}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    async def run(command: str) -> str:
        return await _horizons_get(
            {"COMMAND": _quote(command), "OBJ_DATA": "YES", "MAKE_EPHEM": "NO"}
        )

    first = await run(query)
    text, used_command = first, query.strip()
    name = _extract_name(first) if _is_single_resolve(first) else None
    small_list_text: str | None = None

    # Not a clean major-body resolve (and not an ambiguous major-body list) —
    # try small-body token + wildcard variants.
    if name is None and ";" not in query and "Multiple major-bodies match" not in first:
        for variant in _small_variants(query):
            rt = await run(variant)
            if _is_single_resolve(rt):
                text, used_command, name = rt, variant, _extract_name(rt)
                break
            if (
                small_list_text is None
                and "Matching small-bodies" in rt
                and _parse_small_matches(rt)
            ):
                small_list_text = rt

    if name:
        # Drop a trailing center reference some major-body headers carry
        # (e.g. "Cert-2 … (spacecraft) / Sun" → "Cert-2 … (spacecraft)").
        name = re.split(r"\s+/\s+", name)[0].strip()
        is_small = "Rec #" in text or used_command.endswith(";")
        cmd = used_command
        if is_small and not cmd.endswith(";"):
            cmd += ";"  # lock to the small-body record for the ephemeris call
        # Only the actual Sun (id 10 / name "Sun"), not anything merely
        # containing the word — otherwise the safety guard fires spuriously.
        is_sun = query.strip().lower() in {"10", "sun"} or bool(re.match(r"(?i)^\s*sun\b", name))
        result = HorizonsResolution(
            resolved=True,
            command=cmd,
            name=name,
            kind="small" if is_small else "major",
            is_sun=is_sun,
        )
    elif "Multiple major-bodies match" in first:
        result = HorizonsResolution(resolved=False, candidates=_parse_major_matches(first))
    elif small_list_text is not None:
        result = HorizonsResolution(
            resolved=False, candidates=_parse_small_matches(small_list_text)
        )
    elif "Matching small-bodies" in first:
        result = HorizonsResolution(resolved=False, candidates=_parse_small_matches(first))
    else:
        result = HorizonsResolution(resolved=False, candidates=[])

    _cache_put(key, result, _LOOKUP_TTL)
    return result


# === Ephemeris parsing ========================================================

_DATE_FORMATS = ("%Y-%b-%d %H:%M:%S.%f", "%Y-%b-%d %H:%M:%S", "%Y-%b-%d %H:%M")


def _parse_date(s: str) -> datetime:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"unparseable Horizons date: {s!r}")


def _to_jd(dt: datetime) -> float:
    # UTC Julian Day — matches SensorKit's obstime.jd convention.
    return dt.timestamp() / 86400.0 + 2440587.5


def _to_float(s: str) -> float | None:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_ephemeris(text: str) -> list[HorizonsSample]:
    """Parse the CSV rows between $$SOE/$$EOE into samples."""
    block = re.search(r"\$\$SOE\s*(.*?)\s*\$\$EOE", text, re.DOTALL)
    if not block:
        return []
    samples: list[HorizonsSample] = []
    for line in block.group(1).splitlines():
        line = line.strip()
        if not line:
            continue
        fields = [f.strip() for f in line.split(",")]
        # RA is the first parseable float after the date + presence-flag columns.
        idx = next((i for i in range(1, len(fields)) if _to_float(fields[i]) is not None), None)
        if idx is None or idx + 3 >= len(fields):
            continue
        try:
            dt = _parse_date(fields[0])
        except ValueError:
            continue
        ra = _to_float(fields[idx])
        dec = _to_float(fields[idx + 1])
        if ra is None or dec is None:
            continue
        samples.append(
            HorizonsSample(
                jd=_to_jd(dt),
                utc=dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ra=ra,
                dec=dec,
                ra_rate_arcsec_hr=_to_float(fields[idx + 2]),
                dec_rate_arcsec_hr=_to_float(fields[idx + 3]),
                magnitude=_to_float(fields[idx + 4]) if idx + 4 < len(fields) else None,
                range_au=_to_float(fields[idx + 6]) if idx + 6 < len(fields) else None,
            )
        )
    return samples


def _fmt_time(dt: datetime) -> str:
    """Format a datetime as Horizons' expected 'YYYY-MM-DD HH:MM:SS' in UTC."""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


async def ephemeris(
    command: str,
    lon: float,
    lat: float,
    alt_km: float,
    start: datetime,
    stop: datetime,
    intervals: int = 60,
) -> list[HorizonsSample]:
    """Fetch a sampled Observer-Table ephemeris (RA/Dec ICRF + rates) for an object.

    Args:
        command: Resolved Horizons COMMAND (e.g. "499", "'433;'"). Use
            :func:`resolve` to obtain this.
        lon: Observer East longitude, degrees.
        lat: Observer latitude, degrees.
        alt_km: Observer altitude, kilometers.
        start: Window start (timezone-aware UTC).
        stop: Window stop (timezone-aware UTC).
        intervals: Number of equal sub-intervals (samples = intervals + 1).

    Returns:
        The parsed ephemeris samples.

    Raises:
        HorizonsError: on network failure, an ambiguous command, or unparseable output.
    """
    intervals = max(1, min(2000, intervals))
    text = await _horizons_get(
        {
            "COMMAND": _quote(command),
            "EPHEM_TYPE": "OBSERVER",
            "CENTER": "'coord@399'",
            "COORD_TYPE": "GEODETIC",
            "SITE_COORD": _quote(f"{lon},{lat},{alt_km}"),
            "START_TIME": _quote(_fmt_time(start)),
            "STOP_TIME": _quote(_fmt_time(stop)),
            "STEP_SIZE": str(intervals),  # bare integer = number of intervals
            "QUANTITIES": _quote(QUANTITIES),
            "ANG_FORMAT": "DEG",
            "CSV_FORMAT": "YES",
            "OBJ_DATA": "NO",
            "MAKE_EPHEM": "YES",
        }
    )

    if "$$SOE" not in text:
        # Ambiguous command, or no ephemeris produced — surface a useful error.
        if "Multiple major-bodies match" in text:
            raise HorizonsError("Ambiguous object; resolve via resolve() first")
        raise HorizonsError(f"No ephemeris generated: {text[:300].strip()}")

    samples = _parse_ephemeris(text)
    if not samples:
        raise HorizonsError("Could not parse Horizons ephemeris output")
    return samples


def samples_to_ephemeris_target(samples: list[HorizonsSample]) -> EphemerisTarget:
    """Build an ICRF :class:`EphemerisTarget` from parsed Horizons samples."""
    return EphemerisTarget(
        frame=ReferenceFrame.ICRF,
        jds=[s.jd for s in samples],
        points=[Equatorial(ra=s.ra, dec=s.dec) for s in samples],
    )


# === Window sizing ============================================================
# Ported from SensorView's src/features/skyview/horizons/window.ts: the ephemeris
# span is derived from the actual collect parameters (integration × frames + pad)
# and the sample cadence adapts to the object's sky-motion rate so a fast NEO is
# sampled finely enough to interpolate while a slow asteroid is not over-sampled.

# Rough per-frame readout/settle overhead beyond the pure integration time.
_PER_FRAME_OVERHEAD_S = 2.0
# Fixed slew/settle allowance before the first frame.
_SETTLE_PAD_S = 30.0
# Pads on each end so the ephemeris always covers execution despite queue/slew delay.
_START_PAD_S = 60.0
_END_PAD_S = 60.0

# Target on-sky motion between samples; smaller → denser sampling.
_MAX_ARCSEC_PER_STEP = 5.0
_MIN_INTERVALS = 4
_MAX_INTERVALS = 240  # keep Horizons output (and the request) modest


def estimate_collect_seconds(integration_sec: float, frame_count: int) -> float:
    """Estimated wall-clock duration of a collect, in seconds."""
    return frame_count * integration_sec + _PER_FRAME_OVERHEAD_S * frame_count + _SETTLE_PAD_S


def adaptive_intervals(window_sec: float, rate_arcsec_per_hr: float | None = None) -> int:
    """Choose an interval count so motion-per-step stays under the target threshold."""
    rate_arcsec_per_sec = abs(rate_arcsec_per_hr or 0.0) / 3600.0
    desired_step_sec = (
        _MAX_ARCSEC_PER_STEP / rate_arcsec_per_sec
        if rate_arcsec_per_sec > 0
        else window_sec / _MIN_INTERVALS
    )
    n = math.ceil(window_sec / max(1.0, desired_step_sec))
    return max(_MIN_INTERVALS, min(_MAX_INTERVALS, n))


@dataclass(frozen=True, slots=True)
class HorizonsWindow:
    """A Horizons ephemeris request window."""

    start: datetime
    stop: datetime
    intervals: int


def collect_window(
    integration_sec: float,
    frame_count: int,
    rate_arcsec_per_hr: float | None = None,
    now: datetime | None = None,
) -> HorizonsWindow:
    """Window that brackets the estimated collect, used when building the task."""
    now = now or datetime.now(UTC)
    est_collect_sec = estimate_collect_seconds(integration_sec, frame_count)
    start = now - timedelta(seconds=_START_PAD_S)
    duration_sec = _START_PAD_S + est_collect_sec + _END_PAD_S
    stop = start + timedelta(seconds=duration_sec)
    return HorizonsWindow(
        start=start,
        stop=stop,
        intervals=adaptive_intervals(duration_sec, rate_arcsec_per_hr),
    )
