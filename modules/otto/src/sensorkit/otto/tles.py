# SPDX-License-Identifier: Apache-2.0
"""TLE targets — Spacebook catalog fetch, propagation, and visibility.

Satellites are configured as NORAD IDs (`task.tles`) or orbit regimes
(`task.orbits`); both are served by one bulk fetch of the free Spacebook
catalog, with regime members classified from their mean motion. Positions come
from skyfield SGP4 propagation, aggressively cached so a visibility pass over
a large orbit-regime catalog stays cheap.

Each target source acquires its data its own way — here a bulk `fetch` of the
catalog — then speaks the shared vocabulary the program dispatches on:
`position` reports (altitude, azimuth, rising, hour_angle) for the
already-fetched entry, and `make_target` builds the collect target, a
`TLETarget` dithered by perturbing the orbital elements so the offset carries
through propagation.
"""
import math
import random
from datetime import UTC, datetime
from functools import lru_cache

import httpx
from loguru import logger
from pydantic import BaseModel, Field
from skyfield.api import EarthSatellite, wgs84

from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.otto.utils import hour_angle, time_at, timescale


class TLECache(BaseModel):
    """Cached TLE data."""

    tles: dict[str, dict[str, str]] = Field(default_factory=dict)
    dt: datetime


def classify_orbit(line2: str) -> str:
    """
    Classify a TLE into an orbit regime from its mean motion (revs/day).

    Boundaries match SensorView's catalog classification:
    LEO > 11.25 (period < ~128 min), MEO > 2.0, GEO within 0.99-1.01
    (~1 rev/day), HEO < 2.0, otherwise OTHER.
    """
    try:
        # Mean motion is in columns 53-63 of line 2
        mean_motion = float(line2[52:63])
    except (ValueError, IndexError):
        return "OTHER"

    if mean_motion > 11.25:
        return "LEO"
    elif mean_motion > 2.0:
        return "MEO"
    elif 0.99 < mean_motion < 1.01:
        return "GEO"
    elif mean_motion < 2.0:
        return "HEO"
    return "OTHER"


async def fetch(
        objects: list[str],
        orbits: list[str] | None = None,
        timeout: int = 30
) -> tuple[dict[str, dict[str, str]], int]:
    """
    Fetch TLEs from Spacebook by COMSPOC.

    Spacebook provides free TLE data without authentication.
    API endpoint: https://spacebook.com/api/entity/tle

    Args:
        objects: List of NORAD catalog IDs (e.g., ["25544", "25994"])
        orbits: Orbit regimes (e.g., ["LEO", "GEO"]); satellites classifying
            into any of these are included in addition to `objects`
        timeout: Request timeout in seconds

    Returns:
        Tuple of (tles_dict, status_code) where:
        - tles_dict: Dict of {norad_id: {name, line0, line1, line2}}
        - status_code: HTTP status code (200 for success, other for errors)
    """
    tles = {}
    status_code = 0
    orbits_set = set(orbits or [])

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            tle_url = "https://spacebook.com/api/entity/tle"

            response = await client.get(tle_url)
            status_code = response.status_code

            if response.status_code != 200:
                logger.error(f"Spacebook TLE fetch failed: {response.status_code}")
                return tles, status_code

            # Parse the TLE format response
            # Spacebook returns: LINE1\nLINE2\n for each satellite (2-line format)
            # NOTE: This returns ALL satellites, so we must filter for requested objects
            lines = response.text.strip().split('\n')

            # Convert requested objects to a set for O(1) lookup
            objects_set = set(objects)

            i = 0
            while i < len(lines):
                if i + 1 >= len(lines):
                    break

                line1 = lines[i].strip()
                line2 = lines[i + 1].strip()

                # Validate TLE format
                if not (line1.startswith('1 ') and line2.startswith('2 ')):
                    i += 1
                    continue

                # Extract NORAD ID from line 1 (columns 3-7)
                norad_id = line1[2:7].strip()

                # Only include requested satellites: by NORAD ID or by orbit regime
                if norad_id in objects_set or classify_orbit(line2) in orbits_set:
                    # Note: Spacebook TLE format doesn't have a line0, so we create one
                    line0 = f"0 {norad_id}"

                    tles[norad_id] = {
                        "line0": line0,
                        "line1": line1,
                        "line2": line2
                    }

                    # Optional: early exit if we've found all requested satellites
                    # (only valid without orbit regimes, which need a full catalog scan)
                    if not orbits_set and len(tles) == len(objects_set):
                        logger.debug(f"Found all {len(objects_set)} requested satellites, stopping parse")
                        break

                i += 2

            # Warn if any requested satellites were not found
            missing = objects_set - set(tles.keys())
            if missing:
                logger.warning(f"Could not find TLEs for {len(missing)} satellites: {sorted(missing)}")

    except httpx.TimeoutException:
        logger.error("Spacebook request timed out")
        status_code = 408  # Timeout status code
    except Exception as e:
        logger.exception(f"Error fetching TLEs from Spacebook: {e}")
        status_code = 500  # Generic error code

    return tles, status_code


@lru_cache(maxsize=32768)
def _satellite(line0: str, line1: str, line2: str) -> EarthSatellite:
    return EarthSatellite(line1, line2, line0.split('0')[1].strip(), timescale())


@lru_cache(maxsize=8)
def _observer(latitude: float, longitude: float, elevation: float):
    return wgs84.latlon(latitude, longitude, elevation)


def position(
        tle_data: dict[str, str],
        latitude: float,
        longitude: float,
        altitude_km: float,
) -> tuple[float, float, bool, float] | None:
    """
    Calculate a satellite's current position relative to an observer.

    The timescale, satellite, observer, and per-second time objects are cached
    so bulk passes over a large orbit-regime catalog stay cheap.

    Args:
        tle_data: The satellite's fetched TLE entry (line0/line1/line2)
        latitude: Observer latitude in degrees
        longitude: Observer East longitude in degrees
        altitude_km: Observer altitude in kilometers

    Returns:
        Tuple of (altitude, azimuth, rising, hour_angle), or None if the TLE
        fails to propagate
        - altitude: Current altitude in degrees
        - azimuth: Current azimuth in degrees
        - rising: True if satellite altitude is increasing
        - hour_angle: Hours; negative = east of meridian, positive = west of meridian
    """
    try:
        satellite = _satellite(tle_data["line0"], tle_data["line1"], tle_data["line2"])
        observer = _observer(latitude, longitude, altitude_km * 1000)

        # Current time, quantized to the second so bulk passes share Time objects
        now = datetime.now(UTC)
        time = time_at(int(now.timestamp()))

        # Calculate current position
        difference = satellite - observer
        topocentric = difference.at(time)
        alt, az, distance = topocentric.altaz()

        # Hour angle from the satellite's topocentric right ascension
        ra, _dec, _ = topocentric.radec()
        ha = hour_angle(ra.hours * 15.0, longitude, now)

        # Position one minute in the future to determine if rising/falling
        future_topocentric = difference.at(time_at(int(now.timestamp()) + 60))
        future_alt, _, _ = future_topocentric.altaz()

        # skyfield values are numpy-typed; cast so callers see plain Python types
        return (
            float(alt.degrees),
            float(az.degrees),
            bool(future_alt.degrees > alt.degrees),
            ha,
        )

    except Exception as e:
        logger.exception(f"Error calculating satellite position for {tle_data.get('line0')}: {e}")
        return None


def dither_tle(
        tle: TLE,
        dither_arcsec: float,
        latitude: float,
        longitude: float,
        altitude_km: float,
) -> TLE:
    """Create a dithered copy of a TLE by perturbing inclination and RAAN.

    Computes the observer-satellite range to scale the orbital element
    perturbation so that the resulting on-sky offset matches the requested
    arcsecond value.

    Args:
        tle: Original TLE to dither.
        dither_arcsec: Maximum dither magnitude in arcseconds. The actual
            offset is drawn uniformly from [0, dither_arcsec].
        latitude: Observer latitude in degrees.
        longitude: Observer longitude in degrees.
        altitude_km: Observer altitude in kilometers.

    Returns:
        A new TLE with perturbed orbital elements.
    """
    import satkit  # heavy compiled dep, only needed when dithering

    # Parse the TLE
    lines = []
    if tle.line0:
        lines.append(tle.line0)
    lines.extend([tle.line1, tle.line2])
    sk_tle = satkit.TLE.from_lines(lines)

    # Compute semi-major axis from mean motion (revs/day -> rad/s)
    mu = 3.986004418e14  # Earth GM, m^3/s^2
    n_rad_s = sk_tle.mean_motion * 2 * math.pi / 86400.0
    a = (mu / n_rad_s**2) ** (1 / 3)  # meters

    # Compute the observer-satellite range via the module's cached helpers
    satellite = EarthSatellite(tle.line1, tle.line2, "dither", timescale())
    observer = _observer(latitude, longitude, altitude_km * 1000)
    now = time_at(int(datetime.now(UTC).timestamp()))
    _, _, dist = (satellite - observer).at(now).altaz()
    range_m = dist.m

    # Random magnitude (uniform on [0, dither_arcsec]) and direction
    r = random.uniform(0, dither_arcsec)
    angle = random.uniform(0, 2 * math.pi)
    dither_rad = math.radians(r / 3600.0)

    # Scale element perturbation so on-sky offset matches the desired amount:
    #   on_sky = (a * delta_element) / range  =>  delta_element = on_sky * range / a
    scale = range_m / a
    delta_i = dither_rad * scale * math.cos(angle)
    sin_i = math.sin(math.radians(sk_tle.inclination))
    delta_raan = dither_rad * scale * math.sin(angle) / sin_i if abs(sin_i) > 0.05 else 0.0

    sk_tle.inclination += math.degrees(delta_i)
    sk_tle.raan += math.degrees(delta_raan)

    out = sk_tle.to_3line()

    logger.debug(f"dithered TLE by {r:.1f} arcsec at angle {math.degrees(angle):.0f}° (range={range_m/1000:.0f} km, scale={scale:.2f})")
    return TLE(line0=out[0], line1=out[1], line2=out[2])


def make_target(
    tle_data: dict[str, str],
    *,
    dither_arcsec: float = 0.0,
    latitude: float,
    longitude: float,
    altitude_km: float,
) -> TLETarget:
    """Build the collect target for a satellite: its TLE, dithered if requested.

    Unlike the fixed-coordinate sources, dithering here perturbs the orbital
    elements (see `dither_tle`) so the offset carries through propagation.
    """
    tle = TLE(line0=tle_data["line0"], line1=tle_data["line1"], line2=tle_data["line2"])
    if dither_arcsec > 0:
        tle = dither_tle(
            tle, dither_arcsec, latitude=latitude, longitude=longitude, altitude_km=altitude_km
        )
    return TLETarget(tle=tle)
