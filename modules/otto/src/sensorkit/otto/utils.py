# SPDX-License-Identifier: Apache-2.0
from datetime import UTC, datetime
from enum import Enum
from functools import lru_cache
from typing import Dict, List, Tuple

import httpx
from loguru import logger
from skyfield.api import EarthSatellite, load, wgs84


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


async def fetch_tles(
        objects: List[str],
        orbits: List[str] | None = None,
        timeout: int = 30
) -> Tuple[Dict[str, Dict[str, str]], int]:
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
        logger.error("space-track.org request timed out")
        status_code = 408  # Timeout status code
    except Exception as e:
        logger.exception(f"Error fetching TLEs from space-track.org: {e}")
        status_code = 500  # Generic error code

    return tles, status_code


@lru_cache
def _timescale():
    return load.timescale()


@lru_cache(maxsize=16)
def _time_at(unix_seconds: int):
    """Skyfield Time for a unix second, shared so its astrometric caches amortize."""
    return _timescale().from_datetime(datetime.fromtimestamp(unix_seconds, UTC))


@lru_cache(maxsize=32768)
def _satellite(line0: str, line1: str, line2: str) -> EarthSatellite:
    return EarthSatellite(line1, line2, line0.split('0')[1].strip(), _timescale())


@lru_cache(maxsize=8)
def _observer(latitude: float, longitude: float, elevation: float):
    return wgs84.latlon(latitude, longitude, elevation)


def calculate_satellite_position(
        tles: dict[str, dict[str, str]],
        object: str,
        latitude: float,
        longitude: float,
        elevation: float,
) -> tuple[float, float, bool, float] | None:
    """
    Calculate a satellite's current position relative to an observer.

    The timescale, satellite, observer, and per-second time objects are cached
    so bulk passes over a large orbit-regime catalog stay cheap.

    Args:
        tles: Dictionary of TLEs keyed by NORAD ID
        object: NORAD ID
        latitude: Observer latitude in degrees
        longitude: Observer longitude in degrees
        elevation: Observer elevation in meters

    Returns:
        Tuple of (altitude, azimuth, is_rising, hour_angle) or None if no TLE found
        - altitude: Current altitude in degrees
        - azimuth: Current azimuth in degrees
        - rising: True if satellite altitude is increasing
        - hour_angle: Hours; negative = east of meridian, positive = west of meridian
    """
    # Check if we have TLE for this object
    if object not in tles:
        return None

    tle_data = tles[object]

    try:
        satellite = _satellite(tle_data["line0"], tle_data["line1"], tle_data["line2"])
        observer = _observer(latitude, longitude, elevation)

        # Current time, quantized to the second so bulk passes share Time objects
        now_seconds = int(datetime.now(UTC).timestamp())
        now = _time_at(now_seconds)

        # Calculate current position
        difference = satellite - observer
        topocentric = difference.at(now)
        alt, az, distance = topocentric.altaz()

        altitude = alt.degrees
        azimuth = az.degrees

        # Hour angle: HA = LST - RA, wrapped to [-12, 12). Negative = east of
        # meridian, positive = west.
        ra, _dec, _ = topocentric.radec()
        last_hours = (now.gast + longitude / 15.0) % 24.0
        hour_angle = ((last_hours - ra.hours + 12.0) % 24.0) - 12.0

        # Position one minute in the future to determine if rising/falling
        future_topocentric = difference.at(_time_at(now_seconds + 60))
        future_alt, _, _ = future_topocentric.altaz()

        # Determine if rising or falling
        rising = future_alt.degrees > altitude

        return altitude, azimuth, rising, hour_angle

    except Exception as e:
        logger.exception(f"Error calculating satellite position for {object}: {e}")
        return None


def dither_tle(
        tle: "TLE",
        dither_arcsec: float,
        latitude: float,
        longitude: float,
        elevation: float,
) -> "TLE":
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
        elevation: Observer elevation in meters.

    Returns:
        A new TLE with perturbed orbital elements.
    """
    import math
    import random as _random

    import satkit

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

    # Compute observer-satellite range via Skyfield
    ts = _timescale()
    satellite = EarthSatellite(tle.line1, tle.line2, "dither", ts)
    observer = wgs84.latlon(latitude, longitude, elevation)
    now = ts.from_datetime(datetime.now(UTC))
    _, _, dist = (satellite - observer).at(now).altaz()
    range_m = dist.m

    # Random magnitude (uniform on [0, dither_arcsec]) and direction
    r = _random.uniform(0, dither_arcsec)
    angle = _random.uniform(0, 2 * math.pi)
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

    from sensorkit.astro.common import TLE as _TLE
    logger.debug(f"dithered TLE by {r:.1f} arcsec at angle {math.degrees(angle):.0f}° (range={range_m/1000:.0f} km, scale={scale:.2f})")
    return _TLE(line0=out[0], line1=out[1], line2=out[2])


class ListType(Enum):
    """Enum for object list types."""
    WHITELIST = "whitelist"
    GRAYLIST = "graylist"
    BLACKLIST = "blacklist"


class ObjectListManager:
    def __init__(self, state, save_callback):
        self.state = state
        self._save_callback = save_callback

    def _get_list(self, list_type: ListType) -> List[str]:
        return getattr(self.state, list_type.value)

    async def move_object(self, obj: str, from_list: ListType, to_list: ListType) -> bool:
        """
        Move an object from one list to another.

        Args:
            obj: NORAD ID to move
            from_list: Source list type
            to_list: Destination list type

        Returns:
            True if successful, False otherwise
        """
        source = self._get_list(from_list)
        dest = self._get_list(to_list)

        # Remove from source
        source.remove(obj)

        # Add to destination if not already there
        if obj not in dest:
            dest.append(obj)

        # Persist changes
        await self._save_callback()

        logger.debug(f"moved {obj} from {from_list.value} to {to_list.value}")
        return True


