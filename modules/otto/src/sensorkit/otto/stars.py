# SPDX-License-Identifier: Apache-2.0
"""Star targets — name resolution and visibility.

Star names are resolved through the CDS Sesame service (SIMBAD/NED/VizieR) via
astropy's `SkyCoord.from_name`, which accepts proper names (`Vega`), Bright Star
Catalogue numbers (`HR 7001`), Hipparcos numbers (`HIP 91262`), Bayer
designations (`alf Lyr`), and deep-sky identifiers (`M31`) — a superset of the
BSC5 and Hipparcos catalogs SensorView bundles, without shipping catalog data.

A star's position is static, so a name is resolved once and cached; there is no
refresh cycle and nothing goes stale.

Each target source acquires its data its own way — here a once-ever per-name
`resolve` — then speaks the shared vocabulary the program dispatches on:
`position` reports (altitude, azimuth, rising, hour_angle) in closed form (no
propagation needed for a fixed target), and `make_target` builds the collect
target, a fixed ICRF `ICRSTarget` the mount tracks sidereally.
"""
from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime

from astropy.coordinates import SkyCoord
from loguru import logger
from pydantic import BaseModel, Field

from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import ICRSTarget
from sensorkit.otto.utils import dither_offset, hour_angle, normalize_degrees


class StarCache(BaseModel):
    """Resolved star coordinates, keyed by configured name.

    Positions are fixed, so this never goes stale — it only saves re-querying
    the name resolver after a restart.
    """

    stars: dict[str, Equatorial] = Field(default_factory=dict)
    dt: datetime


async def resolve(name: str) -> Equatorial | None:
    """Resolve a star name to fixed ICRF coordinates.

    Args:
        name: Star name, catalog designation, or deep-sky identifier.

    Returns:
        The resolved coordinates, or None if the name is unknown or the
        resolver is unreachable — the reason is logged either way. A caller
        retrying later distinguishes the two: an unknown name never resolves.
    """

    def lookup() -> Equatorial:
        # from_name is blocking network I/O; keep it off the event loop
        coord = SkyCoord.from_name(name.strip())
        return Equatorial(ra=float(coord.ra.deg), dec=float(coord.dec.deg))

    try:
        coords = await asyncio.to_thread(lookup)
    except Exception as e:
        logger.warning(f"Could not resolve star {name!r}: {e}")
        return None

    logger.info(f"resolved star {name!r} -> ra={coords.ra:.5f} dec={coords.dec:.5f}")
    return coords


def position(
    coords: Equatorial,
    latitude: float,
    longitude: float,
    now: datetime | None = None,
) -> tuple[float, float, bool, float]:
    """Current position of a fixed ICRF target relative to an observer.

    Mirrors the other sources' `position` contract, but it always succeeds:
    a star has a position at every instant, so there is no None case.

    Deliberately coarse. The hour angle is exact (it agrees with astropy's
    apparent sidereal time to well under a second), but the altitude and azimuth
    pair J2000 coordinates with apparent sidereal time, ignoring precession,
    nutation, and aberration — so they run ~0.3 degrees off a rigorous ICRS to
    AltAz transform, a gap that grows slowly with epoch as precession
    accumulates. That is immaterial here: these values only gate the
    `altitude_min` floor and order a scan by hour angle. Pointing is unaffected,
    since the mount receives the exact ICRS coordinates and transforms them
    itself. Doing it this way keeps a per-object visibility check cheap and
    avoids astropy's IERS auto-download inside a long-lived service.

    Args:
        coords: The star's fixed ICRF coordinates, in degrees.
        latitude: Observer latitude in degrees.
        longitude: Observer East longitude in degrees.
        now: Evaluation time, defaulting to the current UTC time.

    Returns:
        Tuple of (altitude, azimuth, rising, hour_angle)
        - altitude: Current altitude in degrees
        - azimuth: Current azimuth in degrees, from north through east
        - rising: True while the star is climbing toward transit
        - hour_angle: Hours; negative = east of meridian, positive = west
    """
    now = now or datetime.now(UTC)
    ha = hour_angle(coords.ra, longitude, now)

    h = math.radians(ha * 15.0)
    dec = math.radians(coords.dec)
    lat = math.radians(latitude)

    sin_altitude = math.sin(dec) * math.sin(lat) + math.cos(dec) * math.cos(lat) * math.cos(h)
    altitude = math.degrees(math.asin(max(-1.0, min(1.0, sin_altitude))))
    azimuth = normalize_degrees(
        math.degrees(
            math.atan2(
                -math.cos(dec) * math.sin(h),
                math.sin(dec) * math.cos(lat) - math.cos(dec) * math.sin(lat) * math.cos(h),
            )
        )
    )

    # A fixed target climbs until it transits, so east of the meridian is rising
    return altitude, azimuth, ha < 0, ha


def make_target(coords: Equatorial, *, dither_arcsec: float = 0.0) -> ICRSTarget:
    """Build the collect target for a star: its fixed ICRF position, dithered if requested.

    No window and no propagation — the mount tracks the position sidereally for
    as long as the block runs.
    """
    ra, dec = coords.ra, coords.dec
    if dither_arcsec > 0:
        delta_ra, delta_dec = dither_offset(dec, dither_arcsec)
        ra, dec = normalize_degrees(ra + delta_ra), dec + delta_dec
    return ICRSTarget(coords=Equatorial(ra=ra, dec=dec))
