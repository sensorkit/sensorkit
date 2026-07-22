# SPDX-License-Identifier: Apache-2.0
"""Shared machinery for Otto's target sources.

The math every source leans on lives here — time scales, hour angles, on-sky
dither offsets — together with the whitelist/graylist/blacklist bookkeeping.
Anything specific to one target source belongs in its own module (`tles.py`,
`horizons.py`, `stars.py`), not here.
"""
import math
import random
from datetime import UTC, datetime
from enum import Enum
from functools import lru_cache
from typing import List

from loguru import logger
from skyfield.api import load


@lru_cache
def timescale():
    return load.timescale()


@lru_cache(maxsize=16)
def time_at(unix_seconds: int):
    """Skyfield Time for a unix second, shared so its astrometric caches amortize."""
    return timescale().from_datetime(datetime.fromtimestamp(unix_seconds, UTC))


def to_jd(dt: datetime) -> float:
    """UTC Julian Day for a datetime, matching SensorKit's obstime.jd convention."""
    return dt.timestamp() / 86400.0 + 2440587.5


def hour_angle(ra_deg: float, longitude: float, now: datetime) -> float:
    """Hour angle of a right ascension: HA = LST - RA, in hours, wrapped to [-12, 12).

    Negative = east of the meridian (approaching transit), positive = west.
    The scan ordering in every target source keys off this.

    Args:
        ra_deg: Right ascension in degrees.
        longitude: Observer East longitude in degrees.
        now: Evaluation time (UTC). Quantized to the second so bulk passes
            share cached skyfield Time objects.
    """
    # gast is numpy-typed; cast once so callers see plain Python floats
    gast = float(time_at(int(now.timestamp())).gast)
    lst = (gast + longitude / 15.0) % 24.0
    return ((lst - ra_deg / 15.0 + 12.0) % 24.0) - 12.0


def normalize_degrees(angle: float) -> float:
    """Wrap an angle into [0, 360).

    Guards the floating-point corner where a tiny negative value wraps to
    exactly 360.0, which `% 360.0` alone does not exclude.
    """
    wrapped = angle % 360.0
    return 0.0 if wrapped >= 360.0 else wrapped


def interpolate_angle(start: float, end: float, fraction: float) -> float:
    """Interpolate between two angles in degrees, taking the shorter way around.

    Plain linear interpolation swings the long way whenever a pair straddles
    0/360 — an azimuth stepping 359° -> 1° would read as 180°.

    Args:
        start: Angle at fraction 0, in degrees.
        end: Angle at fraction 1, in degrees.
        fraction: Position between the two, normally in [0, 1].

    Returns:
        The interpolated angle, wrapped into [0, 360).
    """
    delta = (end - start + 180.0) % 360.0 - 180.0
    return normalize_degrees(start + fraction * delta)


def dither_offset(dec: float, dither_arcsec: float) -> tuple[float, float]:
    """Random on-sky offset for a pointing, in degrees.

    The magnitude is drawn uniformly from [0, dither_arcsec] at a random
    position angle. The RA component is de-projected by cos(dec) so the
    resulting on-sky offset matches the requested arcsecond value.

    Args:
        dec: Reference declination in degrees.
        dither_arcsec: Maximum on-sky offset in arcseconds.

    Returns:
        Tuple of (delta_ra_degrees, delta_dec_degrees).
    """
    magnitude = random.uniform(0, dither_arcsec) / 3600.0
    angle = random.uniform(0, 2 * math.pi)
    cos_dec = max(math.cos(math.radians(dec)), 1e-3)  # guard near the poles
    delta_ra = magnitude * math.cos(angle) / cos_dec
    delta_dec = magnitude * math.sin(angle)
    logger.debug(
        f"dither offset {magnitude * 3600:.1f} arcsec at angle {math.degrees(angle):.0f}°"
    )
    return delta_ra, delta_dec


def sidereal_frames(track_mode: str, num_frames: int) -> list[int]:
    """Frame numbers to collect under sidereal tracking for a track_mode.

    rate          -> none (follow the target for every frame)
    rate_sidereal -> final frame only (star-pinned astrometric anchor)
    sidereal      -> every frame (pin the stars at acquisition; the object
                     streaks and is not re-acquired between frames)
    """
    match track_mode:
        case "sidereal":
            return list(range(num_frames))
        case "rate_sidereal":
            return [num_frames - 1]
        case _:
            return []


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

    async def move_object(self, obj: str, from_list: ListType, to_list: ListType) -> None:
        """
        Move an object from one list to another.

        Args:
            obj: Object name to move
            from_list: Source list type
            to_list: Destination list type

        Raises:
            ValueError: If the object is not on `from_list`.
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
