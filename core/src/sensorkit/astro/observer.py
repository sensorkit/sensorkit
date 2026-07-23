# SPDX-License-Identifier: Apache-2.0
import asyncio
import functools
from datetime import datetime
from typing import ClassVar, cast

from astropy.coordinates import EarthLocation
from skyfield import almanac as almanac
from skyfield import api as skyfield
from skyfield.jpllib import SpiceKernel

from sensorkit.astro.coords import astropy_unit


# TODO: Present incarnation is largely driven by Agent configuration usage. This may change
#       substantially or be entirely replaced.
class EarthObserver:
    """High-level topocentric observer backed by skyfield."""

    timescale: ClassVar[skyfield.Timescale | None] = None
    ephem: ClassVar[SpiceKernel | None] = None
    _bootstrap_lock: ClassVar[asyncio.Lock] = asyncio.Lock()

    @staticmethod
    async def bootstrap():
        """Load the skyfield timescale and ephemeris data if not already loaded."""
        async with EarthObserver._bootstrap_lock:
            if EarthObserver.timescale is None:
                # Load both before publishing either. `timescale` is the gate variable
                # checked by get(); assign it LAST so "timescale set" always implies
                # "ephem loaded". Otherwise a concurrent get() can observe timescale set
                # while the slow de421.bsp load is still in flight and construct with
                # ephem=None. The two assignments have no await between them, so no other
                # task can interleave (this also makes bootstrap cancellation-atomic).
                timescale = await asyncio.to_thread(skyfield.load.timescale)
                ephem = await asyncio.to_thread(skyfield.load, "de421.bsp")
                EarthObserver.ephem = ephem
                EarthObserver.timescale = timescale

    @classmethod
    async def get(cls, *args, **kwargs):
        """Bootstrap skyfield and return a new EarthObserver instance."""
        if cls.timescale is None:
            await cls.bootstrap()
        return cls(*args, **kwargs)

    def __init__(self, lat_deg: float, lon_deg: float, elev_m: float = 0.0):
        self.topos = skyfield.Topos(
            latitude_degrees=lat_deg,
            longitude_degrees=lon_deg,
            elevation_m=elev_m,
        )
        self.observer = self.ephem["earth"] + self.topos

    @functools.cache
    def to_astropy(self):
        """Return an astropy EarthLocation for this observer (cached)."""
        deg = astropy_unit("deg")
        return EarthLocation(
            lon=self.topos.longitude.degrees * deg,
            lat=self.topos.latitude.degrees * deg,
            height=self.topos.elevation.m * astropy_unit("m"),
        )

    def get_sunrise_times(self, from_time: datetime, to_time: datetime):
        """Return all sunrise times between *from_time* and *to_time*."""
        return tuple(
            cast(datetime, time.utc_datetime())
            for time in almanac.find_risings(
                self.observer,
                self.ephem["sun"],
                self.timescale.from_datetime(from_time),
                self.timescale.from_datetime(to_time),
            )[0]
        )

    def get_sunrise_time(self, from_time: datetime, to_time: datetime, latest: bool = False):
        """Return the first (or last if *latest*) sunrise time in the window, or None."""
        times = self.get_sunrise_times(from_time, to_time)
        return times[-1 if latest else 0] if times else None

    def get_sunset_times(self, from_time: datetime, to_time: datetime):
        """Return all sunset times between *from_time* and *to_time*."""
        return tuple(
            cast(datetime, time.utc_datetime())
            for time in almanac.find_settings(
                self.observer,
                self.ephem["sun"],
                self.timescale.from_datetime(from_time),
                self.timescale.from_datetime(to_time),
            )[0]
        )

    def get_sunset_time(self, from_time: datetime, to_time: datetime, latest: bool = False):
        """Return the first (or last if *latest*) sunset time in the window, or None."""
        times = self.get_sunset_times(from_time, to_time)
        return times[-1 if latest else 0] if times else None
