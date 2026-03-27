import asyncio
import functools
from datetime import datetime
from typing import ClassVar, cast

from astropy.coordinates import EarthLocation
from astropy.units import Unit
from skyfield import almanac as almanac
from skyfield import api as skyfield
from skyfield.jpllib import SpiceKernel

DEG = Unit("deg")
METERS = Unit("m")


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
                EarthObserver.timescale = await asyncio.to_thread(skyfield.load.timescale)
                EarthObserver.ephem = await asyncio.to_thread(skyfield.load, "de421.bsp")

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
        return EarthLocation(
            lon=self.topos.longitude.degrees * DEG,
            lat=self.topos.latitude.degrees * DEG,
            height=self.topos.elevation.m * METERS,
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
