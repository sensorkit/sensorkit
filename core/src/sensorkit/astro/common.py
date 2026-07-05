# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto
from typing import TYPE_CHECKING

from pydantic import BaseModel

from sensorkit.common.keyword import declare_keyword

if TYPE_CHECKING:
    from sensorkit.astro.coords import Equatorial, Geodetic, Horizontal


class ReferenceFrame(StrEnum):
    """Supported celestial/terrestrial reference frames."""
    ALTAZ = auto()
    CIRF = auto()
    GCRF = auto()
    ICRF = auto()
    ITRF = auto()
    TEME = auto()

    def to_astropy(self):
        """Return the corresponding astropy coordinate frame class."""
        import astropy.coordinates as ac

        match self:
            case ReferenceFrame.CIRF:
                return ac.CIRS
            case ReferenceFrame.GCRF:
                return ac.GCRS
            case ReferenceFrame.ICRF:
                return ac.ICRS
            case ReferenceFrame.ITRF:
                return ac.ITRS
            case _:
                return ac.frame_transform_graph.lookup_name(self.value)


@dataclass(frozen=True, slots=True)
class TLE:
    """A Two-Line Element set, optionally with a name line (line0)."""
    line0: str | None
    line1: str
    line2: str

    def to_list(self):
        """Return the TLE as a list of strings, omitting line0 if it is None."""
        if self.line0 is not None:
            return [self.line0, self.line1, self.line2]
        else:
            return [self.line1, self.line2]

    @property
    def norad_id(self):
        raw = self.line1[2:7].strip()

        return (
            str((ord(raw[0].upper()) - ord("A") + 10) * 10000 + int(raw[1:]))
            if raw and raw[0].isalpha()
            else str(int(raw))
        )


@declare_keyword
class SitePosition(BaseModel):
    """Geographic location of the observing site in degrees and km."""
    latitude_degrees: float
    longitude_degrees: float
    altitude_km: float

    @classmethod
    def from_coords(cls, coords: Geodetic):
        """Create a SitePosition from a Geodetic coordinate object."""
        return cls(
            latitude_degrees=coords.lat,
            longitude_degrees=coords.lon,
            altitude_km=coords.elev / 1000,
        )

    def to_coords(self):
        """Convert to a Geodetic coordinate object."""
        from sensorkit.astro.coords import Geodetic
        return Geodetic(
            lon=self.longitude_degrees,
            lat=self.latitude_degrees,
            elev=self.altitude_km * 1000,
        )


@declare_keyword
class AltAzPointing(BaseModel):
    """Current telescope pointing in horizontal (altitude/azimuth) coordinates."""
    altitude_degrees: float
    azimuth_degrees: float

    @classmethod
    def from_coords(cls, coords: Horizontal):
        """Create an AltAzPointing from a Horizontal coordinate object."""
        return cls(altitude_degrees=coords.alt, azimuth_degrees=coords.az)


@declare_keyword
class RADecPointing(BaseModel):
    """Current telescope pointing in equatorial (RA/Dec) coordinates."""
    right_ascension_hours: float
    declination_degrees: float
    reference_frame: ReferenceFrame = ReferenceFrame.ICRF

    @classmethod
    def from_coords(cls, coords: Equatorial):
        """Create an RADecPointing from an Equatorial coordinate object."""
        return cls(right_ascension_hours=coords.ra, declination_degrees=coords.dec)

    def to_coords(self):
        """Convert to an Equatorial coordinate object."""
        from sensorkit.astro.coords import Equatorial
        return Equatorial(ra=self.right_ascension_hours * 15, dec=self.declination_degrees)

    @property
    def ra_hms(self):
        """Right ascension formatted as hours, minutes, seconds."""
        return self.to_coords().ra_hms

    @property
    def dec_dms(self):
        """Declination formatted as degrees, minutes, seconds."""
        return self.to_coords().dec_dms
