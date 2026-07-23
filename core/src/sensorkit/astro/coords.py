# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import numpy as np

from sensorkit.astro.common import ReferenceFrame

if TYPE_CHECKING:
    from astropy.coordinates import SkyCoord
    from astropy.units import Unit


@functools.lru_cache
def astropy_unit(spec: str | Unit) -> Unit:
    """Resolve a unit spec to an astropy Unit, memoizing the parse.

    Passing an already-resolved Unit is a cheap identity check; string specs are
    parsed once and cached, which matters for compound units like "m/s" whose
    grammar parse is otherwise paid on every call.
    """
    from astropy.units import Unit

    return Unit(spec)


type Coordinates = Horizontal | Equatorial | Geodetic | Cartesian


@dataclass(frozen=True, slots=True)
class Horizontal:
    """Horizontal coordinates."""
    az: float
    alt: float

    def to_astropy(
        self,
        units: str | Unit = "deg",
        **kwargs,
    ):
        """Convert to an astropy SkyCoord in the AltAz frame."""
        from astropy import coordinates as ac

        units = astropy_unit(units)
        return ac.SkyCoord(
            az=self.az * units,
            alt=self.alt * units,
            frame=ReferenceFrame.ALTAZ,
            **kwargs,
        )


@dataclass(frozen=True, slots=True)
class Equatorial:
    """Equatorial coordinates."""
    ra: float
    dec: float

    def to_astropy(
        self,
        units: str | Unit = "deg",
        ra_units: str | Unit | None = None,
        frame: ReferenceFrame = ReferenceFrame.ICRF,
        **kwargs,
    ):
        """Convert to an astropy SkyCoord in the given equatorial frame."""
        from astropy import coordinates as ac

        dec_unit = astropy_unit(units)
        ra_unit = astropy_unit(ra_units) if ra_units is not None else dec_unit
        return ac.SkyCoord(
            ra=self.ra * ra_unit,
            dec=self.dec * dec_unit,
            frame=frame.to_astropy(),
            **kwargs,
        )

    @property
    def ra_hms(self):
        """Right ascension formatted as a `"H M S"` string."""
        ra = self.ra / 15
        hr = int(ra)
        fractional = abs(ra - hr)
        min = int(fractional * 60)
        sec = (fractional * 60 - min) * 60
        return f"{hr} {min} {sec}"

    @property
    def dec_dms(self):
        """Declination formatted as a `"D M S"` string."""
        deg = int(self.dec)
        fractional = abs(self.dec - deg)
        arcmin = int(fractional * 60)
        arcsec = (fractional * 60 - arcmin) * 60
        return f"{deg} {arcmin} {arcsec}"


@dataclass(frozen=True, slots=True)
class Geodetic:
    """Geodetic coordinates."""
    lon: float
    lat: float
    elev: float

    @functools.cache
    def to_astropy(self, angle_units: str | Unit = "deg", distance_units: str | Unit = "m"):
        """Convert to an astropy EarthLocation (result is cached)."""
        from astropy import coordinates as ac

        angle = astropy_unit(angle_units)
        distance = astropy_unit(distance_units)
        return ac.EarthLocation(
            lon=self.lon * angle,
            lat=self.lat * angle,
            height=self.elev * distance,
        )


@dataclass(frozen=True, slots=True)
class Cartesian:
    """A 3-D Cartesian vector (x, y, z)."""
    x: float
    y: float
    z: float

    def __mul__(self, other):
        match other:
            case float() | int():
                return Cartesian(self.x * other, self.y * other, self.z * other)
            case Cartesian():
                return Cartesian(self.x * other.x, self.y * other.y, self.z * other.z)
            case _:
                raise RuntimeError(f"cannot multiply Cartesian by {type(other)}")


@dataclass(frozen=True, slots=True)
class StateVector:
    """Representation of a state vector."""
    t: datetime
    r: Cartesian
    v: Cartesian

    def to_numpy(self):
        """Return a 6-element numpy array `[rx, ry, rz, vx, vy, vz]`."""
        return np.array([self.r.x, self.r.y, self.r.z, self.v.x, self.v.y, self.v.z])

    @classmethod
    def from_astropy(
        cls,
        coord: SkyCoord,
        position_units: str | Unit = "m",
        velocity_units: str | Unit = "m/s",
    ):
        """Construct a StateVector from an astropy SkyCoord with Cartesian position and velocity."""
        pos = astropy_unit(position_units)
        vel = astropy_unit(velocity_units)
        r = coord.cartesian
        v = coord.velocity
        return cls(
            coord.obstime.to_datetime(UTC),
            Cartesian(
                r.x.to_value(pos),
                r.y.to_value(pos),
                r.z.to_value(pos),
            ),
            Cartesian(
                v.d_x.to_value(vel),
                v.d_y.to_value(vel),
                v.d_z.to_value(vel),
            ),
        )

    def to_astropy(
        self,
        position_units: str | Unit = "m",
        velocity_units: str | Unit = "m/s",
        frame: ReferenceFrame = ReferenceFrame.ICRF,
    ):
        """Convert to an astropy SkyCoord with Cartesian representation and differential."""
        from astropy import coordinates as ac

        pos = astropy_unit(position_units)
        vel = astropy_unit(velocity_units)
        return ac.SkyCoord(
            x=self.r.x * pos,
            y=self.r.y * pos,
            z=self.r.z * pos,
            v_x=self.v.x * vel,
            v_y=self.v.y * vel,
            v_z=self.v.z * vel,
            frame=frame.to_astropy(),
            representation_type="cartesian",
            differential_type="cartesian",
            obstime=self.t,
        )
