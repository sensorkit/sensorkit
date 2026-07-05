# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
from astropy import coordinates as ac
from astropy.units import Unit

from sensorkit.astro.common import ReferenceFrame

DEG = Unit("deg")
KM = Unit("km")
METERS = Unit("m")
SEC = Unit("s")


type Coordinates = Horizontal | Equatorial | Geodetic | Cartesian


@dataclass(frozen=True, slots=True)
class Horizontal:
    """Horizontal coordinates."""
    az: float
    alt: float

    def to_astropy(
        self,
        units: Unit = DEG,
        **kwargs,
    ):
        """Convert to an astropy SkyCoord in the AltAz frame."""
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
        units: Unit = DEG,
        ra_units: Unit | None = None,
        frame: ReferenceFrame = ReferenceFrame.ICRF,
        **kwargs,
    ):
        """Convert to an astropy SkyCoord in the given equatorial frame."""
        return ac.SkyCoord(
            ra=self.ra * (ra_units if ra_units is not None else units),
            dec=self.dec * units,
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
    def to_astropy(self, angle_units: Unit = DEG, distance_units: Unit = METERS):
        """Convert to an astropy EarthLocation (result is cached)."""
        return ac.EarthLocation(
            lon=self.lon * angle_units,
            lat=self.lat * angle_units,
            height=self.elev * distance_units,
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
        coord: ac.SkyCoord,
        position_units: Unit = METERS,
        velocity_units: Unit = METERS / SEC,
    ):
        """Construct a StateVector from an astropy SkyCoord with Cartesian position and velocity."""
        r = coord.cartesian
        v = coord.velocity
        return cls(
            coord.obstime.to_datetime(UTC),
            Cartesian(
                r.x.to_value(position_units),
                r.y.to_value(position_units),
                r.z.to_value(position_units),
            ),
            Cartesian(
                v.d_x.to_value(velocity_units),
                v.d_y.to_value(velocity_units),
                v.d_z.to_value(velocity_units),
            ),
        )

    def to_astropy(
        self,
        position_units: Unit = METERS,
        velocity_units: Unit = METERS / SEC,
        frame: ReferenceFrame = ReferenceFrame.ICRF,
    ):
        """Convert to an astropy SkyCoord with Cartesian representation and differential."""
        return ac.SkyCoord(
            x=self.r.x * position_units,
            y=self.r.y * position_units,
            z=self.r.z * position_units,
            v_x=self.v.x * velocity_units,
            v_y=self.v.y * velocity_units,
            v_z=self.v.z * velocity_units,
            frame=frame.to_astropy(),
            representation_type="cartesian",
            differential_type="cartesian",
            obstime=self.t,
        )
