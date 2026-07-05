# SPDX-License-Identifier: Apache-2.0
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Self

import satkit
from astropy.coordinates import GCRS
from astropy.time import Time
from astropy.units import Unit

from sensorkit.astro.common import TLE
from sensorkit.astro.coords import Cartesian, StateVector

METERS = Unit("m")
SEC = Unit("s")


class Trajectory(ABC):
    """Abstract orbital trajectory that can be propagated and sampled."""

    @abstractmethod
    async def propagate(self, when: datetime | timedelta) -> Self:
        """Propagate the trajectory to the given point in time."""

    @abstractmethod
    def sample(self, epoch: datetime = None) -> GCRS:
        """Interpolate the state at the given time and return as an astropy coordinate."""


class OrbitalTrajectory(Trajectory):
    """Trajectory derived from numerical orbital propagation of a state vector via satkit."""

    def __init__(self, sv: StateVector, *, _result: satkit.propresult = None):
        self.sv = sv
        self._result = _result

    async def propagate(self, when: datetime | timedelta) -> Self:
        result = await asyncio.to_thread(
            satkit.propagate,
            self.sv.to_numpy(),
            satkit.time.from_datetime(self.sv.t),
            satkit.time.from_datetime(when),
        )
        return OrbitalTrajectory(
            StateVector(
                result.time.as_datetime(),
                Cartesian(*result.pos),
                Cartesian(*result.vel),
            ),
            _result=result,
        )

    def sample(self, epoch: datetime = None):
        t = satkit.time.now() if epoch is None else satkit.time.from_datetime(epoch)
        vec = self._result.interp(t)
        return GCRS(
            x=vec[0] * METERS,
            y=vec[1] * METERS,
            z=vec[2] * METERS,
            v_x=vec[3] * METERS / SEC,
            v_y=vec[4] * METERS / SEC,
            v_z=vec[5] * METERS / SEC,
            obstime=Time(t.as_jd(), format="jd"),
            representation_type="cartesian",
        )


class TLETrajectory(Trajectory):
    """Trajectory derived from SGP4 propagation of a Two-Line Element set."""

    def __init__(self, tle: TLE):
        self.tle = satkit.TLE.from_lines(tle.to_list())

    async def propagate(self, when: datetime | timedelta) -> Self:
        return self

    def sample(self, epoch: datetime = None) -> GCRS:
        t = satkit.time.now() if epoch is None else satkit.time.from_datetime(epoch)
        teme_p, teme_v = satkit.sgp4(self.tle, t)
        q = satkit.frametransform.qteme2gcrf(t)
        gcrf_p = q * teme_p
        gcrf_v = q * teme_v
        return GCRS(
            x=gcrf_p[0] * METERS,
            y=gcrf_p[1] * METERS,
            z=gcrf_p[2] * METERS,
            v_x=gcrf_v[0] * METERS / SEC,
            v_y=gcrf_v[1] * METERS / SEC,
            v_z=gcrf_v[2] * METERS / SEC,
            obstime=Time(t.as_jd(), format="jd"),
            representation_type="cartesian",
        )
