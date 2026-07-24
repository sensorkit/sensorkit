# SPDX-License-Identifier: Apache-2.0
import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Self

import numpy as np
import satkit
from astropy.coordinates import GCRS
from astropy.time import Time

from sensorkit.astro.common import TLE
from sensorkit.astro.coords import Cartesian, StateVector, astropy_unit


def _gcrs_cartesian(pos: np.ndarray, vel: np.ndarray, jds: np.ndarray | float) -> GCRS:
    """Build a vectorized GCRS coordinate from position/velocity arrays and Julian dates.

    `pos` and `vel` are shaped `(..., 3)` in meters and meters per second; `jds`
    broadcasts against the leading axis.
    """
    m = astropy_unit("m")
    mps = astropy_unit("m/s")
    return GCRS(
        x=pos[..., 0] * m,
        y=pos[..., 1] * m,
        z=pos[..., 2] * m,
        v_x=vel[..., 0] * mps,
        v_y=vel[..., 1] * mps,
        v_z=vel[..., 2] * mps,
        obstime=Time(jds, format="jd"),
        representation_type="cartesian",
    )


class Trajectory(ABC):
    """Abstract orbital trajectory that can be propagated and sampled."""

    @abstractmethod
    async def propagate(self, when: datetime | timedelta) -> Self:
        """Propagate the trajectory to the given point in time."""

    @abstractmethod
    def sample(self, epochs: Sequence[datetime] | None = None) -> GCRS:
        """Interpolate the state at each epoch.

        With no epochs, samples once at the current time and returns a scalar
        coordinate. A sequence yields one array-valued GCRS spanning the epochs.
        """


class OrbitalTrajectory(Trajectory):
    """Trajectory derived from numerical orbital propagation of a state vector via satkit."""

    def __init__(self, sv: StateVector, *, _result: satkit.propresult | None = None):
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

    def sample(self, epochs: Sequence[datetime] | None = None) -> GCRS:
        times = (
            [satkit.time.now()]
            if epochs is None
            else [satkit.time.from_datetime(e) for e in epochs]
        )
        vecs = np.array([self._result.interp(t) for t in times])
        jds = np.array([t.as_jd() for t in times])
        gcrs = _gcrs_cartesian(vecs[:, :3], vecs[:, 3:], jds)
        return gcrs[0] if epochs is None else gcrs


class TLETrajectory(Trajectory):
    """Trajectory derived from SGP4 propagation of a Two-Line Element set."""

    def __init__(self, tle: TLE):
        self.tle = satkit.TLE.from_lines(tle.to_list())

    async def propagate(self, when: datetime | timedelta) -> Self:
        return self

    def sample(self, epochs: Sequence[datetime] | None = None) -> GCRS:
        times = (
            [satkit.time.now()]
            if epochs is None
            else [satkit.time.from_datetime(e) for e in epochs]
        )
        pos, vel, jds = [], [], []

        for t in times:
            teme_p, teme_v = satkit.sgp4(self.tle, t)
            q = satkit.frametransform.qteme2gcrf(t)
            pos.append(q * teme_p)
            vel.append(q * teme_v)
            jds.append(t.as_jd())

        gcrs = _gcrs_cartesian(np.array(pos), np.array(vel), np.array(jds))
        return gcrs[0] if epochs is None else gcrs
