# SPDX-License-Identifier: Apache-2.0
from datetime import UTC, datetime

import pytest

from sensorkit.astro.common import TLE
from sensorkit.astro.trajectory import TLETrajectory

_ISS_TLE = TLE(
    line0="0 ISS (ZARYA)",
    line1="1 25544U 98067A   25015.39697524  .00024300  00000-0  43163-3 0  9997",
    line2="2 25544  51.6408 343.8792 0001934 100.3261   3.0329 15.50054085491466",
)


def test_tle_trajectory_sample():
    """TLETrajectory.sample() returns GCRS with LEO-magnitude position."""
    traj = TLETrajectory(_ISS_TLE)
    # Sample near the TLE epoch.
    epoch = datetime(2025, 1, 15, 9, 32, 0, tzinfo=UTC)
    gcrs = traj.sample(epoch)

    # ISS orbits at ~400 km altitude → ~6778 km from Earth centre.
    import numpy as np

    pos_km = np.array([
        gcrs.cartesian.x.to_value("km"),
        gcrs.cartesian.y.to_value("km"),
        gcrs.cartesian.z.to_value("km"),
    ])
    distance_km = np.linalg.norm(pos_km)
    assert 6000 < distance_km < 7200


@pytest.mark.asyncio
async def test_tle_trajectory_propagate_noop():
    """propagate() returns self (TLE trajectories are stateless)."""
    traj = TLETrajectory(_ISS_TLE)
    result = await traj.propagate(datetime(2025, 2, 1, tzinfo=UTC))
    assert result is traj
