# SPDX-License-Identifier: Apache-2.0
"""Autoslew AxisRates — the published RA/Dec rate must be the target's INERTIAL rate.

Zero while sidereal/fixed; the real rate while following a moving target (this is
what lands in FITS RA_RATE, which the repo requires to be inertial).
"""

from datetime import UTC, datetime

import pytest

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import FrameTarget, RateTarget
from sensorkit.std import AxisRates, FollowTarget


def _axis_rates(mock_sk):
    return [c.args[0] for c in mock_sk.publish.call_args_list if isinstance(c.args[0], AxisRates)]


@pytest.mark.asyncio
async def test_sidereal_publishes_zero_inertial_rate(telescope, _mock_sk_device):
    await telescope.telescope_follow_target(
        FollowTarget(target=FrameTarget(frame=ReferenceFrame.ICRF))
    )
    assert telescope._icrf_rate == (0.0, 0.0)
    published = _axis_rates(_mock_sk_device)
    assert published
    assert published[-1].right_ascension.velocity == 0.0
    assert published[-1].declination.velocity == 0.0


@pytest.mark.asyncio
async def test_rate_target_publishes_its_inertial_rate(telescope, _mock_sk_device):
    rt = RateTarget(
        frame=ReferenceFrame.ICRF,
        rates=Equatorial(ra=0.01, dec=0.002),  # deg/s
        initial_time=datetime.now(UTC),
        initial_frame=ReferenceFrame.ICRF,
        initial_coords=Equatorial(ra=90.0, dec=20.0),
    )

    await telescope.telescope_follow_target(FollowTarget(target=rt))

    assert telescope._icrf_rate == (0.01, 0.002)
    published = _axis_rates(_mock_sk_device)
    assert published
    assert published[-1].right_ascension.velocity == pytest.approx(0.01)
    assert published[-1].declination.velocity == pytest.approx(0.002)
    # Alt/Az rates are derived from the RA/Dec rate, so they must not be pinned to zero.
    assert published[-1].azimuth.velocity != 0.0 or published[-1].altitude.velocity != 0.0
