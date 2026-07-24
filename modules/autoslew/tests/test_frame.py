# SPDX-License-Identifier: Apache-2.0
"""Autoslew telescope FrameTarget sidereal-hold sentinel."""

import pytest

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import FrameTarget
from sensorkit.std import FollowTarget


@pytest.mark.asyncio
async def test_frame_icrf_enables_sidereal_tracking(telescope):
    telescope.telescope._properties["Tracking"] = False
    await telescope.telescope_follow_target(
        FollowTarget(target=FrameTarget(frame=ReferenceFrame.ICRF))
    )
    assert telescope._tracking is True
    assert telescope.telescope._properties["Tracking"] is True


@pytest.mark.asyncio
async def test_frame_altaz_disables_tracking(telescope):
    telescope.telescope._properties["Tracking"] = True
    telescope._tracking = True
    await telescope.telescope_follow_target(
        FollowTarget(target=FrameTarget(frame=ReferenceFrame.ALTAZ))
    )
    assert telescope._tracking is False
    assert telescope.telescope._properties["Tracking"] is False
