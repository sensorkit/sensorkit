# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with FrameTarget (sidereal on/off)."""

import pytest

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import FrameTarget
from sensorkit.std import FollowTarget


class TestFrameTarget:
    @pytest.mark.asyncio
    async def test_icrf_frame_enables_tracking(self, client, mount):
        await mount.mount_follow_target(
            FollowTarget(target=FrameTarget(frame=ReferenceFrame.ICRF))
        )

        assert len(client.find_requests("/mount/tracking_on")) == 1
        assert mount._sidereal is True

    @pytest.mark.asyncio
    async def test_altaz_frame_disables_tracking(self, client, mount):
        client.set_status(**{"mount.is_tracking": "false"})

        await mount.mount_follow_target(
            FollowTarget(target=FrameTarget(frame=ReferenceFrame.ALTAZ))
        )

        assert len(client.find_requests("/mount/tracking_off")) == 1
        assert mount._sidereal is False
