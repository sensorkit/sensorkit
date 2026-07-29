# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with AltAzTarget."""

import pytest

from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.std import FollowTarget


@pytest.fixture
def altaz_client(client):
    """A client reporting a settled, non-tracking mount, as after an alt-az goto."""
    client.set_status(**{"mount.is_slewing": "false", "mount.is_tracking": "false"})
    return client


class TestAltAzTarget:
    @pytest.mark.asyncio
    async def test_goto_alt_az(self, altaz_client, mount):
        await mount.mount_follow_target(
            FollowTarget(target=AltAzTarget(coords=Horizontal(az=180.0, alt=60.0)))
        )

        reqs = altaz_client.find_requests("/mount/goto_alt_az")
        assert len(reqs) == 1
        assert reqs[0][1] == {"alt_degs": 60.0, "az_degs": 180.0}

    @pytest.mark.asyncio
    async def test_altaz_follow_is_not_sidereal(self, altaz_client, mount):
        await mount.mount_follow_target(
            FollowTarget(target=AltAzTarget(coords=Horizontal(az=180.0, alt=60.0)))
        )

        # An alt-az goto holds a fixed horizontal position; nothing tracks the sky.
        assert mount._sidereal is False
