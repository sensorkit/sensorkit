# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with AltAzTarget."""

import pytest
from conftest import MockPWI4Client


class TestAltAzTarget:
    @pytest.mark.asyncio
    async def test_goto_alt_az(self):
        client = MockPWI4Client()

        await client.request(
            "/mount/goto_alt_az",
            params={"alt_degs": 60.0, "az_degs": 180.0},
        )

        reqs = client.find_requests("/mount/goto_alt_az")
        assert len(reqs) == 1
        assert reqs[0][1] == {"alt_degs": 60.0, "az_degs": 180.0}

    @pytest.mark.asyncio
    async def test_poll_slew_complete(self):
        client = MockPWI4Client()
        client.set_status(**{"mount.is_slewing": "false"})

        result = await client.poll(
            lambda s: not client.get_bool(s, "mount.is_slewing"),
        )

        assert not client.get_bool(result, "mount.is_slewing")
