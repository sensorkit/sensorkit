"""Test mount FollowTarget with FrameTarget (sidereal on/off)."""

import pytest
from conftest import MockPWI4Client


class TestFrameTarget:
    @pytest.mark.asyncio
    async def test_tracking_on(self):
        client = MockPWI4Client()

        await client.request("/mount/tracking_on")

        reqs = client.find_requests("/mount/tracking_on")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_tracking_off(self):
        client = MockPWI4Client()

        await client.request("/mount/tracking_off")

        reqs = client.find_requests("/mount/tracking_off")
        assert len(reqs) == 1
