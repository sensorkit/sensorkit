"""Test mount FollowTarget with TLETarget."""

import pytest
from conftest import MockPWI4Client


TLE_LINE0 = "ISS (ZARYA)"
TLE_LINE1 = "1 25544U 98067A   26100.50000000  .00016717  00000-0  10270-3 0  9002"
TLE_LINE2 = "2 25544  51.6400 200.0000 0007000 300.0000  60.0000 15.50000000100000"


class TestTLETarget:
    @pytest.mark.asyncio
    async def test_follow_tle(self):
        client = MockPWI4Client()

        await client.request(
            "/mount/follow_tle",
            params={"line1": TLE_LINE0, "line2": TLE_LINE1, "line3": TLE_LINE2},
        )

        reqs = client.find_requests("/mount/follow_tle")
        assert len(reqs) == 1
        assert reqs[0][1]["line1"] == TLE_LINE0
        assert reqs[0][1]["line2"] == TLE_LINE1
        assert reqs[0][1]["line3"] == TLE_LINE2

    @pytest.mark.asyncio
    async def test_poll_tracking(self):
        client = MockPWI4Client()
        client.set_status(**{"mount.is_tracking": "true"})

        result = await client.poll(
            lambda s: client.get_bool(s, "mount.is_tracking"),
        )

        assert client.get_bool(result, "mount.is_tracking")
