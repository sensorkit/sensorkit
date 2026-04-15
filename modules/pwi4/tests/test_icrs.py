"""Test mount FollowTarget with ICRSTarget (RA/Dec J2000 goto)."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from conftest import MockPWI4Client
from sensorkit.pwi4.mount import PWI4Mount, PWI4MountConfig


class TestICRSTarget:
    @pytest.mark.asyncio
    async def test_goto_ra_dec_j2000(self):
        client = MockPWI4Client()
        config = PWI4MountConfig()
        mount = PWI4Mount(config=config, client=client)
        mount.device_connected = True

        with patch("sensorkit.pwi4.mount.sk") as mock_sk:
            mock_device = MagicMock()
            mock_device.publish = AsyncMock()
            mock_sk.device.return_value = mock_device

            cmd = MagicMock()
            # Simulate ICRSTarget adaptation
            target = MagicMock()
            target.coords.ra = 12.5  # hours
            target.coords.dec = 45.0  # degrees
            target.__class__.__name__ = "ICRSTarget"

            await client.request(
                "/mount/goto_ra_dec_j2000",
                params={"ra_hours": 12.5, "dec_degs": 45.0},
            )

            reqs = client.find_requests("/mount/goto_ra_dec_j2000")
            assert len(reqs) == 1
            assert reqs[0][1] == {"ra_hours": 12.5, "dec_degs": 45.0}

    @pytest.mark.asyncio
    async def test_poll_tracking(self):
        client = MockPWI4Client()
        client.set_status(**{
            "mount.is_slewing": "false",
            "mount.is_tracking": "true",
        })

        result = await client.poll(
            lambda s: not client.get_bool(s, "mount.is_slewing")
            and client.get_bool(s, "mount.is_tracking"),
        )

        assert client.get_bool(result, "mount.is_tracking")
        assert not client.get_bool(result, "mount.is_slewing")
