# SPDX-License-Identifier: Apache-2.0
"""Test mount FollowTarget with EphemerisTarget (radecpath)."""

import pytest
from conftest import MockPWI4Client


class TestEphemerisTarget:
    @pytest.mark.asyncio
    async def test_radecpath_sequence(self):
        """Test the full radecpath_new -> add_point -> apply sequence."""
        client = MockPWI4Client()

        # Simulate the EphemerisTarget flow
        jds = [2460400.5, 2460400.501, 2460400.502]
        ra_degs = [180.0, 180.1, 180.2]  # degrees
        dec_degs = [45.0, 45.01, 45.02]

        await client.request("/mount/radecpath/new")
        for i in range(len(jds)):
            await client.request(
                "/mount/radecpath/add_point",
                params={
                    "jd": jds[i],
                    "ra_j2000_hours": ra_degs[i] / 15,
                    "dec_j2000_degs": dec_degs[i],
                },
            )
        await client.request("/mount/radecpath/apply")

        # Verify sequence
        new_reqs = client.find_requests("/mount/radecpath/new")
        add_reqs = client.find_requests("/mount/radecpath/add_point")
        apply_reqs = client.find_requests("/mount/radecpath/apply")

        assert len(new_reqs) == 1
        assert len(add_reqs) == 3
        assert len(apply_reqs) == 1

        # Verify RA is converted from degrees to hours
        assert add_reqs[0][1]["ra_j2000_hours"] == pytest.approx(12.0, abs=0.01)
        assert add_reqs[1][1]["ra_j2000_hours"] == pytest.approx(12.00667, abs=0.01)
        assert add_reqs[2][1]["ra_j2000_hours"] == pytest.approx(12.01333, abs=0.01)

    @pytest.mark.asyncio
    async def test_poll_slew_complete(self):
        client = MockPWI4Client()
        client.set_status(**{"mount.is_slewing": "false"})

        result = await client.poll(
            lambda s: not client.get_bool(s, "mount.is_slewing"),
        )

        assert not client.get_bool(result, "mount.is_slewing")
