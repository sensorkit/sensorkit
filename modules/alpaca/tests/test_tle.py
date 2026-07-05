# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca telescope TLE FollowTarget."""

import pytest


@pytest.mark.xfail(reason="TLE tracking not yet fully testable")
@pytest.mark.asyncio
async def test_telescope_follow_tle():
    """Placeholder for TLE follow target test.

    TLE tracking requires a real satellite TLE, Skyfield propagation, and
    realistic timing that is difficult to mock reliably.
    """
    assert False
