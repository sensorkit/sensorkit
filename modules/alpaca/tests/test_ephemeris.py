"""Tests for Alpaca telescope Ephemeris FollowTarget."""

import pytest


@pytest.mark.xfail(reason="Ephemeris tracking not yet fully testable")
@pytest.mark.asyncio
async def test_telescope_follow_ephemeris():
    """Placeholder for Ephemeris follow target test.

    Ephemeris tracking requires interpolation over a precomputed set of
    time/position/velocity points that is complex to mock reliably.
    """
    assert False
