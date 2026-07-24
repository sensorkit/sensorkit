# SPDX-License-Identifier: Apache-2.0
"""Autoslew telescope TLE FollowTarget — native ASA sat:* flow (motors-first)."""

import pytest

from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.std import FollowTarget

_ISS = TLE(
    line0="ISS (ZARYA)",
    line1="1 25544U 98067A   26196.50000000  .00016717  00000-0  10270-3 0  9005",
    line2="2 25544  51.6400 208.0000 0006703 130.0000 325.0000 15.72000000000010",
)


@pytest.mark.asyncio
async def test_follow_tle_stages_native_sat_flow(telescope):
    await telescope.telescope_follow_target(FollowTarget(target=TLETarget(tle=_ISS)))

    actions = telescope.telescope.actions()

    # Motors are engaged first, the pass is staged, then tracking starts.
    for expected in (
        "telescope:motoron",
        "sat:name",
        "sat:startalt",
        "sat:line1",
        "sat:line2",
        "sat:start",
    ):
        assert expected in actions, f"missing action {expected!r} (got {actions})"

    # Ordering: motors + both element lines must precede sat:start.
    start = actions.index("sat:start")
    assert actions.index("telescope:motoron") < start
    assert actions.index("sat:line1") < start
    assert actions.index("sat:line2") < start


@pytest.mark.asyncio
async def test_follow_tle_passes_element_lines(telescope):
    await telescope.telescope_follow_target(FollowTarget(target=TLETarget(tle=_ISS)))

    # The staged line1/line2 must be the target's actual TLE lines.
    calls = {args[0]: args[1] for (m, args, _kw) in telescope.telescope.calls if m == "Action"}
    assert calls["sat:line1"] == _ISS.line1
    assert calls["sat:line2"] == _ISS.line2
