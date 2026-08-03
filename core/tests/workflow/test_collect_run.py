# SPDX-License-Identifier: Apache-2.0
"""
CollectRunner, and the claim the unification rests on: one hook object
serves both frontends.
"""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.workflow import (
    OP_APPLY,
    OP_EXPOSE,
    AbortSignal,
    Collect,
    CollectRunner,
    FramePlan,
    LifecycleRunner,
    OpContext,
    Step,
    SyncPoint,
)

CAM = ("main-ota", "sel", "port-a")
WIDE = ("piggyback", "wide")


def hook(*, fail: frozenset[str] = frozenset(), dwell: float = 0.0,
         seen: list | None = None):
    async def perform(ctx: OpContext) -> object:
        if seen is not None:
            seen.append((ctx.op.ref, ctx.op.op))
        if ctx.op.ref in fail:
            raise RuntimeError("simulated fault")
        if dwell:
            await asyncio.sleep(dwell)
        return f"{ctx.op.ref}:{ctx.op.op}"
    return perform


ONE = Collect(name="one", steps=(
    Step(settings={"tcs-1": "M51"}, plans={CAM: FramePlan(0.0, n_frames=2)}),))


@pytest.mark.asyncio
async def test_a_collect_dispatches_applies_and_exposures(topo, devices):
    seen: list = []
    report = await CollectRunner(hook(seen=seen)).run(topo, devices, ONE)
    assert report.ok
    assert ("sel-1", OP_APPLY) in seen and ("tcs-1", OP_APPLY) in seen
    assert seen.count(("cam-sci", OP_EXPOSE)) == 2      # one call per frame


@pytest.mark.asyncio
async def test_every_frame_gets_its_own_result(topo, devices):
    report = await CollectRunner(hook()).run(topo, devices, ONE)
    exposures = [r for n, r in report.with_status("ok")
                 if not isinstance(n.payload, SyncPoint)
                 and n.payload.op == OP_EXPOSE]
    assert [r.value for r in exposures] == ["cam-sci:expose"] * 2


@pytest.mark.asyncio
async def test_a_sync_point_never_reaches_the_hook(topo, devices):
    seen: list = []
    aligned = Collect(steps=(
        Step(settings={"tcs-1": "M51"}, align="midpoint",
             plans={CAM: FramePlan(0.0), WIDE: FramePlan(0.0)}),))
    report = await CollectRunner(hook(seen=seen)).run(topo, devices, aligned)
    assert report.ok
    assert all(op != "sync" for _, op in seen)
    # ...but it is still a node, and still reported.
    assert any(isinstance(n.payload, SyncPoint) for n in report.graph.nodes)


@pytest.mark.asyncio
async def test_a_failed_apply_skips_exactly_the_frames_it_invalidates(
        topo, devices):
    report = await CollectRunner(hook(fail=frozenset({"fw-shared"}))).run(
        topo, devices, Collect(steps=(
            Step(settings={"tcs-1": "M51", "fw-shared": "clear"},
                 plans={CAM: FramePlan(0.0, n_frames=3)}),)))
    assert not report.ok
    assert [n.payload.ref for n, _ in report.causes] == ["fw-shared"]
    # All three frames, named individually — not one opaque block, and
    # each one a frame that was owed and not taken.
    assert len(report.with_status("skipped")) == 3
    assert len(report.failures) == 4


@pytest.mark.asyncio
async def test_nothing_raises_past_validation(topo, devices):
    """Unlike lifecycle, a collect has no raise policy: outcomes are
    the report."""
    report = await CollectRunner(hook(fail=frozenset({"tcs-1"}))).run(
        topo, devices, ONE)
    assert not report.ok and report.failures


@pytest.mark.asyncio
async def test_an_abort_is_absorbed_and_reported(topo, devices):
    abort = AbortSignal()
    task = asyncio.create_task(
        CollectRunner(hook(dwell=10.0)).run(topo, devices, ONE, abort=abort))
    await asyncio.sleep(0.02)                   # the first apply is in flight
    abort.fire("weather")
    report = await task                         # returns; does not raise
    assert report.aborted
    assert all(r.status == "cancelled" for r in report.results.values())


@pytest.mark.asyncio
async def test_validation_failures_raise_before_any_dispatch(topo, devices):
    seen: list = []
    with pytest.raises(ValueError, match="not a collect target"):
        await CollectRunner(hook(seen=seen)).run(topo, devices, Collect(steps=(
            Step(plans={("guide-scope", "guider"): FramePlan(1)}),)))
    assert seen == []


# ---- the point of the unification ---------------------------------------

@pytest.mark.asyncio
async def test_one_hook_object_serves_both_runners(topo, devices, init_table):
    """A deployment registers its drivers once. The same callable homes
    a filter wheel during init and applies a filter during a collect."""
    calls: list[tuple[str, str]] = []

    async def perform(ctx: OpContext) -> object:
        calls.append((ctx.op.ref, ctx.op.op))
        return None

    await LifecycleRunner(perform).run(devices, init_table)
    await CollectRunner(perform).run(topo, devices, ONE)

    assert ("fw-a", "home") in calls            # from the phase table
    assert ("tcs-1", OP_APPLY) in calls         # from the collect
    assert ("cam-sci", OP_EXPOSE) in calls


@pytest.mark.asyncio
async def test_both_frontends_produce_the_same_payload_type(
        topo, devices, init_table):
    """Not just structurally alike — literally one class, so a
    dispatcher written against it works for either."""
    seen: list = []

    async def perform(ctx: OpContext) -> object:
        seen.append(type(ctx.op))
        return None

    await LifecycleRunner(perform).run(devices, init_table)
    await CollectRunner(perform).run(topo, devices, ONE)
    assert len(set(seen)) == 1


@pytest.mark.asyncio
async def test_a_collect_apply_resolves_like_a_device_keyed_table_entry(
        topo, devices):
    """`match="device"` with the device's traits populated is what lets
    one driver table answer both frontends: fw-a's apply finds the
    filter-wheel handler by walking traits, exactly as a
    `{device: fw-a}` entry would."""
    seen: list = []

    async def perform(ctx: OpContext) -> object:
        if ctx.op.ref == "fw-a":
            seen.append((ctx.op.match, ctx.op.trait, ctx.op.traits))
        return None

    await CollectRunner(perform).run(topo, devices, Collect(steps=(
        Step(plans={CAM: FramePlan(0.0, settings={"fw-a": "clear"})}),)))
    assert seen == [("device", None, ("filter_wheel",))]


@pytest.mark.asyncio
async def test_a_runner_holds_nothing_but_the_hook(topo, devices):
    """So one runner serves any number of sensors — the views are
    arguments, not state."""
    runner = CollectRunner(hook())
    assert vars(runner) == {"perform": runner.perform}
    a, b = await asyncio.gather(runner.run(topo, devices, ONE),
                                runner.run(topo, devices, ONE))
    assert a.ok and b.ok


@pytest.mark.asyncio
async def test_a_fast_peer_finishes_its_whole_block_inside_one_slow_frame(
        topo, devices):
    """The runtime statement of test_collect_compile's
    `test_concurrent_blocks_never_couple_frame_to_frame`.

    Worth having twice: the edge assertions there would still pass if
    the *executor* serialised frames, and `format_graph` renders two
    equal-length blocks as interleaved levels, which looks exactly like
    lockstep. Only a clock distinguishes them."""
    pair = Collect(name="pair", steps=(
        Step(settings={"tcs-1": "M51"},
             plans={CAM: FramePlan(0.30, n_frames=3),
                    WIDE: FramePlan(0.02, n_frames=3)}),))
    done: dict[str, float] = {}
    loop = asyncio.get_event_loop()
    t0 = loop.time()

    async def perform(ctx: OpContext) -> object:
        if ctx.op.op == OP_EXPOSE:
            await asyncio.sleep(ctx.op.params["exposure_s"])
            done[f"{ctx.op.ref}[{ctx.op.params['frame']}]"] = loop.time() - t0
        return None

    report = await CollectRunner(perform).run(topo, devices, pair)
    assert report.ok
    # cam-wide's whole block lands inside cam-sci's *first* frame, which
    # cannot happen if the rendered levels were barriers.
    assert done["cam-wide[2]"] < done["cam-sci[0]"]
    # And the collect costs the slow camera alone, not the sum.
    assert max(done.values()) == pytest.approx(0.9, abs=0.2)
