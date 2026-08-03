# SPDX-License-Identifier: Apache-2.0
"""
LifecycleRunner: the op hook, per-run state, and the raise policy that
sits above dag's never-raising executor.
"""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.workflow import (
    AbortSignal,
    Entry,
    LifecycleError,
    LifecycleRunner,
    OpContext,
    Phase,
    PhaseTable,
)


def table(*phases: Phase, on_failure: str = "stop") -> PhaseTable:
    return PhaseTable(name="t", phases=phases, on_failure=on_failure)


def hook(*, failures: frozenset[tuple[str, str]] = frozenset(),
         dwell: float = 0.0, seen: list | None = None):
    async def perform(ctx: OpContext) -> object:
        if seen is not None:
            seen.append((ctx.op.ref, ctx.op.op, ctx.node.group))
        if (ctx.op.ref, ctx.op.op) in failures:
            raise RuntimeError("simulated fault")
        if dwell:
            await asyncio.sleep(dwell)
        return f"{ctx.op.ref}:{ctx.op.op}"
    return perform


@pytest.mark.asyncio
async def test_hook_return_value_is_recorded_as_a_measurement(devices):
    report = await LifecycleRunner(hook()).run(devices, table(
        Phase(name="p", entries=(Entry(trait="mount", ops="unpark"),))))
    assert report.ok
    assert [r.value for r in report.results.values()] == ["tcs-1:unpark"]


@pytest.mark.asyncio
async def test_run_state_carries_between_phases(devices):
    async def perform(ctx: OpContext) -> object:
        if ctx.op.op == "measure":
            ctx.run.state["focus"] = 12.345
            return None
        return ctx.run.state["focus"]           # a later phase consumes it

    report = await LifecycleRunner(perform).run(devices, table(
        Phase(name="a", entries=(Entry(trait="mount", ops="measure"),)),
        Phase(name="b", entries=(Entry(trait="mount", ops="apply"),))))
    assert report.ok
    assert [n.payload.op for n, r in report.with_status("ok")
            if r.value == 12.345] == ["apply"]


@pytest.mark.asyncio
async def test_a_failed_step_reverses_nothing(devices):
    """No automatic recovery: the steps that already succeeded stay
    done, and the report says so."""
    calls: list[str] = []

    async def perform(ctx: OpContext) -> object:
        calls.append(ctx.op.op)
        if ctx.op.op == "open":
            raise RuntimeError("stuck")
        return None

    with pytest.raises(LifecycleError) as e:
        await LifecycleRunner(perform).run(devices, table(
            Phase(name="a", entries=(Entry(trait="mount", ops="unpark"),)),
            Phase(name="b", entries=(Entry(trait="dome", ops="open"),))))
    assert calls == ["unpark", "open"]
    assert [n.payload.op for n, _ in e.value.report.with_status("ok")] == ["unpark"]


@pytest.mark.asyncio
async def test_stop_policy_raises_with_the_report_attached(devices):
    with pytest.raises(LifecycleError, match="simulated fault") as e:
        await LifecycleRunner(hook(failures=frozenset({("tcs-1", "unpark")}))).run(
            devices, table(
                Phase(name="p", entries=(Entry(trait="mount", ops="unpark"),))))
    assert [n.payload.ref for n, _ in e.value.report.failures] == ["tcs-1"]


@pytest.mark.asyncio
async def test_skip_policy_raises_only_at_the_end(devices):
    seen: list = []
    with pytest.raises(LifecycleError):
        await LifecycleRunner(
            hook(failures=frozenset({("tcs-1", "stop_tracking")}), seen=seen)
        ).run(devices, table(
            Phase(name="a", entries=(Entry(trait="mount", ops="stop_tracking"),)),
            Phase(name="b", entries=(Entry(trait="dome", ops="close"),)),
            on_failure="skip"))
    assert ("dome-1", "close", "b") in seen      # later phase still ran


@pytest.mark.asyncio
async def test_optional_failure_degrades_without_raising(devices, init_table):
    # fw-a's home is optional in demo.yaml, and its configure step
    # requires that home per device and is optional too, so exactly one
    # dependent skips and neither reaches the raise policy.
    report = await LifecycleRunner(
        hook(failures=frozenset({("fw-a", "home")}))).run(devices, init_table)
    assert not report.failures
    assert [n.payload.ref for n, _ in report.degraded] == ["fw-a"]
    assert [n.payload.op for n, _ in report.with_status("skipped")] == [
        "set_default_filter"]


@pytest.mark.asyncio
async def test_a_skipped_required_step_raises_though_nothing_required_failed(
        devices):
    """The trap the two flags exist to close: marking a step optional
    without also freeing what follows it leaves the dependent skipped,
    and a skipped required step is not a successful run."""
    with pytest.raises(LifecycleError, match="did not run") as e:
        await LifecycleRunner(hook(failures=frozenset({("dome-1", "stop")}))).run(
            devices, table(Phase(name="p", entries=(
                Entry(trait="dome",
                      ops=({"op": "stop", "optional": True}, "close")),)),
                on_failure="skip"))
    report = e.value.report
    assert [n.payload.op for n, _ in report.failures] == ["close"]
    assert [n.payload.op for n, _ in report.degraded] == ["stop"]


@pytest.mark.asyncio
async def test_continue_carries_the_teardown_past_a_failed_step(devices):
    """The same table with the halt declaring its own blast radius: the
    close runs, and the halt's failure stays out of the report."""
    seen: list = []
    report = await LifecycleRunner(
        hook(failures=frozenset({("dome-1", "stop")}), seen=seen)
    ).run(devices, table(Phase(name="p", entries=(
        Entry(trait="dome",
              ops=({"op": "stop", "optional": True, "on_failure": "continue"},
                   "close")),)),
        on_failure="skip"))
    assert ("dome-1", "close", "p") in seen
    assert not report.failures
    assert [n.payload.op for n, _ in report.degraded] == ["stop"]


@pytest.mark.asyncio
async def test_a_later_phase_runs_though_an_earlier_one_failed(
        devices, deinit_table):
    """The motivating case: the mirror cover jams and the dome still
    closes. A phase barrier is a soft edge, so `skip` — which
    propagates along hard edges only — never reaches across one. The
    teardown still reports the jam."""
    seen: list = []
    with pytest.raises(LifecycleError, match="cover-1"):
        await LifecycleRunner(
            hook(failures=frozenset({("cover-1", "close")}), seen=seen)
        ).run(devices, deinit_table)
    assert ("dome-1", "close", "close-enclosure") in seen
    assert ("tcs-1", "park", "park") in seen
    assert sum(1 for s in seen if s[1] == "disconnect") == len(devices.refs)


@pytest.mark.asyncio
async def test_one_step_can_stop_a_table_that_skips_everywhere_else(
        devices, deinit_table):
    """demo.yaml's mount halt: a teardown that otherwise skips past
    failures abandons itself rather than close a dome onto a moving
    telescope."""
    seen: list = []
    with pytest.raises(LifecycleError):
        await LifecycleRunner(
            hook(failures=frozenset({("tcs-1", "stop_tracking")}), seen=seen)
        ).run(devices, deinit_table)
    assert not [s for s in seen if s[1] == "close"]


@pytest.mark.asyncio
async def test_domain_abort_reports_without_raising(devices, init_table):
    abort = AbortSignal()
    task = asyncio.create_task(
        LifecycleRunner(hook(dwell=0.02)).run(devices, init_table, abort=abort))
    await asyncio.sleep(0.05)
    abort.fire("humidity 95%")
    report = await task
    assert report.aborted


@pytest.mark.asyncio
async def test_foreign_cancellation_is_not_swallowed(devices, init_table):
    task = asyncio.create_task(
        LifecycleRunner(hook(dwell=0.02)).run(
            devices, init_table, abort=AbortSignal()))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_a_runner_serves_concurrent_runs(devices):
    """The runner holds nothing but the hook: per-run state lives in
    the RunContext, so two runs cannot see each other's."""
    states: list[dict] = []

    async def perform(ctx: OpContext) -> object:
        ctx.run.state[ctx.op.ref] = ctx.run.name
        states.append(ctx.run.state)
        await asyncio.sleep(0.01)
        return None

    runner = LifecycleRunner(perform)
    t = table(Phase(name="p", entries=(Entry(trait="mount", ops="unpark"),)))
    a, b = await asyncio.gather(runner.run(devices, t), runner.run(devices, t))
    assert a.ok and b.ok
    assert len(states) == 2 and states[0] is not states[1]
