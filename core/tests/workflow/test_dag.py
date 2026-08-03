# SPDX-License-Identifier: Apache-2.0
"""
The executor, on hand-built graphs. Edge semantics, failure policies,
and the abort/cancellation split — the parts no compiler can prove.
"""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.workflow import AbortSignal, DagRunner, Graph, Node, format_graph, topo_order


def graph(nodes: list[Node], deps: dict[int, set[int]] | None = None,
          hard: dict[int, set[int]] | None = None) -> Graph:
    deps, hard = deps or {}, hard or {}
    ids = [n.id for n in nodes]
    all_deps = {i: frozenset(deps.get(i, set()) | hard.get(i, set())) for i in ids}
    return Graph(nodes=tuple(nodes), deps=all_deps,
                 hard={i: frozenset(hard.get(i, set())) for i in ids})


def n(i: int, *, on_failure: str = "skip", optional: bool = False,
      delay: float = 0.0) -> Node:
    """A node defaulting to `skip` — the rung that isolates edge
    semantics, so a test naming no policy is testing the edges."""
    return Node(id=i, label=f"n{i}", group="g", payload=i,
                on_failure=on_failure, optional=optional, delay_s=delay)


def recorder(fail: set[int] = frozenset(), dwell: float = 0.0):
    """Dispatcher that records call order and fails the named nodes."""
    seen: list[object] = []

    async def dispatch(node: Node) -> object:
        if dwell:
            await asyncio.sleep(dwell)
        seen.append(node.payload)
        if node.payload in fail:
            raise RuntimeError(f"boom {node.payload}")
        return node.payload * 10

    return dispatch, seen


# ---- IR -----------------------------------------------------------------

def test_topo_order_detects_cycles():
    g = Graph(nodes=(n(0), n(1)),
              deps={0: frozenset({1}), 1: frozenset({0})},
              hard={0: frozenset(), 1: frozenset()})
    with pytest.raises(ValueError, match="dependency cycle"):
        topo_order(g)


def test_format_graph_merges_independent_nodes_into_one_level():
    g = graph([n(0), n(1), n(2)], deps={2: {0, 1}})
    assert format_graph(g) == (
        "on_failure: skip\n[g]\n    n0\n    n1\n[g]\n    n2")


def test_format_graph_annotates_only_what_deviates():
    g = graph([n(0), n(1, on_failure="continue"), n(2, optional=True)])
    assert format_graph(g).splitlines() == [
        "on_failure: skip", "[g]", "    n0", "    n1  (continue)",
        "    n2  (optional)"]


# ---- results are values, not just outcomes ------------------------------

@pytest.mark.asyncio
async def test_dispatcher_return_values_land_in_the_report():
    dispatch, _ = recorder()
    report = await DagRunner(dispatch).execute(graph([n(0), n(1)]))
    assert report.ok
    assert {r.value for r in report.results.values()} == {0, 10}


# ---- edge semantics -----------------------------------------------------

@pytest.mark.asyncio
async def test_hard_dependency_failure_skips_the_dependent_and_cascades():
    dispatch, seen = recorder(fail={0})
    g = graph([n(0), n(1), n(2)], hard={1: {0}, 2: {1}})
    report = await DagRunner(dispatch).execute(g)
    assert [r.status for _, r in sorted(report.results.items())] == [
        "failed", "skipped", "skipped"]
    assert seen == [0]


@pytest.mark.asyncio
async def test_soft_dependency_failure_still_runs_the_dependent():
    dispatch, seen = recorder(fail={0})
    g = graph([n(0), n(1)], deps={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[1].status == "ok"
    assert seen == [0, 1]


# ---- optional: reporting only -------------------------------------------

@pytest.mark.asyncio
async def test_optional_failure_degrades_without_sparing_its_dependents():
    """`optional` answers whether the run failed and nothing else: the
    dependent skips exactly as it would behind a required node."""
    dispatch, _ = recorder(fail={0})
    g = graph([n(0, optional=True), n(1, optional=True)], hard={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert not report.ok
    assert [nd.id for nd, _ in report.degraded] == [0]
    assert report.failures == []
    assert report.results[1].status == "skipped"


@pytest.mark.asyncio
async def test_a_skipped_required_node_is_a_failure():
    """A required step that never ran leaves the sensor in the same
    state as one that ran and failed. Without this, marking a cheap
    upstream step optional would silently excuse everything behind it."""
    dispatch, _ = recorder(fail={0})
    g = graph([n(0, optional=True), n(1)], hard={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert [nd.id for nd, _ in report.failures] == [1]
    assert [nd.id for nd, _ in report.causes] == [0]     # the skip's reason


@pytest.mark.asyncio
async def test_cancelled_nodes_are_not_counted_as_failures():
    """A run that ended reports that once, at run level, rather than as
    a failure per node it did not reach."""
    dispatch, _ = recorder(fail={0})
    g = graph([n(0, on_failure="stop"), n(1)], deps={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[1].status == "cancelled"
    assert [nd.id for nd, _ in report.failures] == [0]


# ---- the on_failure ladder ----------------------------------------------

@pytest.mark.asyncio
async def test_stop_halts_dispatch_and_cancels_the_rest():
    dispatch, seen = recorder(fail={0})
    g = graph([n(0, on_failure="stop"), n(1)], deps={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[1].status == "cancelled"
    assert seen == [0]


@pytest.mark.asyncio
async def test_skip_keeps_dispatching_past_the_failure():
    dispatch, seen = recorder(fail={0})
    g = graph([n(0), n(1)], deps={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[1].status == "ok"
    assert [nd.id for nd, _ in report.failures] == [0]


@pytest.mark.asyncio
async def test_continue_lets_hard_dependents_run_anyway():
    """The dome's halt-before-close: a hard edge out of a node that
    declared `continue` orders without propagating."""
    dispatch, seen = recorder(fail={0})
    g = graph([n(0, on_failure="continue"), n(1)], hard={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[1].status == "ok"
    assert seen == [0, 1]


@pytest.mark.asyncio
async def test_continue_is_read_off_the_dependency_even_when_it_was_skipped():
    """0 -> 1(continue) -> 2: node 1 never ran, and still does not hold
    node 2 back. The alternative has node 2 running when node 1 failed
    but not when it was skipped, which is not a defensible difference."""
    dispatch, seen = recorder(fail={0})
    g = graph([n(0), n(1, on_failure="continue"), n(2)],
              hard={1: {0}, 2: {1}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[1].status == "skipped"
    assert report.results[2].status == "ok"
    assert seen == [0, 2]


@pytest.mark.asyncio
async def test_one_node_can_stop_a_run_that_skips_everywhere_else():
    """The ladder is per node, so a table's policy is a default and not
    a ceiling — the safety step halts a teardown that otherwise skips."""
    dispatch, seen = recorder(fail={1})
    g = graph([n(0), n(1, on_failure="stop"), n(2)], deps={2: {0, 1}})
    report = await DagRunner(dispatch).execute(g)
    assert report.results[2].status == "cancelled"
    assert seen == [0, 1]


# ---- timing -------------------------------------------------------------

@pytest.mark.asyncio
async def test_delay_s_defers_dispatch_after_dependencies_resolve():
    dispatch, seen = recorder()
    g = graph([n(0, delay=0.05), n(1)])
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await DagRunner(dispatch).execute(g)
    assert seen == [1, 0]                       # the delayed node lands last
    assert loop.time() - t0 >= 0.05


# ---- abort vs. cancellation ---------------------------------------------

@pytest.mark.asyncio
async def test_abort_is_absorbed_and_reported():
    dispatch, _ = recorder(dwell=0.05)
    abort = AbortSignal()
    g = graph([n(0), n(1)], deps={1: {0}})
    task = asyncio.create_task(
        DagRunner(dispatch).execute(g, abort=abort))
    await asyncio.sleep(0.01)
    abort.fire("weather")
    report = await task                          # returns; does not raise
    assert report.aborted
    assert abort.reason == "weather"


@pytest.mark.asyncio
async def test_abort_fired_before_the_run_starts_is_delivered_on_entry():
    dispatch, seen = recorder()
    abort = AbortSignal()
    abort.fire("closed for the night")
    report = await DagRunner(dispatch).execute(graph([n(0)]), abort=abort)
    assert report.aborted
    # NOTE: the pre-fired abort does NOT keep the first wave from being
    # dispatched. `_enter` cancels the run task, but `execute` then runs
    # synchronously to its first await, creating tasks for every ready
    # node on the way; the CancelledError only lands at `asyncio.wait`.
    # Instant ops therefore complete; slow ones start and are drained.
    # This is what the code does today, not necessarily what it should:
    # for an init table, level 0 is the whole connect phase.
    assert seen == [0]
    assert report.results[0].status == "ok"


@pytest.mark.asyncio
async def test_pre_fired_abort_still_starts_and_drains_the_first_wave():
    started: list[int] = []

    async def dispatch(node: Node) -> object:
        started.append(node.payload)
        await asyncio.sleep(10)
        return None

    abort = AbortSignal()
    abort.fire("closed for the night")
    g = graph([n(0), n(1), n(2)], deps={2: {0}})
    report = await DagRunner(dispatch).execute(g, abort=abort)
    assert report.aborted
    assert started == [0, 1]                     # both level-0 nodes ran
    assert all(r.status == "cancelled" for r in report.results.values())


@pytest.mark.asyncio
async def test_foreign_cancellation_propagates_after_draining():
    started = asyncio.Event()
    finished: list[int] = []

    async def dispatch(node: Node) -> object:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            finished.append(node.payload)        # drained, not orphaned
            raise
        return None

    task = asyncio.create_task(
        DagRunner(dispatch).execute(graph([n(0)]), abort=AbortSignal()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished == [0]


@pytest.mark.asyncio
async def test_an_abort_binds_to_one_run_at_a_time():
    dispatch, _ = recorder(dwell=0.05)
    abort = AbortSignal()
    runner = DagRunner(dispatch)
    g = graph([n(0)])
    task = asyncio.create_task(runner.execute(g, abort=abort))
    await asyncio.sleep(0.01)
    with pytest.raises(RuntimeError, match="already bound"):
        await runner.execute(g, abort=abort)
    await task


@pytest.mark.asyncio
async def test_a_rejected_binding_does_not_unbind_the_owning_run():
    """The double-bind check sits outside the try, so the loser's exit
    must not clear the winner's task — otherwise a later fire() would
    silently do nothing."""
    dispatch, _ = recorder(dwell=0.05)
    abort = AbortSignal()
    runner = DagRunner(dispatch)
    g = graph([n(0)])
    task = asyncio.create_task(runner.execute(g, abort=abort))
    await asyncio.sleep(0.01)
    with pytest.raises(RuntimeError):
        await runner.execute(g, abort=abort)
    abort.fire("still bound")
    report = await task
    assert report.aborted


# ---- reporting ----------------------------------------------------------

@pytest.mark.asyncio
async def test_summary_names_every_non_ok_node():
    dispatch, _ = recorder(fail={0})
    g = graph([n(0), n(1)], hard={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    summary = report.summary()
    assert "failed=1" in summary and "skipped=1" in summary
    assert "boom 0" in summary
