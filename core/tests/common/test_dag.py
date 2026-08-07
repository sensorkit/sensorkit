# SPDX-License-Identifier: Apache-2.0
"""
The executor, on hand-built graphs. Edge semantics, failure policies,
and the abort/cancellation split: the parts no compiler can prove.
"""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.common.dag import DagRunner, Graph, Node


def graph(nodes: list[Node], deps: dict[int, set[int]] | None = None,
          hard: dict[int, set[int]] | None = None) -> Graph:
    deps, hard = deps or {}, hard or {}
    ids = [n.id for n in nodes]
    all_deps = {i: frozenset(deps.get(i, set()) | hard.get(i, set())) for i in ids}
    return Graph(nodes=tuple(nodes), deps=all_deps,
                 hard={i: frozenset(hard.get(i, set())) for i in ids})


def n(i: int, *, on_failure: str = "skip", optional: bool = False,
      delay: float = 0.0) -> Node:
    """A node defaulting to `skip`, the rung that isolates edge
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
        g.topo_order()


def test_a_cycle_is_named_without_what_waits_on_it():
    """n0 is ordered and n1 only waits on the cycle, so neither is named."""
    g = graph([n(0), n(1), n(2), n(3)],
              deps={1: {0, 3}, 2: {3}, 3: {2}})

    with pytest.raises(ValueError, match="cycle in graph: n3 -> n2 -> n3$"):
        g.topo_order()


def test_format_graph_merges_independent_nodes_into_one_level():
    g = graph([n(0), n(1), n(2)], deps={2: {0, 1}})
    assert g.format() == (
        "on_failure: skip\n[g]\n    n0\n    n1\n[g]\n    n2")


def test_format_graph_annotates_only_what_deviates():
    g = graph([n(0), n(1, on_failure="continue"), n(2, optional=True)])
    assert g.format().splitlines() == [
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
    a ceiling, so the safety step halts a teardown that otherwise skips."""
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
async def test_a_run_given_no_predicate_propagates_every_cancellation():
    dispatch, _ = recorder(dwell=0.05)
    task = asyncio.create_task(DagRunner(dispatch).execute(graph([n(0)])))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---- cancellation arriving while the run drains -------------------------



class Holding:
    """A node that finishes its own teardown however often it is cancelled.

    Stands in for a call being interrupted, which the run has to wait out."""

    def __init__(self):
        self.started = asyncio.Event()
        self.draining = asyncio.Event()
        self.release = asyncio.Event()
        self.finished: list[object] = []

    async def dispatch(self, node: Node) -> object:
        self.started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.draining.set()

            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    pass

            self.finished.append(node.payload)
            raise

        return None














@pytest.mark.asyncio
async def test_node_results_are_kept_when_a_cancellation_propagates():
    dispatch, _ = recorder()
    holding = Holding()
    results: dict = {}

    async def either(node: Node) -> object:
        if node.payload == 1:
            return await holding.dispatch(node)
        return await dispatch(node)

    task = asyncio.create_task(DagRunner(either).execute(
        graph([n(0), n(1), n(2)], deps={1: {0}, 2: {1}}), results=results))

    async with asyncio.timeout(1.0):
        await holding.started.wait()

    task.cancel()
    holding.release.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert {i: r.status for i, r in results.items()} == {
        0: "ok", 1: "cancelled", 2: "cancelled"}


# ---- reporting ----------------------------------------------------------

@pytest.mark.asyncio
async def test_summary_names_every_non_ok_node():
    dispatch, _ = recorder(fail={0})
    g = graph([n(0), n(1)], hard={1: {0}})
    report = await DagRunner(dispatch).execute(g)
    summary = report.summary()
    assert "failed=1" in summary and "skipped=1" in summary
    assert "boom 0" in summary
