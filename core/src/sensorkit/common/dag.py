# SPDX-License-Identifier: Apache-2.0
"""Generic dependency-graph IR and executor, with no knowledge of what a node does.

* `Node` is one unit of work. Its payload is opaque, and each frontend's dispatcher
  interprets its own. `delay_s` delays dispatch once dependencies resolve.
* `Graph` is nodes plus typed edges. A soft edge only orders, and the dependent
  runs whatever the outcome. A hard edge propagates failure, so a node whose hard
  dependency did not succeed is skipped, and skips cascade.
* `GraphBuilder` accumulates nodes and edges, and checks for cycles when sealing.
* `DagRunner` executes a graph against a dispatcher and never raises for node
  failures. Outcomes are the `RunReport`, and raise policy is the frontend's.

Three independent per-node settings make up the failure model.

`Node.on_failure` is how far a failure spreads.

| value | meaning |
|---|---|
| `stop` | hard edges stay hard, and the failure stops dispatching |
| `skip` | hard edges stay hard, so dependents skip and skips cascade |
| `continue` | outgoing hard edges behave as soft |

It is read off the dependency, so a node states its own blast radius. It applies
to any resolution other than `ok`, so a skipped `continue` node still lets its
dependents run.

`Node.optional` is whether a failure fails the run, and nothing else.

`Node.override` records an outcome without dispatching. `ok` satisfies hard edges
as a success does, and `skipped` cascades as a skip does, carrying the reason and
staying out of `RunReport.failures`.

`RunReport.failures` holds non-optional nodes that failed or were skipped, since a
required step that did not run means the run did not do what it said. Cancelled
nodes are excluded, since the run reports its own cancellation.

A cancelled run cancels its in-flight nodes and waits for them, so no node's work
outlives its run. Each cancellation reaching the run, including one arriving
while it drains, is offered to the caller's `Absorbed` predicate as it arrives.
The run is reported as aborted only if the predicate claims all of them, and
otherwise the first it declines propagates once the nodes drain.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Literal

from loguru import logger

type Absorbed = Callable[[BaseException], bool]
"""Whether a cancellation reaching a run was the caller's own domain abort.

Answered by whoever executes the run, since a domain abort and a shutdown both
arrive as plain cancellation and only the caller knows which it issued. Asked
as each cancellation arrives, so it answers from the caller's intent at that
moment, and not again once one is declined. An answer is never revisited, so one
it declines is never outweighed by one it claimed. A run given none propagates
every cancellation."""

type OnFailure = Literal["stop", "skip", "continue"]
"""How far a node's failure spreads. A frontend's table-level setting is the
default its nodes take."""


@dataclass(frozen=True)
class NodeOverride:
    """What to record for a node instead of dispatching it.

    `reason` is required, since a report of a step not running has to say who
    decided.
    """

    outcome: Literal["ok", "skipped"]
    reason: str


@dataclass(frozen=True)
class Node:
    id: int
    label: str                        # one-line human description
    group: str                        # display grouping: phase / step
    payload: object                   # frontend-defined; opaque here
    on_failure: OnFailure = "stop"    # blast radius of my failure
    optional: bool = False            # True: my failure degrades, not fails
    delay_s: float = 0.0              # timed start after deps resolve
    override: NodeOverride | None = None    # set: don't dispatch, record this


@dataclass(frozen=True)
class Graph:
    nodes: tuple[Node, ...]
    deps: dict[int, frozenset[int]]   # all dependencies (scheduling order)
    hard: dict[int, frozenset[int]]   # subset whose failure skips this node

    def topo_order(self) -> list[int]:
        """Node ids in an order where every dependency precedes its dependents.

        Raises:
            ValueError: The graph has a dependency cycle. The message names one
                cycle by its node labels.
        """
        indeg = {n.id: len(self.deps[n.id]) for n in self.nodes}
        dependents: dict[int, list[int]] = {n.id: [] for n in self.nodes}

        for nid, ds in self.deps.items():
            for d in ds:
                dependents[d].append(nid)

        order = [nid for nid, deg in indeg.items() if deg == 0]
        for nid in order:
            for dep in dependents[nid]:
                indeg[dep] -= 1
                if indeg[dep] == 0:
                    order.append(dep)

        if len(order) < len(self.nodes):
            labels = {n.id: n.label for n in self.nodes}
            cycle = self._cycle(set(indeg) - set(order))
            raise ValueError("dependency cycle in graph: " + " -> ".join(
                labels[nid] for nid in cycle))

        return order

    def _cycle(self, stuck: set[int]) -> list[int]:
        """One cycle among the nodes the topological sort could not order.

        Each of them waits on another of them, so following those edges
        revisits a node.
        """
        path: list[int] = []
        nid = min(stuck)

        while nid not in path:
            path.append(nid)
            nid = min(self.deps[nid] & stuck)

        return path[path.index(nid):] + [nid]

    def format(self) -> str:
        """Dry-run view, grouping nodes into topological levels.

        The header names the prevailing `on_failure`. A node is annotated only
        where it deviates, is optional, or is overridden, and an override's
        reason replaces the rest.

        !!! warning "A level is not a barrier"

            A level is each node's earliest possible depth, and nothing
            synchronizes it. Peers on one line need not start together, and peers
            on different lines are ordered only by edges. Read edges, not levels,
            to see what waits for what.
        """
        level: dict[int, int] = {}
        for nid in self.topo_order():
            level[nid] = 1 + max((level[d] for d in self.deps[nid]), default=-1)

        groups: dict[int, list[Node]] = {}
        for n in self.nodes:
            groups.setdefault(level[n.id], []).append(n)

        prevailing = Counter(n.on_failure for n in self.nodes).most_common(1)
        default = prevailing[0][0] if prevailing else "stop"

        def marks(n: Node) -> str:
            if n.override is not None:
                return f"  (override {n.override.outcome}: {n.override.reason})"
            flags = ([n.on_failure] if n.on_failure != default else []) + (
                ["optional"] if n.optional else [])
            return f"  ({', '.join(flags)})" if flags else ""

        lines = [f"on_failure: {default}"]
        for lvl in sorted(groups):
            labels = dict.fromkeys(n.group for n in groups[lvl])
            lines.append(f"[{' + '.join(labels)}]")
            lines += [f"    {n.label}{marks(n)}" for n in groups[lvl]]

        return "\n".join(lines)


class GraphBuilder:
    """Mutable accumulator for a `Graph`.

    Ids follow insertion order, and a compiler may index back into nodes it has
    added to decide later edges. `build` freezes the edges and checks for cycles.
    """

    def __init__(self) -> None:
        self._nodes: list[Node] = []
        self._soft: list[set[int]] = []
        self._hard: list[set[int]] = []

    def __len__(self) -> int:
        return len(self._nodes)

    def __getitem__(self, nid: int) -> Node:
        return self._nodes[nid]

    def add(self, label: str, group: str, payload: object, *,
            soft: Iterable[int] = (), hard: Iterable[int] = (),
            on_failure: OnFailure = "stop", optional: bool = False,
            delay_s: float = 0.0, override: NodeOverride | None = None) -> int:
        """Append a node and return its id."""
        nid = len(self._nodes)
        self._nodes.append(Node(id=nid, label=label, group=group,
                                payload=payload, on_failure=on_failure,
                                optional=optional, delay_s=delay_s,
                                override=override))
        self._soft.append(set(soft))
        self._hard.append(set(hard))
        return nid

    def require(self, nid: int, deps: Iterable[int]) -> None:
        """Add hard edges to an already-added node, for dependencies only known
        once the rest of a phase has been built."""
        self._hard[nid] |= set(deps)

    def order(self, nid: int, deps: Iterable[int]) -> None:
        """Add soft edges to an already-added node, for ordering only known once
        the rest of a phase has been built."""
        self._soft[nid] |= set(deps)

    def build(self) -> Graph:
        graph = Graph(
            nodes=tuple(self._nodes),
            deps={i: frozenset(self._soft[i] | self._hard[i])
                  for i in range(len(self._nodes))},
            hard={i: frozenset(self._hard[i])
                  for i in range(len(self._nodes))},
        )
        graph.topo_order()      # raises on cycles
        return graph


type Dispatch = Callable[[Node], Awaitable[object]]


@dataclass
class NodeResult:
    status: Literal["ok", "failed", "skipped", "cancelled"]
    error: BaseException | None = None
    value: object = None              # whatever the dispatcher returned
    # The override reason when nothing was dispatched, this node's or the one its
    # skip follows from. Status is left as is, so edge rules read it unchanged.
    overridden: str | None = None

    def report_line(self, node: Node) -> str | None:
        """One report line for a node worth mentioning, or None for a plain
        success."""
        if self.overridden:
            head = f"    {'overridden':<10}"
            return f"{head} {node.group}: {node.label}: {self.overridden}"
        if self.status == "ok":
            return None

        why = f": {self.error}" if self.error else ""
        return f"    {self.status:<10} {node.group}: {node.label}{why}"


@dataclass
class RunReport:
    """First-class outcome of a run, including degraded outcomes that raise
    nothing."""

    name: str
    graph: Graph
    results: dict[int, NodeResult]
    aborted: bool = False

    def with_status(self, *statuses: str) -> list[tuple[Node, NodeResult]]:
        return [(n, self.results[n.id]) for n in self.graph.nodes
                if n.id in self.results and self.results[n.id].status in statuses]

    @property
    def failures(self) -> list[tuple[Node, BaseException | None]]:
        """Non-optional nodes that failed or never ran, which a frontend's raise
        policy reads.

        A skipped node carries no error, and its cause is among `causes`. Overridden
        nodes and the skips following from them are excluded.
        """
        return [(n, r.error) for n, r in self.with_status("failed", "skipped")
                if not n.optional and r.overridden is None]

    @property
    def overridden(self) -> list[tuple[Node, str]]:
        """Nodes an override kept from running, and the skips that followed, each
        with its reason."""
        return [
            (n, r.overridden)
            for n in self.graph.nodes
            if (r := self.results.get(n.id)) is not None and r.overridden is not None
        ]

    @property
    def causes(self) -> list[tuple[Node, BaseException | None]]:
        """The failures everything else followed from.

        A node skips only when a hard dependency did not succeed, so the roots are
        exactly the failed nodes.
        """
        return [(n, r.error) for n, r in self.with_status("failed")]

    @property
    def degraded(self) -> list[tuple[Node, BaseException | None]]:
        return [(n, r.error) for n, r in self.with_status("failed")
                if n.optional]

    @property
    def ok(self) -> bool:
        return not self.aborted and all(
            r.status == "ok" for r in self.results.values())

    def summary(self) -> str:
        # Overridden nodes count as their own kind, since an operator most needs to
        # tell a decided skip from a failure.
        counts = Counter("overridden" if r.overridden else r.status for r in self.results.values())

        head = f"[{self.name}] " + "  ".join(
            f"{k}={counts[k]}" for k in
            ("ok", "failed", "skipped", "overridden", "cancelled")
            if k in counts)
        if self.aborted:
            head += "  (aborted)"

        lines = [head]
        for n in self.graph.nodes:
            r = self.results.get(n.id)
            if r is not None and (line := r.report_line(n)) is not None:
                lines.append(line)

        return "\n".join(lines)


def log_summary(report: RunReport) -> None:
    """Log what a run did, since a report otherwise reaches nobody unless a
    frontend raises on it.

    Warns for anything short of a clean run, so a degraded outcome is visible
    without one.
    """
    if report.ok:
        logger.info(report.summary())
    else:
        logger.warning(report.summary())


class RunState:
    """Bookkeeping for one execution: what has resolved, what is in flight, what
    is left, and the rules for moving a node from one to the other.

    Nothing here dispatches. A runner settles the state, starts whatever is ready,
    and hands each task back as it ends. `drain` is where the in-flight ones go
    when the run is cancelled.
    """

    def __init__(self, graph: Graph,
                 results: dict[int, NodeResult] | None = None):
        self.graph = graph
        self.nodes = {n.id: n for n in graph.nodes}
        self.results: dict[int, NodeResult] = {} if results is None else results
        self.running: dict[asyncio.Task, int] = {}
        self.pending = set(self.nodes)
        self.stop = False

    def ready(self, nid: int) -> bool:
        return all(d in self.results for d in self.graph.deps[nid])

    def blockers(self, nid: int) -> list[NodeResult]:
        """Resolutions of a node's hard dependencies that keep it from running.

        Ordering edges are satisfied by any resolution, and so is a hard
        dependency that declared `on_failure="continue"`, since it states its own
        blast radius.
        """
        return [
            r
            for d in self.graph.hard[nid]
            if (r := self.results.get(d)) is not None
            and r.status != "ok"
            and self.nodes[d].on_failure != "continue"
        ]

    def skip_pass(self) -> bool:
        moved = False
        for nid in list(self.pending):
            if not (causes := self.blockers(nid)):
                continue
            # A skip following only from overridden steps is the override's doing
            # rather than a failure, so it carries the reason on and stays out of
            # `failures`. One genuine failure among the causes makes it an
            # ordinary skip again.
            excused = (causes[0].overridden
                       if all(c.overridden for c in causes) else None)
            self.results[nid] = NodeResult("skipped", overridden=excused)
            self.pending.discard(nid)
            moved = True

        return moved

    def resolve_skips(self) -> None:
        """Skip every node a hard dependency blocks, cascading along hard edges."""
        while self.skip_pass():
            pass

    def override_pass(self) -> bool:
        moved = False
        for nid in sorted(self.pending):
            if self.ready(nid) and (ov := self.nodes[nid].override) is not None:
                self.results[nid] = NodeResult(ov.outcome, overridden=ov.reason)
                self.pending.discard(nid)
                moved = True

        return moved

    def settle(self) -> None:
        """Resolve everything that resolves without dispatching.

        An overridden node can make dependents ready or skip them, and both must
        settle before anything ready dispatches. Skips resolve first each round, so
        a node whose hard dependency genuinely failed keeps its cause.
        """
        moved = True
        while moved:
            self.resolve_skips()
            if self.stop:
                return
            moved = self.override_pass()

    def take_ready(self) -> list[int]:
        """Remove and return the pending nodes whose dependencies have resolved."""
        nids = [nid for nid in sorted(self.pending) if self.ready(nid)]
        self.pending.difference_update(nids)
        return nids

    def record(self, task: asyncio.Task) -> None:
        """Record how a dispatched node ended, and whether it stops the run."""
        nid = self.running.pop(task)

        if task.cancelled():
            self.results[nid] = NodeResult("cancelled")
        elif (err := task.exception()) is not None:
            self.results[nid] = NodeResult("failed", err)
            self.stop = self.stop or self.nodes[nid].on_failure == "stop"
        else:
            self.results[nid] = NodeResult("ok", value=task.result())

    def cancel_pending(self) -> None:
        for nid in self.pending:
            self.results[nid] = NodeResult("cancelled")
        self.pending.clear()

    async def drain(self, absorbed: Absorbed | None = None
                    ) -> asyncio.CancelledError | None:
        """Cancel the in-flight nodes, wait for every one to end, then record how
        each ended.

        A cancellation reaching the drain itself is passed on to the nodes still
        running and held, so the drain never ends before its nodes do.

        Args:
            absorbed: Which of those cancellations were the caller's own domain
                abort, asked as each arrives until one is declined.

        Returns:
            The first cancellation reaching the drain that `absorbed` declined.
        """
        declined: asyncio.CancelledError | None = None

        for task in self.running:
            task.cancel()

        while not all(task.done() for task in self.running):
            try:
                await asyncio.wait(self.running)
            except asyncio.CancelledError as e:
                if declined is None and not _claims(absorbed, e):
                    declined = e

                for task in self.running:
                    task.cancel()

        for task in list(self.running):
            self.record(task)

        return declined


def _claims(absorbed: Absorbed | None,
            cancellation: asyncio.CancelledError) -> bool:
    """Whether the caller claims a cancellation as its own domain abort."""
    return absorbed is not None and absorbed(cancellation)


class DagRunner:
    """Generic DAG executor.

    The graph is the API's IR, so callers compile, inspect (`Graph.format`),
    verify, then execute.
    """

    def __init__(self, dispatch: Dispatch):
        self.dispatch = dispatch

    async def execute(self, graph: Graph, *, name: str = "",
                      absorbed: Absorbed | None = None,
                      results: dict[int, NodeResult] | None = None) -> RunReport:
        """Run a graph to its end, or until a cancellation drains it.

        Args:
            graph: What to run.
            name: What the report is called.
            absorbed: Which cancellations were the caller's own domain abort.
            results: Where each node's result is recorded as it ends. A caller
                passes its own to keep them once a cancellation propagates,
                since no report is returned then.

        Returns:
            What every node did, marked aborted where `absorbed` claimed every
            cancellation that reached the run.

        Raises:
            asyncio.CancelledError: The first cancellation `absorbed` declined,
                once every in-flight node has ended and been recorded.
        """
        state = RunState(graph, results)

        try:
            while True:
                self._start_ready(state)
                if not state.running:
                    if state.pending and not state.stop:
                        raise RuntimeError(   # unreachable: builder checks cycles
                            f"{name}: dependency deadlock")
                    break

                done, _ = await asyncio.wait(
                    state.running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    state.record(task)

            state.resolve_skips()
            state.cancel_pending()

            return RunReport(name, graph, state.results)

        except asyncio.CancelledError as cancellation:
            # Classified now, since the caller's intent may change while the
            # nodes drain. A hard cancellation is final, so nothing later is
            # asked about.
            hard = not _claims(absorbed, cancellation)

            # Drain in-flight nodes whoever cancelled, or their work outlives
            # the run.
            declined = await state.drain(None if hard else absorbed)
            state.cancel_pending()

            if hard:
                raise

            if declined is None:
                return RunReport(name, graph, state.results, aborted=True)

            # Chained to the cancellation whose drain it arrived during.
            raise declined from cancellation

    def _start_ready(self, state: RunState) -> None:
        """Settle what needs no dispatch, then dispatch whatever is left ready."""
        state.settle()
        if state.stop:
            return

        for nid in state.take_ready():
            task = asyncio.create_task(self._run_node(state.nodes[nid]))
            state.running[task] = nid

    async def _run_node(self, node: Node) -> object:
        if node.delay_s > 0:
            await asyncio.sleep(node.delay_s)
        return await self.dispatch(node)
