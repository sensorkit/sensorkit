# SPDX-License-Identifier: Apache-2.0
"""Generic dependency-graph IR and executor.

This module knows nothing about devices, phases, or frames.

* **`Node`** — one unit of work. `payload` is opaque here; each frontend defines
  its payload types and supplies the dispatcher that interprets them. `delay_s`
  sleeps after dependencies resolve, before dispatch — timed starts (e.g.
  midpoint-aligned exposures).
* **`Graph`** — nodes plus typed edges. **Soft** edges are pure ordering: the
  dependent waits for them to resolve but runs regardless of their outcome.
  **Hard** edges propagate failure: if a hard dependency did not succeed, the node
  is skipped, and skips cascade along hard edges.
* **`GraphBuilder`** — accumulates nodes and edges and seals them into a `Graph`,
  checking for cycles on the way out. Every compiler builds through it, so the
  edge bookkeeping — and remembering to run that check — is written once.
* **`DagRunner`** — executes a `Graph` against a dispatcher. It never raises for
  node failures: outcomes are the `RunReport`, and raise policy (e.g.
  `lifecycle.LifecycleError`) belongs to the frontend. Whatever the dispatcher
  returns is recorded in `NodeResult.value`, so a run's report is a record of what
  the ops measured, not only of whether they worked.

Two independent per-node questions, and keeping them independent is the whole of
the failure model:

**`Node.on_failure` — how far does my failure spread?** A ladder, and a frontend's
table-level setting is only the default every node takes.

| value | meaning |
|---|---|
| `stop` | my hard edges stay hard, and my failure stops dispatching |
| `skip` | my hard edges stay hard: dependents skip, and skips cascade |
| `continue` | my outgoing hard edges behave as soft — nothing skips because of me |

**`Node.optional` — does my failure fail the run?** Nothing else. A node may be
optional and still stop the run, or non-optional and let everything through.

And one question an author does not answer at all:

**`Node.override` — do I run at all?** Set by whoever compiled the graph rather
than by the table behind it. The node is never dispatched and `NodeOverride.outcome`
is recorded in its place: `ok` satisfies hard edges exactly as a success does,
`skipped` cascades exactly as a skip does. The cascade carries the reason forward,
so neither the node nor what follows it reaches `RunReport.failures`. Skips resolve
first, so an override can only ever remove work — a node whose hard dependency
genuinely failed still records `skipped` with its cause intact.

The ladder is read off the dependency at skip time, not off the dependent, so a
step states its own blast radius rather than every successor stating what it
tolerates. It is consulted for any non-`ok` resolution and not only for `failed`:
a `continue` node that was itself skipped still lets its dependents run, which is
what keeps the ladder monotone (otherwise a node would run when its predecessor
failed but not when the predecessor was skipped).

`RunReport.failures` — the set that decides the raise — holds non-optional nodes
that `failed` **or** were `skipped`: if a required step did not run, the run did
not do what it said, and how it came not to run is not the caller's problem.
`cancelled` is excluded, because a run that ended (aborted, or dispatch stopped)
already reports that as a run-level outcome and would otherwise report every
downstream node a second time.

A run that is cancelled — by its `abort.AbortSignal` or by anything else — cancels
its in-flight nodes and waits for them, because an op still running after its run
has gone is an orphaned device command. Its own abort is absorbed and reported
(`aborted=True`, no raise); any other cancellation propagates once the nodes are
drained.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Literal

from sensorkit.workflow.abort import AbortSignal

type OnFailure = Literal["stop", "skip", "continue"]
"""How far a node's failure spreads. A frontend's table-level setting is the
default its nodes take."""


@dataclass(frozen=True)
class NodeOverride:
    """What to record for a node instead of dispatching it.

    `reason` is required: a report saying a step did not run has to say who decided
    that.
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


class GraphBuilder:
    """Mutable accumulator for a `Graph`.

    Ids are assigned in insertion order, and a compiler may read back the nodes it
    has already added (`builder[nid]`) to decide the next one's edges — which is
    how a phase barrier finds the steps belonging to one device. `build` freezes
    the edge sets and runs the cycle check, so a compiler cannot forget it.
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
        """Add hard edges to an already-added node — for dependencies that are only
        known once the rest of a phase has been built."""
        self._hard[nid] |= set(deps)

    def build(self) -> Graph:
        graph = Graph(
            nodes=tuple(self._nodes),
            deps={i: frozenset(self._soft[i] | self._hard[i])
                  for i in range(len(self._nodes))},
            hard={i: frozenset(self._hard[i])
                  for i in range(len(self._nodes))},
        )
        topo_order(graph)       # raises on cycles
        return graph


def topo_order(graph: Graph) -> list[int]:
    indeg = {n.id: len(graph.deps[n.id]) for n in graph.nodes}
    dependents: dict[int, list[int]] = {n.id: [] for n in graph.nodes}

    for nid, ds in graph.deps.items():
        for d in ds:
            dependents[d].append(nid)

    order = [nid for nid, deg in indeg.items() if deg == 0]
    for nid in order:
        for dep in dependents[nid]:
            indeg[dep] -= 1
            if indeg[dep] == 0:
                order.append(dep)

    if len(order) < len(graph.nodes):
        raise ValueError("dependency cycle in graph")

    return order


def format_graph(graph: Graph) -> str:
    """Dry-run view: nodes grouped into topological levels.

    Derived concurrency is visible as merged levels — nodes from different groups
    sharing a level run together.

    The header names the prevailing `on_failure`, and a node is annotated only
    where it deviates from it, is optional, or has an override answering for it —
    so a teardown table reads as its policy plus the handful of exceptions, which
    is the question someone opens a dry run to answer. An override is the one
    exception a caller rather than an author introduced, so it displaces the rest
    of the annotation: a node that will not be dispatched cannot fail, and its
    reason is what a reader needs instead.

    !!! warning "A level is not a barrier"

        A level is each node's earliest possible depth. Nothing synchronises a
        level, so peers on one line need not start together and peers on different
        lines are not ordered unless an edge says so. Two equal-length frame blocks
        therefore render interleaved — `[1/3]` beside `[1/3]`, then `[2/3]` beside
        `[2/3]` — which reads like lockstep and is not: the fast camera runs its
        whole block inside the slow one's first frame. Read edges, not levels, when
        the question is what waits for what.
    """
    level: dict[int, int] = {}
    for nid in topo_order(graph):
        level[nid] = 1 + max((level[d] for d in graph.deps[nid]), default=-1)

    groups: dict[int, list[Node]] = {}
    for n in graph.nodes:
        groups.setdefault(level[n.id], []).append(n)

    prevailing = Counter(n.on_failure for n in graph.nodes).most_common(1)
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


type Dispatch = Callable[[Node], Awaitable[object]]


@dataclass
class NodeResult:
    status: Literal["ok", "failed", "skipped", "cancelled"]
    error: BaseException | None = None
    value: object = None              # whatever the dispatcher returned
    # Why nothing was dispatched: this node's own override, or the one its skip
    # follows from. The status stays what the graph believes, so every edge rule
    # reads unchanged.
    overridden: str | None = None


@dataclass
class RunReport:
    """First-class outcome of a run — including degraded outcomes that raise
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
        """Non-optional nodes that failed or never ran — the set a frontend's raise
        policy reads.

        A skipped node carries no error; its cause is among `causes`. A node an
        override answered for is excluded, and so are the skips following from one:
        not running because a caller said so is the opposite of a run that did not
        do what it said.
        """
        return [(n, r.error) for n, r in self.with_status("failed", "skipped")
                if not n.optional and r.overridden is None]

    @property
    def overridden(self) -> list[tuple[Node, str]]:
        """Nodes an override kept from running, and the skips that followed from
        one, each with its reason.

        Nothing here went wrong, which is why `failures` passes over it — what was
        not attempted is a separate question from what did not work, and a caller
        that cares asks it here.
        """
        return [(n, r.overridden) for n in self.graph.nodes
                if (r := self.results.get(n.id)) is not None
                and r.overridden is not None]

    @property
    def causes(self) -> list[tuple[Node, BaseException | None]]:
        """The failures everything else followed from.

        A node is only ever skipped because a hard dependency did not succeed, so
        the causal roots are exactly the nodes that failed — which is what an error
        message should name, however wide the skip cascade.
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
        # Overridden nodes are counted and listed as their own kind rather than
        # under the status they carry: "the dome did not open because we said so"
        # and "the dome did not open" are the two things an operator most needs to
        # tell apart.
        counts: dict[str, int] = {}
        for r in self.results.values():
            key = "overridden" if r.overridden else r.status
            counts[key] = counts.get(key, 0) + 1

        head = f"[{self.name}] " + "  ".join(
            f"{k}={counts[k]}" for k in
            ("ok", "failed", "skipped", "overridden", "cancelled")
            if k in counts)
        if self.aborted:
            head += "  (aborted)"

        lines = [head]
        for n in self.graph.nodes:
            if (r := self.results.get(n.id)) is None:
                continue
            if r.overridden:
                why = f": {r.overridden}"
            elif r.status != "ok":
                why = f": {r.error}" if r.error else ""
            else:
                continue
            status = "overridden" if r.overridden else r.status
            lines.append(f"    {status:<10} {n.group}: {n.label}{why}")

        return "\n".join(lines)


class DagRunner:
    """Generic DAG executor.

    The graph is the API's IR — callers compile, inspect (`format_graph`), verify,
    then execute.
    """

    def __init__(self, dispatch: Dispatch):
        self.dispatch = dispatch

    async def execute(self, graph: Graph, *, name: str = "",
                      abort: AbortSignal | None = None) -> RunReport:
        nodes = {n.id: n for n in graph.nodes}
        results: dict[int, NodeResult] = {}
        pending = set(nodes)
        running: dict[asyncio.Task, int] = {}
        stop = False

        def resolve_skips() -> None:
            # A node whose *hard* dependency did not succeed is skipped; skips
            # cascade along hard edges. Soft (ordering) edges are satisfied by any
            # resolution, and so is a hard dependency that declared
            # `on_failure="continue"` — the dependency states its own blast radius.
            moved = True
            while moved:
                moved = False
                for nid in list(pending):
                    causes = [r for d in graph.hard[nid]
                              if (r := results.get(d)) is not None
                              and r.status != "ok"
                              and nodes[d].on_failure != "continue"]
                    if not causes:
                        continue
                    # A skip following only from overridden steps is the override's
                    # doing rather than a failure, so it carries the reason on and
                    # stays out of `failures`. One genuine failure among the causes
                    # is enough to make it an ordinary skip again.
                    excused = (causes[0].overridden
                               if all(c.overridden for c in causes) else None)
                    results[nid] = NodeResult("skipped", overridden=excused)
                    pending.discard(nid)
                    moved = True

        def start_ready() -> None:
            # Settle everything that resolves without dispatching, then dispatch
            # what is left. An overridden node needs no await, so it can both make
            # its dependents ready and cascade skips into them — and either has to
            # happen before anything ready reaches the hook, or a dependent gets
            # dispatched in the same pass that was about to skip it. Skips resolve
            # first each time round, so a node whose hard dependency genuinely
            # failed is skipped with its cause intact and its own override never
            # consulted.
            def ready(nid: int) -> bool:
                return all(d in results for d in graph.deps[nid])

            moved = True
            while moved:
                resolve_skips()
                if stop:
                    return
                moved = False
                for nid in sorted(pending):
                    if ready(nid) and (ov := nodes[nid].override) is not None:
                        results[nid] = NodeResult(ov.outcome,
                                                  overridden=ov.reason)
                        pending.discard(nid)
                        moved = True

            for nid in sorted(pending):
                if ready(nid):
                    running[asyncio.create_task(
                        self._run_node(nodes[nid]))] = nid
                    pending.discard(nid)

        def record(task: asyncio.Task, nid: int) -> None:
            if task.cancelled():
                results[nid] = NodeResult("cancelled")
            elif (err := task.exception()) is not None:
                results[nid] = NodeResult("failed", err)
            else:
                results[nid] = NodeResult("ok", value=task.result())

        if abort is not None:
            abort._enter()      # outside the try: a rejected binding must
        try:                    # not run the _exit that unbinds its owner
            while True:
                start_ready()
                if not running:
                    if pending and not stop:
                        raise RuntimeError(   # unreachable: builder checks cycles
                            f"{name}: dependency deadlock")
                    break
                done, _ = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    nid = running.pop(task)
                    record(task, nid)
                    if (results[nid].status == "failed"
                            and nodes[nid].on_failure == "stop"):
                        stop = True

            resolve_skips()
            for nid in pending:
                results[nid] = NodeResult("cancelled")
            pending.clear()

            return RunReport(name, graph, results)

        except asyncio.CancelledError:
            # Ours or not, the in-flight ops go first: leaving them running would
            # leave hardware moving with nobody waiting.
            await self._drain(running, record)
            for nid in pending:
                results[nid] = NodeResult("cancelled")
            pending.clear()

            if abort is not None and abort._absorb():
                return RunReport(name, graph, results, aborted=True)

            raise
        finally:
            if abort is not None:
                abort._exit()

    async def _run_node(self, node: Node) -> object:
        if node.delay_s > 0:
            await asyncio.sleep(node.delay_s)
        return await self.dispatch(node)

    @staticmethod
    async def _drain(running: dict[asyncio.Task, int],
                     record: Callable[[asyncio.Task, int], None]) -> None:
        """Cancel the in-flight nodes and wait for them to finish handling it, then
        record how each ended."""
        for task in running:
            task.cancel()

        try:
            await asyncio.gather(*running, return_exceptions=True)
        except asyncio.CancelledError:
            pass          # cancelled again mid-drain: the nodes are already
                          # cancelled, so recording is all that is left — and the
                          # extra cancel stays on the count, so _absorb will
                          # decline to absorb and it gets honoured

        for task, nid in running.items():
            record(task, nid)

        running.clear()
