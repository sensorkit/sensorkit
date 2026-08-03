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

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal


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
