# SPDX-License-Identifier: Apache-2.0
"""
The executor, on hand-built graphs. Edge semantics, failure policies,
and the abort/cancellation split: the parts no compiler can prove.
"""
from __future__ import annotations


import pytest

from sensorkit.common.dag import Graph, Node


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



# ---- edge semantics -----------------------------------------------------





# ---- optional: reporting only -------------------------------------------







# ---- the on_failure ladder ----------------------------------------------











# ---- timing -------------------------------------------------------------



# ---- abort vs. cancellation ---------------------------------------------













# ---- cancellation arriving while the run drains -------------------------



















# ---- reporting ----------------------------------------------------------
