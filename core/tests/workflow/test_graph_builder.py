# SPDX-License-Identifier: Apache-2.0
"""
GraphBuilder: the edge bookkeeping shared by both compilers, and the
cycle check `build()` runs so that neither can forget it.
"""
from __future__ import annotations

import pytest

from sensorkit.workflow import GraphBuilder


def test_ids_are_assigned_in_insertion_order():
    g = GraphBuilder()
    assert [g.add(f"n{i}", "grp", i) for i in range(3)] == [0, 1, 2]
    assert len(g) == 3


def test_deps_are_the_union_of_soft_and_hard():
    g = GraphBuilder()
    a = g.add("a", "grp", None)
    b = g.add("b", "grp", None)
    c = g.add("c", "grp", None, soft={a}, hard={b})
    graph = g.build()
    assert graph.deps[c] == frozenset({a, b})
    assert graph.hard[c] == frozenset({b})       # soft edges never skip


def test_nodes_can_be_read_back_while_building():
    """How a phase barrier finds the steps belonging to one device."""
    g = GraphBuilder()
    g.add("a", "grp", {"ref": "tcs-1"})
    g.add("b", "grp", {"ref": "dome-1"})
    assert [i for i in range(len(g)) if g[i].payload["ref"] == "tcs-1"] == [0]


def test_require_adds_hard_edges_after_the_fact():
    """after_refs edges are only knowable once the phase is built."""
    g = GraphBuilder()
    a = g.add("a", "grp", None)
    b = g.add("b", "grp", None, soft={a})
    g.require(b, {a})
    graph = g.build()
    assert graph.hard[b] == frozenset({a})
    assert graph.deps[b] == frozenset({a})       # already there, not doubled


def test_node_attributes_pass_through():
    g = GraphBuilder()
    nid = g.add("lbl", "phase-1", "payload", optional=True, delay_s=1.5)
    node = g.build().nodes[nid]
    assert (node.label, node.group, node.payload) == ("lbl", "phase-1", "payload")
    assert node.optional is True and node.delay_s == 1.5


def test_build_rejects_a_cycle():
    g = GraphBuilder()
    a = g.add("a", "grp", None)
    b = g.add("b", "grp", None, hard={a})
    g.require(a, {b})
    with pytest.raises(ValueError, match="dependency cycle"):
        g.build()


def test_an_empty_builder_builds_an_empty_graph():
    graph = GraphBuilder().build()
    assert graph.nodes == () and graph.deps == {} and graph.hard == {}
