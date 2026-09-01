# SPDX-License-Identifier: Apache-2.0
"""Compiling a phase table against a bound sensor.

Tables are authored inline, since what a test is about is usually one ordering
rule and a whole document would bury it. The sensor is `sensor.yaml`, so what
a device supports is real and an omission is reachable through `cam-acq`.

`lower` turns steps into nodes, so the assertions read the graph. A node is
found by what it runs and where, never by an id, since emission order shifts
whenever a step ahead of it omits.
"""
from __future__ import annotations

import textwrap

import pytest
import yaml

from sensorkit.sensor.lifecycle import LifecycleWorkflow, compile_lifecycle
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.workflow import Operation, OperatorRule


def table(text: str) -> LifecycleWorkflow:
    """One table authored inline, under the name an operation id reports."""
    return LifecycleWorkflow.model_validate(
        {"name": "t", "fail_fast": True,
         **yaml.safe_load(textwrap.dedent(text))})


def compiled(text: str, facts, **kw):
    """One workflow from an inline table."""
    return compile_lifecycle(table(text), facts, **kw)


def at_node(graph, nid: int) -> str:
    """What one node runs and where, as a test reads it."""
    payload = graph.nodes[nid].payload

    return f"{payload.target.device}.{payload.command.model_tag()}"


def node(graph, label: str) -> int:
    """The node running one command on one device.

    Labelled `device.Command`, which is unique within a graph since two entries
    of one group may not reach one placement.
    """
    return next(n.id for n in graph.nodes
                if isinstance(n.payload, Operation)
                and at_node(graph, n.id) == label)


def hard_on(graph, label: str) -> set[str]:
    """What a node does not run unless it succeeded."""
    return {at_node(graph, d) for d in graph.hard[node(graph, label)]}


def soft_on(graph, label: str) -> set[str]:
    """What a node waits for, whatever the outcome."""
    nid = node(graph, label)

    return {at_node(graph, d) for d in graph.deps[nid] - graph.hard[nid]}


def running(graph) -> set[str]:
    """Everything the graph runs."""
    return {at_node(graph, n.id) for n in graph.nodes}


CONNECT_THEN_HOME = """
    phases:
      - name: connect
        entries:
          - select: {device: mount}
            ops: Connect
            id: connect-mount
          - select: {device: dome}
            ops: Connect
      - name: home
        entries:
          - select: {device: mount}
            ops: Home
    """


# Phase order


def test_a_phase_follows_the_previous_one_by_default(facts):
    graph = compiled(CONNECT_THEN_HOME, facts).graph

    assert soft_on(graph, "mount.Home") == {"mount.Connect", "dome.Connect"}
    assert hard_on(graph, "mount.Home") == set()


def test_after_names_the_phases_a_phase_follows(facts):
    graph = compiled("""
        phases:
          - name: one
            entries:
              - select: {device: mount}
                ops: Connect
          - name: two
            entries:
              - select: {device: dome}
                ops: Connect
          - name: three
            after: [one]
            entries:
              - select: {device: cover}
                ops: Connect
        """, facts).graph

    assert soft_on(graph, "cover.Connect") == {"mount.Connect"}


def test_a_phase_with_nothing_here_passes_its_predecessors_through(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
          - name: guiding
            entries: []
          - name: home
            entries:
              - select: {device: mount}
                ops: Home
        """, facts).graph

    assert soft_on(graph, "mount.Home") == {"mount.Connect"}


def test_after_naming_a_later_phase_raises(facts):
    with pytest.raises(ValueError, match="after names unknown or later"):
        compiled("""
            phases:
              - name: one
                after: [two]
                entries:
                  - select: {device: mount}
                    ops: Connect
              - name: two
                entries:
                  - select: {device: dome}
                    ops: Connect
            """, facts)


def test_two_phases_of_one_name_raise(facts):
    with pytest.raises(ValueError, match="duplicate phase name 'one'"):
        compiled("""
            phases:
              - name: one
                entries:
                  - select: {device: mount}
                    ops: Connect
              - name: one
                entries:
                  - select: {device: dome}
                    ops: Connect
            """, facts)


def test_two_entries_of_one_id_raise(facts):
    with pytest.raises(ValueError, match="duplicate entry id 'same'"):
        compiled("""
            phases:
              - name: one
                entries:
                  - select: {device: mount}
                    ops: Connect
                    id: same
              - name: two
                entries:
                  - select: {device: dome}
                    ops: Connect
                    id: same
            """, facts)


# Selection


def test_an_entry_runs_its_ops_on_every_placement_it_selects(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {instrument: true}
                ops: Connect
        """, facts).graph

    assert running(graph) == {"cam-sci.Connect", "cam-guide.Connect",
                              "cam-acq.Connect"}


def test_exclude_narrows_what_select_admitted(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {instrument: true}
                exclude: {tag: guiding}
                ops: Connect
        """, facts).graph

    assert running(graph) == {"cam-sci.Connect", "cam-acq.Connect"}


def test_an_entry_selecting_nothing_raises(facts):
    with pytest.raises(ValueError, match="entry selects no device"):
        compiled("""
            phases:
              - name: connect
                entries:
                  - select: {tag: absent}
                    ops: Connect
            """, facts)


def test_two_entries_of_one_phase_reaching_one_placement_raise(facts):
    with pytest.raises(ValueError,
                       match="'mount' is reached by two entries"):
        compiled("""
            phases:
              - name: connect
                entries:
                  - select: {kind: any}
                    ops: Connect
                  - select: {device: mount}
                    ops: Home
            """, facts)


def test_overlap_is_rejected_before_anything_about_capability(facts):
    # `cam-acq` cannot deinit, and the default is to refuse that. The table is
    # still malformed for a reason the hardware has no say in.
    with pytest.raises(ValueError,
                       match="'cam-acq' is reached by two entries"):
        compiled("""
            phases:
              - name: park
                entries:
                  - select: {instrument: true}
                    ops: Deinit
                  - select: {device: cam-acq}
                    ops: Deinit
            """, facts)


# Operations at one placement


def test_ops_run_serially_at_each_placement(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: cam-sci}
                ops: [Connect, Init]
        """, facts).graph

    assert hard_on(graph, "cam-sci.Init") == {"cam-sci.Connect"}


def test_sequence_completion_waits_only_for_the_attempt(facts):
    graph = compiled("""
        phases:
          - name: park
            entries:
              - select: {device: cam-sci}
                ops:
                  - Deinit
                  - command: Disconnect
                    sequence: completion
        """, facts).graph

    assert soft_on(graph, "cam-sci.Disconnect") == {"cam-sci.Deinit"}
    assert hard_on(graph, "cam-sci.Disconnect") == set()


def test_placements_of_one_entry_do_not_wait_on_each_other(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {instrument: true}
                ops: Connect
        """, facts).graph

    assert graph.deps[node(graph, "cam-guide.Connect")] == frozenset()


def test_an_entry_head_carries_the_phase_order_and_the_rest_inherit(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
          - name: home
            entries:
              - select: {device: pickoff}
                ops: [Connect, Home]
        """, facts).graph

    assert soft_on(graph, "pickoff.Connect") == {"mount.Connect"}
    assert soft_on(graph, "pickoff.Home") == set()


# require


















# Joins










# What reaches the node


def test_fail_fast_resolves_operation_then_phase_then_table(facts):
    graph = compiled("""
        fail_fast: true
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
          - name: park
            fail_fast: false
            entries:
              - select: {device: cam-sci}
                ops:
                  - Deinit
                  - command: Disconnect
                    fail_fast: true
        """, facts).graph

    assert graph.nodes[node(graph, "mount.Connect")].on_failure == "stop"
    assert graph.nodes[node(graph, "cam-sci.Deinit")].on_failure == "skip"
    assert graph.nodes[node(graph, "cam-sci.Disconnect")].on_failure == "stop"


def test_optional_and_the_authored_deadline_reach_the_node(facts):
    graph = compiled("""
        phases:
          - name: home
            entries:
              - select: {device: mount}
                ops:
                  - command: Home
                    optional: true
                    timeout_s: 12.0
        """, facts).graph
    placed = graph.nodes[node(graph, "mount.Home")]

    assert placed.optional
    assert placed.payload.timeout_s == 12.0


def test_a_deadline_rule_reaches_the_operation(facts, definition):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
        """, facts, deadlines=definition.deadlines).graph

    assert graph.nodes[node(graph, "mount.Connect")].payload.timeout_s == 30.0


def test_an_unsupported_command_omits_where_the_entry_says_so(facts):
    workflow = compiled("""
        phases:
          - name: initialize
            entries:
              - select: {instrument: true}
                ops:
                  - command: Init
                    unsupported: omit
        """, facts)

    assert running(workflow.graph) == {"cam-sci.Init", "cam-guide.Init"}
    assert [o.target.device for o in workflow.omissions] == ["cam-acq"]


def test_an_unsupported_command_is_an_error_by_default(facts):
    with pytest.raises(ValueError,
                       match="'cam-acq' does not support 'Init'"):
        compiled("""
            phases:
              - name: initialize
                entries:
                  - select: {instrument: true}
                    ops: Init
            """, facts)


def test_an_operation_id_names_the_line_and_the_device(facts):
    graph = compiled("""
        phases:
          - name: park
            entries:
              - select: {device: cam-sci}
                ops: [Deinit, Disconnect]
                id: park-science
        """, facts).graph

    assert graph.nodes[node(graph, "cam-sci.Deinit")].payload.id == (
        "t/park/park-science/0@cam-sci")
    assert graph.nodes[node(graph, "cam-sci.Disconnect")].payload.id == (
        "t/park/park-science/1@cam-sci")


def test_an_operator_rule_amends_the_operations_it_addresses(facts):
    rule = OperatorRule(reason="mount is down", select=IsRef(device="mount"),
                        outcome="skipped")
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {kind: any}
                ops: Connect
        """, facts, rules=(rule,)).graph
    override = graph.nodes[node(graph, "mount.Connect")].override

    assert override.outcome == "skipped"
    assert override.reason == "mount is down"
    assert graph.nodes[node(graph, "dome.Connect")].override is None


def test_a_compiled_table_reports_the_facts_it_stood_on(facts):
    workflow = compiled(CONNECT_THEN_HOME, facts)

    assert workflow.name == "t"
    assert "tests" in workflow.provenance


# Inheriting across an omission








# Cleanup


























# The document the package ships its tests against
