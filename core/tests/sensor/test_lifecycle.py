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


def test_a_clause_waits_on_an_entry_of_an_earlier_phase(facts):
    graph = compiled("""
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
                require: connect-mount
        """, facts).graph

    assert hard_on(graph, "mount.Home") == {"mount.Connect"}
    assert soft_on(graph, "mount.Home") == set()


def test_a_clause_on_completion_waits_only_for_the_attempt(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
                id: connect-mount
          - name: home
            entries:
              - select: {device: mount}
                ops: Home
                require:
                  - name: connect-mount
                    on: completion
        """, facts).graph

    assert soft_on(graph, "mount.Home") == {"mount.Connect"}
    assert hard_on(graph, "mount.Home") == set()


def test_a_clause_refines_a_followed_phase_and_leaves_the_others(facts):
    graph = compiled("""
        phases:
          - name: one
            entries:
              - select: {device: mount}
                ops: Connect
                id: connect-mount
              - select: {device: dome}
                ops: Connect
          - name: two
            entries:
              - select: {device: cover}
                ops: Connect
          - name: home
            after: [one, two]
            entries:
              - select: {device: mount}
                ops: Home
                require: connect-mount
        """, facts).graph

    assert hard_on(graph, "mount.Home") == {"mount.Connect"}
    assert soft_on(graph, "mount.Home") == {"cover.Connect"}


def test_refinement_is_per_entry(facts):
    graph = compiled("""
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
                require: connect-mount
              - select: {device: pickoff}
                ops: Home
        """, facts).graph

    assert soft_on(graph, "mount.Home") == set()
    assert soft_on(graph, "pickoff.Home") == {"mount.Connect", "dome.Connect"}


def test_a_clause_naming_a_followed_phase_refines_it(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {kind: any}
                ops: Connect
          - name: home
            entries:
              - select: {device: wheel}
                ops: Home
                require:
                  - name: connect
                    join: same-device
        """, facts).graph

    assert hard_on(graph, "wheel.Home") == {"wheel.Connect"}
    assert soft_on(graph, "wheel.Home") == set()


def test_a_clause_may_name_a_later_peer(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
                require: later
              - select: {device: dome}
                ops: Connect
                id: later
        """, facts).graph

    assert hard_on(graph, "mount.Connect") == {"dome.Connect"}


def test_a_clause_naming_an_unknown_target_raises(facts):
    with pytest.raises(ValueError, match="unknown or later phase/entry"):
        compiled("""
            phases:
              - name: connect
                entries:
                  - select: {device: mount}
                    ops: Connect
                    require: ghost
            """, facts)


def test_a_clause_naming_a_later_phase_raises(facts):
    with pytest.raises(ValueError, match="unknown or later phase/entry 'two'"):
        compiled("""
            phases:
              - name: one
                entries:
                  - select: {device: mount}
                    ops: Connect
                    require: two
              - name: two
                entries:
                  - select: {device: dome}
                    ops: Connect
            """, facts)


# Joins


def test_same_device_narrows_to_this_placement_device(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {kind: any}
                ops: Connect
                id: connect-all
          - name: home
            entries:
              - select: {supports: Home}
                ops: Home
                require:
                  - name: connect-all
                    join: same-device
        """, facts).graph

    assert hard_on(graph, "foc-sci.Home") == {"foc-sci.Connect"}
    assert hard_on(graph, "mount.Home") == {"mount.Connect"}


def test_same_chain_narrows_to_what_a_placement_looks_through(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {kind: any}
                ops: Connect
                id: connect-all
          - name: initialize
            entries:
              - select: {device: cam-sci}
                ops: Init
                require:
                  - name: connect-all
                    join: same-chain
        """, facts).graph

    assert hard_on(graph, "cam-sci.Init") == {
        "mount.Connect", "dome.Connect", "cover.Connect", "pickoff.Connect",
        "foc-sci.Connect", "wheel.Connect", "cam-sci.Connect"}


def test_a_clause_matching_no_step_raises(facts):
    with pytest.raises(ValueError, match="require 'empty' matches no step"):
        compiled("""
            phases:
              - name: empty
                entries: []
              - name: home
                after: []
                entries:
                  - select: {device: mount}
                    ops: Home
                    require: empty
            """, facts)


def test_a_join_emptying_a_clause_raises(facts):
    with pytest.raises(ValueError,
                       match="join='same-device' matches no step for 'dome'"):
        compiled("""
            phases:
              - name: connect
                entries:
                  - select: {device: mount}
                    ops: Connect
                    id: connect-mount
              - name: enable
                entries:
                  - select: {device: dome}
                    ops: Enable
                    require:
                      - name: connect-mount
                        join: same-device
            """, facts)


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


def test_a_compiled_table_takes_its_name(facts):
    assert compiled(CONNECT_THEN_HOME, facts).name == "t"


# Inheriting across an omission


def test_a_serial_op_inherits_what_an_omitted_predecessor_waited_for(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
          - name: park
            entries:
              - select: {device: cam-acq}
                ops:
                  - command: Init
                    unsupported: omit
                  - Connect
        """, facts).graph

    assert soft_on(graph, "cam-acq.Connect") == {"mount.Connect"}


def test_a_success_dependency_through_an_omission_weakens_to_completion(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
                id: connect-mount
          - name: park
            entries:
              - select: {device: cam-acq}
                ops:
                  - command: Init
                    unsupported: omit
                  - Connect
                require:
                  - name: connect-mount
                    on: completion
        """, facts).graph

    assert hard_on(graph, "cam-acq.Connect") == set()
    assert soft_on(graph, "cam-acq.Connect") == {"mount.Connect"}


def test_a_clause_naming_an_entry_that_wholly_omitted_inherits_it(facts):
    graph = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
                id: connect-mount
          - name: initialize
            entries:
              - select: {device: cam-acq}
                ops:
                  - command: Init
                    unsupported: omit
                id: init-acq
                require: connect-mount
          - name: home
            entries:
              - select: {device: pickoff}
                ops: Home
                require: init-acq
        """, facts).graph

    assert hard_on(graph, "pickoff.Home") == {"mount.Connect"}


# Cleanup


TEARDOWN = """
    phases:
      - name: initialize
        entries:
          - select: {device: cam-sci}
            ops: Init
            id: init-science
    cleanup:
      - name: halt
        when: failure
        timeout_s: 20.0
        armed_by: [init-science]
        entries:
          - select: {device: mount}
            ops: Stop
    """


def test_a_cleanup_spec_becomes_its_own_graph(facts):
    workflow = compiled(TEARDOWN, facts)
    halt = workflow.cleanup[0]

    assert running(halt.graph) == {"mount.Stop"}
    assert (halt.when, halt.timeout_s) == ("failure", 20.0)
    assert halt.origin.source == "halt"


def test_arming_resolves_to_the_operations_the_named_entries_became(facts):
    workflow = compiled(TEARDOWN, facts)
    armed = workflow.cleanup[0].armed_by
    graph = workflow.graph

    assert armed == (graph.nodes[node(graph, "cam-sci.Init")].payload,)


def test_a_cleanup_arming_on_nothing_is_unconditional(facts):
    workflow = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
        cleanup:
          - name: halt
            entries:
              - select: {device: mount}
                ops: Stop
        """, facts)

    assert workflow.cleanup[0].armed_by is None


def test_a_cleanup_arming_on_an_empty_list_is_never_armed(facts):
    workflow = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
        cleanup:
          - name: halt
            armed_by: []
            entries:
              - select: {device: mount}
                ops: Stop
        """, facts)

    assert workflow.cleanup[0].armed_by == ()


def test_arming_only_on_omitted_operations_stays_never_armed(facts):
    workflow = compiled("""
        phases:
          - name: initialize
            entries:
              - select: {device: cam-acq}
                ops:
                  - command: Init
                    unsupported: omit
                id: init-acq
        cleanup:
          - name: halt
            armed_by: [init-acq]
            entries:
              - select: {device: mount}
                ops: Stop
        """, facts)

    assert workflow.cleanup[0].armed_by == ()


def test_arming_on_an_entry_no_phase_declares_raises(facts):
    with pytest.raises(ValueError, match="armed_by names entries no phase"):
        compiled("""
            phases:
              - name: connect
                entries:
                  - select: {device: mount}
                    ops: Connect
            cleanup:
              - name: halt
                armed_by: [ghost]
                entries:
                  - select: {device: mount}
                    ops: Stop
            """, facts)


def test_a_cleanup_entry_orders_itself_against_its_own_peers(facts):
    workflow = compiled("""
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
        cleanup:
          - name: halt
            entries:
              - select: {device: mount}
                ops: Stop
                require: disconnected
              - select: {device: dome}
                ops: Disconnect
                id: disconnected
        """, facts)
    graph = workflow.cleanup[0].graph

    assert hard_on(graph, "mount.Stop") == {"dome.Disconnect"}


def test_a_cleanup_clause_naming_something_outside_it_raises(facts):
    with pytest.raises(ValueError,
                       match="require names entries outside it, connected"):
        compile_lifecycle(
            table("""
                phases:
                  - name: connect
                    entries:
                      - select: {device: mount}
                        ops: Connect
                        id: connected
                cleanup:
                  - name: halt
                    entries:
                      - select: {device: mount}
                        ops: Stop
                        require: connected
                """), facts)


def test_two_cleanup_entries_reaching_one_placement_raise(facts):
    with pytest.raises(ValueError,
                       match="cleanup 'halt': 'mount' is reached by two"):
        compiled("""
            phases:
              - name: connect
                entries:
                  - select: {device: mount}
                    ops: Connect
            cleanup:
              - name: halt
                entries:
                  - select: {device: mount}
                    ops: Stop
                  - select: {kind: any}
                    ops:
                      - command: Disconnect
                        unsupported: omit
            """, facts)


def test_a_cleanup_takes_the_table_failure_policy(facts):
    workflow = compiled("""
        fail_fast: false
        phases:
          - name: connect
            entries:
              - select: {device: mount}
                ops: Connect
        cleanup:
          - name: halt
            entries:
              - select: {device: mount}
                ops: Stop
        """, facts)
    graph = workflow.cleanup[0].graph

    assert graph.nodes[node(graph, "mount.Stop")].on_failure == "skip"


def test_an_operator_rule_reaches_a_cleanup_graph(facts):
    rule = OperatorRule(reason="mount is down", select=IsRef(device="mount"),
                        outcome="skipped")
    workflow = compiled(TEARDOWN, facts, rules=(rule,))
    graph = workflow.cleanup[0].graph

    assert graph.nodes[node(graph, "mount.Stop")].override.outcome == "skipped"


# The document the package ships its tests against


def test_the_authored_document_compiles(facts, definition):
    compiled_tables = [compile_lifecycle(t, facts,
                                         deadlines=definition.deadlines)
                       for t in definition.tables]

    assert [w.name for w in compiled_tables] == ["bring-up", "shutdown"]


def test_the_bring_up_table_orders_itself_as_authored(facts, definition):
    workflow = compile_lifecycle(definition.tables[0], facts,
                                 deadlines=definition.deadlines)
    graph = workflow.graph

    # `init-instruments` requires `connect-all` on the same chain, so it waits
    # on what `cam-sci` looks through and on nothing behind the other port.
    assert hard_on(graph, "cam-sci.Init") == {
        "mount.Connect", "dome.Connect", "cover.Connect", "pickoff.Connect",
        "foc-sci.Connect", "wheel.Connect", "cam-sci.Connect"}
    assert soft_on(graph, "cam-sci.Init") == set()
    assert workflow.cleanup[0].when == "failure"
