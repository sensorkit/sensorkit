# SPDX-License-Identifier: Apache-2.0
"""Explaining a definition with no device facts and a compiled workflow.

A definition audit is read through its findings, since whether a check was
answered or deferred is the claim. A workflow audit is read through the facts
its description states about one node or one section, found by the name the
audit gives it, so a case asserts what was said and not how it was laid out.

Workflows are compiled from real tables and intents against `sensor.yaml`, and
nothing runs, since rendering needs no hardware.
"""
from __future__ import annotations

import textwrap

import pytest
import yaml

from sensorkit.sensor.audit import AuditReport, audit_definition, audit_workflow
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    RequestEpoch,
    compile_collect,
    pack,
)
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.lifecycle import LifecycleWorkflow, compile_lifecycle
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.topology import Device, Structure, Unit
from sensorkit.sensor.workflow import (
    DeadlineRule,
    ExecutableWorkflow,
    Operation,
    OperatorRule,
)
from sensorkit.std.collect import CameraParameterSet, Collect
from sensorkit.std.traits import Stop

from .common import REPORTED, TARGET, operation, pointing, snapshot_of


def table(text: str, name: str = "t", fail_fast: bool = True
          ) -> LifecycleWorkflow:
    return LifecycleWorkflow.model_validate(
        {"name": name, "fail_fast": fail_fast,
         **yaml.safe_load(textwrap.dedent(text))})


def block(report: AuditReport, header: str) -> list[str]:
    """The lines an audit says under one heading, the heading included.

    A heading is the start of a line, and what belongs to it is indented
    deeper.
    """
    lines = report.description.splitlines()
    start = next(i for i, line in enumerate(lines)
                 if line.strip().startswith(header))
    depth = len(lines[start]) - len(lines[start].lstrip())
    held = [lines[start]]

    for line in lines[start + 1:]:
        if len(line) - len(line.lstrip()) <= depth:
            break

        held.append(line)

    return held


def node(report: AuditReport, name: str) -> list[str]:
    """The lines an audit says about one node, by the name it gives it."""
    return block(report, f"{name}, in ")


def statuses(report: AuditReport) -> dict[tuple, str]:
    return {(f.origin.source, *f.origin.path): f.status
            for f in report.findings}


# The definition


NESTED = """
    sensor:
      name: nested
      components:
        - device: mount
          traits: MustConnect
        - device: cam
          instrument: true
          tags: science
    tables:
      t:
        fail_fast: false
        phases:
          - name: p
            fail_fast: true
            entries:
              - select:
                  all_of:
                    - MustConnect
                    - not: {any_of: [{tag: science}, {device: cam}]}
                exclude: {not: {supports: Home}}
                ops:
                  - command: Connect
                    fail_fast: false
                  - command: Disconnect
                    sequence: completion
                    timeout_s: 12.0
    deadlines:
      - trait: MustConnect
        command: Connect
        seconds: 30.0
    """


def test_a_valid_definition_answers_what_it_can_and_defers_the_rest():
    report = audit_definition(
        SensorDefinition.from_yaml(textwrap.dedent(NESTED)))
    found = statuses(report)

    assert found[("structure",)] == "valid"
    assert found[("t",)] == "valid"
    # The trait assertion, the entry's reach and the trait deadline all wait on
    # binding.
    assert found[("structure", "mount")] == "deferred"
    assert found[("t", "p", 0)] == "deferred"
    assert found[("deadlines", 0)] == "deferred"
    assert "invalid" not in found.values()


def test_a_selection_is_rendered_as_authored_including_negation():
    report = audit_definition(
        SensorDefinition.from_yaml(textwrap.dedent(NESTED)))
    reach = next(f for f in report.findings if f.origin.path == ("p", 0))
    entry = block(report, "entry 0")

    selected = ("all of (trait MustConnect, not (any of (tag science, "
                "device cam)))")
    assert f"match {selected} excluding not (supports Home)" in reach.message
    assert f"      select {selected}" in entry
    assert "      exclude not (supports Home)" in entry


def test_policy_sequencing_and_timeouts_say_where_they_came_from():
    report = audit_definition(
        SensorDefinition.from_yaml(textwrap.dedent(NESTED)))
    connect, disconnect = [line for line in block(report, "entry 0")
                           if line.startswith("        ")]

    assert "not fail-fast, as the operation states" in connect
    assert "first at each target" in connect
    assert "deadline from the rules" in connect
    assert "fail-fast, from the phase" in disconnect
    assert "after the completion of the previous" in disconnect
    assert "deadline 12 s, explicit" in disconnect
    assert "Connect on trait MustConnect, 30 s" in report.description


def test_a_deferred_selection_names_no_target_and_counts_none(definition):
    # Nothing on this sensor satisfies the trait, and an audit cannot know it.
    tables = (table("""
        phases:
          - name: p
            entries:
              - select: {supports: Home}
                ops: Home
              - select: Nowhere
                ops: Connect
        """),)
    report = audit_definition(definition.model_copy(update={"tables": tables}))
    reaches = [f for f in report.findings if f.origin.source == "t"
               and f.status == "deferred"]
    devices = ("mount", "dome", "cover", "pickoff", "foc-sci", "wheel",
               "cam-sci", "cam-guide", "cam-acq")

    assert statuses(report)[("t",)] == "valid"
    assert len(reaches) == 2

    for finding in reaches:
        assert "answered at binding" in finding.message
        assert not any(device in finding.message for device in devices)
        assert not any(c.isdigit() for c in finding.message)

    for position in (0, 1):
        assert "      targets answered at binding" in block(
            report, f"entry {position}")


def test_structural_and_reference_errors_are_reported_where_they_are():
    structure = Structure(name="twice", components=(
        Device(device="cam"),
        Unit(unit="bench", components=(Device(device="cam"),))))
    definition = SensorDefinition(sensor=structure, tables=(
        table("""
            phases:
              - name: p
                entries:
                  - select: {kind: any}
                    ops: Connect
                    require: nowhere
            """, name="dangling"),
        table("""
            phases:
              - name: p
                entries:
                  - select: {kind: any}
                    ops: Connect
            cleanup:
              - name: halt
                armed_by: [missing]
                entries:
                  - select: {kind: any}
                    ops: Stop
            """, name="unarmable"),
        table("""
            phases:
              - name: p
                entries:
                  - select: {kind: any}
                    ops: Connect
            """, name="fine")))
    report = audit_definition(definition)
    invalid = {f.origin.source: f.message for f in report.findings
               if f.status == "invalid"}

    assert set(invalid) == {"structure", "dangling", "unarmable"}
    assert "'cam' is placed twice" in invalid["structure"]
    assert "require names nothing: nowhere" in invalid["dangling"]
    assert "armed_by names nothing: missing" in invalid["unarmable"]
    assert statuses(report)[("fine",)] == "valid"
    # What was authored is still explained.
    assert "  unit bench" in report.description


def test_a_document_level_error_is_reported_once(definition):
    once = table("""
        phases:
          - name: p
            entries:
              - select: {kind: any}
                ops: Connect
        """, name="twice")
    report = audit_definition(
        definition.model_copy(update={"tables": (once, once)}))
    invalid = [f for f in report.findings if f.status == "invalid"]

    assert [(f.origin.source, f.message) for f in invalid] == [
        ("definition", "a table is named twice: twice")]


def test_a_deadline_rule_for_an_absent_device_is_reported_once(definition):
    stray = DeadlineRule(target=("device", "cam-spare"), command="Init",
                         seconds=10.0)
    report = audit_definition(definition.model_copy(
        update={"deadlines": (*definition.deadlines, stray)}))
    invalid = [f for f in report.findings if f.status == "invalid"]

    assert [(f.origin.source, f.message) for f in invalid] == [
        ("deadlines", "deadline rules target devices the structure does not "
                      "hold: cam-spare")]
    assert "  Init on device cam-spare, 10 s" in report.description


def test_deadline_rules_for_placed_devices_are_valid(definition):
    assert statuses(audit_definition(definition))[("deadlines",)] == "valid"


def test_cleanup_declarations_show_eligibility_and_arming(definition):
    report = audit_definition(definition)
    halt = block(report, "cleanup 'halt'")

    assert "runs after a required failure" in halt[0]
    assert "    total deadline 60 s" in halt
    assert "    armed once any operation of 'init-instruments' is attempted" in (
        halt)
    assert "requires 'connect-all' on success, joining same-chain, in place " \
           "of the wait on all of 'connect'" in report.description


# The compiled workflow


def test_success_and_completion_read_apart_where_levels_look_alike(facts):
    workflow = compile_lifecycle(table("""
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
                require: connect-mount
              - select: {device: dome}
                ops: Connect
        """), facts)
    home = operation(workflow, "Home", "mount")
    dome = operation(workflow, "Connect", "dome")
    ids = {n.payload: n.id for n in workflow.graph.nodes}
    report = audit_workflow(workflow)

    # One predecessor each, so both sit on one level.
    assert workflow.graph.deps[ids[home]] == workflow.graph.deps[ids[dome]]
    assert ("      needs the success of t/connect/connect-mount/0@mount"
            in node(report, home.id))
    assert ("      waits for the completion of t/connect/connect-mount/0@mount"
            in node(report, dome.id))


@pytest.fixture(scope="module")
def collecting(topology) -> BoundSensor:
    """`sensor.yaml` with a mount that tracks, a pickoff that can be
    positioned, and two guide-port cameras that capture."""
    sensor, _ = BoundSensor.bind(topology, snapshot_of({
        **REPORTED,
        "mount": (REPORTED["mount"][0] + ("FollowTarget",), ()),
        "pickoff": (REPORTED["pickoff"][0] + ("SelectPort",), ()),
        "cam-guide": (REPORTED["cam-guide"][0] + ("CameraCapture",), ()),
        "cam-acq": (REPORTED["cam-acq"][0] + ("CameraCapture",), ()),
    }))

    return sensor


@pytest.fixture(scope="module")
def darks(collecting) -> ExecutableWorkflow:
    """Pointing twice under different deadlines, then two guide cameras
    aligned on their midpoints, one of them taking two darks."""
    intent = CollectIntent(
        name="c", prepare=(pointing(timeout_s=120.0),),
        cleanup=(CommandRequest(command=Stop(), subject="sensor"),),
        epochs=(RequestEpoch(
            align="midpoint", settings=(pointing(),),
            units=(
                InstrumentRequest(
                    id="darks", select=IsRef(device="cam-guide"),
                    acquisition=AcquisitionRequest(integration_time_s=4.0,
                                                   count=2),
                    collect=Collect(target=TARGET, target_id="dark",
                                    params=CameraParameterSet(integration_time_seconds=4.0,
                                                              frame_count=2))),
                InstrumentRequest(
                    id="light", select=IsRef(device="cam-acq"),
                    acquisition=AcquisitionRequest(integration_time_s=2.0,
                                                   timeout_s=9.0)))),))
    deadlines = (DeadlineRule(target=("any", None), command="CameraCapture",
                              seconds=30.0),)

    return compile_collect(pack(intent, collecting), collecting,
                           deadlines=deadlines)


def frames(workflow: ExecutableWorkflow, device: str) -> list[Operation]:
    return [n.payload for n in workflow.graph.nodes
            if isinstance(n.payload, Operation)
            and n.payload.target.device == device
            and n.payload.acquisition is not None]


def test_deadlines_policy_delays_and_ordering_nodes(darks):
    report = audit_workflow(darks)
    align = block(report, "ordering 'align midpoints")
    first, second = frames(darks, "cam-guide")
    light = frames(darks, "cam-acq")[0]

    # An ordering node carries no payload, and the audit says so.
    assert "      sends nothing, and resolves once what it waits on has" in (
        align)
    assert "      deadline 30 s" in node(report, first.id)
    assert "      deadline 9 s" in node(report, light.id)
    assert ("      on failure, stops dispatching anything further in its "
            "graph, fail-fast") in node(report, first.id)
    # The shorter block starts late, so both midpoints coincide.
    assert "      starts 3 s after what it waits on has resolved" in node(
        report, light.id)
    assert not any("starts" in line for line in node(report, first.id))
    assert not any("starts" in line for line in node(report, second.id))
    assert ("      waits for the completion of ordering 'align midpoints of "
            "epoch 0'") in node(report, light.id)


def test_operations_that_read_alike_are_told_apart_by_origin(darks):
    report = audit_workflow(darks)
    prepared, opening = [n for n in darks.graph.nodes
                         if isinstance(n.payload, Operation)
                         and n.payload.target.device == "mount"]

    assert prepared.label == opening.label
    assert prepared.payload.id != opening.payload.id
    assert "      deadline 120 s" in node(report, prepared.payload.id)
    assert "      no deadline" in node(report, opening.payload.id)


def test_acquisitions_show_frames_keywords_and_when_the_header_is_taken(darks):
    report = audit_workflow(darks)

    for index, frame in enumerate(frames(darks, "cam-guide")):
        said = node(report, frame.id)
        number = frame.acquisition.frame_number

        assert (f"      acquisition of request 'darks', index {index}, frame "
                f"{number}") in said
        assert any('"target_id": "dark"' in line for line in said)
        assert any(f'"frame_number": {number}' in line for line in said)
        assert ("      header sampled at dispatch, so the command above is "
                "planned and is not the header-bearing command sent") in said


def test_omissions_and_overrides_read_apart(facts, definition):
    rule = OperatorRule(reason="mount is down", select=IsRef(device="mount"),
                        commands=("Home",), outcome="skipped")
    workflow = compile_lifecycle(definition.tables[0], facts,
                                 deadlines=definition.deadlines, rules=(rule,))
    report = audit_workflow(workflow)
    omitted = block(report, "omitted at compile")
    overrides = block(report, "operator overrides")
    home = operation(workflow, "Home", "mount")

    assert any(line.startswith("  Init on cam-acq @ ota/guide/cam-acq, from "
                               "bring-up/initialize/init-instruments/0")
               and "'cam-acq' does not support 'Init'" in line
               for line in omitted)
    assert not any("cam-acq" in line for line in overrides)
    assert overrides[1:] == [f"  {home.id}, skipped, because mount is down"]
    assert not any(home.id in line for line in omitted)
    assert ("      overridden, recorded skipped without dispatching, because "
            "mount is down") in node(report, home.id)


ARMING = """
    phases:
      - name: connect
        entries:
          - select: {device: mount}
            ops: Connect
            id: connect-mount
          - select: {device: cam-acq}
            ops: {command: Init, unsupported: omit}
            id: init-acq
    cleanup:
      - name: unconditional
        entries: [{select: {device: mount}, ops: Stop}]
      - name: never
        when: cancelled
        armed_by: []
        entries: [{select: {device: mount}, ops: Stop}]
      - name: triggered
        when: failure_or_cancelled
        timeout_s: 20.0
        armed_by: [connect-mount]
        entries: [{select: {device: mount}, ops: Stop}]
      - name: lost-trigger
        when: failure
        armed_by: [init-acq]
        entries: [{select: {device: mount}, ops: Stop}]
    """


def test_cleanup_eligibility_and_every_arming_state(facts):
    workflow = compile_lifecycle(table(ARMING), facts)
    report = audit_workflow(workflow)
    connect = operation(workflow, "Connect", "mount")
    never = block(report, "cleanup 'never'")
    triggered = block(report, "cleanup 'triggered'")

    assert "runs however the run ended" in block(
        report, "cleanup 'unconditional'")[0]
    assert "  armed unconditionally" in block(report,
                                              "cleanup 'unconditional'")
    assert "runs after a domain abort" in never[0]
    assert "  never armed, so it will not run" in never
    assert "a domain abort or both, and once where both hold" in triggered[0]
    assert "  total deadline 20 s, beside each command's own" in triggered
    assert triggered[2:4] == ["  armed once any of these is attempted",
                              f"    {connect.id}"]
    # Every named trigger omitted at compile, and the cleanup stays unarmed.
    assert "  never armed, so it will not run" in block(
        report, "cleanup 'lost-trigger'")


def test_provenance_reaches_both_families(facts, definition, darks):
    lifecycle = compile_lifecycle(definition.tables[1], facts)

    for workflow in (lifecycle, darks):
        lines = audit_workflow(workflow).description.splitlines()

        assert lines[1] == f"compiled against {workflow.provenance}"
        assert "taken 2026-01-01" in lines[1]
        assert "not show that the hardware is unchanged now" in lines[2]


def carried(payload: Operation | None) -> tuple | None:
    """What one node's payload holds, as values to compare."""
    if payload is None:
        return None

    acquisition = payload.acquisition
    keywords = None if acquisition is None else dict(acquisition.keywords)

    return payload.command.model_dump(), payload.timeout_s, keywords


def seen(workflow: ExecutableWorkflow) -> tuple[list, list]:
    """Everything a workflow's graphs hold, as values to compare."""
    graphs = (workflow.graph, *(c.graph for c in workflow.cleanup))
    nodes = [(n.label, n.on_failure, n.optional, n.delay_s, n.override,
              carried(n.payload))
             for graph in graphs for n in graph.nodes]

    return nodes, [(g.deps, g.hard) for g in graphs]


def test_audits_are_deterministic_and_leave_their_inputs_alone(definition,
                                                               darks):
    authored = definition.model_dump()
    compiled = seen(darks)

    assert audit_definition(definition) == audit_definition(definition)
    assert audit_workflow(darks) == audit_workflow(darks)
    assert definition.model_dump() == authored
    assert seen(darks) == compiled
