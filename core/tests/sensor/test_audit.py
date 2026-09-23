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

from sensorkit.sensor.audit import AuditReport, audit_definition
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
from sensorkit.sensor.lifecycle import LifecycleWorkflow
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.topology import Device, Structure, Unit
from sensorkit.sensor.workflow import DeadlineRule, ExecutableWorkflow
from sensorkit.std.collect import CameraParameterSet, Collect
from sensorkit.std.traits import Stop

from .common import REPORTED, TARGET, pointing, snapshot_of


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
