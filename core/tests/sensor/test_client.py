# SPDX-License-Identifier: Apache-2.0
"""A session, from discovery through planning to a run it owns.

The session connects to `BENCH` on the rig, with one more device registered and
in no definition, so discovery is shown to read only what the definition names.
"""
from __future__ import annotations

import asyncio
import copy
import dataclasses
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from sensorkit.sensor.client import Sensor
from sensorkit.sensor.collect import (
    CollectIntent,
    CommandRequest,
    RequestEpoch,
    SettingUnsatisfiable,
)
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.dispatch import Interruption
from sensorkit.sensor.execution import ExecutionState, WorkflowError
from sensorkit.sensor.lifecycle import LifecycleWorkflow, compile_lifecycle
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.workflow import OperatorRule
from sensorkit.std.optics import SetFilter

from .common import (
    BENCH,
    abort_only,
    asking,
    authored,
    ended,
    finished,
    operation,
    placement,
    ran,
    reached,
)


@pytest.fixture(scope="module")
def handles():
    """What each registered device handles, which is also what it reports.

    `extra` is on the backend and in no definition, so discovery never reads
    it.
    """
    return {
        "mount": ("Home", "Stop", "FollowTarget", "Abort"),
        "foc-e": ("ChangeFocusPosition",),
        "cam-e": ("CameraCapture", "Abort"),
        "wheel-w": ("SetFilter",),
        "cam-w": ("CameraCapture", "Abort"),
        "extra": ("Home",),
    }


@pytest.fixture(scope="module")
def document() -> SensorDefinition:
    return authored(BENCH, """
        tables:
          home:
            fail_fast: true
            phases:
              - name: home
                entries:
                  - select: {device: mount}
                    ops: Home
                    id: home-mount
            cleanup:
              - name: park
                armed_by: [home-mount]
                entries:
                  - select: {device: mount}
                    ops: Stop
          focus-home:
            fail_fast: true
            phases:
              - name: home
                entries:
                  - select: {device: foc-e}
                    ops: Home
        deadlines:
          - device: mount
            command: Home
            seconds: 7.0
          - device: cam-e
            command: CameraCapture
            seconds: 9.0
        """)


@pytest_asyncio.fixture
async def session(kit, rig, document) -> Sensor:
    return await Sensor.connect(document, kit)


def table(session: Sensor, name: str) -> LifecycleWorkflow:
    return next(t for t in session.definition.tables if t.name == name)


def frames(device: str = "cam-e", count: int = 1, **kw) -> CollectIntent:
    return CollectIntent(name="frames", epochs=(
        RequestEpoch(units=(asking(device, count, **kw),)),))


def untouched(state: ExecutionState) -> bool:
    return (state.run is None and not state.attempted
            and not state.interruptions and not state.cleanup)


# Discovery


@pytest.mark.asyncio
async def test_discovery_reads_the_devices_the_definition_names(session, rig,
                                                              handles):
    reported = dict(session.sensor.capabilities.devices)

    assert list(reported) == ["mount", "foc-e", "cam-e", "wheel-w", "cam-w"]
    assert reported["mount"].supported_commands >= set(handles["mount"])
    assert reported["wheel-w"].supported_commands >= {"SetFilter"}
    assert rig.log == []


@pytest.mark.asyncio
async def test_the_snapshot_says_when_it_was_taken_and_what_it_covers(
        kit, rig, document):
    before = datetime.now(UTC)
    session = await Sensor.connect(document, kit)
    after = datetime.now(UTC)
    capabilities = session.sensor.capabilities

    assert before <= capabilities.taken <= after
    assert capabilities.source == "device discovery"
    assert capabilities.taken.isoformat() in capabilities.provenance
    assert "device discovery" in capabilities.provenance

    for device in ("mount", "foc-e", "cam-e", "wheel-w", "cam-w"):
        assert f"'{device}'" in capabilities.provenance

    assert "extra" not in capabilities.provenance


@pytest.mark.asyncio
async def test_a_device_that_reported_nothing_fails_connection(kit, rig):
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: mount
            - device: cam-x
              instrument: true
        """)

    with pytest.raises(ValueError, match="reported nothing: 'cam-x'"):
        await Sensor.connect(definition, kit)


@pytest.mark.asyncio
async def test_an_entity_that_is_not_a_device_fails_connection(
        kit, service_context, rig):
    await service_context.register_entity("gate")
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: gate
        """)

    with pytest.raises(ValueError, match="'gate' is not a device"):
        await Sensor.connect(definition, kit)


@pytest.mark.asyncio
async def test_a_declared_trait_the_device_lacks_fails_connection(kit, rig):
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: mount
              traits: camera
        """)

    with pytest.raises(ValueError,
                       match="'camera', which its device does not satisfy"):
        await Sensor.connect(definition, kit)


@pytest.mark.asyncio
async def test_a_declared_trait_the_device_has_binds(kit, rig):
    session = await Sensor.connect(authored("""
        sensor:
          name: bench
          components:
            - device: cam-e
              instrument: true
              traits: camera
        """), kit)

    assert "camera" in session.sensor.traits(placement(session.sensor, "cam-e"))


@pytest.mark.asyncio
async def test_a_malformed_definition_fails_before_discovery(kit, rig,
                                                             document):
    # Copied without validation, so connecting is what has to catch it.
    doubled = document.model_copy(update={"tables": document.tables * 2})

    with pytest.raises(ValueError, match="a table is named twice"):
        await Sensor.connect(doubled, kit)


@pytest.mark.asyncio
async def test_a_failed_connection_leaves_nothing_running_and_the_client_open(
        kit, service_context, rig):
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: cam-x
              instrument: true
        """)
    before = asyncio.all_tasks()

    with pytest.raises(ValueError, match="reported nothing"):
        await Sensor.connect(definition, kit)

    assert asyncio.all_tasks() - before == set()

    # The caller's client still reaches the backend, and connects once the
    # device is there.
    await rig.serve(service_context, "cam-x", ("CameraCapture",))
    session = await Sensor.connect(definition, kit)
    report = await session.execute(session.plan_collect(frames("cam-x")))

    assert report.outcome == "completed"
    assert len(rig["cam-x"].sent("CameraCapture")) == 1


# Planning


@pytest.mark.asyncio
async def test_a_table_plans_and_runs_with_no_adapter(session, rig):
    report = await session.execute(session.plan_lifecycle(table(session,
                                                                "home")))

    assert report.outcome == "completed"
    assert rig.arrived() == ["mount Home", "mount Stop"]
    assert report.provenance == session.sensor.capabilities.provenance


@pytest.mark.asyncio
async def test_a_collect_plans_and_runs_with_no_adapter(session, rig):
    report = await session.execute(session.plan_collect(frames(count=2)))

    assert report.outcome == "completed"
    assert len(rig["cam-e"].sent("CameraCapture")) == 2
    assert report.provenance == session.sensor.capabilities.provenance


@pytest.mark.asyncio
async def test_planning_errors_arrive_before_any_command(session, rig):
    with pytest.raises(ValueError, match="'Home'"):
        session.plan_lifecycle(table(session, "focus-home"))

    # Nothing on the east chain changes a filter.
    with pytest.raises(SettingUnsatisfiable):
        session.plan_collect(frames(settings=(
            CommandRequest(command=SetFilter(filter="r")),)))

    assert rig.log == []


@pytest.mark.asyncio
async def test_definition_deadlines_and_caller_rules_reach_the_workflow(
        session):
    skip = OperatorRule(reason="mount homed by hand tonight",
                        commands=("Home",), outcome="skipped")
    lenient = OperatorRule(reason="east camera is flaky",
                           select=IsRef(device="cam-e"), optional=True)
    homing = session.plan_lifecycle(table(session, "home"), rules=(skip,))
    collecting = session.plan_collect(frames(), rules=(lenient,))
    home = operation(homing, "Home")
    capture = operation(collecting, "CameraCapture")
    home_node = next(n for n in homing.graph.nodes if n.payload is home)
    capture_node = next(n for n in collecting.graph.nodes
                        if n.payload is capture)

    assert home.timeout_s == 7.0
    assert capture.timeout_s == 9.0
    assert home_node.override is not None
    assert home_node.override.reason == "mount homed by hand tonight"
    assert capture_node.optional


# Issuance


@pytest.mark.asyncio
async def test_a_directly_compiled_workflow_is_refused(session, rig):
    direct = compile_lifecycle(table(session, "home"), session.sensor,
                               deadlines=session.definition.deadlines)
    state = ExecutionState()

    with pytest.raises(ValueError, match="did not issue"):
        await session.execute(direct, state=state)

    assert rig.log == []
    assert untouched(state)

    # The refusal left the state fresh, so a real run can still claim it.
    await session.execute(session.plan_lifecycle(table(session, "home")),
                          state=state)

    assert state.run is not None and state.run.ok


@pytest.mark.asyncio
async def test_another_sessions_workflow_is_refused(kit, rig, document):
    first = await Sensor.connect(document, kit)
    second = await Sensor.connect(document, kit)
    workflow = first.plan_collect(frames())

    with pytest.raises(ValueError, match="did not issue"):
        await second.execute(workflow)

    assert rig.log == []

    await first.execute(workflow)

    assert len(rig["cam-e"].sent("CameraCapture")) == 1


@pytest.mark.asyncio
async def test_issuance_is_the_object_and_not_an_equal_copy(session, rig):
    issued = session.plan_lifecycle(table(session, "home"))

    for twin in (dataclasses.replace(issued), copy.copy(issued)):
        assert twin.graph is issued.graph

        with pytest.raises(ValueError, match="did not issue"):
            await session.execute(twin)

    assert rig.log == []

    await session.execute(issued)

    assert rig.arrived() == ["mount Home", "mount Stop"]


# Overlap


@pytest.mark.asyncio
async def test_an_overlapping_run_is_refused_during_the_main_run(session,
                                                                 rig):
    mount = rig["mount"]
    homing = mount.hold("Home")
    running = rig.start(
        session.execute(session.plan_lifecycle(table(session, "home"))))
    other = session.plan_collect(frames())
    state = ExecutionState()

    await reached(mount.arrival("Home"))

    with pytest.raises(ValueError, match="has not ended"):
        await session.execute(other, state=state)

    assert untouched(state)
    assert rig["cam-e"].sent("CameraCapture") == []
    assert not running.done()

    homing.set()
    report = await finished(running)

    assert report.outcome == "completed"
    assert report.run.ok and report.cleanup[0].run.ok
    assert mount.sent("Abort") == []

    # Once the first run has settled, the refused one runs on the same state.
    await session.execute(other, state=state)

    assert len(rig["cam-e"].sent("CameraCapture")) == 1
    assert state.run is not None and state.run.ok


@pytest.mark.asyncio
async def test_an_overlapping_run_is_refused_during_cleanup(session, rig):
    mount = rig["mount"]
    stopping = mount.hold("Stop")
    running = rig.start(
        session.execute(session.plan_lifecycle(table(session, "home"))))
    other = session.plan_collect(frames())

    await reached(mount.arrival("Stop"))

    with pytest.raises(ValueError, match="has not ended"):
        await session.execute(other)

    assert not running.done()

    stopping.set()
    report = await finished(running)

    assert report.outcome == "completed"
    assert ran(report) == ["park"]

    await session.execute(other)

    assert len(rig["cam-e"].sent("CameraCapture")) == 1


@pytest.mark.asyncio
async def test_a_domain_abort_holds_the_session_until_its_cleanup_ends(
        session, rig):
    mount = rig["mount"]
    mount.hold("Home")
    stopping = mount.hold("Stop")
    homing = session.plan_lifecycle(table(session, "home"))
    running = rig.start(session.execute(homing, in_domain=abort_only))
    other = session.plan_collect(frames())

    await reached(mount.arrival("Home"))
    running.cancel("abort")
    await reached(mount.arrival("Stop"))

    with pytest.raises(ValueError, match="has not ended"):
        await session.execute(other)

    assert not running.done()

    stopping.set()
    report = await finished(running)

    assert (report.outcome, report.reason) == ("aborted", "abort")
    assert report.interruptions == {
        operation(homing, "Home"): Interruption("acknowledged")}
    assert ran(report) == ["park"]
    assert rig.ended_before("mount Abort", "mount Stop")

    # Once cleanup has settled the session takes another run.
    assert (await session.execute(other)).outcome == "completed"


@pytest.mark.asyncio
async def test_an_overlapping_run_is_refused_while_a_cancellation_drains(
        session, rig):
    mount = rig["mount"]
    mount.hold("Home")
    answering = mount.hold("Abort")
    homing = session.plan_lifecycle(table(session, "home"))
    home = operation(homing, "Home")
    state = ExecutionState()
    running = rig.start(session.execute(homing, state=state))
    other = session.plan_collect(frames())
    refused = ExecutionState()

    await reached(mount.arrival("Home"))
    running.cancel()
    await reached(mount.arrival("Abort"))

    with pytest.raises(ValueError, match="has not ended"):
        await session.execute(other, state=refused)

    assert untouched(refused)
    assert not running.done()

    answering.set()
    await ended(running)

    # Hard cancellation propagated, skipped cleanup, and left the caller's
    # state holding what the run did.
    assert running.cancelled()
    assert state.attempted == {home}
    assert state.interruptions == {home: Interruption("acknowledged")}
    assert state.run is not None
    assert [r.status for r in state.run.results.values()] == ["cancelled"]
    assert state.cleanup == []
    assert mount.sent("Stop") == []

    await session.execute(other, state=refused)

    assert len(rig["cam-e"].sent("CameraCapture")) == 1


# Reporting


@pytest.mark.asyncio
async def test_a_failed_run_reports_its_provenance_and_frees_the_session(
        session, rig):
    rig["mount"].refusing["Home"] = 1

    with pytest.raises(WorkflowError, match="in the run") as raised:
        await session.execute(session.plan_lifecycle(table(session, "home")))

    assert raised.value.report.provenance == (
        session.sensor.capabilities.provenance)
    assert "device discovery" in raised.value.report.provenance

    report = await session.execute(session.plan_collect(frames()))

    assert report.outcome == "completed"
