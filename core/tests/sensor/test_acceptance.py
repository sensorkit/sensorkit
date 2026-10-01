# SPDX-License-Identifier: Apache-2.0
"""Acceptance of the sensor library, from an authored definition to a report.

Each scenario runs the public path a site runs. A definition is loaded and
composed, a session connects through a caller-owned client, and workflows are
planned, audited and executed through that session. Connection reads what the
rig's devices report, and commands cross the real request machinery.

Collect scenarios run on `BENCH`. Lifecycle scenarios add an enclosure and a
mirror cover. One device is on the backend and in no definition. Every command
has a finite deadline, and nothing asserts one order where the graph allows
several.

Behavior owned by one module is tested with that module.
"""
from __future__ import annotations

import copy
from collections import Counter, defaultdict

import pytest
import pytest_asyncio

from sensorkit.astro.common import AltAzPointing, SitePosition
from sensorkit.common.dag import Graph
from sensorkit.data.context import Context
from sensorkit.sensor.audit import audit_definition, audit_workflow
from sensorkit.sensor.client import Sensor
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    RequestEpoch,
)
from sensorkit.sensor.lifecycle import Entry, LifecycleWorkflow, OpSpec, Phase
from sensorkit.sensor.policies import (
    SensorPolicies,
    compose_deadlines,
    compose_tables,
)
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.standard_task import SIDEREAL, translate
from sensorkit.sensor.workflow import ExecutableWorkflow, Operation
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import ConfigureCameraSensor
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import ChangeFocusPosition, Filter, FocusPosition
from sensorkit.std.traits import Home, Stop

from .common import (
    BENCH,
    BRANCHES,
    TARGET,
    authored,
    finished,
    operation,
    reached,
    status,
)

pytestmark = pytest.mark.timeout(60)

OBSERVATORY = """
    sensor:
      name: observatory
      components:
        - device: dome
        - device: mount
        - device: cover
    """ + BRANCHES

SITE = """
    tables:
      stop:
        fail_fast: false
        phases:
          - name: halt
            entries:
              - select: {device: mount}
                ops: Stop
    deadlines:
      - any: true
        command: OpenEnclosure
        seconds: 90.0
      - device: mount
        command: Init
        seconds: 12.0
    """
"""What an observatory authors beside its policies.

Its `stop` table replaces the generated one. Its enclosure rule replaces the
generated rule for the same target and command, and its mount rule outranks the
generated trait rule without replacing it."""

BENCH_DEVICES = ["mount", "foc-e", "cam-e", "wheel-w", "cam-w"]

POLICIES = SensorPolicies()

SURVEY = StandardCollectTask(
    target=TARGET, sidereal_frames=[1],
    camera_params=[
        CameraParameterSet(integration_time_seconds=0.1, frame_count=3,
                           gain=1.5),
        CameraParameterSet(integration_time_seconds=0.2, frame_count=2,
                           filter_name="r"),
        CameraParameterSet(integration_time_seconds=0.3, frame_count=2,
                           filter_name="b"),
    ])
"""Three exposures over three pointing segments, the second sidereal.

The configured exposure can only be taken east and both filtered ones only
west. The west exposures ask one wheel for two filters, so they alternate on
it, while the east camera needs nothing from that wheel."""

INTEGRATION = {"exposure-0": 0.1, "exposure-1": 0.2, "exposure-2": 0.3}


@pytest.fixture(scope="module")
def handles():
    """What each registered device handles, which is also what it reports.

    Only the east camera takes a sensor configuration and only the west branch
    changes a filter, so a standard exposure asking for either has one home.
    """
    return {
        "dome": ("Init", "Deinit", "OpenEnclosure", "CloseEnclosure", "Stop",
                 "Abort"),
        "mount": ("Init", "Deinit", "MoveToPark", "SetParkPosition", "Home",
                  "FollowTarget", "Stop", "Abort"),
        "cover": ("OpenMirrorCover", "CloseMirrorCover", "Stop"),
        "foc-e": ("ChangeFocusPosition",),
        "cam-e": ("CameraCapture", "ConfigureCameraSensor", "Abort"),
        "wheel-w": ("SetFilter",),
        "cam-w": ("CameraCapture", "Abort"),
        "extra": ("Home",),
    }


@pytest_asyncio.fixture
async def session(kit, rig) -> Sensor:
    """A session on the bench, with a finite deadline for everything a
    scenario sends."""
    return await Sensor.connect(authored(BENCH, """
        deadlines:
          - {any: true, command: FollowTarget, seconds: 5.0}
          - {any: true, command: Stop, seconds: 5.0}
          - {any: true, command: Home, seconds: 5.0}
          - {any: true, command: SetFilter, seconds: 5.0}
          - {any: true, command: ChangeFocusPosition, seconds: 5.0}
          - {any: true, command: ConfigureCameraSensor, seconds: 5.0}
          - {any: true, command: CameraCapture, seconds: 5.0}
        """), kit)


async def site(kit, policies: SensorPolicies = POLICIES
               ) -> tuple[Sensor, dict[str, LifecycleWorkflow]]:
    """Connect the observatory, composing its policies as a site would."""
    definition = compose_deadlines(authored(OBSERVATORY, SITE),
                                   policies.deadlines())
    session = await Sensor.connect(definition, kit)
    tables = compose_tables(definition, policies.tables(),
                            session.sensor)

    return session, {table.name: table for table in tables}


def operations(graph: Graph) -> list[Operation]:
    """A graph's operations, each after what it waits on."""
    nodes = {node.id: node for node in graph.nodes}

    return [nodes[nid].payload for nid in graph.topo_order()
            if isinstance(nodes[nid].payload, Operation)]


def frames(workflow: ExecutableWorkflow) -> list[Operation]:
    return [op for op in operations(workflow.graph)
            if op.acquisition is not None]


def planned(workflow: ExecutableWorkflow) -> Counter[str]:
    """Every command the run and its cleanup would send, by device."""
    graphs = (workflow.graph, *(c.graph for c in workflow.cleanup))

    return Counter(f"{op.target.device} {op.command.model_tag()}"
                   for graph in graphs for op in operations(graph))


def grouped(ops: list[Operation], key) -> dict[str, list[Operation]]:
    groups: defaultdict[str, list[Operation]] = defaultdict(list)

    for op in ops:
        groups[key(op)].append(op)

    return dict(groups)


def run_section(description: str) -> dict[str, set[str]]:
    """An audit's run section, as each node's heading and the lines under it."""
    lines = description.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("run, "))
    blocks: dict[str, set[str]] = {}
    block: set[str] = set()

    for line in lines[start + 1:lines.index("", start)]:
        if line.startswith("   "):
            block.add(line.strip())
        else:
            block = blocks[line.strip()] = set()

    return blocks


def compiled_as(workflow: ExecutableWorkflow) -> dict[str, set[str]]:
    """What an audit must state for each run node, being its edges and
    deadline, under the heading it names the node by."""
    graph = workflow.graph
    names = {node.id: (node.payload.id if isinstance(node.payload, Operation)
                       else f"ordering '{node.label}'")
             for node in graph.nodes}
    stated = {}

    for node in graph.nodes:
        hard = graph.hard[node.id]
        lines = {f"needs the success of {names[dep]}" if dep in hard
                 else f"waits for the completion of {names[dep]}"
                 for dep in graph.deps[node.id]}

        match node.payload:
            case Operation(timeout_s=None):
                lines.add("no deadline")
            case Operation(timeout_s=seconds):
                lines.add(f"deadline {seconds:g} s")

        stated[f"{names[node.id]}, in '{node.group}'"] = lines

    return stated


def assert_describes(description: str, workflow: ExecutableWorkflow) -> None:
    """Assert that an audit states each run node, deadline and edge as compiled.

    Compares the rendered run section with the graph the executor is given.
    Edges must match exactly, and every stated line must be present.
    """
    blocks = run_section(description)
    edge = ("needs the success of", "waits for the completion of")

    for heading, lines in compiled_as(workflow).items():
        block = blocks.pop(heading)

        assert lines <= block
        assert {line for line in block if line.startswith(edge)} <= lines

    assert blocks == {}


# Lifecycle


@pytest.mark.asyncio
async def test_a_site_composes_plans_and_audits_before_anything_is_sent(
        kit, rig):
    session, tables = await site(kit)
    definition = session.definition
    init = session.plan_lifecycle(tables["init"])
    described = audit_definition(
        definition.model_copy(update={"tables": tuple(tables.values())}))
    compiled = audit_workflow(init)
    rules = Counter((rule.target, rule.command) for rule in definition.deadlines)
    seconds = {(rule.target, rule.command): rule.seconds
               for rule in definition.deadlines}

    # Connection read what the structure names, and nothing so far sent a
    # command.
    assert list(session.sensor.details) == [
        "dome", "mount", "cover", *BENCH_DEVICES[1:]]
    assert rig.log == []

    # Authored rules replace their generated namesakes and nothing else.
    assert rules[("any", None), "OpenEnclosure"] == 1
    assert seconds[("any", None), "OpenEnclosure"] == 90.0
    assert seconds[("trait", "mount"), "Init"] == 30.0
    assert seconds[("device", "mount"), "Init"] == 12.0

    # Each resolves onto the operation it addresses.
    assert operation(init, "OpenEnclosure", "dome").timeout_s == 90.0
    assert operation(init, "Init", "mount").timeout_s == 12.0
    assert operation(init, "Init", "dome").timeout_s == 300.0
    assert operation(init, "OpenMirrorCover", "cover").timeout_s == 60.0

    # The authored table replaced the generated one of its name whole.
    authored_stop, = (t for t in definition.tables if t.name == "stop")

    assert tables["stop"] is authored_stop
    assert sorted(tables) == ["init", "recover", "shutdown", "standby", "stop"]

    # Both audits agree with what was composed and compiled.
    assert [f for f in described.findings if f.status == "invalid"] == []
    assert "table 'stop'" in described.description
    assert "table 'init'" in described.description
    assert_describes(compiled.description, init)
    assert rig.log == []


@pytest.mark.asyncio
async def test_a_generated_bring_up_runs(kit, rig):
    session, tables = await site(kit)
    init = session.plan_lifecycle(tables["init"])
    report = await session.execute(init)

    assert report.outcome == "completed"
    assert report.run.ok
    assert report.run.graph is init.graph
    assert report.attempted == set(operations(init.graph))
    assert Counter(rig.arrived()) == planned(init) - Counter(
        f"{device} Stop" for device in ("dome", "mount", "cover"))

    # The halt is armed by every entry and only runs after a failure or an
    # abort.
    assert init.cleanup[0].when == "failure_or_cancelled"
    assert report.cleanup == ()
    assert all(d.sent("Stop") == [] for d in rig.devices.values())

    assert rig.ended_before("dome OpenEnclosure", "mount Init")
    assert rig.ended_before("mount Init", "cover OpenMirrorCover")


@pytest.mark.asyncio
async def test_a_guaranteed_shutdown_closes_the_dome_after_everything_else(
        kit, rig):
    session, tables = await site(kit, SensorPolicies(always_deinit_dome=True))
    shutdown = session.plan_lifecycle(tables["shutdown"])
    close = operation(shutdown, "CloseEnclosure", "dome")
    report = await session.execute(shutdown)

    assert report.outcome == "completed"
    assert report.run.ok
    assert status(report.run, close) == "ok"
    assert rig.ended_before("cover CloseMirrorCover", "dome CloseEnclosure")
    assert rig.ended_before("mount Deinit", "dome CloseEnclosure")
    assert rig.ended_before("dome Stop", "dome CloseEnclosure")
    assert rig.ended_before("dome CloseEnclosure", "dome Deinit")


# Standard collect


@pytest.mark.asyncio
async def test_a_standard_collect_sends_what_it_planned(session, rig):
    workflow = session.plan_collect(translate(SURVEY))
    by_camera = grouped(frames(workflow), lambda op: op.target.device)
    report = await session.execute(workflow)

    # Every planned command arrived once, and nothing else, so the runtime
    # added no stop or abort of its own.
    assert report.outcome == "completed"
    assert Counter(rig.arrived()) == planned(workflow)

    # Each frame was taken for its own exposure and under its own number.
    for camera, ops in by_camera.items():
        assert sorted(
            (c.context[Collect].frame_number, c.integration_time)
            for c in rig[camera].sent("CameraCapture")) == sorted(
            (op.acquisition.frame_number, INTEGRATION[op.acquisition.request])
            for op in ops)

    assert rig["cam-e"].sent("ConfigureCameraSensor") == [
        ConfigureCameraSensor(gain=1.5)]
    assert [c.filter for c in rig["wheel-w"].sent("SetFilter")] == [
        "r", "b", "r", "b"]
    assert [c.target for c in rig["mount"].sent("FollowTarget")] == [
        TARGET, SIDEREAL, TARGET]


@pytest.mark.asyncio
async def test_branches_overlap_and_wait_only_where_they_share_hardware(
        session, rig):
    east, west = rig["cam-e"], rig["cam-w"]
    wheel, mount = rig["wheel-w"], rig["mount"]
    workflow = session.plan_collect(translate(SURVEY))
    first_east = east.hold("CameraCapture")
    first_west = west.hold("CameraCapture")
    running = rig.start(session.execute(workflow))

    # Both first frames are in flight at once.
    await reached(east.arrival("CameraCapture"))
    await reached(west.arrival("CameraCapture"))
    first_west.set()

    # With the first east frame still held, the west branch changes its
    # filter between its own frames and on into the next segment.
    await reached(wheel.arrival("SetFilter", 3))

    assert mount.sent("FollowTarget") == [FollowTarget(target=TARGET)]

    first_east.set()
    report = await finished(running)
    change = rig.at("mount FollowTarget", 2)

    assert report.outcome == "completed"

    # The branches overlapped, and a west filter change did not wait for the
    # east frame.
    assert ("cam-e CameraCapture" in west.overlapping("CameraCapture")
            or "cam-w CameraCapture" in east.overlapping("CameraCapture"))
    assert "cam-e CameraCapture" in wheel.overlapping("SetFilter", 2)
    assert "cam-e CameraCapture" in wheel.overlapping("SetFilter", 3)

    # Each filter change waited for the west frame taken under the one before.
    for count in (2, 3):
        assert "cam-w CameraCapture" not in wheel.overlapping("SetFilter",
                                                              count)
        assert rig.at("cam-w CameraCapture ends", count - 1) < rig.at(
            "wheel-w SetFilter", count)

    # The shared pointing change waited for every frame of its segment on
    # both branches, and the next frames on both waited for it.
    assert not any(line.endswith("CameraCapture")
                   for line in mount.overlapping("FollowTarget", 2))
    assert rig.at("cam-e CameraCapture ends") < change
    assert rig.at("cam-w CameraCapture ends", 2) < change
    assert rig.at("mount FollowTarget ends", 2) < rig.at(
        "cam-e CameraCapture", 2)
    assert rig.at("mount FollowTarget ends", 2) < rig.at(
        "cam-w CameraCapture", 3)

    # Cleanup began once the main work had drained.
    assert mount.overlapping("Stop") == frozenset()
    assert rig.at("mount Stop") > max(
        i for i, line in enumerate(rig.log)
        if line.endswith("CameraCapture ends"))


@pytest.mark.asyncio
async def test_each_header_is_sampled_when_its_frame_is_sent(session, rig):
    east, west = rig["cam-e"], rig["cam-w"]
    site_position = SitePosition(latitude_degrees=20.7, longitude_degrees=-156.3,
                                 altitude_km=3.0)
    before = AltAzPointing(altitude_degrees=30.0, azimuth_degrees=90.0)
    after = AltAzPointing(altitude_degrees=31.0, azimuth_degrees=91.0)
    reported = {"mount": Context(before),
                "foc-e": Context(FocusPosition(position=1200.0)),
                "wheel-w": Context(Filter(name="clear"))}
    workflow = session.plan_collect(translate(SURVEY))
    plan = [(op, op.command.model_copy(deep=True),
             copy.deepcopy(op.acquisition.keywords))
            for op in frames(workflow)]
    first_east = east.hold("CameraCapture")
    running = rig.start(session.execute(workflow,
                                      contexts=lambda: dict(reported),
                                      base=Context(site_position)))

    await reached(east.arrival("CameraCapture"))
    reported["mount"] = Context(after)
    first_east.set()
    report = await finished(running)
    east_headers = [c.context for c in east.sent("CameraCapture")]
    west_headers = [c.context for c in west.sent("CameraCapture")]

    assert report.outcome == "completed"

    # Pointing was read as each frame was sent, not once for the run.
    assert [h[AltAzPointing] for h in east_headers] == [before, after, after]

    # Every header has the base, what its own chain reports and what was
    # planned for that frame, and nothing from the other branch.
    for header in east_headers + west_headers:
        assert header[SitePosition] == site_position
        assert header.get(AltAzPointing) in (before, after)

    for header in east_headers:
        assert header[FocusPosition] == FocusPosition(position=1200.0)
        assert header.get(Filter) is None
        assert header[Collect].params.gain == 1.5

    # The filter asked for stays apart from the filter the wheel reports.
    for header in west_headers:
        assert header.get(FocusPosition) is None
        assert header[Filter] == Filter(name="clear")
        assert header[Collect].params.filter_name in ("r", "b")

    # Each header carries its own frame's planned keywords.
    for camera, headers in (("cam-e", east_headers), ("cam-w", west_headers)):
        assert sorted(h[Collect].frame_number for h in headers) == sorted(
            op.acquisition.frame_number for op, _, _ in plan
            if op.target.device == camera)

    assert len({id(h) for h in east_headers + west_headers}) == 7

    # Dispatch sent copies, and what was planned is as it was.
    for op, command, keywords in plan:
        assert op.command == command
        assert op.command.context is None
        assert op.acquisition.keywords == keywords


# Without adapters


@pytest.mark.asyncio
async def test_models_built_directly_plan_audit_and_run(session, rig):
    homing = LifecycleWorkflow(name="home", fail_fast=True, phases=(
        Phase(name="home", entries=(
            Entry(select=IsRef(device="mount"), ops=(OpSpec(command=Home()),)),
        )),))
    focused = CollectIntent(
        name="focused", epochs=(RequestEpoch(units=(InstrumentRequest(
            id="focused", select=IsRef(device="cam-e"),
            acquisition=AcquisitionRequest(integration_time_s=0.5, count=2),
            settings=(CommandRequest(
                command=ChangeFocusPosition(position=900)),)),)),),
        cleanup=(CommandRequest(command=Stop(), subject="sensor"),))
    workflows = (session.plan_lifecycle(homing), session.plan_collect(focused))

    for workflow in workflows:
        assert_describes(audit_workflow(workflow).description, workflow)

    assert rig.log == []

    for workflow in workflows:
        report = await session.execute(workflow)

        assert report.outcome == "completed"

    assert Counter(rig.arrived()) == planned(workflows[0]) + planned(
        workflows[1])
    assert rig.ended_before("foc-e ChangeFocusPosition", "cam-e CameraCapture")
    assert [c.integration_time for c in rig["cam-e"].sent("CameraCapture")] == [
        0.5, 0.5]
