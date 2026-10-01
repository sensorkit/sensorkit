# SPDX-License-Identifier: Apache-2.0
"""Generated tables and deadline rules, composed and run.

Generated tables are ordinary definitions, so each case composes them against a
bound sensor, compiles them with the real compiler and, where the behavior is a
runtime one, runs them through the executor against devices on the fake
backend.

A node is found by what it runs and where, as `device.Command`, never by an id.
"""
from __future__ import annotations

import asyncio
import textwrap
from collections.abc import Callable

import pytest
import yaml

from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.dispatch import Interruption
from sensorkit.sensor.execution import (
    ExecutionState,
    WorkflowError,
    WorkflowExecutor,
)
from sensorkit.sensor.lifecycle import (
    LifecycleWorkflow,
    OpSpec,
    compile_lifecycle,
)
from sensorkit.sensor.policies import (
    SensorPolicies,
    compose_deadlines,
    compose_tables,
)
from sensorkit.sensor.topology import Structure
from sensorkit.sensor.workflow import DeadlineRule, ExecutableWorkflow, Operation
from sensorkit.std.traits import Stop

from .common import (
    Rig,
    abort_only,
    finished,
    observed,
    ran,
    reached,
    sensor_of,
    status,
)

DEFAULTS = SensorPolicies()

DEPLOYED_FIELDS = {
    "concurrent_dome_and_mount_init": (bool, False),
    "concurrent_dome_and_mount_deinit": (bool, False),
    "concurrent_dome_init_open": (bool, False),
    "concurrent_dome_deinit_close": (bool, False),
    "always_deinit_dome": (bool, False),
    "dome_open_close_timeout": (float, 120.0),
    "dome_init_timeout": (float, 300.0),
    "dome_deinit_timeout": (float, 300.0),
    "concurrent_mount_and_mirror_cover_init": (bool, False),
    "mirror_cover_open_close_timeout": (float, 60.0),
    "mount_init_timeout": (float, 30.0),
    "mount_home_timeout": (float, 300.0),
}
"""Each field of a deployed policy block, in order, with its type and default."""

ADDED_FIELDS = {
    "mount_deinit_timeout": (float, 60.0),
    "stop_timeout": (float, 30.0),
    "follow_target_timeout": (float, 300.0),
    "filter_change_timeout": (float, 30.0),
    "camera_configure_timeout": (float, 30.0),
    "focus_change_timeout": (float, 30.0),
    "default_timeout": (float, 300.0),
}
"""Each field added since, with its type and default, so a deployed block
without it still loads."""


def structure(*devices: str) -> Structure:
    return Structure.model_validate(
        {"components": [{"device": d} for d in devices]})


def definition_of(sensor: BoundSensor, **fields) -> SensorDefinition:
    return SensorDefinition(sensor=sensor.topology.structure, **fields)


def composed(sensor: BoundSensor, policies=DEFAULTS,
             definition: SensorDefinition | None = None
             ) -> dict[str, LifecycleWorkflow]:
    """Every table a site would plan, keyed by name."""
    definition = definition or definition_of(sensor)

    return {table.name: table for table in compose_tables(
        definition, policies.tables(), sensor)}


def compiled(sensor: BoundSensor, name: str, policies=DEFAULTS,
             definition: SensorDefinition | None = None) -> ExecutableWorkflow:
    definition = definition or definition_of(sensor)

    return compile_lifecycle(composed(sensor, policies, definition)[name],
                             sensor, deadlines=definition.deadlines)


def label(graph, nid: int) -> str:
    payload = graph.nodes[nid].payload

    return f"{payload.target.device}.{payload.command.model_tag()}"


def node(graph, name: str) -> int:
    return next(n.id for n in graph.nodes if label(graph, n.id) == name)


def running(graph) -> set[str]:
    return {label(graph, n.id) for n in graph.nodes}


def waits(graph, name: str) -> tuple[set[str], set[str]]:
    """What a node needs to succeed, and what it waits for whatever the outcome."""
    nid = node(graph, name)

    return ({label(graph, d) for d in graph.hard[nid]},
            {label(graph, d) for d in graph.deps[nid] - graph.hard[nid]})


def edges(graph) -> set[tuple[str, str, bool]]:
    """Every edge as a test reads it, and whether it is hard."""
    return {(label(graph, d), label(graph, n.id), d in graph.hard[n.id])
            for n in graph.nodes for d in graph.deps[n.id]}


def labeled(graph, name: str) -> Operation:
    return graph.nodes[node(graph, name)].payload


def stops(rig: Rig) -> dict[str, int]:
    return {name: len(device.sent("Stop"))
            for name, device in rig.devices.items()}


@pytest.fixture(scope="module")
def handles():
    """What each device handles, which is also what it reports.

    The mount, dome and cover satisfy the mount, enclosure and mirror cover
    traits. The camera satisfies none of them and still has a Stop, so it shows
    a halt addressing a device whatever its traits.
    """
    return {
        "mount": ("Connect", "Init", "Deinit", "MoveToPark", "SetParkPosition",
                  "Stop", "Home", "FollowTarget", "Abort"),
        "dome": ("Connect", "Init", "Deinit", "OpenEnclosure",
                 "CloseEnclosure", "Stop", "Abort"),
        "cover": ("Connect", "OpenMirrorCover", "CloseMirrorCover", "Stop"),
        "cam": ("Connect", "Stop"),
    }


@pytest.fixture(scope="module")
def devices(handles) -> tuple[str, ...]:
    return tuple(handles)


@pytest.fixture(scope="module")
def bound(handles) -> Callable[..., BoundSensor]:
    """Binds a sensor holding the named devices, each reporting what it handles
    unless a case passes what it reports instead."""
    def bind(*devices: str, reported=handles) -> BoundSensor:
        return sensor_of(structure(*devices),
                         {device: (reported[device], ()) for device in devices})

    return bind


@pytest.fixture(scope="module")
def sensor(bound, devices) -> BoundSensor:
    return bound(*devices)


# Policy fields


def test_every_policy_field_keeps_its_name_type_and_default():
    fields = SensorPolicies.model_fields

    assert set(fields) == set(DEPLOYED_FIELDS) | set(ADDED_FIELDS)

    for name, expected in (DEPLOYED_FIELDS | ADDED_FIELDS).items():
        assert (fields[name].annotation, fields[name].default) == expected, name


def test_a_deployed_policy_block_loads_unchanged():
    block = {"always_deinit_dome": True, "dome_init_timeout": 12.5}

    defaults = {name: default for name, (_, default)
                in (DEPLOYED_FIELDS | ADDED_FIELDS).items()}

    assert SensorPolicies.model_validate(block).model_dump() == defaults | block


@pytest.mark.parametrize("name", ["minimum_target_altitude_degrees",
                                 "sun_separation_degrees", "moon_separation_degrees",
                                 "dome_init_timout"])
def test_unused_and_misspelled_policies_are_rejected(name):
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SensorPolicies.model_validate({name: 20.0})


# Generation


def test_generation_names_four_tables_and_standby_is_init():
    tables = {table.name: table for table in SensorPolicies().tables()}

    assert list(tables) == ["init", "standby", "shutdown", "recover"]
    assert tables["standby"] == tables["init"].model_copy(
        update={"name": "standby"})


@pytest.mark.parametrize("policies", [
    SensorPolicies(),
    SensorPolicies(**{name: True for name, field in
                      SensorPolicies.model_fields.items()
                      if field.annotation is bool}),
])
def test_every_generated_operation_omits_what_its_device_lacks(policies):
    specs: list[OpSpec] = [
        op for table in policies.tables()
        for group in (*table.phases, *table.cleanup)
        for entry in group.entries for op in entry.ops]

    assert specs
    assert {op.unsupported for op in specs} == {"omit"}


def test_the_bring_up_halt_is_an_ordinary_authored_stop(sensor, devices):
    (halt,) = composed(sensor)["init"].cleanup

    assert halt.name == "halt"
    assert halt.when == "failure_or_cancelled"
    assert halt.armed_by == ("init-open-enclosure", "init-mount",
                             "open-mirror-cover")
    assert [op.command for entry in halt.entries
            for op in entry.ops] == [Stop()]

    (cleanup,) = compiled(sensor, "init").cleanup

    assert running(cleanup.graph) == {f"{d}.Stop" for d in devices}


def test_the_bring_up_halt_does_not_take_the_init_failure_policy(sensor):
    workflow = compiled(sensor, "init")
    (cleanup,) = workflow.cleanup

    assert {n.on_failure for n in workflow.graph.nodes} == {"stop"}
    assert {(n.on_failure, n.optional) for n in cleanup.graph.nodes} == {
        ("skip", True)}


def test_recover_gathers_every_connect_before_any_stop(sensor, devices):
    graph = compiled(sensor, "recover").graph
    connects = {f"{device}.Connect" for device in devices}

    assert running(graph) == connects | {f"{d}.Stop" for d in devices}

    for device in devices:
        assert waits(graph, f"{device}.Stop") == (set(), connects)

    assert not any(n.optional for n in graph.nodes)


# Concurrency flags


BEFORE_MOUNT = {"dome.Init", "dome.OpenEnclosure"}


@pytest.mark.parametrize(("flags", "expected"), [
    ({}, {
        "dome.OpenEnclosure": ({"dome.Init"}, set()),
        "mount.Init": (set(), BEFORE_MOUNT),
        "cover.OpenMirrorCover": (set(), BEFORE_MOUNT | {"mount.Init"}),
    }),
    ({"concurrent_dome_init_open": True}, {
        "dome.OpenEnclosure": (set(), set()),
        "mount.Init": (set(), BEFORE_MOUNT),
        "cover.OpenMirrorCover": (set(), BEFORE_MOUNT | {"mount.Init"}),
    }),
    ({"concurrent_dome_and_mount_init": True}, {
        "mount.Init": (set(), set()),
        "cover.OpenMirrorCover": (set(), BEFORE_MOUNT | {"mount.Init"}),
    }),
    ({"concurrent_mount_and_mirror_cover_init": True}, {
        "mount.Init": (set(), BEFORE_MOUNT),
        "cover.OpenMirrorCover": (set(), BEFORE_MOUNT),
    }),
    ({"concurrent_dome_and_mount_init": True,
      "concurrent_mount_and_mirror_cover_init": True}, {
        "mount.Init": (set(), set()),
        "cover.OpenMirrorCover": (set(), set()),
    }),
    ({"concurrent_dome_init_open": True,
      "concurrent_mount_and_mirror_cover_init": True}, {
        "mount.Init": (set(), BEFORE_MOUNT),
        "cover.OpenMirrorCover": (set(), BEFORE_MOUNT),
    }),
])
def test_init_orders_the_dome_as_the_flags_say(sensor, flags, expected):
    graph = compiled(sensor, "init", SensorPolicies(**flags)).graph

    for name, wanted in expected.items():
        assert waits(graph, name) == wanted, name


@pytest.mark.parametrize(("flags", "expected"), [
    ({}, {
        "mount.Deinit": (set(), {"cover.CloseMirrorCover"}),
        "dome.Stop": (set(), {"mount.Deinit"}),
        "dome.CloseEnclosure": (set(), {"dome.Stop"}),
        "dome.Deinit": ({"dome.CloseEnclosure"}, set()),
    }),
    ({"concurrent_dome_and_mount_deinit": True}, {
        "dome.Stop": (set(), {"cover.CloseMirrorCover"}),
        "dome.CloseEnclosure": (set(), {"dome.Stop"}),
    }),
    ({"concurrent_dome_deinit_close": True}, {
        "dome.CloseEnclosure": (set(), {"dome.Stop"}),
        "dome.Deinit": (set(), {"dome.Stop"}),
    }),
    ({"always_deinit_dome": True}, {
        "dome.CloseEnclosure": (set(), {"dome.Stop"}),
        "dome.Deinit": (set(), {"dome.CloseEnclosure"}),
    }),
])
def test_shutdown_orders_the_dome_as_the_flags_say(sensor, flags, expected):
    graph = compiled(sensor, "shutdown", SensorPolicies(**flags)).graph

    for name, wanted in expected.items():
        assert waits(graph, name) == wanted, name


@pytest.mark.parametrize("flags", [
    {}, {"concurrent_dome_deinit_close": True},
    {"concurrent_dome_and_mount_deinit": True},
])
def test_always_deinit_dome_holds_on_the_compiled_graph(sensor, flags):
    graph = compiled(sensor, "shutdown",
                     SensorPolicies(always_deinit_dome=True, **flags)).graph

    # The deinit waits on the close for completion alone, nothing is fail-fast,
    # and nothing the enclosure entry runs hard-depends on anything.
    assert "dome.CloseEnclosure" not in waits(graph, "dome.Deinit")[0]
    assert {n.on_failure for n in graph.nodes} == {"skip"}

    for name in ("dome.CloseEnclosure", "dome.Deinit"):
        assert waits(graph, name)[0] == set()


# Absent equipment


@pytest.mark.parametrize("absent", [{"dome"}, {"cover"}, {"dome", "cover"}])
@pytest.mark.parametrize("policies", [
    SensorPolicies(),
    SensorPolicies(concurrent_dome_init_open=True,
                   concurrent_dome_deinit_close=True, always_deinit_dome=True),
])
def test_every_generated_table_compiles_without_some_equipment(
        bound, devices, absent, policies):
    sensor = bound(*(d for d in devices if d not in absent))

    for table in policies.tables():
        workflow = compiled(sensor, table.name, policies)

        assert not {label.split(".")[0] for label in running(workflow.graph)
                    } & absent


def test_an_empty_phase_passes_its_ordering_through(bound):
    no_dome = bound("mount", "cover", "cam")
    init = compiled(no_dome, "init").graph
    shutdown = compiled(no_dome, "shutdown").graph

    assert waits(init, "mount.Init") == (set(), set())
    assert waits(init, "cover.OpenMirrorCover") == (set(), {"mount.Init"})
    assert running(shutdown) == {"cover.CloseMirrorCover", "mount.Deinit"}

    no_cover = bound("mount", "dome", "cam")
    shutdown = compiled(no_cover, "shutdown").graph

    assert waits(shutdown, "mount.Deinit") == (set(), set())
    assert waits(shutdown, "dome.Stop") == (set(), {"mount.Deinit"})


def test_pruning_drops_the_arming_references_it_pruned(bound):
    (halt,) = composed(bound("mount", "cover"))["init"].cleanup

    assert halt.armed_by == ("init-mount", "open-mirror-cover")
    assert composed(bound("mount", "cover"))["init"].phases[0].entries == ()


def test_an_init_with_nothing_to_bring_up_carries_no_halt(bound):
    init = composed(bound("cam"))["init"]

    assert init.cleanup == ()
    assert not compiled(bound("cam"), "init").graph.nodes


GENERATED = """
    name: generated
    fail_fast: true
    phases:
      - name: run
        entries:
          - select: {device: absent}
            ops: Init
            id: pruned
          - select: {device: dome}
            ops: Init
            id: kept
    cleanup:
      - name: arming-emptied
        armed_by: [pruned]
        entries:
          - select: {device: mount}
            ops: Stop
      - name: arming-narrowed
        armed_by: [pruned, kept]
        entries:
          - select: {device: absent}
            ops: Stop
          - select: {device: mount}
            ops: Stop
      - name: unconditional
        entries:
          - select: {device: mount}
            ops: Stop
      - name: nothing-left
        armed_by: [kept]
        entries:
          - select: {device: absent}
            ops: Stop
    """


def test_pruning_narrows_or_drops_each_cleanup(sensor):
    generated = LifecycleWorkflow.model_validate(
        yaml.safe_load(textwrap.dedent(GENERATED)))
    (table,) = compose_tables(definition_of(sensor), (generated,), sensor)

    assert [entry.id for entry in table.phases[0].entries] == ["kept"]
    assert {spec.name: spec.armed_by for spec in table.cleanup} == {
        "arming-narrowed": ("kept",), "unconditional": None}
    assert [len(spec.entries) for spec in table.cleanup] == [1, 1]


def test_an_authored_table_with_the_same_empty_entry_still_raises(bound):
    no_dome = bound("mount", "cover")
    init = next(t for t in SensorPolicies().tables() if t.name == "init")
    definition = definition_of(no_dome, tables=(init,))

    # Authored, so it replaces the generated table whole and is not pruned.
    assert composed(no_dome, definition=definition)["init"] is init

    with pytest.raises(ValueError, match="selects no device"):
        compile_lifecycle(init, no_dome)


def test_the_selection_helper_decides_nothing(sensor, bound):
    no_dome = bound("mount", "cover")
    init = next(t for t in SensorPolicies().tables() if t.name == "init")
    enclosure = init.phases[0].entries[0]

    assert enclosure.targets(no_dome) == ()
    assert [p.device for p in enclosure.targets(sensor)] == ["dome"]


def test_composition_rejects_what_loading_would(sensor):
    broken = LifecycleWorkflow.model_validate(yaml.safe_load(textwrap.dedent("""
        name: broken
        fail_fast: true
        phases:
          - name: one
            entries:
              - select: {device: mount}
                ops: Init
          - name: one
            entries:
              - select: {device: dome}
                ops: Init
        """)))

    with pytest.raises(ValueError, match="a phase is named twice"):
        compose_tables(definition_of(sensor), (broken,), sensor)


# Generated and authored equivalence


AUTHORED_SHUTDOWN = """
    sensor:
      components:
        - device: mount
        - device: dome
        - device: cover
        - device: cam
    tables:
      shutdown:
        fail_fast: false
        phases:
          - name: optics
            entries:
              - select: {supports: CloseMirrorCover}
                ops: {command: CloseMirrorCover, unsupported: omit}
          - name: mount
            after: [optics]
            entries:
              - select: {supports: FollowTarget}
                ops: {command: Deinit, unsupported: omit}
          - name: halt
            after: [mount]
            entries:
              - select: {supports: CloseEnclosure}
                ops:
                  command: Stop
                  optional: true
                  fail_fast: false
                  unsupported: omit
          - name: enclosure
            after: [halt]
            entries:
              - select: {supports: CloseEnclosure}
                ops:
                  - {command: CloseEnclosure, unsupported: omit}
                  - command: Deinit
                    unsupported: omit
                    sequence: completion
    """


def test_an_authored_table_replaces_a_generated_one_whole(sensor):
    definition = SensorDefinition.from_yaml(textwrap.dedent(AUTHORED_SHUTDOWN))
    tables = compose_tables(definition, SensorPolicies().tables(),
                            sensor)

    assert [t.name for t in tables] == ["shutdown", "init", "standby",
                                        "recover"]
    assert tables[0] is definition.tables[0]


@pytest.mark.asyncio
async def test_generated_and_authored_tables_run_the_same(sensor, executor,
                                                          rig):
    policies = SensorPolicies(always_deinit_dome=True)
    authored = SensorDefinition.from_yaml(textwrap.dedent(AUTHORED_SHUTDOWN))
    generated = compiled(sensor, "shutdown", policies)
    written = compiled(sensor, "shutdown", policies, authored)

    assert authored.tables[0] == next(
        t for t in policies.tables() if t.name == "shutdown")
    assert edges(written.graph) == edges(generated.graph)
    assert [(n.on_failure, n.optional) for n in written.graph.nodes] == [
        (n.on_failure, n.optional) for n in generated.graph.nodes]

    # Once in each run.
    rig["dome"].refusing["CloseEnclosure"] = 2
    outcomes = []

    for workflow in (generated, written):
        rig.log.clear()

        with pytest.raises(WorkflowError) as failed:
            await executor.execute(workflow)

        report = failed.value.report
        outcomes.append((rig.arrived(), sorted(
            label(report.run.graph, n.id) for n, _ in report.run.failures)))

    assert outcomes[0] == outcomes[1]
    assert outcomes[0][1] == ["dome.CloseEnclosure"]


# always_deinit_dome, run
#
# A site guaranteeing its dome closes configures `always_deinit_dome`. Whatever
# goes wrong before the close, the close is attempted. A returned report marked
# completed means the dome acknowledged a successful close, and only a domain
# abort or hard cancellation leaves the close unattempted.


GUARANTEED = SensorPolicies(
    always_deinit_dome=True, mirror_cover_open_close_timeout=0.2,
    mount_deinit_timeout=0.2, stop_timeout=0.2, dome_open_close_timeout=0.2,
    dome_deinit_timeout=0.2)
"""The guarantee, with deadlines short enough that a hung operation ends
within the case."""


def shutting_down(sensor: BoundSensor) -> ExecutableWorkflow:
    """The guaranteed shutdown, bounded by its generated deadlines."""
    definition = compose_deadlines(definition_of(sensor),
                                   GUARANTEED.deadlines())

    return compiled(sensor, "shutdown", GUARANTEED, definition)


@pytest.mark.parametrize("fault", ["refused", "hung"])
@pytest.mark.parametrize(("device", "command", "abortable"), [
    ("cover", "CloseMirrorCover", False),
    ("mount", "Deinit", True),
    ("dome", "Stop", True),
])
@pytest.mark.asyncio
async def test_the_dome_closes_whatever_went_wrong_before_it(
        sensor, executor, rig, device, command, abortable, fault):
    shutdown = shutting_down(sensor)
    close = labeled(shutdown.graph, "dome.CloseEnclosure")
    faulty = labeled(shutdown.graph, f"{device}.{command}")

    match fault:
        case "refused":
            rig[device].refusing[command] = 1
        case "hung":
            rig[device].hold(command)

    try:
        report = await finished(rig.start(executor.execute(shutdown)))
        raised = None
    except WorkflowError as error:
        report, raised = error.report, error

    # The close was sent once and acknowledged, and the dome deinit followed.
    assert len(rig["dome"].sent("CloseEnclosure")) == 1
    assert status(report.run, close) == "ok"
    assert len(rig["dome"].sent("Deinit")) == 1

    # A required failure raises. The halt before the close is optional, so its
    # failure degrades a run that still completed with the dome closed.
    optional = (device, command) == ("dome", "Stop")

    assert (raised is None) == optional
    assert status(report.run, faulty) == "failed"

    if raised is not None:
        (node, error), = report.run.causes

        assert node.payload is faulty
        assert isinstance(error, TimeoutError) == (fault == "hung")

    # Only a hung call is interrupted, with one Abort where the device has it.
    expected = {} if fault == "refused" else {faulty: Interruption(
        "acknowledged" if abortable else "unsupported")}

    assert report.interruptions == expected
    assert sum(len(d.sent("Abort")) for d in rig.devices.values()) == (
        fault == "hung" and abortable)


@pytest.mark.parametrize(("flags", "device", "command"), [
    ({}, "dome", "CloseEnclosure"),
    ({"concurrent_dome_deinit_close": True}, "dome", "CloseEnclosure"),
    ({"concurrent_dome_and_mount_deinit": True}, "mount", "Deinit"),
])
@pytest.mark.asyncio
async def test_always_deinit_dome_deinits_the_dome_whatever_failed(
        sensor, executor, rig, flags, device, command):
    rig[device].refusing[command] = 1
    workflow = compiled(sensor, "shutdown",
                        SensorPolicies(always_deinit_dome=True, **flags))

    with pytest.raises(WorkflowError):
        await executor.execute(workflow)

    assert len(rig["dome"].sent("CloseEnclosure")) == 1
    assert len(rig["dome"].sent("Deinit")) == 1


@pytest.mark.parametrize("fault", ["refused", "hung"])
@pytest.mark.asyncio
async def test_a_close_that_does_not_succeed_is_never_reported_complete(
        sensor, executor, rig, fault):
    shutdown = shutting_down(sensor)
    close = labeled(shutdown.graph, "dome.CloseEnclosure")

    match fault:
        case "refused":
            rig["dome"].refusing["CloseEnclosure"] = 1
        case "hung":
            rig["dome"].hold("CloseEnclosure")

    with pytest.raises(WorkflowError, match="CloseEnclosure") as raised:
        await finished(rig.start(executor.execute(shutdown)))

    report = raised.value.report
    (node, error), = report.run.causes

    assert node.payload is close
    assert status(report.run, close) == "failed"
    assert isinstance(error, TimeoutError) == (fault == "hung")
    assert report.interruptions == ({} if fault == "refused" else {
        close: Interruption("acknowledged")})


@pytest.mark.parametrize(("device", "command"), [
    ("mount", "Deinit"), ("dome", "CloseEnclosure")])
@pytest.mark.asyncio
async def test_a_domain_abort_is_reported_as_one_whether_or_not_the_dome_closed(
        sensor, executor, rig, device, command):
    shutdown = shutting_down(sensor)
    close = labeled(shutdown.graph, "dome.CloseEnclosure")
    interrupted = labeled(shutdown.graph, f"{device}.{command}")
    rig[device].hold(command)
    running = rig.start(executor.execute(shutdown, in_domain=abort_only))

    await reached(rig[device].arrival(command))
    running.cancel("abort")
    report = await finished(running)

    assert (report.outcome, report.reason) == ("aborted", "abort")
    assert status(report.run, close) != "ok"
    assert report.interruptions == {interrupted: Interruption("acknowledged")}
    assert rig["dome"].sent("Deinit") == []

    # The close was attempted only where the abort arrived during it.
    assert (close in report.attempted) == (command == "CloseEnclosure")
    assert len(rig["dome"].sent("CloseEnclosure")) == (
        command == "CloseEnclosure")


@pytest.mark.parametrize(("device", "command"), [
    ("mount", "Deinit"), ("dome", "CloseEnclosure")])
@pytest.mark.asyncio
async def test_hard_cancellation_leaves_the_close_as_it_found_it(
        sensor, executor, rig, device, command):
    shutdown = shutting_down(sensor)
    close = labeled(shutdown.graph, "dome.CloseEnclosure")
    state = ExecutionState()
    rig[device].hold(command)
    running = rig.start(executor.execute(shutdown, in_domain=abort_only,
                                         state=state))

    await reached(rig[device].arrival(command))
    running.cancel("shutdown")

    with pytest.raises(asyncio.CancelledError):
        await finished(running)

    assert state.run is not None
    assert status(state.run, close) != "ok"
    assert (close in state.attempted) == (command == "CloseEnclosure")
    assert state.cleanup == []
    assert rig["dome"].sent("Deinit") == []


@pytest.mark.asyncio
async def test_without_the_flag_an_earlier_failure_withholds_the_close(
        sensor, executor, rig):
    shutdown = compiled(sensor, "shutdown")
    close = labeled(shutdown.graph, "dome.CloseEnclosure")
    rig["cover"].refusing["CloseMirrorCover"] = 1

    with pytest.raises(WorkflowError, match="CloseMirrorCover") as raised:
        await executor.execute(shutdown)

    report = raised.value.report

    assert rig["dome"].sent("CloseEnclosure") == []
    assert close not in report.attempted
    assert status(report.run, close) != "ok"


@pytest.mark.asyncio
async def test_without_the_flag_a_failed_close_withholds_the_deinit(
        sensor, executor, rig):
    rig["dome"].refusing["CloseEnclosure"] = 1

    with pytest.raises(WorkflowError):
        await executor.execute(compiled(sensor, "shutdown"))

    assert rig["dome"].sent("Deinit") == []


# Omission and failure


@pytest.mark.asyncio
async def test_an_unsupported_stop_is_omitted_and_the_close_inherits(
        clients, rig, bound, devices, handles):
    lacking = bound(*devices, reported={
        **handles, "dome": tuple(c for c in handles["dome"] if c != "Stop")})
    workflow = compiled(lacking, "shutdown")

    assert [(o.target.device, o.command) for o in workflow.omissions] == [
        ("dome", "Stop")]
    assert waits(workflow.graph, "dome.CloseEnclosure") == (
        set(), {"mount.Deinit"})

    report = await WorkflowExecutor(lacking, clients).execute(workflow)

    assert report.run.ok
    assert rig["dome"].sent("Stop") == []
    assert len(rig["dome"].sent("Deinit")) == 1


@pytest.mark.asyncio
async def test_a_refused_stop_degrades_the_run_and_the_close_follows(
        sensor, executor, rig):
    rig["dome"].refusing["Stop"] = 1
    workflow = compiled(sensor, "shutdown")

    assert workflow.omissions == ()

    report = await executor.execute(workflow)

    assert [label(report.run.graph, n.id) for n, _ in report.run.degraded] == [
        "dome.Stop"]
    assert not report.run.failures
    assert len(rig["dome"].sent("Deinit")) == 1


# Bring-up cleanup


@pytest.mark.asyncio
async def test_a_successful_bring_up_halts_nothing(sensor, executor, rig):
    report = await executor.execute(compiled(sensor, "init"))

    assert report.outcome == "completed"
    assert report.cleanup == ()
    assert sum(stops(rig).values()) == 0


@pytest.mark.asyncio
async def test_a_failed_bring_up_halts_once(sensor, executor, rig):
    rig["dome"].refusing["OpenEnclosure"] = 1

    with pytest.raises(WorkflowError) as failed:
        await executor.execute(compiled(sensor, "init"))

    report = failed.value.report

    assert report.outcome == "completed"
    assert ran(report) == ["halt"]
    assert stops(rig) == {"mount": 1, "dome": 1, "cover": 1, "cam": 1}
    assert rig["mount"].sent("Init") == []


@pytest.mark.asyncio
async def test_the_halt_goes_on_past_a_refused_stop(sensor, executor, rig):
    rig["dome"].refusing["OpenEnclosure"] = 1
    rig["mount"].refusing["Stop"] = 1

    with pytest.raises(WorkflowError) as failed:
        await executor.execute(compiled(sensor, "init"))

    (halt,) = failed.value.report.cleanup

    assert [label(halt.run.graph, n.id) for n, _ in halt.run.degraded] == [
        "mount.Stop"]
    assert stops(rig) == {"mount": 1, "dome": 1, "cover": 1, "cam": 1}


@pytest.mark.asyncio
async def test_an_aborted_bring_up_halts_once(sensor, executor, rig):
    rig["dome"].hold("Init")
    run = rig.start(executor.execute(compiled(sensor, "init"),
                                               in_domain=abort_only))
    await reached(rig["dome"].arrival("Init"))
    run.cancel("abort")
    report = await finished(run)

    assert report.outcome == "aborted"
    assert not report.run.failures
    assert ran(report) == ["halt"]
    assert stops(rig) == {"mount": 1, "dome": 1, "cover": 1, "cam": 1}


@pytest.mark.asyncio
async def test_a_failed_then_aborted_bring_up_halts_once(sensor, executor,
                                                         rig, observer):
    rig["dome"].refusing["Init"] = 1
    rig["mount"].hold("Init")
    workflow = compiled(sensor, "init",
                        SensorPolicies(concurrent_dome_and_mount_init=True))

    with observer.subscription() as queue:
        run = rig.start(executor.execute(workflow, in_domain=abort_only))

        await reached(rig["mount"].arrival("Init"))
        await observed(queue, "dome", "Init", "failed")
        run.cancel("abort")

    # The established failure still raises, carrying the abort's reason.
    with pytest.raises(WorkflowError, match="in the run, Init") as failed:
        await finished(run)

    report = failed.value.report

    assert (report.outcome, report.reason) == ("aborted", "abort")
    assert report.interruptions == {
        labeled(workflow.graph, "mount.Init"): Interruption("acknowledged")}
    assert ran(report) == ["halt"]
    assert stops(rig) == {"mount": 1, "dome": 1, "cover": 1, "cam": 1}
    assert rig["dome"].sent("OpenEnclosure") == []


@pytest.mark.asyncio
async def test_hard_cancellation_of_a_bring_up_halts_nothing(sensor, executor,
                                                             rig):
    rig["dome"].hold("Init")
    state = ExecutionState()
    run = rig.start(executor.execute(
        compiled(sensor, "init"), in_domain=abort_only, state=state))
    await reached(rig["dome"].arrival("Init"))
    run.cancel("teardown")

    with pytest.raises(asyncio.CancelledError):
        await finished(run)

    assert state.cleanup == []
    assert sum(stops(rig).values()) == 0
    assert len(rig["dome"].sent("Abort")) == 1


OMITTED_TRIGGER = """
    name: generated
    fail_fast: true
    phases:
      - name: run
        entries:
          - select: {device: cam}
            ops: {command: Home, unsupported: omit}
            id: home-camera
          - select: {device: dome}
            ops: Init
    cleanup:
      - name: halt
        when: failure_or_cancelled
        armed_by: [home-camera]
        entries:
          - select: {device: mount}
            ops: Stop
    """


@pytest.mark.asyncio
async def test_a_halt_armed_only_by_omitted_work_never_runs(sensor, executor,
                                                            rig):
    generated = LifecycleWorkflow.model_validate(
        yaml.safe_load(textwrap.dedent(OMITTED_TRIGGER)))
    (table,) = compose_tables(definition_of(sensor), (generated,), sensor)
    workflow = compile_lifecycle(table, sensor)

    assert table.cleanup[0].armed_by == ("home-camera",)
    assert workflow.cleanup[0].armed_by == ()

    rig["dome"].refusing["Init"] = 1

    with pytest.raises(WorkflowError) as failed:
        await executor.execute(workflow)

    assert failed.value.report.cleanup == ()
    assert rig["mount"].sent("Stop") == []


# Deadlines


POLICIES = SensorPolicies(dome_init_timeout=11.0, dome_open_close_timeout=12.0,
                          mount_init_timeout=13.0,
                          mirror_cover_open_close_timeout=14.0)


def test_generated_trait_rules_resolve_onto_what_they_name(sensor):
    definition = compose_deadlines(definition_of(sensor),
                                   POLICIES.deadlines())
    workflow = compiled(sensor, "init", POLICIES, definition)

    assert {name: labeled(workflow.graph, name).timeout_s for name in (
        "dome.Init", "dome.OpenEnclosure", "mount.Init",
        "cover.OpenMirrorCover")} == {
        "dome.Init": 11.0, "dome.OpenEnclosure": 12.0, "mount.Init": 13.0,
        "cover.OpenMirrorCover": 14.0}
    assert {n.payload.timeout_s
            for n in workflow.cleanup[0].graph.nodes} == {POLICIES.stop_timeout}


def test_policy_deadlines_scope_by_trait_only_where_limits_differ():
    rules = {(rule.target, rule.command): rule.seconds
             for rule in SensorPolicies().deadlines()}

    assert rules == {
        (("trait", "enclosure"), "Init"): 300.0,
        (("trait", "enclosure"), "Deinit"): 300.0,
        (("trait", "mount"), "Init"): 30.0,
        (("trait", "mount"), "Home"): 300.0,
        (("trait", "mount"), "Deinit"): 60.0,
        (("any", None), "OpenEnclosure"): 120.0,
        (("any", None), "CloseEnclosure"): 120.0,
        (("any", None), "OpenMirrorCover"): 60.0,
        (("any", None), "CloseMirrorCover"): 60.0,
        (("any", None), "FollowTarget"): 300.0,
        (("any", None), "SetFilter"): 30.0,
        (("any", None), "ConfigureCameraSensor"): 30.0,
        (("any", None), "ChangeFocusPosition"): 30.0,
        (("any", None), "Stop"): 30.0,
        (("any", None), None): 300.0,
    }


SHORT_DOME = ("Connect", "OpenEnclosure", "CloseEnclosure", "Stop", "Abort")
"""A dome reporting no Init or Deinit, so it fails the enclosure trait."""

SHORT_MOUNT = ("Connect", "Init", "Deinit", "Stop", "FollowTarget", "Abort")
"""A mount reporting no park or home, so it fails the mount trait."""


@pytest.mark.asyncio
async def test_a_dome_failing_its_trait_still_closes_under_a_deadline(
        bound, devices, handles, clients, observer, rig):
    short = bound(*devices, reported=handles | {"dome": SHORT_DOME})
    definition = compose_deadlines(definition_of(short), DEFAULTS.deadlines())
    shutdown = compiled(short, "shutdown", DEFAULTS, definition)

    assert labeled(shutdown.graph, "dome.CloseEnclosure").timeout_s == (
        DEFAULTS.dome_open_close_timeout)

    await WorkflowExecutor(short, clients, events=observer).execute(shutdown)

    assert len(rig["dome"].sent("CloseEnclosure")) == 1
    assert not rig["dome"].sent("Deinit")


def test_a_mount_failing_its_trait_is_driven_under_the_default(
        bound, devices, handles):
    short = bound(*devices, reported=handles | {"mount": SHORT_MOUNT})
    definition = compose_deadlines(definition_of(short), DEFAULTS.deadlines())

    for table in ("init", "shutdown"):
        workflow = compiled(short, table, DEFAULTS, definition)
        mount = {name: labeled(workflow.graph, name).timeout_s
                 for name in running(workflow.graph)
                 if name.startswith("mount.")}

        assert mount == ({"mount.Init": DEFAULTS.default_timeout}
                         if table == "init"
                         else {"mount.Deinit": DEFAULTS.default_timeout})


@pytest.mark.parametrize("absent", [set(), {"dome"}, {"cover"}, {"mount"}])
@pytest.mark.parametrize("flags", [
    {}, {"always_deinit_dome": True}, {"concurrent_dome_deinit_close": True},
    {"concurrent_dome_and_mount_deinit": True},
    {"always_deinit_dome": True, "concurrent_dome_deinit_close": True,
     "concurrent_dome_and_mount_deinit": True},
])
def test_every_generated_shutdown_operation_has_a_deadline(
        bound, devices, absent, flags):
    policies = SensorPolicies(**flags)
    sensor = bound(*(d for d in devices if d not in absent))
    definition = compose_deadlines(definition_of(sensor),
                                   policies.deadlines())
    workflow = compiled(sensor, "shutdown", policies, definition)
    deadlines = {label(workflow.graph, n.id): n.payload.timeout_s
                 for n in workflow.graph.nodes}

    assert deadlines
    assert None not in deadlines.values(), deadlines


def test_an_authored_rule_replaces_a_generated_one(sensor):
    authored = DeadlineRule(target=("trait", "enclosure"), command="Init",
                            seconds=5.0)
    definition = compose_deadlines(
        definition_of(sensor, deadlines=(authored,)),
        POLICIES.deadlines())

    assert [rule for rule in definition.deadlines
            if rule.command == "Init"
            and rule.target == ("trait", "enclosure")] == [authored]
    assert labeled(compiled(sensor, "init", POLICIES, definition).graph,
                     "dome.Init").timeout_s == 5.0


def test_a_device_rule_outranks_a_generated_trait_rule(sensor):
    definition = compose_deadlines(
        definition_of(sensor, deadlines=(DeadlineRule(
            target=("device", "dome"), command="Init", seconds=6.0),)),
        POLICIES.deadlines())

    assert len(definition.deadlines) == len(POLICIES.deadlines()) + 1
    assert labeled(compiled(sensor, "init", POLICIES, definition).graph,
                     "dome.Init").timeout_s == 6.0


def test_an_explicit_timeout_still_wins(sensor):
    definition = SensorDefinition.from_yaml(textwrap.dedent("""
        sensor:
          components:
            - device: mount
            - device: dome
            - device: cover
            - device: cam
        tables:
          init:
            fail_fast: true
            phases:
              - name: enclosure
                entries:
                  - select: enclosure
                    ops: {command: Init, timeout_s: 7.0}
        """))
    definition = compose_deadlines(definition, POLICIES.deadlines())

    assert labeled(compiled(sensor, "init", POLICIES, definition).graph,
                     "dome.Init").timeout_s == 7.0
