# SPDX-License-Identifier: Apache-2.0
"""Compiling a bound collect into what waits for what.

The claim under test is that a device changes once nothing is collecting through
it, that an acquisition requires every setting it is taken under, and that
nothing else waits. Intents are authored by hand and packed, except where a
standard task is the point, so the compiler sees what a caller would give it.

Most cases read edges. A few run the graph through `DagRunner`, with a dispatch
that fails chosen operations, since which acquisitions a failure skips is the
behavior the edges exist for. Nothing here sleeps on a timing.

The bench has two cameras behind one focuser and a third behind its own wheel,
all under one mount. Selector cases use `sensor.yaml`.
"""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.common.dag import DagRunner, Node, RunReport
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    BoundCollect,
    BoundEpoch,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    RequestEpoch,
    compile_collect,
    pack,
)
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.standard_task import SIDEREAL, translate
from sensorkit.sensor.workflow import (
    Acquisition,
    DeadlineRule,
    ExecutableWorkflow,
    Operation,
    RoutedCommand,
)
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import (
    Binning,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
)
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import ChangeFocusPosition, SelectPort, SetFilter
from sensorkit.std.traits import Stop, TemperatureUnit

from .common import REPORTED, TARGET, pointing, sensor_of, snapshot_of


def adding(reported, **extra: tuple[str, ...]):
    """What devices report, with some commands added to some of them."""
    return {device: (commands + extra.get(device.replace("-", "_"), ()),
                     keywords)
            for device, (commands, keywords) in reported.items()}


POSITIONED = adding(REPORTED, pickoff=("SelectPort",))
"""`sensor.yaml` with a pickoff that can be positioned."""



@pytest.fixture(scope="module")
def bench() -> BoundSensor:
    """Every camera captures and configures, the east pair share a focuser,
    and the west camera has a wheel of its own."""
    camera = ("CameraCapture", "ConfigureCameraSensor",
              "ConfigureCameraCooler")

    return sensor_of("""
        name: bench
        components:
          - device: mount
          - unit: east
            components:
              - device: foc-e
              - device: cam-e1
                instrument: true
              - device: cam-e2
                instrument: true
          - unit: west
            components:
              - device: wheel-w
              - device: cam-w
                instrument: true
        """, {
        "mount": (("FollowTarget", "Stop"), ()),
        "foc-e": (("ChangeFocusPosition",), ()),
        "wheel-w": (("SetFilter",), ()),
        "cam-e1": (camera, ()),
        "cam-e2": (camera, ()),
        "cam-w": (camera, ()),
    })


@pytest.fixture(scope="module")
def ported(topology) -> BoundSensor:
    """`sensor.yaml` with a pickoff that can be positioned and cameras that
    can capture."""
    sensor, _ = BoundSensor.bind(topology, snapshot_of(adding(
        POSITIONED, cam_sci=("CameraCapture",), cam_guide=("CameraCapture",),
        cam_acq=("CameraCapture",))))

    return sensor


def asking(id: str, device: str, count: int = 1, seconds: float = 1.0,
           timeout_s: float | None = None, **kw) -> InstrumentRequest:
    """One request, pinned to one instrument so a case says where work goes."""
    return InstrumentRequest(
        id=id, select=IsRef(device=device),
        acquisition=AcquisitionRequest(integration_time_s=seconds, count=count,
                                       timeout_s=timeout_s),
        **kw)


def compiled(sensor: BoundSensor, *epochs: RequestEpoch,
             **kw) -> ExecutableWorkflow:
    """One hand-authored intent, packed and compiled."""
    deadlines = kw.pop("deadlines", ())

    return compile_collect(
        pack(CollectIntent(name="demo", epochs=epochs, **kw), sensor), sensor,
        deadlines=deadlines)


def focus(position: float) -> CommandRequest:
    return CommandRequest(command=ChangeFocusPosition(position=position))


def configured(x: int, **kw) -> CommandRequest:
    return CommandRequest(command=ConfigureCameraSensor(binning=Binning(x=x,
                                                                        y=x)),
                          **kw)


def cooled(celsius: float) -> CommandRequest:
    setpoint = CameraSensorTemperature(temperature=celsius,
                                       units=TemperatureUnit.CELSIUS)

    return CommandRequest(command=ConfigureCameraCooler(enable=True,
                                                        setpoint=setpoint))


def filtered(name: str, **kw) -> CommandRequest:
    return CommandRequest(command=SetFilter(filter=name), **kw)


def commanding(workflow: ExecutableWorkflow, device: str,
               command: str) -> list[Node]:
    """The nodes sending one command to one device, in emission order."""
    return [n for n in workflow.graph.nodes
            if isinstance(n.payload, Operation)
            and n.payload.target.device == device
            and n.payload.command.model_tag() == command]


def frames(workflow: ExecutableWorkflow, device: str) -> list[Node]:
    return commanding(workflow, device, "CameraCapture")


def only(nodes: list[Node]) -> Node:
    assert len(nodes) == 1, [n.label for n in nodes]

    return nodes[0]


def ordering(workflow: ExecutableWorkflow) -> list[Node]:
    return [n for n in workflow.graph.nodes if n.payload is None]


def requires(workflow: ExecutableWorkflow, node: Node) -> frozenset[int]:
    """What a node needs to have succeeded."""
    return workflow.graph.hard[node.id]


def follows(workflow: ExecutableWorkflow, node: Node) -> frozenset[int]:
    """What a node only waits for."""
    return workflow.graph.deps[node.id] - workflow.graph.hard[node.id]


def upstream(workflow: ExecutableWorkflow, node: Node) -> set[int]:
    """Everything a node transitively waits for."""
    seen: set[int] = set()
    frontier = set(workflow.graph.deps[node.id])

    while frontier:
        nid = frontier.pop()
        seen.add(nid)
        frontier |= workflow.graph.deps[nid] - seen

    return seen


def ids(nodes: list[Node]) -> set[int]:
    return {n.id for n in nodes}


async def run(workflow: ExecutableWorkflow, failing=lambda op: False
              ) -> RunReport:
    """Run the main graph, failing the operations `failing` picks."""
    async def dispatch(node: Node) -> None:
        if node.payload is not None and failing(node.payload):
            raise RuntimeError(f"{node.payload.id} refused")

    return await DagRunner(dispatch).execute(workflow.graph, name=workflow.name)


def outcomes(report: RunReport, nodes: list[Node]) -> list[str]:
    return [report.results[n.id].status for n in nodes]


# Resource ordering


def test_independent_instruments_overlap_across_epoch_boundaries(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("long", "cam-e1", count=3, seconds=60.0),)),
        RequestEpoch(units=(asking("short", "cam-w",
                                   settings=(filtered("r"),)),)))
    east = ids(frames(workflow, "cam-e1"))

    assert not east & upstream(workflow,
                               only(commanding(workflow, "wheel-w",
                                               "SetFilter")))
    assert not east & upstream(workflow, only(frames(workflow, "cam-w")))


def test_a_shared_setting_waits_for_every_reader_and_nothing_else(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-e1", settings=(focus(0.0),)),
                            asking("b", "cam-e2"),
                            asking("c", "cam-w"))),
        RequestEpoch(units=(asking("d", "cam-e1", settings=(focus(1.0),)),
                            asking("e", "cam-w"))))
    refocus = commanding(workflow, "foc-e", "ChangeFocusPosition")[1]
    west = frames(workflow, "cam-w")

    # Draining is ordering, so a failed frame does not stop the focuser.
    assert follows(workflow, refocus) == {frames(workflow, "cam-e1")[0].id,
                                          frames(workflow, "cam-e2")[0].id,
                                          commanding(workflow, "foc-e",
                                                     "ChangeFocusPosition")[0].id}
    assert requires(workflow, refocus) == frozenset()
    assert not ids(west) & upstream(workflow, refocus)
    assert refocus.id not in upstream(workflow, west[1])


def test_a_retarget_drains_every_instrument_under_the_mount(bench):
    workflow = compiled(
        bench,
        RequestEpoch(settings=(pointing(),),
                     units=(asking("a", "cam-e1"), asking("b", "cam-w"))),
        RequestEpoch(settings=(pointing(SIDEREAL),),
                     units=(asking("c", "cam-e1"),)))
    retarget = commanding(workflow, "mount", "FollowTarget")[1]

    assert {frames(workflow, "cam-e1")[0].id,
            frames(workflow, "cam-w")[0].id} <= follows(workflow, retarget)


def test_an_acquisition_requires_every_governing_setting_on_its_chain(bench):
    workflow = compiled(
        bench,
        RequestEpoch(settings=(pointing(),),
                     units=(asking("a", "cam-w", count=2,
                                   settings=(filtered("r"), configured(2))),)))
    governing = ids([*commanding(workflow, "mount", "FollowTarget"),
                     *commanding(workflow, "wheel-w", "SetFilter"),
                     *commanding(workflow, "cam-w", "ConfigureCameraSensor")])
    first, second = frames(workflow, "cam-w")

    assert requires(workflow, first) == governing
    assert requires(workflow, second) == governing
    assert follows(workflow, second) == {first.id}


def test_command_types_on_one_device_govern_separately_and_serialize(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w", settings=(configured(2),)),)),
        RequestEpoch(units=(asking("b", "cam-w", settings=(cooled(-10.0),)),)))
    binned = only(commanding(workflow, "cam-w", "ConfigureCameraSensor"))
    cooler = only(commanding(workflow, "cam-w", "ConfigureCameraCooler"))
    later = frames(workflow, "cam-w")[1]

    assert {binned.id, cooler.id} <= requires(workflow, later)
    assert binned.id in follows(workflow, cooler)


def test_two_types_in_one_epoch_still_take_the_device_in_turn(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w",
                                   settings=(configured(2), cooled(-10.0))),)))
    binned = only(commanding(workflow, "cam-w", "ConfigureCameraSensor"))
    cooler = only(commanding(workflow, "cam-w", "ConfigureCameraCooler"))

    assert follows(workflow, cooler) == {binned.id}


def test_a_later_command_of_one_type_replaces_its_requirement(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w", settings=(configured(2),)),)),
        RequestEpoch(units=(asking("b", "cam-w", settings=(
            CommandRequest(command=ConfigureCameraSensor(gain=1.5)),)),)))
    first, second = commanding(workflow, "cam-w", "ConfigureCameraSensor")
    later = frames(workflow, "cam-w")[1]

    # No field-level merge. The partial command is the later statement.
    assert second.id in requires(workflow, later)
    assert first.id not in requires(workflow, later)


@pytest.mark.asyncio
async def test_a_failed_setting_skips_exactly_the_frames_it_invalidates(
        bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-e1", settings=(configured(2),)),
                            asking("b", "cam-w"))),
        RequestEpoch(units=(asking("c", "cam-e1", settings=(cooled(-10.0),)),
                            asking("d", "cam-w"))),
        fail_fast=False)
    report = await run(workflow, lambda op: isinstance(
        op.command, ConfigureCameraSensor))

    # The cooling setpoint does not stand in for the binning that failed.
    assert outcomes(report, frames(workflow, "cam-e1")) == ["skipped"] * 2
    assert outcomes(report, frames(workflow, "cam-w")) == ["ok"] * 2
    assert outcomes(report, commanding(workflow, "cam-e1",
                                       "ConfigureCameraCooler")) == ["ok"]


@pytest.mark.asyncio
async def test_a_later_success_of_one_type_supersedes_a_failure(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w", settings=(filtered("r"),)),)),
        RequestEpoch(units=(asking("b", "cam-w", settings=(filtered("g"),)),)),
        fail_fast=False)
    report = await run(workflow, lambda op: op.command == SetFilter(filter="r"))

    assert outcomes(report, frames(workflow, "cam-w")) == ["skipped", "ok"]


def test_fail_fast_reaches_every_node(bench):
    epoch = RequestEpoch(settings=(pointing(),),
                         units=(asking("a", "cam-w", count=2),))

    assert {n.on_failure for n in compiled(bench, epoch).graph.nodes} == {
        "stop"}
    assert {n.on_failure for n in compiled(bench, epoch,
                                           fail_fast=False).graph.nodes} == {
        "skip"}


# Elision


def test_an_equal_setting_is_elided_and_still_governs(bench):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w", settings=(filtered("r"),)),)),
        RequestEpoch(units=(asking("b", "cam-w", settings=(filtered("r"),)),)))
    wheel = only(commanding(workflow, "wheel-w", "SetFilter"))

    assert all(wheel.id in requires(workflow, frame)
               for frame in frames(workflow, "cam-w"))


@pytest.mark.parametrize(("before", "after"), [
    (5.0, 9.0), (None, 5.0), (5.0, None),
], ids=["different", "inherited-then-explicit", "explicit-then-inherited"])
def test_equal_commands_under_different_deadlines_stay_distinct(bench, before,
                                                                after):
    workflow = compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w", settings=(
            filtered("r", timeout_s=before),)),)),
        RequestEpoch(units=(asking("b", "cam-w", settings=(
            filtered("r", timeout_s=after),)),)))
    first, second = commanding(workflow, "wheel-w", "SetFilter")

    assert (first.payload.timeout_s, second.payload.timeout_s) == (before,
                                                                   after)
    assert second.id in requires(workflow, frames(workflow, "cam-w")[1])


# Selectors and alignment


def test_selector_positions_follow_the_participants(ported):
    workflow = compiled(
        ported,
        RequestEpoch(units=(asking("a", "cam-sci"),)),
        RequestEpoch(units=(asking("b", "cam-guide"),)),
        RequestEpoch(units=(asking("c", "cam-guide"),)))
    science, guide = commanding(workflow, "pickoff", "SelectPort")

    assert (science.payload.command, guide.payload.command) == (
        SelectPort(port="science"), SelectPort(port="guide"))
    assert frames(workflow, "cam-sci")[0].id in follows(workflow, guide)
    assert all(guide.id in requires(workflow, frame)
               for frame in frames(workflow, "cam-guide"))


def bound_by_hand(sensor: BoundSensor, units, settings=()) -> BoundCollect:
    return BoundCollect(name="demo", epochs=(
        BoundEpoch(units=units, settings=settings),))


def test_a_hand_bound_epoch_commanding_a_positioned_selector_raises(ported,
                                                                    at):
    collect = bound_by_hand(
        ported, asking("a", "cam-sci").expand(at("cam-sci"), 0, 0),
        settings=(RoutedCommand(target=at("pickoff"),
                                command=SelectPort(port="guide")),))

    with pytest.raises(ValueError, match="do not command 'pickoff'"):
        compile_collect(collect, ported)


def test_a_hand_bound_epoch_with_exclusive_participants_raises(ported, at):
    units = (*asking("a", "cam-sci").expand(at("cam-sci"), 0, 0),
             *asking("b", "cam-guide").expand(at("cam-guide"), 0, 0))

    with pytest.raises(ValueError, match="different ports of selector"):
        compile_collect(bound_by_hand(ported, units), ported)


def test_midpoint_alignment_offsets_each_block_from_one_ordering_step(bench):
    workflow = compiled(
        bench,
        RequestEpoch(align="midpoint", settings=(pointing(),),
                     units=(asking("a", "cam-e1", count=2),
                            asking("b", "cam-w", seconds=3.0))))
    align = only(ordering(workflow))
    east, west = frames(workflow, "cam-e1"), frames(workflow, "cam-w")
    slew = only(commanding(workflow, "mount", "FollowTarget"))

    assert align.label.startswith("align")
    assert slew.id in follows(workflow, align)
    assert [f.delay_s for f in east] == pytest.approx([0.5, 0.0])
    assert west[0].delay_s == pytest.approx(0.0)
    assert align.id in follows(workflow, east[0])
    assert align.id in follows(workflow, west[0])
    # The offset starts the block, and the block keeps its requirements.
    assert slew.id in requires(workflow, east[0])


def test_start_alignment_adds_no_ordering_step(bench):
    workflow = compiled(
        bench, RequestEpoch(units=(asking("a", "cam-e1", count=2),
                                   asking("b", "cam-w", seconds=3.0))))

    assert ordering(workflow) == []
    assert {n.delay_s for n in workflow.graph.nodes} == {0.0}


def test_midpoint_alignment_is_local_to_each_child(ported):
    # cam-sci is on the other port, so it takes a child of its own and is
    # aligned with nothing.
    workflow = compiled(
        ported,
        RequestEpoch(align="midpoint",
                     units=(asking("a", "cam-sci", seconds=5.0),
                            asking("b", "cam-guide"),
                            asking("c", "cam-acq", seconds=3.0))))
    align = only(ordering(workflow))

    assert align.group == "epoch 1"
    assert frames(workflow, "cam-sci")[0].delay_s == 0.0
    assert frames(workflow, "cam-guide")[0].delay_s == pytest.approx(1.0)


# Preparation


def test_equal_preparation_and_opening_pointing_are_one_operation(bench):
    workflow = compiled(
        bench,
        RequestEpoch(settings=(pointing(),), units=(asking("a", "cam-w"),)),
        prepare=(pointing(),))
    slew = only(commanding(workflow, "mount", "FollowTarget"))

    assert slew.group == "prepare"
    assert slew.id in requires(workflow, only(frames(workflow, "cam-w")))


def test_different_deadlines_keep_preparation_and_opening_apart(bench):
    workflow = compiled(
        bench,
        RequestEpoch(settings=(pointing(),), units=(asking("a", "cam-w"),)),
        prepare=(pointing(timeout_s=120.0),))
    prepared, opening = commanding(workflow, "mount", "FollowTarget")

    assert (prepared.payload.timeout_s, opening.payload.timeout_s) == (120.0,
                                                                       None)
    assert follows(workflow, opening) == {prepared.id}
    assert {prepared.id, opening.id} <= requires(
        workflow, only(frames(workflow, "cam-w")))


def test_an_opening_sidereal_frame_acquires_the_target_then_tracks(bench):
    task = StandardCollectTask(
        target=TARGET, sidereal_frames=[0],
        camera_params=CameraParameterSet(integration_time_seconds=1.0,
                                         frame_count=2))
    workflow = compile_collect(pack(translate(task), bench), bench)
    acquire, sidereal, resume = commanding(workflow, "mount", "FollowTarget")

    assert [n.payload.command.target for n in (acquire, sidereal, resume)] == [
        TARGET, SIDEREAL, TARGET]
    assert acquire.group == "prepare"
    assert acquire.id in follows(workflow, sidereal)
    assert {acquire.id, sidereal.id} <= requires(
        workflow, next(f for f in workflow.graph.nodes
                       if f.payload is not None
                       and f.payload.acquisition is not None))


@pytest.mark.parametrize("opening", [(), (pointing(),), (pointing(SIDEREAL),)],
                         ids=["absent", "elided", "different"])
@pytest.mark.asyncio
async def test_a_failed_preparation_blocks_every_acquisition(bench, opening):
    workflow = compiled(
        bench,
        RequestEpoch(settings=opening,
                     units=(asking("a", "cam-e1", count=2),
                            asking("b", "cam-w"))),
        RequestEpoch(units=(asking("c", "cam-w"),)),
        prepare=(pointing(),), fail_fast=False)
    prepared = only([n for n in workflow.graph.nodes if n.group == "prepare"])
    report = await run(workflow, lambda op: op is prepared.payload)

    assert report.results[prepared.id].status == "failed"
    assert set(outcomes(report, [*frames(workflow, "cam-e1"),
                                 *frames(workflow, "cam-w")])) == {"skipped"}


def slewing_while_filtering(bench, **kw) -> ExecutableWorkflow:
    """A slew as preparation and a filter move on another device, both needed
    by one frame."""
    return compiled(
        bench,
        RequestEpoch(units=(asking("a", "cam-w", settings=(filtered("r"),)),)),
        prepare=(pointing(),), **kw)


@pytest.mark.asyncio
async def test_a_filter_moves_while_the_slew_is_in_flight(bench):
    workflow = slewing_while_filtering(bench)
    filtering = asyncio.Event()
    slewed = asyncio.Event()
    seen: list[str] = []

    async def dispatch(node: Node) -> None:
        match node.payload:
            case Operation(command=FollowTarget()):
                seen.append("slew starts")
                # Waiting on the filter proves they overlap. A timeout rather
                # than a hang is what a regression looks like.
                await asyncio.wait_for(filtering.wait(), 5.0)
                seen.append("slew ends")
                slewed.set()
            case Operation(command=SetFilter()):
                seen.append("filter starts")
                filtering.set()
                await asyncio.wait_for(slewed.wait(), 5.0)
                seen.append("filter ends")
            case Operation(acquisition=Acquisition()):
                seen.append("capture starts")

    report = await DagRunner(dispatch).execute(workflow.graph)

    assert report.ok
    assert seen.index("filter starts") < seen.index("slew ends")
    assert seen[-1] == "capture starts"
    assert seen.count("capture starts") == 1


@pytest.mark.parametrize("failing", [FollowTarget, SetFilter],
                         ids=["slew", "filter"])
@pytest.mark.asyncio
async def test_either_failing_keeps_the_frame_blocked(bench, failing):
    workflow = slewing_while_filtering(bench, fail_fast=False)
    report = await run(workflow, lambda op: isinstance(op.command, failing))
    other = SetFilter if failing is FollowTarget else FollowTarget
    settled = {type(n.payload.command): report.results[n.id].status
               for n in workflow.graph.nodes
               if isinstance(n.payload, Operation)
               and n.payload.acquisition is None}

    assert settled == {failing: "failed", other: "ok"}
    assert outcomes(report, frames(workflow, "cam-w")) == ["skipped"]


# Cleanup


def test_cleanup_is_its_own_graph_armed_by_preparation(bench):
    workflow = compiled(
        bench, RequestEpoch(units=(asking("a", "cam-w"),)),
        prepare=(pointing(), filtered("r")),
        cleanup=(CommandRequest(command=Stop(), subject="sensor",
                                timeout_s=3.0),),
        cleanup_timeout_s=20.0)
    cleanup = only(list(workflow.cleanup))
    prepared = [n.payload for n in workflow.graph.nodes
                if n.group == "prepare"]

    # Arming asks whether each was attempted, so a slew that failed still arms.
    assert len(prepared) == 2
    assert cleanup.armed_by == tuple(prepared)
    assert (cleanup.when, cleanup.timeout_s) == ("always", 20.0)
    assert cleanup.origin.source == "demo"
    assert [n.payload.timeout_s for n in cleanup.graph.nodes] == [3.0]
    assert commanding(workflow, "mount", "Stop") == []


def test_a_collect_with_nothing_prepared_arms_on_any_command(bench):
    workflow = compiled(
        bench,
        RequestEpoch(align="midpoint",
                     units=(asking("a", "cam-w", settings=(filtered("r"),)),
                            asking("b", "cam-e1", seconds=3.0))),
        cleanup=(CommandRequest(command=Stop(), subject="sensor"),))
    commands = tuple(n.payload for n in workflow.graph.nodes
                     if n.payload is not None)

    # The alignment step sends nothing, so it arms nothing.
    assert len(ordering(workflow)) == 1
    assert len(commands) == 3
    assert only(list(workflow.cleanup)).armed_by == commands


def test_a_collect_with_no_command_never_arms_its_cleanup(bench):
    # Bound by hand, since routing needs participants and there are none.
    mount = next(p for p in bench.topology.placements() if p.device == "mount")
    workflow = compile_collect(
        BoundCollect(name="demo", epochs=(),
                     cleanup=(RoutedCommand(target=mount, command=Stop()),)),
        bench)

    assert workflow.graph.nodes == ()
    assert only(list(workflow.cleanup)).armed_by == ()


def test_collect_cleanup_is_never_fail_fast(bench):
    workflow = compiled(
        bench, RequestEpoch(units=(asking("a", "cam-w"),)),
        prepare=(pointing(),),
        cleanup=(CommandRequest(command=Stop(), subject="sensor"),
                 CommandRequest(command=Stop(), subject="sensor",
                                timeout_s=2.0)),
        fail_fast=True)
    cleanup = only(list(workflow.cleanup))
    first, second = cleanup.graph.nodes

    assert {n.on_failure for n in workflow.graph.nodes} == {"stop"}
    assert {n.on_failure for n in cleanup.graph.nodes} == {"skip"}
    assert cleanup.graph.deps[second.id] == {first.id}
    assert cleanup.graph.hard[second.id] == frozenset()


def test_a_collect_with_no_cleanup_plans_none(bench):
    workflow = compiled(bench, RequestEpoch(units=(asking("a", "cam-w"),)),
                        prepare=(pointing(),))

    assert workflow.cleanup == ()


# What survives compilation


def test_units_reach_their_operations_unchanged(bench):
    intent = CollectIntent(name="demo", epochs=(
        RequestEpoch(units=(asking("a", "cam-w", count=2, timeout_s=12.0,
                                   assignment="a",
                                   settings=(filtered("r", timeout_s=4.0),)),
                            )),
        RequestEpoch(units=(asking("a", "cam-w", timeout_s=12.0,
                                   assignment="a",
                                   settings=(filtered("g"),)),))))
    collect = pack(intent, bench)
    workflow = compile_collect(collect, bench)
    units = [unit for epoch in collect.epochs for unit in epoch.units]
    operations = [n.payload for n in frames(workflow, "cam-w")]

    assert [op.acquisition for op in operations] == [u.acquisition
                                                     for u in units]
    assert all(op.acquisition is u.acquisition
               for op, u in zip(operations, units, strict=True))
    assert [(op.acquisition.index, op.acquisition.frame_number)
            for op in operations] == [(0, 0), (1, 1), (2, 2)]
    assert {op.timeout_s for op in operations} == {12.0}
    assert [n.payload.timeout_s
            for n in commanding(workflow, "wheel-w", "SetFilter")] == [4.0,
                                                                       None]


def test_a_deadline_rule_resolves_what_nothing_authored(bench):
    workflow = compiled(
        bench, RequestEpoch(units=(asking("a", "cam-w", count=2),
                                   asking("b", "cam-e1", timeout_s=7.0))),
        deadlines=(DeadlineRule(target=("any", None), command="CameraCapture",
                                seconds=30.0),))

    assert [f.payload.timeout_s for f in frames(workflow, "cam-w")] == [
        30.0, 30.0]
    assert only(frames(workflow, "cam-e1")).payload.timeout_s == 7.0


def test_the_workflow_takes_the_collect_name(bench):
    workflow = compiled(bench, RequestEpoch(units=(asking("a", "cam-w"),)))

    assert workflow.name == "demo"


def test_a_command_the_device_cannot_perform_is_a_compile_error(topology):
    # Nothing on this snapshot can capture.
    sensor, _ = BoundSensor.bind(topology, snapshot_of(POSITIONED))

    with pytest.raises(ValueError, match="does not support 'CameraCapture'"):
        compiled(sensor, RequestEpoch(units=(asking("a", "cam-acq"),)))


# Standard tasks and direct authoring


SEVERAL = StandardCollectTask(
    target=TARGET, sidereal_frames=[2],
    camera_params=[
        CameraParameterSet(integration_time_seconds=1.0, frame_count=4,
                           filter_name="r"),
        CameraParameterSet(integration_time_seconds=1.0, frame_count=2,
                           binning_x=2, binning_y=2, gain=1.5),
        CameraParameterSet(integration_time_seconds=1.0, frame_count=3,
                           filter_name="r")])
"""Three exposures of four, two and three frames, the third frame sidereal.

Only the west chain changes a filter, so both filtered exposures go there."""


@pytest.mark.asyncio
async def test_a_standard_task_translates_packs_and_compiles(bench):
    collect = pack(translate(SEVERAL), bench)
    workflow = compile_collect(collect, bench)
    report = await run(workflow)
    captured = [n for n in workflow.graph.nodes
                if n.payload is not None and n.payload.acquisition is not None]
    # Compilation takes each instrument's units as one block, so the order
    # compared is each instrument's own.
    planned = sorted(((u.target.device, u.acquisition.frame_number),
                      epoch_index, u.acquisition)
                     for epoch_index, epoch in enumerate(collect.epochs)
                     for u in epoch.units)
    emitted = sorted(((n.payload.target.device,
                       n.payload.acquisition.frame_number),
                      int(n.group.removeprefix("epoch ")),
                      n.payload.acquisition)
                     for n in captured)

    assert report.ok
    assert emitted == planned
    assert all(a is b for (_, _, a), (_, _, b) in zip(emitted, planned,
                                                      strict=True))
    assert {(a.request, key[0]) for key, _, a in planned} == {
        ("exposure-0", "cam-w"), ("exposure-1", "cam-e1"),
        ("exposure-2", "cam-w")}
    assert [(a.index, epoch) for _, epoch, a in planned
            if a.request == "exposure-0"] == [(0, 0), (1, 0), (2, 1), (3, 2)]

    for node in captured:
        acquisition = node.payload.acquisition
        cards = dict(acquisition.keywords[Collect].get_fits_cards())

        assert cards["FRAMENUM"][0] == acquisition.frame_number


def test_a_standard_task_points_once_and_stops_the_mount(bench):
    workflow = compile_collect(pack(translate(SEVERAL), bench), bench)
    slews = commanding(workflow, "mount", "FollowTarget")
    cleanup = only(list(workflow.cleanup))

    # Preparation stands in for the opening pointing, then sidereal and back.
    assert [(n.group, n.payload.command.target) for n in slews] == [
        ("prepare", TARGET), ("epoch 1", SIDEREAL), ("epoch 2", TARGET)]
    assert cleanup.armed_by == (slews[0].payload,)
    assert [n.payload.command for n in cleanup.graph.nodes] == [Stop()]


@pytest.mark.asyncio
async def test_a_directly_authored_collect_needs_no_adapter(bench):
    fanned = InstrumentRequest(
        id="flat", acquisition=AcquisitionRequest(integration_time_s=0.5,
                                                  count=2, distribute="each"),
        settings=(configured(1),))
    workflow = compiled(
        bench, RequestEpoch(settings=(pointing(),), units=(fanned,)),
        prepare=(pointing(),),
        cleanup=(CommandRequest(command=Stop(), subject="sensor"),))
    report = await run(workflow)

    assert report.ok
    assert {device: len(frames(workflow, device))
            for device in ("cam-e1", "cam-e2", "cam-w")} == {
        "cam-e1": 2, "cam-e2": 2, "cam-w": 2}
