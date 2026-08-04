# SPDX-License-Identifier: Apache-2.0
"""Performing workflow ops against real devices.

Two halves. The resolution ladder, the command vocabulary and the deadlines are
pure functions of an `Op` and are exercised without a service. Everything that
reaches a device — a phase table run end to end, a cancelled run, a frame's header
— runs against the real stack on the fake backend, with devices that record what
they were sent.
"""
from __future__ import annotations

import asyncio
import contextlib

import pytest
import pytest_asyncio

import sensorkit.api as sk
from sensorkit.api.declarative import command_handler, declare_device
from sensorkit.astro.common import RADecPointing, ReferenceFrame
from sensorkit.astro.target import FrameTarget
from sensorkit.core.device import Abort, DeviceCommand
from sensorkit.sensor.config import SensorDevices, SensorPolicies
from sensorkit.sensor.derive import (
    CAMERA,
    OTA,
    capability_index,
    derive_structure,
    timeouts,
)
from sensorkit.sensor.dispatch import Dispatcher, compile_supported, unresolved
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.std.instrument import (
    AcquireData,
    Binning,
    CameraCapture,
    ConfigureCameraSensor,
)
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import (
    CloseMirrorCover,
    Filter,
    Filters,
    OpenMirrorCover,
    SetFilter,
)
from sensorkit.std.traits import (
    Connect,
    Deinit,
    Home,
    Init,
    MoveToPark,
    SetParkPosition,
    Stop,
)
from sensorkit.workflow import (
    OP_EXPOSE,
    Attachment,
    Collect,
    CollectRunner,
    Entry,
    FramePlan,
    InstrumentAssembly,
    LifecycleError,
    LifecycleRunner,
    Op,
    OpContext,
    Phase,
    PhaseTable,
    SensorModel,
    SensorPlan,
    Step,
    compile_table,
)

PRIMARY_PATH = (OTA, f"{CAMERA}-1")
"""Where the first instrument of a derived structure sits."""

DEVICE_COMMANDS: dict[str, tuple[type[DeviceCommand], ...]] = {
    "tcs-1": (Init, Deinit, Stop, Home, MoveToPark, SetParkPosition, FollowTarget,
              Connect),
    "dome-1": (Init, Deinit, Stop, OpenEnclosure, CloseEnclosure),
    "cover-1": (Stop, OpenMirrorCover, CloseMirrorCover),
    "cam-1": (CameraCapture, ConfigureCameraSensor, Abort),
    "cam-2": (CameraCapture, Abort),
    "fw-1": (SetFilter,),
    "fw-2": (SetFilter,),
    "spec-1": (AcquireData,),
}
"""What each device of the stack answers to.

Enough per device that its archetype matches, so the capability index a dispatcher
reads is the one a real deployment would publish. Only the mount connects, which is
what makes the unsupported-op rule observable.
"""

FULL = SensorDevices(mount="tcs-1", camera="cam-1", filter_wheel="fw-1",
                     mirror_cover="cover-1", dome="dome-1")


class DeviceLog:
    """Records the commands one device received.

    Command types added to `hang` never return, which is how a run is caught
    mid-op; types added to `reject` raise, standing in for hardware that refuses.
    """

    def __init__(self, name: str):
        self.name = name
        self.commands: list[DeviceCommand] = []
        self.hang: set[type] = set()
        self.reject: set[type] = set()
        self.arrived = asyncio.Event()

    async def handle(self, command: DeviceCommand) -> None:
        self.commands.append(command)
        self.arrived.set()

        if type(command) in self.reject:
            raise RuntimeError(f"{type(command).__name__} refused")

        if type(command) in self.hang:
            await asyncio.Event().wait()

    def types(self) -> list[type]:
        return [type(c) for c in self.commands]

    def of[C: DeviceCommand](self, command: type[C]) -> list[C]:
        return [c for c in self.commands if isinstance(c, command)]


def recording_device(name: str, commands: tuple[type[DeviceCommand], ...]):
    """Declare a device answering the given commands by recording them."""
    device = declare_device(name=name)
    log = DeviceLog(name)

    for command in commands:
        async def handler(cmd, _log=log):
            await _log.handle(cmd)

        handler.__annotations__ = {"cmd": command}
        handler.__name__ = f"handle_{name}_{command.__name__}"
        command_handler(device)(handler)

    return device, log


class Stack:
    """A running service whose devices record what a dispatcher sends them."""

    def __init__(self, logs, clients, capabilities):
        self.logs = logs
        self.clients = clients
        self.capabilities = capabilities

    def log(self, name: str) -> DeviceLog:
        return self.logs[name]

    def plan(self, sensor: SensorModel | None = None, **tables) -> SensorPlan:
        return SensorPlan(sensor=sensor or derive_structure("MySensor", FULL),
                          tables=tables)

    def dispatcher(self, plan: SensorPlan, **kwargs) -> Dispatcher:
        return Dispatcher(plan, self.clients, self.capabilities, **kwargs)


@pytest_asyncio.fixture
async def stack(service):
    """The devices of a sensor, on whichever backend the run selected.

    Built on the shared `service` fixture rather than a backend of its own, so
    `SK_TEST_BACKEND=nats` reaches the concurrent dispatch and the cancellation
    path — the two places a real transport and an in-memory one differ.
    """
    logs = {}

    for name, commands in DEVICE_COMMANDS.items():
        device, log = recording_device(name, commands)
        service.add(device)
        logs[name] = log

    await service.start()

    clients = {name: service.client.device(name) for name in DEVICE_COMMANDS}
    details = {name: await client.get_details()
               for name, client in clients.items()}

    yield Stack(logs, clients, capability_index(details))


def op(ref: str, name: str, **kwargs) -> Op:
    return Op(ref=ref, op=name, **kwargs)


def bare() -> Dispatcher:
    """A dispatcher with no devices — enough for every question about an `Op`."""
    return Dispatcher(SensorPlan(sensor=derive_structure("MySensor", FULL)), {}, {})


# ---- the resolution ladder ----------------------------------------------

def test_a_named_device_beats_every_capability_it_claims():
    dispatcher = bare()
    dispatcher.handlers[("dome-1", "Init")] = "by ref"
    dispatcher.handlers[("enclosure", "Init")] = "by trait"

    assert dispatcher.resolve(
        op("dome-1", "Init", trait="enclosure", traits=("enclosure",),
           match="trait")) == "by ref"


def test_a_trait_match_resolves_on_the_trait_it_named():
    """A ref claiming two traits is addressed as the one the table named."""
    dispatcher = bare()
    dispatcher.handlers[("enclosure", "Stop")] = "as an enclosure"
    dispatcher.handlers[("mount", "Stop")] = "as a mount"

    assert dispatcher.resolve(
        op("tcs-1", "Stop", trait="mount", traits=("enclosure", "mount"),
           match="trait")) == "as a mount"


def test_a_per_device_match_walks_the_traits_the_device_claims():
    """`match="all"` names no capability, so the device's own claims answer."""
    dispatcher = bare()
    dispatcher.handlers[("mount", "Connect")] = "as a mount"

    assert dispatcher.resolve(
        op("tcs-1", "Connect", traits=("mount",), match="all")) == "as a mount"


def test_a_structural_match_does_not_walk_traits():
    """An instrument claims traits too, and walking them would let a filter
    changer's handler answer for an exposure."""
    dispatcher = bare()
    dispatcher.handlers[("camera", OP_EXPOSE)] = "as a camera"

    assert dispatcher.resolve(
        op("cam-1", OP_EXPOSE, traits=("camera",), path=PRIMARY_PATH,
           match="instrument")) == dispatcher.expose


def test_an_unregistered_op_falls_through_to_the_command_registry():
    dispatcher = bare()

    assert dispatcher.resolve(
        op("dome-1", "Init", trait="enclosure", match="trait")
    ) == dispatcher.command


# ---- the command vocabulary ---------------------------------------------

def test_an_op_names_a_registered_command():
    assert bare().command_for(op("dome-1", "Init")) == Init()


def test_op_params_are_the_commands_fields():
    assert bare().command_for(
        op("fw-1", "SetFilter", params={"filter": "r"})) == SetFilter(filter="r")


def test_an_op_naming_no_command_is_an_error():
    with pytest.raises(LookupError, match="not a registered device command"):
        bare().command_for(op("dome-1", "Levitate"))


# ---- deadlines -----------------------------------------------------------

def test_a_deadline_is_read_off_the_capability_being_addressed():
    """The same op on two traits takes two deadlines, which is why a timeout is
    not table content."""
    dispatcher = Dispatcher(
        SensorPlan(sensor=derive_structure("MySensor", FULL)), {}, {},
        timeouts(SensorPolicies(dome_init_timeout=300.0, mount_init_timeout=30.0)))

    assert dispatcher.deadline(
        op("dome-1", "Init", trait="enclosure", match="trait")) == 300.0
    assert dispatcher.deadline(
        op("tcs-1", "Init", trait="mount", match="trait")) == 30.0


def test_an_op_no_policy_names_runs_to_completion():
    dispatcher = Dispatcher(
        SensorPlan(sensor=derive_structure("MySensor", FULL)), {}, {},
        timeouts(SensorPolicies()))

    assert dispatcher.deadline(op("tcs-1", "Deinit", trait="mount",
                                  match="trait")) is None


# ---- lifecycle over real devices ----------------------------------------

def bringup() -> PhaseTable:
    return PhaseTable(name="bringup", on_failure="stop", phases=(
        Phase(name="enclosure", entries=(
            Entry(trait="enclosure", ops=["Init", "OpenEnclosure"]),)),
        Phase(name="optics", entries=(
            Entry(trait="mirror_cover", ops="OpenMirrorCover"),)),
    ))


@pytest.mark.asyncio
async def test_a_phase_table_sends_the_commands_it_names(stack):
    plan = stack.plan(bringup=bringup())
    report = await LifecycleRunner(stack.dispatcher(plan)).run(
        plan.devices, plan.tables["bringup"])

    assert report.ok
    assert stack.log("dome-1").types() == [Init, OpenEnclosure]
    assert stack.log("cover-1").types() == [OpenMirrorCover]


@pytest.mark.asyncio
async def test_a_refused_command_fails_the_run(stack):
    """The distinction the unsupported-op rule exists to keep: a device that can
    do something and would not is still a failure."""
    stack.log("dome-1").reject.add(OpenEnclosure)
    plan = stack.plan(bringup=bringup())

    with pytest.raises(LifecycleError):
        await LifecycleRunner(stack.dispatcher(plan)).run(
            plan.devices, plan.tables["bringup"])

    assert stack.log("cover-1").types() == []


@pytest.mark.asyncio
async def test_an_op_outliving_its_deadline_fails(stack):
    stack.log("dome-1").hang.add(OpenEnclosure)
    plan = stack.plan(bringup=bringup())
    dispatcher = stack.dispatcher(
        plan, deadlines=timeouts(SensorPolicies(dome_open_close_timeout=0.05)))

    with pytest.raises(LifecycleError):
        await LifecycleRunner(dispatcher).run(plan.devices, plan.tables["bringup"])


# ---- ops no device can perform ------------------------------------------

def reconnect() -> PhaseTable:
    return PhaseTable(name="reconnect", on_failure="continue", phases=(
        Phase(name="reconnect", entries=(Entry(match="all", ops="Connect"),)),))


@pytest.mark.asyncio
async def test_unresolved_names_the_ops_no_device_can_perform(stack):
    plan = stack.plan(reconnect=reconnect())
    graph = compile_table(plan.devices, plan.tables["reconnect"])

    assert set(unresolved(stack.capabilities, graph)) == {
        (ref, "Connect") for ref in ("dome-1", "cover-1", "cam-1", "fw-1")}


@pytest.mark.asyncio
async def test_an_unsupported_op_is_answered_for_rather_than_attempted(stack):
    """`optional` alone would tolerate a device that refuses as readily as one
    that never had the command, which is the distinction a report needs."""
    plan = stack.plan(reconnect=reconnect())
    table = plan.tables["reconnect"]
    graph = compile_supported(plan.devices, table, stack.capabilities)

    report = await LifecycleRunner(stack.dispatcher(plan)).execute(
        graph, name=table.name)

    assert stack.log("tcs-1").types() == [Connect]
    assert stack.log("dome-1").types() == []
    assert not report.failures
    assert {node.payload.ref for node, _ in report.overridden} == {
        "dome-1", "cover-1", "cam-1", "fw-1"}


# ---- collects ------------------------------------------------------------

def one_step(**settings) -> Collect:
    return Collect(steps=(Step(plans={PRIMARY_PATH: FramePlan(0.5, n_frames=2)},
                               settings=settings),), name="collect")


def tracking() -> FollowTarget:
    return FollowTarget(target=FrameTarget(frame=ReferenceFrame.ICRF))


@pytest.mark.asyncio
async def test_an_apply_sends_the_command_it_carries(stack):
    """An apply's value is already a command, so there is no per-trait apply
    table anywhere."""
    plan = stack.plan()
    report = await CollectRunner(stack.dispatcher(plan)).run(
        plan.topology, plan.devices, one_step(**{"tcs-1": tracking()}))

    assert report.ok
    assert stack.log("tcs-1").types() == [FollowTarget]
    assert stack.log("cam-1").types() == [CameraCapture, CameraCapture]


@pytest.mark.asyncio
async def test_an_exposure_carries_its_own_optical_path(stack):
    """Two cameras, two wheels, one mount: each frame's header holds the devices
    that shaped it and nothing else."""
    twin = SensorModel(
        name="twin",
        attachments=[Attachment(ref="tcs-1", trait="mount")],
        parts=[
            InstrumentAssembly(name="a", instrument="cam-1", attachments=[
                Attachment(ref="fw-1", trait="filter_changer")]),
            InstrumentAssembly(name="b", instrument="cam-2", attachments=[
                Attachment(ref="fw-2", trait="filter_changer")]),
        ],
    )
    plan = stack.plan(twin)
    contexts = {
        "tcs-1": sk.Context(RADecPointing(right_ascension_hours=6.0,
                                          declination_degrees=45.0)),
        "fw-1": sk.Context(Filters(filters=[Filter(name="r")])),
        "fw-2": sk.Context(Filters(filters=[Filter(name="g")])),
    }
    collect = Collect(steps=(Step(plans={("a",): FramePlan(0.1),
                                         ("b",): FramePlan(0.1)}),))

    report = await CollectRunner(stack.dispatcher(
        plan, contexts=lambda: contexts)).run(
        plan.topology, plan.devices, collect)

    assert report.ok

    for camera, band in (("cam-1", "r"), ("cam-2", "g")):
        context = stack.log(camera).of(CameraCapture)[0].context

        # The wheel on this camera's own chain, and the mount above both.
        assert [f.name for f in context[Filters].filters] == [band]
        assert context[RADecPointing].right_ascension_hours == 6.0


@pytest.mark.asyncio
async def test_frame_numbers_are_monotonic_across_steps(stack):
    """`FramePlan` numbers frames within a block, so the steps a sidereal switch
    creates would restart the count a header reads."""
    numbers: list[int] = []
    plan = stack.plan()
    collect = Collect(steps=tuple(
        Step(plans={PRIMARY_PATH: FramePlan(0.1, n_frames=2)},
             settings={"tcs-1": FollowTarget(target=FrameTarget(frame=frame))})
        for frame in (ReferenceFrame.ICRF, ReferenceFrame.ALTAZ)))

    def record(ctx: OpContext, number: int):
        numbers.append(number)
        return ()

    await CollectRunner(stack.dispatcher(plan, frame_keywords=record)).run(
        plan.topology, plan.devices, collect)

    assert numbers == [0, 1, 2, 3]
    assert stack.log("cam-1").types() == [CameraCapture] * 4


@pytest.mark.asyncio
async def test_a_frame_carries_what_the_collect_says_about_it(stack):
    """The seam a task translation fills: the graph knows which exposure a node
    belongs to, and the translation knows what it was asked for."""
    plan = stack.plan()

    def label(ctx: OpContext, number: int):
        return (Binning(x=number, y=number),)

    await CollectRunner(stack.dispatcher(plan, frame_keywords=label)).run(
        plan.topology, plan.devices, one_step())

    assert [c.context[Binning].x
            for c in stack.log("cam-1").of(CameraCapture)] == [0, 1]


@pytest.mark.asyncio
async def test_an_instrument_that_is_not_a_camera_acquires(stack):
    """`expose` resolves on the instrument rung, which is where the choice
    between an exposure and an acquisition is made."""
    plan = stack.plan(SensorModel(name="odd", parts=[
        InstrumentAssembly(name="only", instrument="spec-1")]))
    collect = Collect(steps=(Step(plans={("only",): FramePlan(0.1)}),))

    report = await CollectRunner(stack.dispatcher(plan)).run(
        plan.topology, plan.devices, collect)

    assert report.ok
    assert stack.log("spec-1").types() == [AcquireData]


# ---- cancellation --------------------------------------------------------

async def cancel(run: asyncio.Task, log: DeviceLog) -> None:
    """Cancel a run that has reached the given device, and wait it out."""
    async with asyncio.timeout(5.0):
        await log.arrived.wait()

    run.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await run


@pytest.mark.asyncio
async def test_a_cancelled_op_halts_its_device(stack):
    """Dropping an ExtendedCall does not reach the device, so a cancelled
    exposure would otherwise leave the camera integrating."""
    camera = stack.log("cam-1")
    camera.hang.add(CameraCapture)
    plan = stack.plan()

    await cancel(asyncio.create_task(CollectRunner(stack.dispatcher(plan)).run(
        plan.topology, plan.devices, one_step())), camera)

    assert camera.types() == [CameraCapture, Abort]


@pytest.mark.asyncio
async def test_a_device_with_nothing_to_halt_is_left_alone(stack):
    """A halt is the strongest command the device claims, and claiming none is a
    legitimate answer."""
    wheel = stack.log("fw-1")
    wheel.hang.add(SetFilter)
    plan = stack.plan()
    collect = Collect(steps=(Step(plans={PRIMARY_PATH: FramePlan(
        0.1, settings={"fw-1": SetFilter(filter="r")})}),))

    await cancel(asyncio.create_task(CollectRunner(stack.dispatcher(plan)).run(
        plan.topology, plan.devices, collect)), wheel)

    assert wheel.types() == [SetFilter]
