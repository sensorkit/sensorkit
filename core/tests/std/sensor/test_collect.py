# SPDX-License-Identifier: Apache-2.0
"""The collect differential, and the translation underneath it.

Two halves. What a `StandardCollectTask` means as a request — which steps it opens,
which commands it implies, what each frame carries — is a pure function of the task
and the manifest, and is exercised without a service.

The rest is the differential: `Sensor.collect` resolves and runs that request where
`LegacySensor.sensor_collect` hand-writes the same sequence, and both are driven
through a controller as the task they are, since a frame's header is assembled from
an executing task's context. What they are compared on is what each device
received, in order, whether the task failed, and what the frames say about
themselves — never the order commands arrived *across* devices, which a compiled
graph exists to change.

One difference is accepted rather than worked around, and asserted here on its own
terms: the halt that ends a collect goes out however the collect ended, where the
hand-written handler let a failed frame propagate and left the mount tracking.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pytest
import pytest_asyncio

from sensorkit.api.declarative import command_handler, declare_device
from sensorkit.astro.common import TLE, SitePosition
from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import CatalogTarget, ICRSTarget, TLETarget
from sensorkit.common.keyword import get_keyword_info
from sensorkit.core.device import Abort, DeviceCommand
from sensorkit.core.task import TaskInfo
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import (
    Binning,
    CameraCapture,
    ConfigureCameraSensor,
    FrameType,
)
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import SetFilter
from sensorkit.std.sensor.client import connect_sensor
from sensorkit.std.sensor.config import Implementation, SensorConfig, SensorDevices
from sensorkit.std.sensor.derive import derive_structure
from sensorkit.std.sensor.impl import StandardSensor
from sensorkit.std.sensor.legacy import LegacyDevices, LegacySensor
from sensorkit.std.sensor.translate import SIDEREAL, frame_targets, translate
from sensorkit.std.traits import Stop
from sensorkit.workflow import (
    DeviceCapabilities,
    RequestResolver,
    SensorPlan,
)

LEGACY = "legacy-sensor"
WORKFLOW = "workflow-sensor"

SITE = SitePosition(latitude_degrees=33.0, longitude_degrees=-117.0,
                    altitude_km=0.5)

DEVICE_COMMANDS: dict[str, tuple[type[DeviceCommand], ...]] = {
    "mount": (FollowTarget, Stop),
    "camera": (CameraCapture, ConfigureCameraSensor, Abort),
    "wheel": (SetFilter,),
}
"""What each device of the stack answers to: the three a standard collect drives."""

FULL = SensorDevices(mount="mount", camera="camera", filter_wheel="wheel")

NO_WHEEL = SensorDevices(mount="mount", camera="camera")

ISS = TLE(
    line0="ISS (ZARYA)",
    line1="1 25544U 98067A   24001.00000000  .00000000  00000-0  00000-0 0  9990",
    line2="2 25544  51.6400 000.0000 0000000 000.0000 000.0000 15.50000000000000",
)

STAR = ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0))

IDENTITY = frozenset({get_keyword_info(TaskInfo).key, "task_id"})
"""Header keys naming the execution rather than the frame, which two runs of one
collect necessarily disagree about."""


def task(target=None, sidereal_frames: Sequence[int] = (),
         **params) -> StandardCollectTask:
    """A standard collect over the stack's devices."""
    return StandardCollectTask(
        target=target if target is not None else TLETarget(tle=ISS),
        camera_params=CameraParameterSet(
            **{"integration_time_seconds": 0.01, "frame_count": 1, **params}),
        sidereal_frames=list(sidereal_frames),
    )


# ---- the translation, without a sensor to run it -------------------------

def resolver(devices: SensorDevices) -> RequestResolver:
    """A resolver over the derived structure, with every device fully capable."""
    plan = SensorPlan(sensor=derive_structure("MySensor", devices))
    capabilities = {
        ref: DeviceCapabilities(commands=frozenset(
            c.model_tag() for c in DEVICE_COMMANDS[ref]))
        for ref in devices.refs()}

    return RequestResolver(plan.topology, plan.devices, capabilities)


def steps_of(collect: StandardCollectTask,
             devices: SensorDevices = FULL) -> tuple:
    return translate(collect, resolver(devices)).steps


def test_a_collect_under_one_target_is_one_step():
    """A step is one configuration epoch, and a collect that never switches
    tracking is one epoch however many frames it takes."""
    steps = steps_of(task(frame_count=5))

    assert len(steps) == 1
    assert steps[0].exposures[0].frame_count == 5


def test_a_sidereal_frame_opens_a_step_of_its_own():
    """Holding the current pointing is a different epoch from following the
    target, so contiguous runs of each become steps."""
    steps = steps_of(task(frame_count=4, sidereal_frames=(1, 2)))

    assert [s.exposures[0].frame_count for s in steps] == [1, 2, 1]
    assert [s.settings[0].command.target for s in steps] == [
        TLETarget(tle=ISS), SIDEREAL, TLETarget(tle=ISS)]


def test_a_collect_that_opens_sidereal_acquires_the_target_first():
    """Holding sidereal holds whatever the mount already has, so a collect whose
    first frame is sidereal has to reach the target before it can hold it. Its
    first step cannot: a step is a configuration epoch with frames in it, and an
    acquisition is neither."""
    opening = translate(task(frame_count=2, sidereal_frames=(0,)), resolver(FULL))
    later = translate(task(frame_count=2, sidereal_frames=(1,)), resolver(FULL))

    assert opening.acquire == TLETarget(tle=ISS)
    assert later.acquire is None


def test_an_inherently_sidereal_target_never_switches():
    """The slew that acquires a star establishes sidereal tracking already, so
    there is nothing for a sidereal frame to switch to."""
    steps = steps_of(task(STAR, frame_count=3, sidereal_frames=(1,)))

    assert len(steps) == 1
    assert frame_targets(task(STAR, frame_count=3,
                              sidereal_frames=(1,))) == (STAR,) * 3


def test_a_frame_carries_the_target_it_was_taken_under():
    """The header and the pointing come from one answer, which is what keeps a
    frame from being labelled with a configuration it was not taken in."""
    translation = translate(task(frame_count=3, sidereal_frames=(1,)),
                            resolver(FULL))
    frames = translation.frames["camera"]

    assert [f.frame_number for f in frames] == [0, 1, 2]
    assert [f.target for f in frames] == [TLETarget(tle=ISS), SIDEREAL,
                                          TLETarget(tle=ISS)]


def test_a_filter_reaches_the_wheel_in_front_of_the_camera():
    settings = steps_of(task(filter_name="r"))[0].exposures[0].settings

    assert [s.command for s in settings] == [SetFilter(filter="r")]


def test_a_filter_is_left_unsaid_where_none_can_be_changed():
    """A site with a fixed filter names it so that the header records it, and has
    nothing to send it to."""
    assert steps_of(task(filter_name="r"), NO_WHEEL)[0].exposures[0].settings == ()


def test_binning_reaches_the_camera_only_when_both_axes_are_given():
    both = steps_of(task(binning_x=2, binning_y=2))[0].exposures[0].settings
    one = steps_of(task(binning_x=2))[0].exposures[0].settings

    assert [s.command for s in both] == [
        ConfigureCameraSensor(binning=Binning(x=2, y=2))]
    assert one == ()


@pytest.mark.parametrize(("collect", "expected"), [
    pytest.param(task(), "25544", id="norad-id-of-a-tle"),
    pytest.param(task(CatalogTarget(object="M31")), "M31", id="catalog-name"),
    pytest.param(task(STAR), None, id="nothing-to-infer"),
])
def test_a_target_id_is_inferred_where_the_task_does_not_say(collect, expected):
    frames = translate(collect, resolver(FULL)).frames["camera"]

    assert frames[0].target_id == expected


def test_a_sensor_with_no_camera_does_not_resolve_a_collect():
    """`sensor_collect`'s own guard, said structurally: no camera means no
    instrument, and no instrument satisfies the task."""
    with pytest.raises(ValueError, match="matched 0 instrument"):
        steps_of(task(), SensorDevices(mount="mount"))


# ---- the stack both implementations run on -------------------------------

class DeviceLog:
    """Records the commands one device received.

    A command type in `reject` is recorded and then refused, standing in for
    hardware that will not do what it can do. One in `gate` waits on an event the
    test holds, which is how a collect is caught with a frame under way.
    """

    def __init__(self, name: str):
        self.name = name
        self.commands: list[DeviceCommand] = []
        self.reject: set[type] = set()
        self.gate: dict[type, asyncio.Event] = {}
        self.arrived = asyncio.Event()

    async def handle(self, command: DeviceCommand) -> None:
        self.commands.append(command)
        self.arrived.set()

        if (gate := self.gate.get(type(command))) is not None:
            await gate.wait()

        if type(command) in self.reject:
            raise RuntimeError(f"{type(command).__name__} refused")

    async def wait_for(self, command: type) -> None:
        """Block until this device has been sent the given command."""
        async with asyncio.timeout(5.0):
            while command not in self.types():
                self.arrived.clear()
                await self.arrived.wait()

    def types(self) -> list[type]:
        return [type(command) for command in self.commands]

    def of[C: DeviceCommand](self, command: type[C]) -> list[C]:
        return [c for c in self.commands if isinstance(c, command)]

    def clear(self) -> None:
        self.commands.clear()
        self.arrived.clear()


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


@dataclass(frozen=True)
class Run:
    """One implementation's turn at a collect."""

    failed: bool
    commands: Mapping[str, list[DeviceCommand]]

    def types(self) -> dict[str, list[type]]:
        return {name: [type(c) for c in sent]
                for name, sent in self.commands.items()}

    def frames(self, device: str = "camera") -> list[CameraCapture]:
        return [c for c in self.commands[device] if isinstance(c, CameraCapture)]


class Stack:
    """A running service whose devices record what either implementation sends.

    Each implementation gets a controller of its own rather than taking turns on
    one, so a handler is registered once and the two runs cannot interfere.
    """

    def __init__(self, service, controllers, logs: dict[str, DeviceLog]):
        self.service = service
        self.controllers = controllers
        self.logs = logs

    def log(self, name: str) -> DeviceLog:
        return self.logs[name]

    def config(self, which: str, devices: SensorDevices) -> SensorConfig:
        return SensorConfig(
            controller_name=which,
            devices=devices,
            site_position=SITE,
            implementation=(Implementation.LEGACY if which == LEGACY
                            else Implementation.WORKFLOW),
        )

    async def control(self, which: str, config: SensorConfig):
        """One implementation, wired to its controller the way attaching wires it.

        The legacy one builds its device set against a live controller; the
        workflow one resolves what every device reports it can do. Both are what
        `on_attach` does, and neither runs when a handler is reached directly.
        """
        impl = self.controllers[which]

        if which == LEGACY:
            control = LegacySensor(config=config)
            control.sensor = LegacyDevices(impl, config.devices, config.policies)

            return control

        control = StandardSensor(config=config)

        with impl.enter_context():
            await control.controller_init()

        return control

    async def run(self, which: str, devices: SensorDevices,
                  collect: StandardCollectTask) -> Run:
        """Execute one collect as the task it is, and record what came of it."""
        impl = self.controllers[which]
        control = await self.control(which, self.config(which, devices))
        impl.task_handler(StandardCollectTask)(control.sensor_collect)
        await impl.start_device_subscriptions()

        for log in self.logs.values():
            log.clear()

        failed = False
        try:
            await self.service.client.controller(which).execute_task(collect)
        except Exception:
            failed = True

        return Run(failed=failed,
                   commands={name: list(log.commands)
                             for name, log in self.logs.items()})

    async def differ(self, devices: SensorDevices,
                     collect: StandardCollectTask) -> tuple[Run, Run]:
        """Run one collect under both implementations and compare what devices got."""
        legacy = await self.run(LEGACY, devices, collect)
        workflow = await self.run(WORKFLOW, devices, collect)

        assert workflow.failed == legacy.failed
        assert workflow.types() == legacy.types()

        return legacy, workflow


@pytest_asyncio.fixture
async def stack(service):
    """The devices of a sensor and a controller per implementation.

    Built on the shared `service` fixture, so `SK_TEST_BACKEND=nats` reaches the
    concurrent dispatch a compiled graph produces.
    """
    logs = {}

    for name, commands in DEVICE_COMMANDS.items():
        device, log = recording_device(name, commands)
        service.add(device)
        logs[name] = log

    await service.start()

    controllers = {which: await service.context.register_controller(which)
                   for which in (LEGACY, WORKFLOW)}

    yield Stack(service, controllers, logs)


# ---- the differential ----------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize(("devices", "collect"), [
    pytest.param(FULL, task(), id="one-frame"),
    pytest.param(FULL, task(frame_count=3), id="three-frames"),
    pytest.param(FULL, task(STAR, frame_count=2), id="a-star"),
    pytest.param(FULL, task(filter_name="r", binning_x=2, binning_y=2),
                 id="filter-and-binning"),
    pytest.param(FULL, task(binning_x=2), id="one-binning-axis"),
    pytest.param(FULL, task(frame_type=FrameType.DARK), id="a-dark"),
    pytest.param(NO_WHEEL, task(filter_name="r"), id="no-wheel-to-set"),
])
async def test_a_collect_sends_what_the_hand_written_sensor_sends(
        stack, devices, collect):
    await stack.differ(devices, collect)


@pytest.mark.asyncio
@pytest.mark.parametrize("sidereal_frames", [(0,), (1,), (1, 2), (0, 1, 2, 3)],
                         ids=["first", "middle", "a-run", "every-frame"])
async def test_a_sidereal_switch_sends_what_the_hand_written_sensor_sends(
        stack, sidereal_frames):
    """The switch the hand-written frame loop makes, said as a step boundary: the
    mount is commanded exactly where tracking changes and nowhere else."""
    _, workflow = await stack.differ(
        FULL, task(frame_count=4, sidereal_frames=sidereal_frames))

    assert len(workflow.frames()) == 4


@pytest.mark.asyncio
async def test_a_failed_frame_fails_the_collect(stack):
    """`on_failure: stop` ends the collect on the first failure, exactly where the
    raise out of the frame loop ended it.

    Compared a device at a time rather than through the differential, because the
    mount is where the two part company — see the halt below.
    """
    stack.log("camera").reject.add(CameraCapture)
    collect = task(frame_count=2)

    legacy = await stack.run(LEGACY, FULL, collect)
    workflow = await stack.run(WORKFLOW, FULL, collect)

    assert legacy.failed and workflow.failed
    assert len(workflow.frames()) == len(legacy.frames()) == 1


@pytest.mark.asyncio
async def test_a_failed_setting_costs_the_frames_it_invalidated(stack):
    """A frame block hard-depends on the settings that configured it, so a wheel
    that will not move takes the frames with it rather than returning them under
    the wrong filter."""
    stack.log("wheel").reject.add(SetFilter)
    workflow = await stack.run(WORKFLOW, FULL, task(frame_count=3,
                                                    filter_name="r"))

    assert workflow.failed
    assert workflow.frames() == []


# ---- what a frame carries ------------------------------------------------

def header(command: CameraCapture) -> dict:
    """A frame's header, minus what names the execution rather than the frame."""
    return {key: value for key, value in command.context.items()
            if key not in IDENTITY}


@pytest.mark.asyncio
@pytest.mark.parametrize("collect", [
    pytest.param(task(frame_count=3), id="three-frames"),
    pytest.param(task(frame_count=4, sidereal_frames=(1, 2)), id="a-switch"),
    pytest.param(task(CatalogTarget(object="M31"), filter_name="r", binning_x=2,
                      binning_y=2), id="named-and-configured"),
])
async def test_each_frame_carries_the_header_the_hand_written_sensor_produced(
        stack, collect):
    """The change this licenses: a header is built per node from the devices on
    that instrument's own optical path, and never persisted to controller state.
    On a derived structure the chain is every configured device, so the answer is
    the one `update_context` produced."""
    legacy, workflow = await stack.differ(FULL, collect)

    assert [header(c) for c in workflow.frames()] == [
        header(c) for c in legacy.frames()]


@pytest.mark.asyncio
async def test_a_frame_says_which_frame_it_is(stack):
    workflow = await stack.run(WORKFLOW, FULL,
                               task(frame_count=3, sidereal_frames=(1,)))
    frames = [c.context[Collect] for c in workflow.frames()]

    assert [f.frame_number for f in frames] == [0, 1, 2]
    assert [f.track_mode for f in frames] == ["rate", "sidereal", "rate"]


# ---- the accepted difference ---------------------------------------------

@pytest.mark.asyncio
async def test_a_failed_collect_still_halts_the_mount(stack):
    """The hand-written handler let a failed frame propagate, so the mount kept
    tracking. Tracking outlives the last frame however the collect ended, and the
    halt is exactly what a failure leaves undone."""
    stack.log("camera").reject.add(CameraCapture)
    workflow = await stack.run(WORKFLOW, FULL, task())

    assert workflow.failed
    assert stack.log("mount").types() == [FollowTarget, Stop]


@pytest.mark.asyncio
async def test_a_cancelled_collect_halts_the_mount_and_the_camera(stack):
    """An abort reaches the library as an ordinary cancellation: the dispatcher
    stops the op it caught mid-flight, and the halt the collect owes its mount is
    composed above it.

    Driven through the client rather than through a controller, which is the other
    half of what splitting the sensor bought: a script can take frames with no
    service of its own, and gets nothing on the header but what it supplied.
    """
    sensor = await connect_sensor(stack.config(WORKFLOW, FULL),
                                  stack.service.client)
    stack.log("camera").gate[CameraCapture] = asyncio.Event()

    run = asyncio.create_task(sensor.collect(task()))
    await stack.log("camera").wait_for(CameraCapture)
    run.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run

    assert stack.log("camera").types() == [CameraCapture, Abort]
    assert stack.log("mount").types() == [FollowTarget, Stop]
