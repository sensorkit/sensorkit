# SPDX-License-Identifier: Apache-2.0
"""The lifecycle differential: two implementations, one configuration.

`Sensor` runs the phase tables `derive_tables` generates; `LegacySensor` runs the
hand-written orchestration those tables were derived from. Both drive the same
devices from the same `sensors:` entry, and what they are compared on is what each
device received, in order, and whether the task failed.

Not the order commands arrived *across* devices: a compiled graph makes independent
work concurrent, which is what it is for, so asserting on the global interleaving
would pin exactly the thing the rewrite exists to change.

The divergences are the other half. Each is accepted rather than worked around, and
each has a test asserting the new behaviour, so neither implementation can drift
into the other's while both ship:

* a failed bring-up lets an in-flight move finish instead of cancelling it;
* a recovery halts every device even when a reconnect failed;
* a halt reaches every mover at once rather than one after another;
* under `always_deinit_dome: false` a mount failure may withhold the dome's close,
  where the hand-written gather absorbed both failures and closed it anyway. The
  flag's two meanings contradict each other there; `true` is the setting that
  promises the close, and that promise is asserted directly.
"""
from __future__ import annotations

import asyncio
import itertools
import uuid
from collections import Counter
from collections.abc import Awaitable, Iterator, Sequence

import pytest
import pytest_asyncio

from sensorkit.api.declarative import command_handler, declare_device
from sensorkit.astro.common import SitePosition
from sensorkit.core.device import DeviceCommand
from sensorkit.core.task import InitTask, RecoverTask, ShutdownTask, StandbyTask
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover
from sensorkit.std.sensor.client import Sensor, connect_sensor
from sensorkit.std.sensor.config import (
    SensorConfig,
    SensorDevices,
    SensorPolicies,
)
from sensorkit.std.sensor.legacy import LegacyDevices, LegacySensor
from sensorkit.std.traits import Connect, Deinit, Init, Stop
from sensorkit.workflow import LifecycleError

CONTROLLER = "ctrl"

SITE = SitePosition(latitude_degrees=33.0, longitude_degrees=-117.0,
                    altitude_km=0.5)

DEVICE_COMMANDS: dict[str, tuple[type[DeviceCommand], ...]] = {
    "mount": (Init, Deinit, Stop, Connect),
    "dome": (Init, Deinit, Stop, OpenEnclosure, CloseEnclosure),
    "cover": (Stop, OpenMirrorCover, CloseMirrorCover),
    "camera": (),
}
"""What each device of the stack answers to.

Only the mount connects and only three devices halt, which is what puts the
unsupported-op rule under the differential rather than beside it: a recovery
addresses every device, and the two implementations have to agree about the ones
that have no command for what they were sent.
"""

FULL = dict(mount="mount", camera="camera", dome="dome", mirror_cover="cover")

INIT_FLAGS = ("concurrent_dome_init_open",
              "concurrent_dome_and_mount_init",
              "concurrent_mount_and_mirror_cover_init")

SHUTDOWN_FLAGS = ("concurrent_dome_deinit_close",
                  "concurrent_dome_and_mount_deinit",
                  "always_deinit_dome")

FAILURES = [
    pytest.param(None, None, id="nothing-fails"),
    pytest.param("cover", CloseMirrorCover, id="cover-sticks"),
    pytest.param("mount", Deinit, id="mount-refuses"),
    pytest.param("dome", CloseEnclosure, id="dome-sticks"),
]
"""Where a shutdown can fail: the first step, the middle one, and the last."""

CONTESTED = ("mount", False, True)
"""The (failure, always_deinit_dome, concurrent_dome_and_mount_deinit) cell where
the legacy flags contradict each other — see the module docstring."""


class DeviceLog:
    """Records the commands one device received.

    A command type in `reject` is recorded and then refused, standing in for
    hardware that will not do what it can do. One in `gate` waits on an event the
    test holds, which is how a run is caught with work under way.
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

    def types(self) -> list[type]:
        """The type of each command received, in order."""
        return [type(command) for command in self.commands]

    def clear(self) -> None:
        self.commands.clear()
        self.arrived.clear()

    async def wait_for(self, command: type) -> None:
        """Block until this device has been sent the given command."""
        async with asyncio.timeout(5.0):
            while command not in self.types():
                self.arrived.clear()
                await self.arrived.wait()


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
    """A running service whose devices record what an implementation sends them."""

    def __init__(self, controller, client, logs: dict[str, DeviceLog]):
        self.controller = controller
        self.client = client
        self.logs = logs

    def log(self, name: str) -> DeviceLog:
        return self.logs[name]

    def config(self, policies: SensorPolicies | None = None,
               **devices: str) -> SensorConfig:
        return SensorConfig(
            controller_name=CONTROLLER,
            devices=SensorDevices(**(devices or FULL)),
            site_position=SITE,
            policies=policies or SensorPolicies(),
        )

    def legacy(self, config: SensorConfig) -> LegacySensor:
        """The hand-written implementation over this stack's devices.

        It builds its device set while attaching to a live controller, which a
        handler invoked directly never gets; this supplies the equivalent one.
        """
        control = LegacySensor(config=config)
        control.sensor = LegacyDevices(self.controller, config.devices,
                                       config.policies)

        return control

    async def workflow(self, config: SensorConfig) -> Sensor:
        return await connect_sensor(config, self.client)

    def received(self) -> dict[str, list[type]]:
        """What every device has been sent so far, per device."""
        return {name: log.types() for name, log in self.logs.items()}

    def clear(self) -> None:
        for log in self.logs.values():
            log.clear()


@pytest_asyncio.fixture
async def stack(service):
    """The devices of a sensor, on whichever backend the run selected.

    Built on the shared `service` fixture, so `SK_TEST_BACKEND=nats` reaches the
    concurrent dispatch a compiled graph produces.
    """
    logs = {}

    for name, commands in DEVICE_COMMANDS.items():
        device, log = recording_device(name, commands)
        service.add(device)
        logs[name] = log

    await service.start()

    yield Stack(await service.context.register_controller(CONTROLLER),
                service.client, logs)


def combinations(flags: Sequence[str]) -> Iterator[pytest.param]:
    """Every way the flags one action reads can be set."""
    for values in itertools.product((False, True), repeat=len(flags)):
        chosen = dict(zip(flags, values, strict=True))
        yield pytest.param(
            SensorPolicies(**chosen),
            id="+".join(f for f, on in chosen.items() if on) or "no-concurrency")


def legacy_action(control: LegacySensor, action: str) -> Awaitable[None]:
    """One lifecycle action of the hand-written implementation, as its handler."""
    task = dict(task_id=uuid.uuid4(), controller_id=CONTROLLER)

    match action:
        case "init":
            return control.sensor_init(InitTask(**task))
        case "standby":
            return control.sensor_standby(StandbyTask(**task))
        case "shutdown":
            return control.sensor_shutdown(ShutdownTask(**task))
        case "recover":
            return control.sensor_recover(RecoverTask(**task))
        case _:
            raise ValueError(f"no legacy handler for '{action}'")


async def failed(run: Awaitable[object]) -> bool:
    """Whether an action failed — the whole of what both implementations say alike.

    The exception itself cannot match: the workflow implementation raises one
    `LifecycleError` carrying a report where the hand-written one raises the first
    failure, or groups them.
    """
    try:
        await run
    except Exception:
        return True

    return False


def unordered(config: SensorConfig, action: str) -> frozenset[str]:
    """Devices whose own ops a policy made concurrent.

    Their arrival order is not something either implementation promises, so it is
    the one place the comparison is by multiset rather than by sequence.
    """
    policies = config.policies

    match action:
        case "init" | "standby" if policies.concurrent_dome_init_open:
            return frozenset({config.devices.dome})
        case "shutdown" if policies.concurrent_dome_deinit_close:
            return frozenset({config.devices.dome})
        case _:
            return frozenset()


async def differ(stack: Stack, config: SensorConfig, action: str) -> None:
    """Run one action under both implementations and compare what devices got."""
    control = stack.legacy(config)

    with stack.controller.enter_context():
        legacy_failed = await failed(legacy_action(control, action))

    legacy = stack.received()
    stack.clear()

    sensor = await stack.workflow(config)
    workflow_failed = await failed(getattr(sensor, action)())
    workflow = stack.received()

    assert workflow_failed == legacy_failed

    loose = unordered(config, action)
    for name, sent in workflow.items():
        if name in loose:
            assert Counter(sent) == Counter(legacy[name]), name
        else:
            assert sent == legacy[name], name


# ---- the differential ----------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["init", "standby"])
@pytest.mark.parametrize("policies", list(combinations(INIT_FLAGS)))
async def test_bring_up_sends_what_the_hand_written_sensor_sends(
        stack, policies, action):
    await differ(stack, stack.config(policies), action)


@pytest.mark.asyncio
@pytest.mark.parametrize("policies", list(combinations(INIT_FLAGS)))
@pytest.mark.parametrize(("device", "command"),
                         [pytest.param("dome", OpenEnclosure, id="dome-sticks"),
                          pytest.param("cover", OpenMirrorCover, id="cover-sticks")])
async def test_a_failed_bring_up_halts_what_it_started(
        stack, policies, device, command):
    """The halt a failed bring-up composes is the `stop_all` the handler ran, and
    it reaches the same devices with the same commands."""
    stack.log(device).reject.add(command)

    await differ(stack, stack.config(policies), "init")


@pytest.mark.asyncio
@pytest.mark.parametrize("policies", list(combinations(SHUTDOWN_FLAGS)))
@pytest.mark.parametrize(("device", "command"), FAILURES)
async def test_shutdown_sends_what_the_hand_written_sensor_sends(
        stack, policies, device, command):
    if (device, policies.always_deinit_dome,
            policies.concurrent_dome_and_mount_deinit) == CONTESTED:
        pytest.skip("the flags contradict each other here; the close the one "
                    "that promises it makes is asserted separately")

    if device is not None:
        stack.log(device).reject.add(command)

    await differ(stack, stack.config(policies), "shutdown")


@pytest.mark.asyncio
async def test_recover_sends_what_the_hand_written_sensor_sends(stack):
    """Including the devices with no command for what they were sent: the
    hand-written recovery tolerated the rejection, and this one never asks."""
    await differ(stack, stack.config(), "recover")


@pytest.mark.asyncio
async def test_a_sensor_with_no_optional_devices_still_agrees(stack):
    """A `SensorDevices` naming nothing but a mount and a camera derives a
    structure with no enclosure and no optics, so most of both tables selects
    nothing at all."""
    config = stack.config(mount="mount", camera="camera")

    await differ(stack, config, "init")


# ---- what always_deinit_dome promises ------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("concurrent", [False, True])
@pytest.mark.parametrize(("device", "command"), FAILURES[1:])
async def test_always_deinit_dome_closes_the_dome_whatever_failed(
        stack, concurrent, device, command):
    """The guarantee the flag exists to make, and the one that has to hold under
    every failure: a phase link is soft by construction, so nothing upstream of the
    close can withhold it."""
    stack.log(device).reject.add(command)
    sensor = await stack.workflow(stack.config(SensorPolicies(
        always_deinit_dome=True, concurrent_dome_and_mount_deinit=concurrent)))

    with pytest.raises(LifecycleError):
        await sensor.shutdown()

    assert CloseEnclosure in stack.log("dome").types()


# ---- the accepted differences --------------------------------------------

@pytest.mark.asyncio
async def test_a_failed_bring_up_lets_an_in_flight_move_finish(stack):
    """The hand-written init cancelled its in-flight siblings on the first failure.

    Cancelling a mount mid-home is the hazard that names, and it costs nothing to
    let the move finish: dispatch stops either way, and the halt still follows.
    """
    stack.log("dome").reject.add(OpenEnclosure)
    release = asyncio.Event()
    stack.log("mount").gate[Init] = release

    sensor = await stack.workflow(stack.config(
        SensorPolicies(concurrent_dome_and_mount_init=True)))
    run = asyncio.create_task(sensor.init())
    await stack.log("mount").wait_for(Init)
    release.set()

    with pytest.raises(LifecycleError) as failure:
        await run

    assert ("mount", "Init") in {(node.payload.ref, node.payload.op)
                                 for node, _ in failure.value.report.with_status("ok")}


@pytest.mark.asyncio
async def test_a_recovery_halts_every_device_even_when_a_reconnect_failed(stack):
    """The hand-written recovery gathered its reconnects and abandoned the halts if
    any of them failed. A recovery table's whole tolerance is `continue` — nothing
    holds anything else back — and halting is the half that matters most."""
    stack.log("mount").reject.add(Connect)
    sensor = await stack.workflow(stack.config())

    with pytest.raises(LifecycleError):
        await sensor.recover()

    assert stack.log("mount").types() == [Connect, Stop]
    assert stack.log("dome").types() == [Stop]


@pytest.mark.asyncio
async def test_a_halt_reaches_every_mover_at_once(stack):
    """The hand-written halt sent its three Stops one after another. Concurrency is
    what a compiled graph exists to provide; per-device sequences are unchanged,
    which is what the differential asserts."""
    release = asyncio.Event()
    movers = ("mount", "dome", "cover")

    for name in movers:
        stack.log(name).gate[Stop] = release

    sensor = await stack.workflow(stack.config())
    run = asyncio.create_task(sensor.stop())

    # All three are under way before any of them returns.
    for name in movers:
        await stack.log(name).wait_for(Stop)

    release.set()
    report = await run

    assert report.ok


@pytest.mark.asyncio
async def test_a_cancelled_bring_up_halts_the_devices(stack):
    """An abort reaches the library as an ordinary cancellation, so the halt the
    hand-written init ran is composed here rather than in the graph."""
    release = asyncio.Event()
    stack.log("dome").gate[OpenEnclosure] = release

    sensor = await stack.workflow(stack.config())
    run = asyncio.create_task(sensor.init())
    await stack.log("dome").wait_for(OpenEnclosure)
    run.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run

    # The dispatcher halts the op it caught mid-flight; the stop table reaches
    # everything the bring-up could have started.
    assert stack.log("dome").types() == [Init, OpenEnclosure, Stop, Stop]
    assert stack.log("mount").types() == [Stop]
