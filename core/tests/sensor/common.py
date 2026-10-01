# SPDX-License-Identifier: Apache-2.0
"""What the sensor tests share besides fixtures.

`sensor.yaml` describes one sensor exercising every structural feature the
package has, so the tests read one document rather than inventing one each.

`REPORTED` is what each device says about itself, and `snapshot_of` turns it
into a capability snapshot, so a test that needs a device to report something
else passes a variant of the mapping rather than building details by hand.

`BENCH` is one mount shared by two instrument branches. The east branch has a
focuser and a camera, and the west branch has a filter wheel and a camera.

A `Rig` serves devices on the fake backend, so commands cross the real request
machinery. Each device records what it received and what else was in flight
when it arrived, and can hold, delay or refuse a command. Ordering is read from
those records and from arrival and ending events, never from sleeping.
"""
from __future__ import annotations

import asyncio
import textwrap
from collections import Counter, defaultdict
from collections.abc import Coroutine, Iterable, Mapping
from pathlib import Path

import yaml

import sensorkit.std.traits  # noqa: F401  registers the command vocabulary
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.common.dag import RunReport
from sensorkit.core.device import DeviceCommand
from sensorkit.core.entity import DeviceDetails
from sensorkit.sensor.binding import BoundSensor, CapabilitySnapshot
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    CommandRequest,
    InstrumentRequest,
)
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.selection import IsRef
from sensorkit.sensor.topology import Placement, Structure, Topology
from sensorkit.sensor.workflow import Cleanup, ExecutableWorkflow, Operation
from sensorkit.std.mount import FollowTarget

SENSOR_YAML = Path(__file__).resolve().parent / "sensor.yaml"

type Reported = Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]]
"""What devices report, as command ids and published keyword keys."""

REPORTED: Reported = {
    "mount": (("Connect", "Disconnect", "Home", "Stop"), ()),
    "dome": (("Connect", "Disconnect", "Enable", "Disable", "Init"),
             ("Enabled",)),
    "cover": (("Connect", "Disconnect"), ()),
    "pickoff": (("Connect", "Disconnect", "Home"), ()),
    "foc-sci": (("Connect", "Disconnect", "Home"), ()),
    "wheel": (("Connect", "Disconnect", "Home"), ()),
    "cam-sci": (("Connect", "Disconnect", "Init", "Deinit"), ()),
    "cam-guide": (("Connect", "Disconnect", "Init"), ()),
    "cam-acq": (("Connect",), ()),
}
"""What each device in `sensor.yaml` reports.

`cam-acq` reports least, which is what makes an unsupported command and a
device satisfying no trait reachable in a test.
"""

BRANCHES = """
        - unit: east
          components:
            - device: foc-e
            - device: cam-e
              instrument: true
        - unit: west
          components:
            - device: wheel-w
            - device: cam-w
              instrument: true
    """
"""The east and west instrument branches, to follow a list of root devices."""

BENCH = """
    sensor:
      name: bench
      components:
        - device: mount
    """ + BRANCHES
"""One mount shared by the east and west branches."""

TARGET = AltAzTarget(coords=Horizontal(az=90.0, alt=30.0))
"""Followed by rate, so a sidereal frame is a change of pointing."""


def snapshot_of(reported: Reported) -> CapabilitySnapshot:
    """A capability snapshot of what these devices report."""
    return CapabilitySnapshot(
        devices=tuple(
            (device, DeviceDetails(supported_commands=frozenset(commands),
                                   published_keywords=frozenset(keywords)))
            for device, (commands, keywords) in reported.items()),
        source="tests")


def sensor_of(structure: str | Structure, reported: Reported) -> BoundSensor:
    """A structure bound to what its devices report, given as YAML or a model."""
    match structure:
        case str():
            structure = Structure.model_validate(
                yaml.safe_load(textwrap.dedent(structure)))

    sensor, _ = BoundSensor.bind(Topology(structure), snapshot_of(reported))

    return sensor


def authored(*parts: str) -> SensorDefinition:
    """A definition loaded from these document parts, each dedented."""
    return SensorDefinition.from_yaml(
        "".join(textwrap.dedent(p) for p in parts))


def placement(sensor: BoundSensor, device: str) -> Placement:
    return next(p for p in sensor.topology.placements() if p.device == device)


def asking(device: str, count: int = 1, **kw) -> InstrumentRequest:
    """A request for frames from one named instrument."""
    return InstrumentRequest(
        id=f"{device}-frames", select=IsRef(device=device),
        acquisition=AcquisitionRequest(integration_time_s=0.1, count=count),
        **kw)


def pointing(target=TARGET, **kw) -> CommandRequest:
    """Following a target, commanded for the whole sensor."""
    return CommandRequest(command=FollowTarget(target=target), subject="sensor",
                          **kw)


def operation(workflow: ExecutableWorkflow | Cleanup, command: str,
              device: str | None = None) -> Operation:
    """The one operation of a run or a cleanup sending this command."""
    found = [n.payload for n in workflow.graph.nodes
             if isinstance(n.payload, Operation)
             and n.payload.command.model_tag() == command
             and device in (None, n.payload.target.device)]
    assert len(found) == 1, found

    return found[0]


def status(run: RunReport, operation: Operation) -> str | None:
    """How one operation's node ended in a run, or None if it has no result."""
    nid = next(n.id for n in run.graph.nodes if n.payload is operation)
    result = run.results.get(nid)

    return None if result is None else result.status


def ran(outcome) -> list[str]:
    """The cleanups a report or an execution state records, by name."""
    return [c.run.name for c in outcome.cleanup]


def abort_only(cancellation: BaseException) -> bool:
    """Claims a cancellation the caller sent as an abort, and nothing else."""
    return str(cancellation) == "abort"


def nth(events: list[asyncio.Event], count: int) -> asyncio.Event:
    """The event for one numbered occurrence, counting from one."""
    while len(events) < count:
        events.append(asyncio.Event())

    return events[count - 1]


async def reached(event: asyncio.Event) -> None:
    async with asyncio.timeout(2.0):
        await event.wait()


async def ended(task: asyncio.Task) -> None:
    """Wait for a task to end, however it ends."""
    async with asyncio.timeout(2.0):
        await asyncio.wait({task})


async def finished(task: asyncio.Task):
    """What a task returns, or what it raises."""
    async with asyncio.timeout(5.0):
        return await task


async def observed(queue: asyncio.Queue, device: str, command: str,
                   outcome: str) -> None:
    """Wait until the executor reports one operation reaching an outcome."""
    async with asyncio.timeout(2.0):
        while True:
            event = await queue.get()
            operation = event.operation

            if (operation.target.device, operation.command.model_tag(),
                    event.outcome) == (device, command, outcome):
                return


class Device:
    """What one device received, and how it answers."""

    def __init__(self, name: str, rig: Rig):
        self.name = name
        self.rig = rig
        self.received: list[DeviceCommand] = []
        self.during: list[frozenset[str]] = []
        self.gates: defaultdict[str, list[asyncio.Event]] = defaultdict(list)
        self.waiting: set[asyncio.Event] = set()
        self.waits: dict[str, asyncio.Event] = {}
        self.refusing: Counter[str] = Counter()
        self.arrivals: defaultdict[str, list[asyncio.Event]] = defaultdict(
            list)
        self.endings: defaultdict[str, list[asyncio.Event]] = defaultdict(list)

    def sent(self, command: str) -> list[DeviceCommand]:
        return [c for c in self.received if c.model_tag() == command]

    def overlapping(self, command: str, count: int = 1) -> frozenset[str]:
        """What else was in flight on the rig when this arrival came."""
        arrivals = [i for i, c in enumerate(self.received)
                    if c.model_tag() == command]

        return self.during[arrivals[count - 1]]

    def hold(self, command: str) -> asyncio.Event:
        """Hold the next arrival of a command until released.

        A held command other than Abort is also released when the device is
        aborted.
        """
        gate = asyncio.Event()
        self.gates[command].append(gate)
        self.rig.gates.append(gate)

        return gate

    def arrival(self, command: str, count: int = 1) -> asyncio.Event:
        return nth(self.arrivals[command], count)

    def ending(self, command: str, count: int = 1) -> asyncio.Event:
        return nth(self.endings[command], count)

    async def handle(self, command: DeviceCommand) -> None:
        tag = command.model_tag()
        key = f"{self.name} {tag}"
        self.received.append(command)
        self.during.append(self.rig.in_flight())
        self.rig.log.append(key)
        self.rig.busy[key] += 1
        count = len(self.sent(tag))
        self.arrival(tag, count).set()

        try:
            if tag in self.waits:
                await self.waits[tag].wait()

            if self.gates[tag]:
                gate = self.gates[tag].pop(0)

                if tag != "Abort":
                    self.waiting.add(gate)

                await gate.wait()
                self.waiting.discard(gate)

            if tag == "Abort":
                for gate in self.waiting:
                    gate.set()

            if self.refusing[tag] > 0:
                self.refusing[tag] -= 1
                raise RuntimeError(f"{tag} refused")
        finally:
            self.rig.busy[key] -= 1
            self.rig.log.append(f"{key} ends")
            self.ending(tag, count).set()


class Rig:
    """The live devices, the order things reached them, and what to release."""

    def __init__(self):
        self.log: list[str] = []
        self.busy: Counter[str] = Counter()
        self.devices: dict[str, Device] = {}
        self.gates: list[asyncio.Event] = []
        self.tasks: set[asyncio.Task] = set()

    def __getitem__(self, name: str) -> Device:
        return self.devices[name]

    async def serve(self, context, name: str,
                    commands: Iterable[str]) -> Device:
        """Serve one device, publishing what it handles once it handles it."""
        impl = await context.register_device(name)
        device = self.devices[name] = Device(name, self)

        for tag in commands:
            impl.command_handler(DeviceCommand.registry.get_type(tag))(
                device.handle)

        await impl.publish_entity_info()

        return device

    def in_flight(self) -> frozenset[str]:
        return frozenset(key for key, n in self.busy.items() if n)

    def arrived(self) -> list[str]:
        """Every arrival in the log, in order."""
        return [line for line in self.log if not line.endswith(" ends")]

    def start(self, run: Coroutine) -> asyncio.Task:
        task = asyncio.create_task(run)
        self.tasks.add(task)

        return task

    def at(self, line: str, count: int = 1) -> int:
        """Where one numbered occurrence of a line is in the log."""
        return [i for i, seen in enumerate(self.log) if seen == line][count - 1]

    def ended_before(self, earlier: str, later: str) -> bool:
        """Whether the first of one command ended before another arrived."""
        return self.at(f"{earlier} ends") < self.at(later)

    async def release(self) -> None:
        """Open every gate, and end whatever a case left running."""
        for gate in self.gates:
            gate.set()

        if not self.tasks:
            return

        _, pending = await asyncio.wait(self.tasks, timeout=5.0)

        for task in pending:
            task.cancel()

        await asyncio.wait(self.tasks, timeout=5.0)
