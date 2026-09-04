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

import textwrap
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import yaml

import sensorkit.std.traits  # noqa: F401  registers the command vocabulary
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
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

TAKEN = datetime(2026, 1, 1, tzinfo=UTC)
"""Provenance the tests never read, fixed so nothing varies by clock."""

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
        taken=TAKEN, source="tests")


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
