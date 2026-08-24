# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures.

The `facts` fixture is a real `BoundSensor` over `sensor.yaml`, so a selection
is answered against the traits binding established rather than against a list
written beside them.

The `rig` fixture serves every device a module's `handles` fixture names, and
teardown releases whatever a case left held and drains the tasks it started.
"""
from __future__ import annotations


import pytest

from sensorkit.sensor.binding import BoundSensor, CapabilitySnapshot
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.topology import Placement, Topology

from .common import REPORTED, SENSOR_YAML, snapshot_of


@pytest.fixture(scope="session")
def definition() -> SensorDefinition:
    return SensorDefinition.load(SENSOR_YAML)


@pytest.fixture(scope="session")
def topology(definition) -> Topology:
    return Topology(definition.sensor)


@pytest.fixture(scope="session")
def snapshot() -> CapabilitySnapshot:
    return snapshot_of(REPORTED)


@pytest.fixture(scope="session")
def facts(topology, snapshot) -> BoundSensor:
    sensor, _ = BoundSensor.bind(topology, snapshot)

    return sensor


@pytest.fixture(scope="session")
def placements(topology) -> tuple[Placement, ...]:
    return topology.placements()


@pytest.fixture(scope="session")
def at(topology):
    """Look one placement up by device key, for a test that names hardware."""
    index = {p.device: p for p in topology.placements()}

    return index.__getitem__
