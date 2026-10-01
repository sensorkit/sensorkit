# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures.

The `facts` fixture is a real `BoundSensor` over `sensor.yaml`, so a selection
is answered against the traits binding established rather than against a list
written beside them.

The `rig` fixture serves every device a module's `handles` fixture names, and
teardown releases whatever a case left held and drains the tasks it started.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from sensorkit.common.aio import AsyncObserver
from sensorkit.core.entity import DeviceDetails
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.dispatch import OperationEvent
from sensorkit.sensor.execution import WorkflowExecutor
from sensorkit.sensor.topology import Placement, Topology

from .common import REPORTED, SENSOR_YAML, Rig, details_of


@pytest.fixture(scope="session")
def definition() -> SensorDefinition:
    return SensorDefinition.load(SENSOR_YAML)


@pytest.fixture(scope="session")
def topology(definition) -> Topology:
    return Topology(definition.sensor)


@pytest.fixture(scope="session")
def details() -> dict[str, DeviceDetails]:
    return details_of(REPORTED)


@pytest.fixture(scope="session")
def facts(topology, details) -> BoundSensor:
    sensor = BoundSensor.bind(topology, details)

    return sensor


@pytest.fixture(scope="session")
def placements(topology) -> tuple[Placement, ...]:
    return topology.placements()


@pytest.fixture(scope="session")
def at(topology):
    """Look one placement up by device key, for a test that names hardware."""
    index = {p.device: p for p in topology.placements()}

    return index.__getitem__


@pytest_asyncio.fixture
async def rig(service_context, handles) -> AsyncIterator[Rig]:
    """Every device `handles` names, served with what it handles."""
    rig = Rig()

    for name, commands in handles.items():
        await rig.serve(service_context, name, commands)

    yield rig

    await rig.release()


@pytest.fixture
def clients(kit, rig):
    return {name: kit.device(name) for name in rig.devices}


@pytest.fixture
def observer() -> AsyncObserver[OperationEvent]:
    return AsyncObserver[OperationEvent]()


@pytest.fixture
def executor(sensor, clients, observer) -> WorkflowExecutor:
    return WorkflowExecutor(sensor, clients, events=observer)
