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

from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.topology import Placement, Topology

from .common import REPORTED, SENSOR_YAML


@pytest.fixture(scope="session")
def definition() -> SensorDefinition:
    return SensorDefinition.load(SENSOR_YAML)


@pytest.fixture(scope="session")
def topology(definition) -> Topology:
    return Topology(definition.sensor)




@pytest.fixture(scope="session")
def facts(topology):
    return ReportedFacts(topology)


@pytest.fixture(scope="session")
def placements(topology) -> tuple[Placement, ...]:
    return topology.placements()


@pytest.fixture(scope="session")
def at(topology):
    """Look one placement up by device key, for a test that names hardware."""
    index = {p.device: p for p in topology.placements()}

    return index.__getitem__

class ReportedFacts:
    def __init__(self, topology):
        self.topology = topology

    def commands(self, device):
        return frozenset(REPORTED[device][0])

    def keywords(self, device):
        return frozenset(REPORTED[device][1])

    def traits(self, placement):
        from sensorkit.core.entity import DeviceDetails
        from sensorkit.core.trait import match_traits
        details = DeviceDetails(supported_commands=self.commands(placement.device),
                                published_keywords=self.keywords(placement.device))
        return frozenset(t.name for t in match_traits(details))

    def tags(self, placement):
        return frozenset(self.topology.record(placement).tags)

    def instrument(self, placement):
        from sensorkit.sensor.topology import Device
        record = self.topology.record(placement)
        return isinstance(record, Device) and record.instrument

    def kind(self, placement):
        from sensorkit.sensor.topology import Selector
        if isinstance(self.topology.record(placement), Selector):
            return "selector"
        return "instrument" if self.instrument(placement) else "device"
