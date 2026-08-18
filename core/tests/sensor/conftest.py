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

from sensorkit.sensor.topology import Placement, Topology, Structure





@pytest.fixture(scope="session")
def topology() -> Topology:
    return Topology(Structure.model_validate({'name': 'demo', 'components': [{'device': 'mount', 'traits': 'MustConnect', 'tags': ['primary']}, {'device': 'dome', 'traits': ['MustConnect', 'MustEnable']}, {'unit': 'ota', 'components': [{'device': 'cover'}, {'selector': 'pickoff', 'traits': 'MustConnect', 'ports': [{'name': 'science', 'components': [{'device': 'foc-sci'}, {'device': 'wheel'}, {'device': 'cam-sci', 'instrument': True, 'tags': ['science']}]}, {'name': 'guide', 'components': [{'device': 'cam-guide', 'instrument': True, 'tags': ['guiding']}, {'device': 'cam-acq', 'instrument': True}]}]}]}]}))






@pytest.fixture(scope="session")
def placements(topology) -> tuple[Placement, ...]:
    return topology.placements()


@pytest.fixture(scope="session")
def at(topology):
    """Look one placement up by device key, for a test that names hardware."""
    index = {p.device: p for p in topology.placements()}

    return index.__getitem__
