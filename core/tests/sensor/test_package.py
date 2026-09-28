# SPDX-License-Identifier: Apache-2.0
"""The package imports every module and exports what it declares."""
from __future__ import annotations

import importlib

import pytest

MODULES = (
    "audit", "binding", "client", "collect", "definition", "dispatch",
    "execution", "impl", "lifecycle", "policies", "selection",
    "standard_task", "topology", "workflow",
)


@pytest.mark.parametrize("name", MODULES)
def test_every_module_imports(name):
    importlib.import_module(f"sensorkit.sensor.{name}")


def test_the_package_exports_what_it_declares():
    import sensorkit.sensor as sensor

    assert sensor.Topology is not None
    assert sensor.Selection is not None
    assert sensor.SensorDefinition is not None

