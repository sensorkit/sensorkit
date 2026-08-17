# SPDX-License-Identifier: Apache-2.0
"""The package imports every module and exports what it declares."""
from __future__ import annotations

import importlib

import pytest

MODULES = (
    "audit", "binding", "client", "collect", "definition", "dispatch",
    "execution", "lifecycle", "policies", "selection",
    "standard_task", "topology", "workflow",
)


@pytest.mark.parametrize("name", MODULES)
def test_every_module_imports(name):
    importlib.import_module(f"sensorkit.sensor.{name}")
