# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures.

`demo.yaml` describes a multi-instrument observatory exercising every structural
feature — a selector with two ports, private and shared devices, a guider, two
chillers — so the tests read one config rather than inventing one per module.
"""
from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pytest

from sensorkit.workflow import DeviceIndex, PhaseTable, SensorModel, SensorPlan, Topology

DEMO_YAML = Path(__file__).resolve().parent / "demo.yaml"
GOLDEN = Path(__file__).resolve().parent / "golden"


@pytest.fixture(scope="session")
def assert_golden() -> Callable[[str, str], None]:
    """Compare a rendered graph against golden/<name>.txt.

    The compile stage is pure data-in/data-out, so the whole IR is checkable this
    way with no event loop and no simulated hardware — a compiled graph is the
    design's central claim, and a golden file makes any change to it show up as a
    diff rather than as a behavioural surprise at 2am.

    Regenerate deliberately, and read the diff::

        SK_REGEN=1 uv run pytest
    """
    def check(name: str, actual: str) -> None:
        path = GOLDEN / f"{name}.txt"
        actual = actual.rstrip("\n") + "\n"
        if os.environ.get("SK_REGEN"):
            path.parent.mkdir(exist_ok=True)
            path.write_text(actual)
            return
        assert path.exists(), f"missing golden file {path}; run with SK_REGEN=1"
        assert actual == path.read_text(), f"graph changed vs {path}"

    return check


@pytest.fixture(scope="session")
def config() -> SensorPlan:
    return SensorPlan.load(DEMO_YAML)


@pytest.fixture(scope="session")
def sensor(config: SensorPlan) -> SensorModel:
    return config.sensor


@pytest.fixture(scope="session")
def topo(config: SensorPlan) -> Topology:
    return config.topology


@pytest.fixture(scope="session")
def devices(config: SensorPlan) -> DeviceIndex:
    return config.devices


@pytest.fixture(scope="session")
def init_table(config: SensorPlan) -> PhaseTable:
    return config.tables["init"]


@pytest.fixture(scope="session")
def deinit_table(config: SensorPlan) -> PhaseTable:
    return config.tables["deinit"]
