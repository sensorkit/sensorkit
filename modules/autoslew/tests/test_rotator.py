# SPDX-License-Identifier: Apache-2.0
"""Autoslew rotator — standard move/stop + ASA rotator:setslewoption/homefind."""

import pytest
from conftest import MockAutoslewSDKDevice

from sensorkit.autoslew.rotator import AutoslewRotatorConfig, AutoslewRotatorState
from sensorkit.std import Stop
from sensorkit.std.instrument import ChangeRotatorPosition


def _rotator(**cfg):
    config = AutoslewRotatorConfig(host="localhost", timeout=5.0, status_frequency=0.05, **cfg)
    d = config.create_device()
    d.state = AutoslewRotatorState()
    d.rotator = MockAutoslewSDKDevice(Connected=True, IsMoving=False, MechanicalPosition=100.0)
    d.telescope = MockAutoslewSDKDevice(Connected=True)  # ASA backbone
    d.device_connected = True
    d.rotator_position = 100.0
    d._can_reverse = False
    d._step_size = None
    return d


@pytest.fixture
def rotator():
    return _rotator()


@pytest.mark.asyncio
async def test_rotator_change_moves_absolute(rotator):
    await rotator.rotator_change(ChangeRotatorPosition(position=45.0))
    moves = [c for c in rotator.rotator.calls if c[0] == "MoveAbsolute"]
    assert moves and moves[-1][1][0] == 45.0


@pytest.mark.asyncio
async def test_rotator_stop_halts(rotator):
    await rotator.rotator_stop(Stop())
    assert any(c[0] == "Halt" for c in rotator.rotator.calls)


@pytest.mark.asyncio
async def test_applies_slew_option():
    d = _rotator(slew_option=2)
    await d._apply_asa_settings()
    setslew = [
        c for c in d.telescope.calls if c[0] == "Action" and c[1][0] == "rotator:setslewoption"
    ]
    assert setslew and setslew[-1][1][1] == "2"


@pytest.mark.asyncio
async def test_no_asa_settings_by_default(rotator):
    await rotator._apply_asa_settings()
    assert not rotator.telescope.actions()


@pytest.mark.asyncio
async def test_rotator_home_fires_homefind(rotator):
    from sensorkit.std import Home

    await rotator.rotator_home(Home())
    assert "rotator:homefind" in rotator.telescope.actions()
    assert rotator.state.has_been_homed is True
