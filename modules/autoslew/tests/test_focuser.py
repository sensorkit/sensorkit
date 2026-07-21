# SPDX-License-Identifier: Apache-2.0
"""Autoslew focuser — standard move/stop + ASA focuser:homefind via the backbone."""

import pytest
from conftest import MockAutoslewSDKDevice

from sensorkit.autoslew.focuser import AutoslewFocuserConfig, AutoslewFocuserState
from sensorkit.std import Stop
from sensorkit.std.optics import ChangeFocusPosition


def _focuser(**cfg):
    config = AutoslewFocuserConfig(host="localhost", timeout=5.0, status_frequency=0.05, **cfg)
    d = config.create_device()
    d.state = AutoslewFocuserState()
    d.focuser = MockAutoslewSDKDevice(Connected=True, Position=1000.0, IsMoving=False)
    d.telescope = MockAutoslewSDKDevice(Connected=True)  # ASA backbone
    d.device_connected = True
    d.focuser_position = 1000.0
    d._absolute = True
    d._max_step = 28000
    d._max_increment = 50000
    d._step_size = 1.0
    d._temp_comp_available = False
    return d


@pytest.fixture
def focuser():
    return _focuser()


@pytest.mark.asyncio
async def test_focuser_change_moves_absolute(focuser):
    await focuser.focuser_change(ChangeFocusPosition(position=5000))
    moves = [c for c in focuser.focuser.calls if c[0] == "Move"]
    assert moves and moves[-1][1][0] == 5000


@pytest.mark.asyncio
async def test_focuser_change_clamps_to_max_step(focuser):
    await focuser.focuser_change(ChangeFocusPosition(position=99999))
    moves = [c for c in focuser.focuser.calls if c[0] == "Move"]
    assert moves[-1][1][0] == 28000


@pytest.mark.asyncio
async def test_focuser_stop_halts(focuser):
    await focuser.focuser_stop(Stop())
    assert any(c[0] == "Halt" for c in focuser.focuser.calls)


@pytest.mark.asyncio
async def test_focuser_home_fires_homefind(focuser):
    from sensorkit.std import Home

    await focuser.focuser_home(Home())
    assert "focuser:homefind" in focuser.telescope.actions()
    assert focuser.state.has_been_homed is True
