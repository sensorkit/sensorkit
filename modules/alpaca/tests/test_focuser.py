# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca focuser device."""

import pytest

from sensorkit.alpaca.focuser import (
    AlpacaFocuserConfig,
    AlpacaFocuserState,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice
from sensorkit.std import Connect, Disconnect


@pytest.fixture
def focuser():
    config = AlpacaFocuserConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    f = config.create_device()
    f.state = AlpacaFocuserState()
    f.device_name = "Focuser"
    device = FakeAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        Position=5000,
        IsMoving=False,
        Absolute=True,
        MaxStep=100000,
        MaxIncrement=100000,
        StepSize=1.0,
        TempCompAvailable=False,
        TempComp=False,
        Temperature=20.0,
    )
    f.focuser = device
    f.device_connected = True
    f.focuser_position = 5000.0
    f._absolute = True
    f._max_step = 100000
    f._max_increment = 100000
    f._step_size = 1.0
    f._temp_comp_available = False
    return f


@pytest.mark.asyncio
async def test_focuser_connect(focuser):
    focuser.device_connected = False
    focuser.focuser._properties["Connected"] = False
    await focuser.focuser_connect(Connect())
    assert focuser.device_connected is True


@pytest.mark.asyncio
async def test_focuser_disconnect(focuser):
    await focuser.focuser_disconnect(Disconnect())
    assert focuser.device_connected is False


@pytest.mark.asyncio
async def test_focuser_move(focuser):
    from sensorkit.std.optics import ChangeFocusPosition

    focuser.focuser._properties["IsMoving"] = False
    await focuser.focuser_change(ChangeFocusPosition(position=10000.0))


@pytest.mark.asyncio
async def test_focuser_change_clamps_to_max_step(focuser):
    from sensorkit.std.optics import ChangeFocusPosition

    moves: list[int] = []
    focuser.focuser.Move = moves.append
    focuser.focuser._properties["IsMoving"] = False

    await focuser.focuser_change(ChangeFocusPosition(position=999999))

    assert moves[-1] == focuser._max_step
