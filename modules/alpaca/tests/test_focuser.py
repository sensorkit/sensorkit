# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca focuser device."""

import pytest

from sensorkit.alpaca.focuser import (
    AlpacaFocuserConfig,
    AlpacaFocuserState,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice, Readings
from sensorkit.std import Connect, Disconnect
from sensorkit.std.focus import ChangeFocusPosition, FocusPosition


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
async def test_focuser_move_verifies_and_publishes_position(focuser, recorder):
    published = await recorder()
    focuser.focuser.Position = 10000

    await focuser.focuser_change(ChangeFocusPosition(position=10000.75))

    assert focuser.focuser.calls("Move") == [((10000,), {})]
    assert focuser.focuser_position == 10000.0
    assert (await published.wait_for(FocusPosition)).position == 10000.0


@pytest.mark.asyncio
@pytest.mark.parametrize("position", [-1.0, 100001.0])
async def test_focuser_rejects_position_outside_travel(focuser, position):
    with pytest.raises(ValueError, match="outside 0 to 100000"):
        await focuser.focuser_change(ChangeFocusPosition(position=position))

    assert focuser.focuser.calls("Move") == []


@pytest.mark.asyncio
async def test_relative_move_uses_fresh_position(focuser):
    focuser._absolute = False
    focuser.focuser_position = 4900.0
    focuser.focuser.Position = Readings(5000, 5100)

    await focuser.focuser_change(ChangeFocusPosition(position=5100))

    assert focuser.focuser.calls("Move") == [((100,), {})]
    assert focuser.focuser_position == 5100.0


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [4800, 5200])
async def test_relative_move_rejects_travel_above_maximum_increment(focuser, target):
    focuser._absolute = False
    focuser._max_increment = 100

    with pytest.raises(ValueError, match="exceeds the maximum increment"):
        await focuser.focuser_change(ChangeFocusPosition(position=target))

    assert focuser.focuser.calls("Move") == []


@pytest.mark.asyncio
async def test_relative_move_requires_current_position(focuser):
    focuser._absolute = False
    focuser.focuser.Position = None

    with pytest.raises(RuntimeError, match="without its current position"):
        await focuser.focuser_change(ChangeFocusPosition(position=5100))

    assert focuser.focuser.calls("Move") == []


@pytest.mark.asyncio
async def test_focuser_rejects_move_that_stops_short(focuser):
    focuser.config.timeout = 0.03
    focuser.config.status_frequency = 0.001
    focuser.focuser.Position = 9500

    with pytest.raises(TimeoutError, match="did not stop at 10000; last position 9500"):
        await focuser.focuser_change(ChangeFocusPosition(position=10000))


@pytest.mark.asyncio
async def test_focuser_waits_for_delayed_motion_and_settling(focuser, recorder):
    published = await recorder()
    focuser.config.status_frequency = 0.001
    focuser.focuser.IsMoving = Readings(False, True, True, False)
    focuser.focuser.Position = Readings(5000, 5000, 10000, 10000)

    await focuser.focuser_change(ChangeFocusPosition(position=10000))

    assert focuser.focuser.IsMoving is False
    assert (await published.wait_for(FocusPosition)).position == 10000.0


@pytest.mark.asyncio
async def test_focuser_requires_position_after_move(focuser):
    focuser.focuser.Position = None

    with pytest.raises(RuntimeError, match="no position after the move"):
        await focuser.focuser_change(ChangeFocusPosition(position=10000))


@pytest.mark.asyncio
async def test_focuser_requires_confirmed_end_of_motion(focuser):
    focuser.config.timeout = 0.05
    focuser.config.status_frequency = 0.001
    focuser.focuser.IsMoving = None
    focuser.focuser.Position = 10000

    with pytest.raises(TimeoutError):
        await focuser.focuser_change(ChangeFocusPosition(position=10000))
