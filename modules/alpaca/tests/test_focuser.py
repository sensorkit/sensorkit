"""Tests for Alpaca focuser device."""

import pytest

from conftest import MockAlpacaSDKDevice
from sensorkit.alpaca.focuser import (
    AlpacaFocuser,
    AlpacaFocuserConfig,
    AlpacaFocuserState,
)


@pytest.fixture
def focuser():
    config = AlpacaFocuserConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    f = config.create_device()
    f.state = AlpacaFocuserState()
    f.device_name = "Focuser"
    mock = MockAlpacaSDKDevice(
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
    f.focuser = mock
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
    import sensorkit.api as sk

    focuser.device_connected = False
    focuser.focuser._properties["Connected"] = False
    await focuser.focuser_connect(sk.Connect())
    assert focuser.device_connected is True


@pytest.mark.asyncio
async def test_focuser_disconnect(focuser):
    import sensorkit.api as sk

    await focuser.focuser_disconnect(sk.Disconnect())
    assert focuser.device_connected is False


@pytest.mark.asyncio
async def test_focuser_move(focuser):
    from sensorkit.std.optics import ChangeFocusPosition

    focuser.focuser._properties["IsMoving"] = False
    await focuser.focuser_change_position(ChangeFocusPosition(position=10000.0))
