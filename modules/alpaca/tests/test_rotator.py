"""Tests for Alpaca rotator device."""

import pytest

from conftest import MockAlpacaSDKDevice
from sensorkit.alpaca.rotator import (
    AlpacaRotator,
    AlpacaRotatorConfig,
    AlpacaRotatorState,
)


@pytest.fixture
def rotator():
    config = AlpacaRotatorConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    r = config.create_device()
    r.state = AlpacaRotatorState()
    r.device_name = "Rotator"
    mock = MockAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        MechanicalPosition=90.0,
        Position=90.0,
        TargetPosition=90.0,
        IsMoving=False,
        CanReverse=False,
        StepSize=0.1,
    )
    r.rotator = mock
    r.device_connected = True
    r.rotator_position = 90.0
    r._can_reverse = False
    r._step_size = 0.1
    return r


@pytest.mark.asyncio
async def test_rotator_connect(rotator):
    import sensorkit.api as sk

    rotator.device_connected = False
    rotator.rotator._properties["Connected"] = False
    await rotator.rotator_connect(sk.Connect())
    assert rotator.device_connected is True


@pytest.mark.asyncio
async def test_rotator_disconnect(rotator):
    import sensorkit.api as sk

    await rotator.rotator_disconnect(sk.Disconnect())
    assert rotator.device_connected is False


@pytest.mark.asyncio
async def test_rotator_move(rotator):
    from sensorkit.std.instrument import ChangeRotatorPosition

    rotator.rotator._properties["IsMoving"] = False
    await rotator.rotator_change(ChangeRotatorPosition(position=180.0))
