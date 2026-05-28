"""Tests for Alpaca cover calibrator device."""

import pytest
from conftest import MockAlpacaSDKDevice

from sensorkit.alpaca.cover_calibrator import (
    _COVER_CLOSED,
    _COVER_OPEN,
    AlpacaCoverCalibratorConfig,
    AlpacaCoverCalibratorState,
)
from sensorkit.models.devices import Stop
from sensorkit.std import Connect, Disconnect


@pytest.fixture
def cover_calibrator():
    config = AlpacaCoverCalibratorConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    cc = config.create_device()
    cc.state = AlpacaCoverCalibratorState()
    cc.device_name = "CoverCalibrator"
    mock = MockAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        CoverState=_COVER_CLOSED,
        CoverMoving=False,
        CalibratorState=0,
        CalibratorChanging=False,
        Brightness=0,
        MaxBrightness=255,
    )
    cc.cover_calibrator = mock
    cc.device_connected = True
    cc._max_brightness = 255
    return cc


@pytest.mark.asyncio
async def test_cover_calibrator_connect(cover_calibrator):
    cover_calibrator.device_connected = False
    cover_calibrator.cover_calibrator._properties["Connected"] = False
    await cover_calibrator.cover_calibrator_connect(Connect())
    assert cover_calibrator.device_connected is True


@pytest.mark.asyncio
async def test_cover_calibrator_disconnect(cover_calibrator):
    await cover_calibrator.cover_calibrator_disconnect(Disconnect())
    assert cover_calibrator.device_connected is False


@pytest.mark.asyncio
async def test_cover_calibrator_open(cover_calibrator):
    from sensorkit.std.optics import OpenMirrorCover

    cover_calibrator.cover_calibrator._properties["CoverState"] = _COVER_OPEN
    cover_calibrator.cover_calibrator._properties["CoverMoving"] = False
    await cover_calibrator.cover_calibrator_open(OpenMirrorCover())


@pytest.mark.asyncio
async def test_cover_calibrator_close(cover_calibrator):
    from sensorkit.std.optics import CloseMirrorCover

    cover_calibrator.cover_calibrator._properties["CoverState"] = _COVER_CLOSED
    cover_calibrator.cover_calibrator._properties["CoverMoving"] = False
    await cover_calibrator.cover_calibrator_close(CloseMirrorCover())


@pytest.mark.asyncio
async def test_cover_calibrator_stop(cover_calibrator):
    await cover_calibrator.cover_calibrator_stop(Stop())
