# SPDX-License-Identifier: Apache-2.0
"""Autoslew mirror cover — open/close via the standard CoverCalibrator device."""

import pytest
from conftest import MockAutoslewSDKDevice

from sensorkit.autoslew.cover_calibrator import (
    AutoslewCoverCalibratorConfig,
    AutoslewCoverCalibratorState,
)
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


def _cover(cover_state=4):
    config = AutoslewCoverCalibratorConfig(host="localhost", timeout=5.0, status_frequency=0.05)
    d = config.create_device()
    d.state = AutoslewCoverCalibratorState()
    d.cover_calibrator = MockAutoslewSDKDevice(Connected=True, CoverState=cover_state)
    d.device_connected = True
    return d


@pytest.mark.asyncio
async def test_cover_open_calls_open_cover():
    d = _cover(cover_state=3)  # already Open -> the settle wait returns at once
    await d.cover_calibrator_open(OpenMirrorCover())
    assert any(c[0] == "OpenCover" for c in d.cover_calibrator.calls)


@pytest.mark.asyncio
async def test_cover_close_calls_close_cover():
    d = _cover(cover_state=1)  # already Closed
    await d.cover_calibrator_close(CloseMirrorCover())
    assert any(c[0] == "CloseCover" for c in d.cover_calibrator.calls)
