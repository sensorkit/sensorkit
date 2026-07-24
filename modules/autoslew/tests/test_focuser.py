# SPDX-License-Identifier: Apache-2.0
"""Autoslew focuser — ASA focuser:homefind via the Telescope backbone.

Standard move/stop/change are inherited unchanged from `AlpacaFocuser` and covered
by the alpaca module's tests; only the ASA extension is exercised here.
"""

import pytest
from conftest import MockAutoslewSDKDevice

from sensorkit.autoslew.focuser import AutoslewFocuserConfig, AutoslewFocuserState
from sensorkit.std import Home


@pytest.fixture
def focuser():
    config = AutoslewFocuserConfig(host="localhost", timeout=5.0, status_frequency=0.05)
    d = config.create_device()
    d.state = AutoslewFocuserState()
    d.focuser = MockAutoslewSDKDevice(Connected=True, Position=1000.0, IsMoving=False)
    d.telescope = MockAutoslewSDKDevice(Connected=True)  # ASA backbone
    d.device_connected = True
    d.focuser_position = 1000.0
    return d


@pytest.mark.asyncio
async def test_focuser_home_fires_homefind(focuser):
    await focuser.focuser_home(Home())
    assert "focuser:homefind" in focuser.telescope.actions()
    assert focuser.state.has_been_homed is True
