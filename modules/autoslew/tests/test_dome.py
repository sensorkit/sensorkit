# SPDX-License-Identifier: Apache-2.0
"""Autoslew dome — StandardEnclosure open/close via the Telescope backbone."""

import pytest
from conftest import MockAutoslewSDKDevice

from sensorkit.autoslew.dome import AutoslewDomeConfig, AutoslewDomeState
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure


@pytest.fixture
def dome():
    config = AutoslewDomeConfig(host="localhost", timeout=5.0, status_frequency=0.05)
    d = config.create_device()
    d.state = AutoslewDomeState()
    d.telescope = MockAutoslewSDKDevice(Connected=True)  # backbone-only device
    d.device_connected = True
    d._is_open = None
    return d


def _opened(mock_sk):
    return [
        c.args[0] for c in mock_sk.publish.call_args_list if type(c.args[0]).__name__ == "Opened"
    ]


@pytest.mark.asyncio
async def test_dome_open_fires_shutter_action(dome, _mock_sk_device):
    await dome.dome_open(OpenEnclosure())
    assert "dome:openshutter" in dome.telescope.actions()
    assert dome._is_open is True
    opened = _opened(_mock_sk_device)
    assert opened and opened[-1].is_open is True


@pytest.mark.asyncio
async def test_dome_close_fires_shutter_action(dome, _mock_sk_device):
    await dome.dome_close(CloseEnclosure())
    assert "dome:closeshutter" in dome.telescope.actions()
    assert dome._is_open is False
    opened = _opened(_mock_sk_device)
    assert opened and opened[-1].is_open is False
