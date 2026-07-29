# SPDX-License-Identifier: Apache-2.0
"""Autoslew dome — StandardEnclosure open/close via the Telescope backbone."""

import pytest

from sensorkit.autoslew.dome import AutoslewDomeConfig, AutoslewDomeState
from sensorkit.std import Opened
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure

from .fakes import FakeAutoslewSDKDevice


@pytest.fixture
def dome():
    config = AutoslewDomeConfig(host="localhost", timeout=5.0, status_frequency=0.05)
    d = config.create_device()
    d.state = AutoslewDomeState()
    d.telescope = FakeAutoslewSDKDevice(Connected=True)  # backbone-only device
    d.device_connected = True
    d._is_open = None
    return d


@pytest.mark.asyncio
async def test_dome_open_fires_shutter_action(dome, recorder):
    published = await recorder()

    await dome.dome_open(OpenEnclosure())

    assert "dome:openshutter" in dome.telescope.actions()
    assert dome._is_open is True
    assert (await published.wait_for(Opened)).is_open is True


@pytest.mark.asyncio
async def test_dome_close_fires_shutter_action(dome, recorder):
    published = await recorder()

    await dome.dome_close(CloseEnclosure())

    assert "dome:closeshutter" in dome.telescope.actions()
    assert dome._is_open is False
    assert (await published.wait_for(Opened)).is_open is False
