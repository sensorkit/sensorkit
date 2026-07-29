# SPDX-License-Identifier: Apache-2.0
"""Autoslew rotator — ASA rotator:setslewoption/homefind via the Telescope backbone.

Standard move/stop are inherited unchanged from `AlpacaRotator` and covered by the
alpaca module's tests; only the ASA extensions are exercised here.
"""

import pytest

from sensorkit.autoslew.rotator import AutoslewRotatorConfig, AutoslewRotatorState

from .fakes import FakeAutoslewSDKDevice


def _rotator(**cfg):
    config = AutoslewRotatorConfig(host="localhost", timeout=5.0, status_frequency=0.05, **cfg)
    d = config.create_device()
    d.state = AutoslewRotatorState()
    d.rotator = FakeAutoslewSDKDevice(Connected=True, IsMoving=False, MechanicalPosition=100.0)
    d.telescope = FakeAutoslewSDKDevice(Connected=True)  # ASA backbone
    d.device_connected = True
    d.rotator_position = 100.0
    d._can_reverse = False
    d._step_size = None
    return d


@pytest.fixture
def rotator():
    return _rotator()


@pytest.mark.asyncio
async def test_applies_slew_option():
    d = _rotator(slew_option=2)
    await d._apply_asa_settings()
    setslew = [
        args for args, _ in d.telescope.calls("Action") if args[0] == "rotator:setslewoption"
    ]
    assert setslew and setslew[-1][1] == "2"


@pytest.mark.asyncio
async def test_no_asa_settings_by_default(rotator):
    await rotator._apply_asa_settings()
    assert not rotator.telescope.actions()


@pytest.mark.asyncio
async def test_rotator_home_fires_homefind(rotator):
    from sensorkit.std import Home

    await rotator.rotator_home(Home())
    assert "rotator:homefind" in rotator.telescope.actions()
    assert rotator.state.has_been_homed is True
