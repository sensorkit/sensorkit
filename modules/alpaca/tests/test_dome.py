# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca dome device."""

import pytest

from sensorkit.alpaca.dome import AlpacaDomeConfig, AlpacaDomeState
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice
from sensorkit.std import Connect, Disconnect, Home, MoveToPark, Stop


@pytest.fixture
def dome():
    config = AlpacaDomeConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    d = config.create_device()
    d.state = AlpacaDomeState()
    d.device_name = "Dome"
    device = FakeAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        AtHome=False,
        AtPark=False,
        Slewing=False,
        ShutterStatus=1,  # closed
        CanFindHome=True,
        CanPark=True,
        CanSetShutter=True,
        CanSetAzimuth=True,
        CanSetAltitude=False,
        CanSetPark=False,
        CanSlave=False,
        CanSyncAzimuth=False,
    )
    d.dome = device
    d.device_connected = True
    d._can_find_home = True
    d._can_park = True
    d._can_set_shutter = True
    d._can_set_azimuth = True
    d._can_set_altitude = False
    d._can_set_park = False
    d._can_slave = False
    d._can_sync_azimuth = False
    return d


@pytest.mark.asyncio
async def test_dome_connect(dome):
    dome.device_connected = False
    dome.dome._properties["Connected"] = False
    await dome.dome_connect(Connect())
    assert dome.device_connected is True


@pytest.mark.asyncio
async def test_dome_disconnect(dome):
    await dome.dome_disconnect(Disconnect())
    assert dome.device_connected is False


@pytest.mark.asyncio
async def test_dome_home(dome):
    dome.dome._properties["AtHome"] = True
    await dome.dome_home(Home())
    assert dome.state.has_been_homed is True


@pytest.mark.asyncio
async def test_dome_park(dome):
    dome.dome._properties["AtPark"] = True
    await dome.dome_park(MoveToPark())


@pytest.mark.asyncio
async def test_dome_stop(dome):
    await dome.dome_stop(Stop())


@pytest.mark.asyncio
async def test_dome_open(dome):
    from sensorkit.std.enclosure import OpenEnclosure

    dome.dome._properties["ShutterStatus"] = 0  # open
    await dome.dome_open(OpenEnclosure())


@pytest.mark.asyncio
async def test_dome_close(dome):
    from sensorkit.std.enclosure import CloseEnclosure

    dome.dome._properties["ShutterStatus"] = 1  # closed
    await dome.dome_close(CloseEnclosure())
