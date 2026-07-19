# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.std import Connect, Disconnect, Home, MoveToPark, Stop
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.thesky.dome import TheSkyDomeConfig, TheSkyDomeState


@pytest.fixture
def dome(simulator):
    host, port = simulator
    config = TheSkyDomeConfig(
        device_type="dome",
        host=host,
        port=port,
        needs_homed=False,
        timeout=5.0,
        status_frequency=0.1,
    )
    d = config.create_device()
    d.state = TheSkyDomeState()
    return d


@pytest.mark.asyncio
async def test_dome_connect(dome):
    await dome.dome_connect(Connect())
    assert dome.device_connected is True


@pytest.mark.asyncio
async def test_dome_disconnect(dome):
    await dome.dome_connect(Connect())
    await dome.dome_disconnect(Disconnect())
    assert dome.device_connected is False


@pytest.mark.asyncio
async def test_dome_park(dome):
    await dome.dome_connect(Connect())
    await dome.dome_park(MoveToPark())


@pytest.mark.asyncio
async def test_dome_home(dome):
    await dome.dome_connect(Connect())
    await dome.dome_home(Home())
    assert dome.state.has_been_homed is True


@pytest.mark.asyncio
async def test_dome_open(dome):
    dome.state = TheSkyDomeState()
    await dome.dome_connect(Connect())
    await dome.dome_open(OpenEnclosure())


@pytest.mark.asyncio
async def test_dome_close(dome):
    dome.state = TheSkyDomeState()
    await dome.dome_connect(Connect())
    await dome.dome_close(CloseEnclosure())


@pytest.mark.asyncio
async def test_dome_stop(dome):
    await dome.dome_connect(Connect())
    await dome.dome_stop(Stop())
