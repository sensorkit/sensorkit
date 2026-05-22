import pytest

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
    import sensorkit.api as sk

    await dome.dome_connect(sk.Connect())
    assert dome.device_connected is True


@pytest.mark.asyncio
async def test_dome_disconnect(dome):
    import sensorkit.api as sk

    await dome.dome_connect(sk.Connect())
    await dome.dome_disconnect(sk.Disconnect())
    assert dome.device_connected is False


@pytest.mark.asyncio
async def test_dome_park(dome):
    import sensorkit.api as sk

    await dome.dome_connect(sk.Connect())
    await dome.dome_park(sk.MoveToPark())


@pytest.mark.asyncio
async def test_dome_home(dome):
    import sensorkit.api as sk

    await dome.dome_connect(sk.Connect())
    await dome.dome_home(sk.Home())
    assert dome.state.has_been_homed is True


@pytest.mark.asyncio
async def test_dome_open(dome):
    import sensorkit.api as sk

    dome.state = TheSkyDomeState()
    await dome.dome_connect(sk.Connect())
    await dome.dome_open(OpenEnclosure())


@pytest.mark.asyncio
async def test_dome_close(dome):
    import sensorkit.api as sk

    dome.state = TheSkyDomeState()
    await dome.dome_connect(sk.Connect())
    await dome.dome_close(CloseEnclosure())


@pytest.mark.asyncio
async def test_dome_stop(dome):
    import sensorkit.api as sk

    await dome.dome_connect(sk.Connect())
    await dome.dome_stop(sk.Stop())
