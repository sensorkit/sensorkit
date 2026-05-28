import pytest

from sensorkit.models.devices import Home, MoveToPark, Stop
from sensorkit.std import Connect, Disconnect
from sensorkit.thesky.telescope import TheSkyTelescopeConfig, TheSkyTelescopeState


@pytest.fixture
def telescope(simulator):
    host, port = simulator
    config = TheSkyTelescopeConfig(
        device_type="mount",
        host=host,
        port=port,
        needs_homed=False,
        timeout=5.0,
        status_frequency=0.1,
    )
    m = config.create_device()
    m.state = TheSkyTelescopeState()
    return m


@pytest.mark.asyncio
async def test_telescope_connect(telescope):
    await telescope.telescope_connect(Connect())
    assert telescope.device_connected is True


@pytest.mark.asyncio
async def test_telescope_disconnect(telescope):
    await telescope.telescope_connect(Connect())
    await telescope.telescope_disconnect(Disconnect())
    assert telescope.device_connected is False


@pytest.mark.asyncio
async def test_telescope_park(telescope):
    await telescope.telescope_connect(Connect())
    # Must unpark first before we can park
    await telescope.telescope_unpark()
    await telescope.telescope_park(MoveToPark())


@pytest.mark.asyncio
async def test_telescope_unpark(telescope):
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()

    resp = await telescope.execute("sky6RASCOMTele.IsParked();")
    assert resp.strip() == "false"


@pytest.mark.asyncio
async def test_telescope_home(telescope):
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_home(Home())


@pytest.mark.asyncio
async def test_telescope_stop(telescope):
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_stop(Stop())

    resp = await telescope.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "0"
