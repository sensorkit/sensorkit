import pytest

from sensorkit.thesky.telescope import TheSkyTelescope, TheSkyTelescopeConfig, TheSkyTelescopeState


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
    import sensorkit.api as sk

    await telescope.telescope_connect(sk.Connect())
    assert telescope.device_connected is True


@pytest.mark.asyncio
async def test_telescope_disconnect(telescope):
    import sensorkit.api as sk

    await telescope.telescope_connect(sk.Connect())
    await telescope.telescope_disconnect(sk.Disconnect())
    assert telescope.device_connected is False


@pytest.mark.asyncio
async def test_telescope_park(telescope):
    import sensorkit.api as sk

    await telescope.telescope_connect(sk.Connect())
    # Must unpark first before we can park
    await telescope.telescope_unpark()
    await telescope.telescope_park(sk.MoveToPark())


@pytest.mark.asyncio
async def test_telescope_unpark(telescope):
    import sensorkit.api as sk

    await telescope.telescope_connect(sk.Connect())
    await telescope.telescope_unpark()

    resp = await telescope.execute("sky6RASCOMTele.IsParked();")
    assert resp.strip() == "false"


@pytest.mark.asyncio
async def test_telescope_home(telescope):
    import sensorkit.api as sk

    await telescope.telescope_connect(sk.Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_home(sk.Home())


@pytest.mark.asyncio
async def test_telescope_stop(telescope):
    import sensorkit.api as sk

    await telescope.telescope_connect(sk.Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_stop(sk.Stop())

    resp = await telescope.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "0"
