import pytest

from sensorkit.thesky.mount import TheSkyMount, TheSkyMountConfig, TheSkyMountState


@pytest.fixture
def mount(simulator):
    host, port = simulator
    config = TheSkyMountConfig(
        device_type="mount",
        host=host,
        port=port,
        needs_homed=False,
        timeout=5.0,
        status_frequency=0.1,
    )
    m = config.create_device()
    m.state = TheSkyMountState()
    return m


@pytest.mark.asyncio
async def test_mount_connect(mount):
    import sensorkit.api as sk

    await mount.mount_connect(sk.Connect())
    assert mount.device_connected is True


@pytest.mark.asyncio
async def test_mount_disconnect(mount):
    import sensorkit.api as sk

    await mount.mount_connect(sk.Connect())
    await mount.mount_disconnect(sk.Disconnect())
    assert mount.device_connected is False


@pytest.mark.asyncio
async def test_mount_park(mount):
    import sensorkit.api as sk

    await mount.mount_connect(sk.Connect())
    # Must unpark first before we can park
    await mount.mount_unpark()
    await mount.mount_park(sk.MoveToPark())


@pytest.mark.asyncio
async def test_mount_unpark(mount):
    import sensorkit.api as sk

    await mount.mount_connect(sk.Connect())
    await mount.mount_unpark()

    resp = await mount.execute("sky6RASCOMTele.IsParked();")
    assert resp.strip() == "false"


@pytest.mark.asyncio
async def test_mount_home(mount):
    import sensorkit.api as sk

    await mount.mount_connect(sk.Connect())
    await mount.mount_unpark()
    await mount.mount_home(sk.Home())


@pytest.mark.asyncio
async def test_mount_stop(mount):
    import sensorkit.api as sk

    await mount.mount_connect(sk.Connect())
    await mount.mount_unpark()
    await mount.mount_stop(sk.Stop())

    resp = await mount.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "0"


