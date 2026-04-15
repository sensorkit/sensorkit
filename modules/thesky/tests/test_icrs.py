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
async def test_mount_follow_icrs(mount):
    import sensorkit.api as sk
    from sensorkit.astro.common import Equatorial
    from sensorkit.astro.target import ICRSTarget

    await mount.mount_connect(sk.Connect())
    await mount.mount_unpark()
    await mount.mount_follow_target(
        sk.FollowTarget(target=ICRSTarget(coords=Equatorial(ra=6.0, dec=20.0)))
    )

    resp = await mount.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "1"
