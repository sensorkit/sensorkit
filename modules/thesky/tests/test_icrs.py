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
async def test_telescope_follow_icrs(telescope):
    import sensorkit.api as sk
    from sensorkit.astro.common import Equatorial
    from sensorkit.astro.target import ICRSTarget

    await telescope.telescope_connect(sk.Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_follow_target(
        sk.FollowTarget(target=ICRSTarget(coords=Equatorial(ra=6.0, dec=20.0)))
    )

    resp = await telescope.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "1"
