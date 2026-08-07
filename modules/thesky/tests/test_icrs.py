# SPDX-License-Identifier: Apache-2.0
import pytest
import pytest_asyncio

from sensorkit.astro.coords import Geodetic
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import Connect, FollowTarget
from sensorkit.thesky.telescope import TheSkyTelescopeConfig, TheSkyTelescopeState


@pytest_asyncio.fixture
async def telescope(simulator):
    host, port = simulator
    config = TheSkyTelescopeConfig(
        device_type="telescope",
        host=host,
        port=port,
        needs_homed=False,
        timeout=5.0,
        status_frequency=0.1,
    )
    m = config.create_device()
    m.state = TheSkyTelescopeState()
    # Normally set during entity_init (on_attach) from TheSky's site info, which
    # these connect-only tests bypass; the value is unused for pass-through adapts.
    m._geodetic = Geodetic(lon=149.0, lat=-31.0, elev=1100.0)
    m.status_loop = AsyncLoop(m.status_publish, interval=config.status_frequency_slow)
    m.fast_loop = AsyncLoop(m._publish_telescope_status, interval=config.status_frequency_fast)

    yield m

    await m.status_loop.stop()
    await m.fast_loop.stop()


@pytest.mark.asyncio
async def test_telescope_follow_icrs(telescope):
    from sensorkit.astro.coords import Equatorial
    from sensorkit.astro.target import ICRSTarget

    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_follow_target(
        FollowTarget(target=ICRSTarget(coords=Equatorial(ra=6.0, dec=20.0)))
    )

    resp = await telescope.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "1"
