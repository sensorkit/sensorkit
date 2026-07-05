# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.astro.coords import Geodetic
from sensorkit.models.devices import FollowTarget
from sensorkit.std import Connect
from sensorkit.thesky.telescope import TheSkyTelescopeConfig, TheSkyTelescopeState


@pytest.fixture
def telescope(simulator):
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
    return m


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
