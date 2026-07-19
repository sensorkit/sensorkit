import pytest
from astropy.time import Time

from sensorkit.astro.coords import Geodetic
from sensorkit.std import Connect, FollowTarget
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
async def test_telescope_follow_ephemeris(telescope):
    """An ICRF EphemerisTarget collapses to a slew plus constant offset rate.

    A linear path moving 0.02 deg in RA and 0.01 deg in Dec per 2 s sample should
    slew to the sample nearest now and apply offset rates of 36 arcsec/s (RA) and
    18 arcsec/s (Dec) via SetTracking -- TheSky takes plain angular arcsec/s, with
    no sidereal scaling.
    """
    from sensorkit.astro.common import ReferenceFrame
    from sensorkit.astro.coords import Equatorial
    from sensorkit.astro.target import EphemerisTarget

    step_days = 2.0 / 86400.0
    now_jd = Time.now().jd
    # Samples straddling "now" so the nearest interior sample (k=0) has a forward
    # neighbour to difference against.
    ks = [-2, -1, 0, 1, 2, 3]
    jds = [now_jd + k * step_days for k in ks]
    points = [Equatorial(ra=100.0 + 0.02 * k, dec=20.0 + 0.01 * k) for k in ks]

    target = EphemerisTarget(frame=ReferenceFrame.ICRF, jds=jds, points=points)

    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_follow_target(FollowTarget(target=target))

    assert (await telescope.execute("sky6RASCOMTele.IsTracking;")).strip() == "1"

    # Slewed to the sample nearest now (k=0).
    assert float(await telescope.execute("sky6RASCOMTele.dRa;")) == pytest.approx(100.0)
    assert float(await telescope.execute("sky6RASCOMTele.dDec;")) == pytest.approx(20.0)

    # rel=1e-3 absorbs float64 precision loss from differencing ~2.46e6 JDs.
    ra_rate = float(await telescope.execute("sky6RASCOMTele.dRaTrackingRate;"))
    dec_rate = float(await telescope.execute("sky6RASCOMTele.dDecTrackingRate;"))
    assert ra_rate == pytest.approx(36.0, rel=1e-3)
    assert dec_rate == pytest.approx(18.0, rel=1e-3)
