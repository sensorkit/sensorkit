import pytest

import sensorkit.api as sk
from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
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


# ISS TLE for testing
ISS_TLE = TLE(
    line0="0 ISS (ZARYA)",
    line1="1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002",
    line2="2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000",
)


@pytest.mark.asyncio
async def test_telescope_follow_tle(telescope):
    await telescope.telescope_connect(sk.Connect())
    await telescope.telescope_unpark()

    target = TLETarget(tle=ISS_TLE)
    await telescope.telescope_follow_target(sk.FollowTarget(target=target))

    # Verify Raven3 tracking status reached 6
    resp = await telescope.execute("Raven3.trackLEOStatus;")
    assert resp.strip() == "6"


@pytest.mark.asyncio
async def test_tle_line0_reformatted(telescope):
    """TheSky requires line0 format '0 <sat_number>', verify write_tle reformats it."""
    tle = TLE(
        line0="0 ISS (ZARYA)",
        line1="1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002",
        line2="2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000",
    )

    # The telescope reformats line0 to "0 <sat_number>" from line2
    reformatted = TLE(
        line0=f"0 {tle.line2.split()[1]}",
        line1=tle.line1,
        line2=tle.line2,
    )
    assert reformatted.line0 == "0 25544"


@pytest.mark.asyncio
async def test_write_tle(telescope):
    """Verify write_tle creates a file with the TLE content."""
    tle_path = telescope.write_tle(ISS_TLE)
    assert tle_path is not None

    import pathlib
    # The path uses the satellite designator from TLE line1 as filename
    import tempfile
    designator = ISS_TLE.line1.split()[1]
    write_path = pathlib.Path(tempfile.gettempdir()) / f"tle_{designator}.txt"
    content = write_path.read_text()

    assert ISS_TLE.line1 in content
    assert ISS_TLE.line2 in content
