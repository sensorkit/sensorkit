# SPDX-License-Identifier: Apache-2.0
import asyncio
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from astropy.time import Time

from sensorkit.astro.common import TLE, ReferenceFrame
from sensorkit.astro.coords import Equatorial, Horizontal
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    RateTarget,
    TLETarget,
)
from sensorkit.common.aio import AsyncLoop
from sensorkit.std import Connect, Disconnect, FollowTarget, Home, MoveToPark, Slewing, Stop
from sensorkit.thesky.device import (
    CommandNotSupportedError,
    TheSkyError,
)
from sensorkit.thesky.telescope import TheSkyTelescopeConfig, TheSkyTelescopeState


@pytest_asyncio.fixture
async def telescope(simulator):
    host, port = simulator
    config = TheSkyTelescopeConfig(
        device_type="telescope",
        host=host,
        port=port,
        timeout=5.0,
        status_frequency_slow=0.1,
        status_frequency_fast=0.1,
    )
    m = config.create_device()
    m.state = TheSkyTelescopeState()
    m.status_loop = AsyncLoop(m.status_publish, interval=config.status_frequency_slow)
    m.fast_loop = AsyncLoop(m._publish_telescope_status, interval=config.status_frequency_fast)

    yield m

    await m.status_loop.stop()
    await m.fast_loop.stop()


@pytest.fixture
def home_unsupported_telescope(telescope):
    """A telescope whose mount rejects FindHome with TheSky error 228."""
    real_execute = telescope.execute

    async def execute(script):
        if "FindHome" in script:
            raise CommandNotSupportedError(
                message="This command is not supported by the selected device", code=228
            )
        return await real_execute(script)

    telescope.execute = execute
    return telescope


@pytest.fixture
def tracking_unsupported_telescope(telescope):
    """A telescope whose mount rejects Abort/SetTracking with TheSky error 228."""
    real_execute = telescope.execute

    async def execute(script):
        if "SetTracking" in script or "Abort" in script:
            raise CommandNotSupportedError(
                message="This command is not supported by the selected device", code=228
            )
        return await real_execute(script)

    telescope.execute = execute
    return telescope


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
async def test_telescope_home_unsupported(home_unsupported_telescope):
    telescope = home_unsupported_telescope
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()

    # FindHome raises 228; homing should be skipped without propagating the error.
    await telescope.telescope_home(Home())

    assert telescope.state.has_been_homed is True
    resp = await telescope.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "0"


@pytest.mark.asyncio
async def test_telescope_home_tracking_unsupported(tracking_unsupported_telescope):
    telescope = tracking_unsupported_telescope
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()

    # SetTracking raises 228; homing should still complete and mark state.
    await telescope.telescope_home(Home())

    assert telescope.state.has_been_homed is True


@pytest.mark.asyncio
async def test_telescope_stop_tracking_unsupported(tracking_unsupported_telescope):
    telescope = tracking_unsupported_telescope
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()

    # Abort/SetTracking raise 228; stop should not propagate the error.
    await telescope.telescope_stop(Stop())


@pytest.mark.asyncio
async def test_telescope_stop(telescope):
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()
    await telescope.telescope_stop(Stop())

    resp = await telescope.execute("sky6RASCOMTele.IsTracking;")
    assert resp.strip() == "0"


@pytest.mark.parametrize(
    "slew_complete, expected_slewing",
    [(-1, False), (0, True), (1, False)],  # -1 = IsSlewComplete unsupported by mount
)
@pytest.mark.asyncio
async def test_status_publish_slew_state(telescope, recorder, slew_complete, expected_slewing):
    # _location is only set in entity_init; None skips the AxisRates math we aren't testing.
    telescope._location = None

    async def execute(script):
        # connected, ra, ra_rate, dec, dec_rate, alt, az, slew_complete, tracking, leo(-1)
        return f"1,10,0,20,0,45,180,{slew_complete},0,-1"

    telescope.execute = execute
    published = await recorder()
    await telescope._publish_telescope_status()

    assert (await published.wait_for(Slewing)).is_slewing is expected_slewing


@pytest.fixture
def dome_query_scripts(telescope):
    """Prepend JavaScript to successive dome queries on the TCP simulator."""
    real_execute = telescope.execute

    def configure(updates):
        pending = iter(updates)
        calls = []

        async def execute(script, **kwargs):
            calls.append(script)
            if "sky6Dome.IsConnected" in script:
                script = next(pending) + script
            return await real_execute(script, **kwargs)

        telescope.execute = execute
        return calls

    return configure


@pytest.mark.parametrize("require_coupled_dome", [False, True])
@pytest.mark.parametrize("status", ["disconnected", "uncoupled", "complete"])
@pytest.mark.asyncio
async def test_dome_completion_policy(telescope, dome_query_scripts, require_coupled_dome, status):
    telescope.config.require_coupled_dome = require_coupled_dome
    match status:
        case "disconnected":
            update = """
                sky6Dome.IsConnected = 0;
                Object.defineProperty(sky6Dome, 'IsCoupled', {
                    enumerable: false,
                    get: function () { throw 'Coupling queried while disconnected'; }
                });
                Object.defineProperty(sky6Dome, 'IsGotoComplete', {
                    enumerable: false,
                    get: function () { throw 'Goto queried while disconnected'; }
                });
            """
        case "uncoupled":
            update = """
                sky6Dome.IsConnected = 1;
                sky6Dome.IsCoupled = 0;
                Object.defineProperty(sky6Dome, 'IsGotoComplete', {
                    enumerable: false,
                    get: function () { throw 'Goto queried while uncoupled'; }
                });
            """
        case "complete":
            update = """
                sky6Dome.IsConnected = 2;
                sky6Dome.IsCoupled = 2;
                sky6Dome.IsGotoComplete = 2;
            """
    calls = dome_query_scripts([update])

    async with asyncio.timeout(1):
        if require_coupled_dome and status != "complete":
            with pytest.raises(RuntimeError, match=f"Required slew-coupled dome is {status}"):
                await telescope._wait_for_coupled_dome()
        else:
            await telescope._wait_for_coupled_dome()

    assert len(calls) == 1


@pytest.mark.parametrize("require_coupled_dome", [False, True])
@pytest.mark.parametrize("lost_state", ["disconnected", "uncoupled"])
@pytest.mark.asyncio
async def test_dome_connection_or_coupling_lost(
    telescope, dome_query_scripts, require_coupled_dome, lost_state
):
    telescope.config.require_coupled_dome = require_coupled_dome
    lost_flag = "IsConnected" if lost_state == "disconnected" else "IsCoupled"
    calls = dome_query_scripts(
        [
            "sky6Dome.IsConnected = 1; sky6Dome.IsCoupled = 1; sky6Dome.IsGotoComplete = 0;",
            f"sky6Dome.{lost_flag} = 0;",
        ]
    )

    async with asyncio.timeout(1):
        if require_coupled_dome:
            with pytest.raises(RuntimeError, match=f"Required slew-coupled dome is {lost_state}"):
                await telescope._wait_for_coupled_dome()
        else:
            await telescope._wait_for_coupled_dome()

    assert len(calls) == 2


@pytest.mark.parametrize("require_coupled_dome", [False, True])
@pytest.mark.parametrize("property_name", ["IsConnected", "IsCoupled", "IsGotoComplete"])
@pytest.mark.asyncio
async def test_dome_query_failure(
    telescope, dome_query_scripts, require_coupled_dome, property_name
):
    telescope.config.require_coupled_dome = require_coupled_dome
    dome_query_scripts(
        [
            f"""
        sky6Dome.IsConnected = 1;
        sky6Dome.IsCoupled = 1;
        Object.defineProperty(sky6Dome, '{property_name}', {{
            enumerable: false,
            get: function () {{ throw new Error('Dome query failed'); }}
        }});
    """
        ]
    )

    with pytest.raises(TheSkyError, match="Dome query failed"):
        await telescope._wait_for_coupled_dome()


@pytest.mark.parametrize("connected,coupled", [(0, 1), (1, 0)])
@pytest.mark.asyncio
async def test_required_dome_checked_before_target_slew(
    telescope, dome_query_scripts, connected, coupled
):
    await telescope.telescope_connect(Connect())
    telescope.config.require_coupled_dome = True
    calls = dome_query_scripts(
        [f"sky6Dome.IsConnected = {connected}; sky6Dome.IsCoupled = {coupled};"]
    )

    with pytest.raises(RuntimeError, match="Required slew-coupled dome"):
        await telescope.telescope_follow_target(
            FollowTarget(target=ICRSTarget(coords=Equatorial(ra=100, dec=20)))
        )

    assert not any("SlewTo" in script for script in calls)
    assert not telescope.fast_loop.active


@pytest.mark.parametrize(
    "target_kind", ["icrs", "altaz", "tle", "rate", "ephemeris", "frame", "altaz_frame"]
)
@pytest.mark.asyncio
async def test_target_acquisition_waits_for_dome(telescope, dome_query_scripts, target_kind):
    now = Time.now().jd
    match target_kind:
        case "icrs":
            target = ICRSTarget(coords=Equatorial(ra=100, dec=20))
        case "altaz":
            target = AltAzTarget(coords=Horizontal(az=100, alt=45))
        case "tle":
            target = TLETarget(
                tle=TLE(
                    line0="0 ISS (ZARYA)",
                    line1="1 25544U 98067A   24100.50000000  .00016717  00000-0  10270-3 0  9002",
                    line2="2 25544  51.6400 200.0000 0001234  90.0000 270.0000 15.49000000400000",
                )
            )
        case "rate":
            target = RateTarget(
                frame=ReferenceFrame.ICRF,
                initial_frame=ReferenceFrame.ICRF,
                initial_time=datetime.now(UTC),
                initial_coords=Equatorial(ra=100, dec=20),
                rates=Equatorial(ra=0.01, dec=0.005),
            )
        case "ephemeris":
            target = EphemerisTarget(
                frame=ReferenceFrame.ICRF,
                jds=[now, now + 2 / 86400],
                points=[Equatorial(ra=100, dec=20), Equatorial(ra=100.02, dec=20.01)],
            )
        case "frame":
            target = FrameTarget(frame=ReferenceFrame.ICRF)
        case "altaz_frame":
            target = FrameTarget(frame=ReferenceFrame.ALTAZ)

    await telescope.telescope_connect(Connect())
    await telescope.execute("sky6RASCOMTele.IsSlewComplete = 1;")
    telescope.config.require_coupled_dome = True
    calls = dome_query_scripts(
        [
            """
            sky6Dome.IsConnected = 1;
            sky6Dome.IsCoupled = 1;
            Object.defineProperty(sky6Dome, 'IsGotoComplete', {
                enumerable: false,
                get: function () { throw 'Goto queried before mount acquisition'; }
            });
        """,
            "sky6Dome.IsGotoComplete = 0;",
            "sky6Dome.IsGotoComplete = 1;",
        ]
    )

    await telescope.telescope_follow_target(FollowTarget(target=target))

    dome_queries = [i for i, script in enumerate(calls) if "sky6Dome.IsConnected" in script]
    assert len(dome_queries) == 3
    mount_queries = [
        i
        for i, script in enumerate(calls)
        if "Out=[sky6RASCOMTele.IsSlewComplete" in script or script == "Raven3.trackLEOStatus;"
    ]
    assert dome_queries[0] < mount_queries[-1] < dome_queries[1]
    assert telescope.fast_loop.active == (target_kind != "altaz_frame")


@pytest.mark.parametrize("require_coupled_dome", [False, True])
@pytest.mark.asyncio
async def test_dome_wait_timeout(telescope, require_coupled_dome):
    await telescope.telescope_connect(Connect())
    await telescope.execute(
        "sky6Dome.IsConnected = 1; sky6Dome.IsCoupled = 1; sky6Dome.IsGotoComplete = 0;"
    )
    telescope.config.require_coupled_dome = require_coupled_dome
    telescope.config.timeout = 0.3

    with pytest.raises(TimeoutError):
        await telescope.telescope_follow_target(
            FollowTarget(target=ICRSTarget(coords=Equatorial(ra=100, dec=20)))
        )

    assert telescope.fast_loop.active
