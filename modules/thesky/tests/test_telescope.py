# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.std import Connect, Disconnect, FollowTarget, Home, MoveToPark, Slewing, Stop
from sensorkit.thesky.device import (
    CommandNotSupportedError,
    MountCommandInProgressError,
    ProcessAbortedError,
)
from sensorkit.thesky.telescope import TheSkyTelescopeConfig, TheSkyTelescopeState


@pytest.fixture
def telescope(simulator):
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
    return m


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


@pytest.mark.asyncio
async def test_end_leo_track_no_track(telescope):
    """With no satellite track live (trackLEOStatus 0), _end_leo_track is a no-op."""
    await telescope.telescope_connect(Connect())
    assert await telescope._end_leo_track() is False


@pytest.mark.asyncio
async def test_end_leo_track_aborts_live_track(telescope):
    """A live track (status 6) is aborted and polled back to 0."""
    await telescope.telescope_connect(Connect())
    # Start a satellite track in the simulator.
    await telescope.execute("Raven3.trackLEOBegin();")
    assert (await telescope.execute("Raven3.trackLEOStatus;")).strip() == "6"

    assert await telescope._end_leo_track() is True
    assert (await telescope.execute("Raven3.trackLEOStatus;")).strip() == "0"


@pytest.mark.asyncio
async def test_end_leo_track_tolerates_transient_212(telescope):
    """A transient 212 (ProcessAbortedError) while polling status is tolerated."""
    await telescope.telescope_connect(Connect())
    await telescope.execute("Raven3.trackLEOBegin();")

    real_execute = telescope.execute
    state = {"status_polls": 0}

    async def execute(script):
        if "trackLEOStatus" in script and "Abort" not in script:
            state["status_polls"] += 1
            # First post-abort status read raises 212, then settles to 0.
            if state["status_polls"] == 2:
                raise ProcessAbortedError(message="Process aborted", code=212)
        return await real_execute(script)

    telescope.execute = execute
    assert await telescope._end_leo_track() is True


@pytest.mark.asyncio
async def test_icrf_handoff_ends_leo_track_and_clears_latch(telescope):
    """The field path: a live Raven3 track, then a sidereal (ICRF) follow.

    The leftover track must be aborted and the abort latch cleared with a real
    slew (retried past the 121 race) before SetTracking, so no 7501 fires.
    """
    from sensorkit.astro.coords import Geodetic
    from sensorkit.astro.common import ReferenceFrame
    from sensorkit.astro.target import FrameTarget

    telescope._geodetic = Geodetic(lon=149.0, lat=-31.0, elev=1100.0)
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()

    # Simulate frame 1: a live satellite track.
    await telescope.execute("Raven3.trackLEOBegin();")
    assert (await telescope.execute("Raven3.trackLEOStatus;")).strip() == "6"

    real_execute = telescope.execute
    state = {"slew_attempts": 0}

    async def execute(script):
        if "SlewToRaDec" in script:
            state["slew_attempts"] += 1
            if state["slew_attempts"] < 3:
                raise MountCommandInProgressError(
                    message="A Mount command is already in progress", code=121
                )
        return await real_execute(script)

    telescope.execute = execute

    # Frame 2: switch to sidereal.
    await telescope.telescope_follow_target(
        FollowTarget(target=FrameTarget(frame=ReferenceFrame.ICRF))
    )

    # Track was aborted, the latch-clearing slew was retried past 121, and
    # sidereal tracking is on.
    assert (await telescope.execute("Raven3.trackLEOStatus;")).strip() == "0"
    assert state["slew_attempts"] == 3
    assert (await telescope.execute("sky6RASCOMTele.IsTracking;")).strip() == "1"


@pytest.mark.asyncio
async def test_slew_to_current_radec_retries_past_121(telescope):
    """SlewToRaDec rejected with 121 is retried until accepted (the field race)."""
    await telescope.telescope_connect(Connect())
    await telescope.telescope_unpark()

    real_execute = telescope.execute
    state = {"slew_attempts": 0}

    async def execute(script):
        if "SlewToRaDec" in script:
            state["slew_attempts"] += 1
            # Mount is still busy for the first two attempts, as after a real
            # LEO abort, then accepts the slew.
            if state["slew_attempts"] < 3:
                raise MountCommandInProgressError(
                    message="A Mount command is already in progress", code=121
                )
        return await real_execute(script)

    telescope.execute = execute
    await telescope._slew_to_current_radec()
    assert state["slew_attempts"] == 3


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
