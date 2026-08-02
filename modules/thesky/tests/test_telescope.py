# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.std import Connect, Disconnect, Home, MoveToPark, Slewing, Stop
from sensorkit.thesky.device import (
    CommandNotSupportedError,
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
