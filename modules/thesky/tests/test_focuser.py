import pytest

from sensorkit.std import Connect, Disconnect
from sensorkit.std.optics import ChangeFocusPosition
from sensorkit.thesky.focuser import TheSkyFocuserConfig


@pytest.fixture
def focuser(simulator):
    host, port = simulator
    config = TheSkyFocuserConfig(
        device_type="focuser",
        host=host,
        port=port,
        limit_min=0,
        limit_max=10000,
        timeout=5.0,
        status_frequency=0.1,
    )
    f = config.create_device()
    f.focuser_position = 5000
    return f


@pytest.mark.asyncio
async def test_focuser_connect(focuser):
    await focuser.focuser_connect(Connect())
    assert focuser.device_connected is True


@pytest.mark.asyncio
async def test_focuser_disconnect(focuser):
    await focuser.focuser_connect(Connect())
    await focuser.focuser_disconnect(Disconnect())
    assert focuser.device_connected is False


@pytest.mark.asyncio
async def test_focuser_move_out(focuser):
    await focuser.focuser_connect(Connect())
    # Move out by sending the command, then verify via execute
    await focuser.execute(f"ccdsoftCamera.focMoveOut(1000);")
    resp = await focuser.execute("ccdsoftCamera.focPosition;")
    assert float(resp) == 6000.0


@pytest.mark.asyncio
async def test_focuser_move_in(focuser):
    await focuser.focuser_connect(Connect())
    await focuser.execute(f"ccdsoftCamera.focMoveIn(-1000);")
    resp = await focuser.execute("ccdsoftCamera.focPosition;")
    assert float(resp) == 4000.0


@pytest.mark.asyncio
async def test_focuser_move_no_op(focuser):
    """Moving to current position should be a no-op."""
    await focuser.focuser_connect(Connect())
    await focuser.focuser_move(ChangeFocusPosition(position=5000))


@pytest.mark.asyncio
async def test_focuser_move_exceeds_limits(focuser):
    await focuser.focuser_connect(Connect())
    with pytest.raises(RuntimeError, match="outside limits"):
        await focuser.focuser_move(ChangeFocusPosition(position=20000))
