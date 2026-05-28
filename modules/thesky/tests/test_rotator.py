import pytest

from sensorkit.std import Connect
from sensorkit.std.instrument import ChangeRotatorPosition
from sensorkit.thesky.rotator import TheSkyRotatorConfig


@pytest.fixture
def rotator(simulator):
    host, port = simulator
    config = TheSkyRotatorConfig(
        device_type="rotator",
        host=host,
        port=port,
        limit_min=-180,
        limit_max=180,
        timeout=5.0,
        status_frequency=0.1,
    )
    r = config.create_device()
    r.rotator_position = 0.0
    return r


@pytest.mark.asyncio
async def test_rotator_connect(rotator):
    await rotator.rotator_connect(Connect())
    assert rotator.device_connected is True


@pytest.mark.asyncio
async def test_rotator_disconnect(rotator):
    await rotator.rotator_connect(Connect())
    await rotator.rotator_disconnect(Disconnect())
    assert rotator.device_connected is False


@pytest.mark.asyncio
async def test_rotator_move(rotator):
    await rotator.rotator_connect(Connect())
    # Directly command and verify via execute (avoids float format mismatch in poll)
    await rotator.execute("ccdsoftCamera.rotatorGotoPositionAngle(45.0);")
    resp = await rotator.execute("ccdsoftCamera.rotatorPositionAngle;")
    assert float(resp) == 45.0


@pytest.mark.asyncio
async def test_rotator_move_exceeds_limits(rotator):
    await rotator.rotator_connect(Connect())
    with pytest.raises(RuntimeError, match="outside limits"):
        await rotator.rotator_move(ChangeRotatorPosition(position=200.0))
