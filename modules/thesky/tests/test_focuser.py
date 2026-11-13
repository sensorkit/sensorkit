# SPDX-License-Identifier: Apache-2.0
import asyncio

import pytest

from sensorkit.std import ChangeFocusPosition, Connect, Disconnect
from sensorkit.thesky.device import SCRIPT_FOOTER
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
    await focuser.execute("ccdsoftCamera.focMoveOut(1000);")
    resp = await focuser.execute("ccdsoftCamera.focPosition;")
    assert float(resp) == 6000.0


@pytest.mark.asyncio
async def test_focuser_move_in(focuser):
    await focuser.focuser_connect(Connect())
    await focuser.execute("ccdsoftCamera.focMoveIn(-1000);")
    resp = await focuser.execute("ccdsoftCamera.focPosition;")
    assert float(resp) == 4000.0


@pytest.mark.asyncio
async def test_focuser_move_no_op(focuser):
    """Moving to current position should be a no-op."""
    await focuser.focuser_connect(Connect())
    await focuser.focuser_change(ChangeFocusPosition(position=5000))


@pytest.mark.asyncio
async def test_focuser_move_exceeds_limits(focuser):
    await focuser.focuser_connect(Connect())
    with pytest.raises(RuntimeError, match="outside limits"):
        await focuser.focuser_change(ChangeFocusPosition(position=20000))


@pytest.mark.asyncio
async def test_consecutive_moves_use_live_position(focuser, recorder):
    from sensorkit.std import FocusPosition

    published = await recorder()
    await focuser.focuser_connect(Connect())
    await focuser.focuser_change(ChangeFocusPosition(position=6000))
    await focuser.focuser_change(ChangeFocusPosition(position=6500))

    assert float(await focuser.execute("ccdsoftCamera.focPosition;")) == 6500.0
    assert (await published.wait_for(FocusPosition, count=2)).position == 6500.0


@pytest.mark.asyncio
async def test_stale_cache_does_not_skip_a_move(focuser):
    await focuser.focuser_connect(Connect())
    focuser.focuser_position = 6000.0

    await focuser.focuser_change(ChangeFocusPosition(position=6000))

    assert float(await focuser.execute("ccdsoftCamera.focPosition;")) == 6000.0


@pytest.mark.asyncio
async def test_fractional_target_is_quantized_to_integer_steps(focuser):
    await focuser.focuser_connect(Connect())

    await focuser.focuser_change(ChangeFocusPosition(position=6000.75))

    assert float(await focuser.execute("ccdsoftCamera.focPosition;")) == 6000.0


@pytest.mark.asyncio
async def test_move_restores_asynchronous_setting(focuser):
    await focuser.focuser_connect(Connect())
    await focuser.execute("ccdsoftCamera.Asynchronous = 1;")

    await focuser.focuser_change(ChangeFocusPosition(position=6000))

    assert await focuser.execute("ccdsoftCamera.Asynchronous;") == "1"


@pytest.mark.asyncio
async def test_unreadable_position_fails_and_restores_asynchronous_setting(focuser):
    await focuser.focuser_connect(Connect())
    await focuser.execute("ccdsoftCamera.Asynchronous = 1;")
    await focuser.execute('ccdsoftCamera.focPosition = "unknown";')

    with pytest.raises(ValueError):
        await focuser.focuser_change(ChangeFocusPosition(position=6000))

    assert await focuser.execute("ccdsoftCamera.Asynchronous;") == "1"


@pytest.mark.asyncio
@pytest.mark.parametrize("position", ["5500", "null"])
async def test_move_rejects_unachieved_or_missing_position(position):
    async def hardware(reader, writer):
        try:
            await reader.readuntil(SCRIPT_FOOTER)
            writer.write(f"{position}|No error. Error = 0.".encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(hardware, "127.0.0.1", 0)

    async with server:
        host, port = server.sockets[0].getsockname()
        device = TheSkyFocuserConfig(
            device_type="focuser", host=host, port=port, timeout=1.0
        ).create_device()
        device.device_connected = True

        with pytest.raises((RuntimeError, ValueError)):
            await device.focuser_change(ChangeFocusPosition(position=6000))
