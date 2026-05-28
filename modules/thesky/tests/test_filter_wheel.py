import pytest

from sensorkit.std import Connect, Disconnect
from sensorkit.std.optics import SetFilter
from sensorkit.thesky.filter_wheel import TheSkyFilterWheelConfig


@pytest.fixture
def filter_wheel(simulator):
    host, port = simulator
    config = TheSkyFilterWheelConfig(
        device_type="filter_wheel",
        host=host,
        port=port,
        filters={"R": 0, "G": 1, "B": 2},
        timeout=5.0,
        status_frequency=0.1,
    )
    fw = config.create_device()
    fw._filter_index = {0: "R", 1: "G", 2: "B"}
    fw.filter_wheel_position = 0
    return fw


@pytest.mark.asyncio
async def test_filter_wheel_connect(filter_wheel):
    await filter_wheel.filter_wheel_connect(Connect())
    assert filter_wheel.device_connected is True


@pytest.mark.asyncio
async def test_filter_wheel_disconnect(filter_wheel):
    await filter_wheel.filter_wheel_connect(Connect())
    await filter_wheel.filter_wheel_disconnect(Disconnect())
    assert filter_wheel.device_connected is False


@pytest.mark.asyncio
async def test_set_filter_by_name(filter_wheel):
    await filter_wheel.filter_wheel_connect(Connect())
    await filter_wheel.filter_wheel_set_filter(SetFilter(filter="G"))

    resp = await filter_wheel.execute("ccdsoftCamera.FilterIndexZeroBased;")
    assert int(float(resp)) == 1


@pytest.mark.asyncio
async def test_set_filter_by_index(filter_wheel):
    await filter_wheel.filter_wheel_connect(Connect())
    await filter_wheel.filter_wheel_set_filter(SetFilter(filter=2))

    resp = await filter_wheel.execute("ccdsoftCamera.FilterIndexZeroBased;")
    assert int(float(resp)) == 2


@pytest.mark.asyncio
async def test_set_filter_invalid_name(filter_wheel):
    await filter_wheel.filter_wheel_connect(Connect())
    with pytest.raises(RuntimeError, match="unavailable"):
        await filter_wheel.filter_wheel_set_filter(SetFilter(filter="X"))


@pytest.mark.asyncio
async def test_set_filter_invalid_index(filter_wheel):
    await filter_wheel.filter_wheel_connect(Connect())
    with pytest.raises(RuntimeError, match="unavailable"):
        await filter_wheel.filter_wheel_set_filter(SetFilter(filter=99))
