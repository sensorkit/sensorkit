import asyncio

import pytest

import sensorkit.api as sk
from sensorkit.ascom.filter_wheel import FilterWheelService


class FakeBinding:
    def __init__(self):
        self.published = []

    async def publish(self, model):
        self.published.append(model)


class FakeFilterWheel:
    def __init__(self):
        self.Connected = False
        self._position = 0
        self.Names = ["L", "R", "G", "B"]
        self.FocusOffsets = [0, 10, 10, 10]

    @property
    def Position(self):
        return self._position

    @Position.setter
    def Position(self, value: int):
        # simulate moving
        self._position = -1
        async def _finish():
            await asyncio.sleep(0.01)
            self._position = value
        asyncio.create_task(_finish())


@pytest.mark.asyncio
async def test_filter_wheel_startup_and_connected_publish():
    fw = FakeFilterWheel()
    svc = FilterWheelService(device=fw, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    assert fw.Connected is True
    kinds = {type(m).__name__ for m in svc.device.binding.published}
    assert "Connected" in kinds


@pytest.mark.asyncio
async def test_change_filter_by_name_and_index():
    fw = FakeFilterWheel()
    svc = FilterWheelService(device=fw, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()

    # by index
    await svc.filter_wheel_set_position(sk.SetFilter(filter=2))
    assert fw.Position == 2

    # by name (case-insensitive)
    await svc.filter_wheel_set_position(sk.SetFilter(filter="g"))
    assert fw.Position == 2  # G is index 2


@pytest.mark.asyncio
async def test_filter_publisher_outputs_name_and_position():
    fw = FakeFilterWheel()
    svc = FilterWheelService(device=fw, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    fw._position = 1
    model = await svc.filter()
    assert model.name == "R"
    assert model.position == 1
