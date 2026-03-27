import pytest

import sensorkit.api as sk
from sensorkit.ascom.focuser import FocuserService


class FakeBinding:
    def __init__(self):
        self.published = []

    async def publish(self, model):
        self.published.append(model)


class FakeFocuser:
    def __init__(self):
        self.Connected = False
        self.Position = 100.0

    def Move(self, pos: float):
        self.Position = pos

    def Halt(self):
        pass


@pytest.mark.asyncio
async def test_focuser_lifecycle_and_commands():
    f = FakeFocuser()
    svc = FocuserService(device=f, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    assert f.Connected is True

    await svc.focuser_move(sk.ChangeFocusPosition(position=200.0))
    assert f.Position == 200.0

    await svc.focuser_halt(sk.Stop())

    await svc.focuser_connect(sk.Connect())
    await svc.focuser_disconnect(sk.Disconnect())

    await svc.shutdown()


@pytest.mark.asyncio
async def test_focuser_position_publisher():
    f = FakeFocuser()
    svc = FocuserService(device=f, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    await svc.startup()

    f.Position = 150.0
    model = await svc.position()
    assert model.position == 150.0
