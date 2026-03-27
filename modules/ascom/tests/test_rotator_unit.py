import pytest

import sensorkit.api as sk
from sensorkit.ascom.rotator import RotatorService


class FakeBinding:
    def __init__(self):
        self.published = []

    async def publish(self, model):
        self.published.append(model)


class FakeRotator:
    def __init__(self):
        self.Connected = False
        self.Position = 0.0

    def MoveMechanical(self, pos: float):
        self.Position = pos

    def Halt(self):
        pass


@pytest.mark.asyncio
async def test_rotator_lifecycle_and_commands():
    r = FakeRotator()
    svc = RotatorService(device=r, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    assert r.Connected is True

    await svc.rotator_move(sk.ChangeRotatorPosition(position=123.4))
    assert r.Position == 123.4

    await svc.rotator_halt(sk.Stop())

    await svc.rotator_connect(sk.Connect())
    await svc.rotator_disconnect(sk.Disconnect())

    await svc.shutdown()


@pytest.mark.asyncio
async def test_rotator_position_publisher():
    r = FakeRotator()
    svc = RotatorService(device=r, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    await svc.startup()

    r.Position = 77.0
    model = await svc.status_publish()
    assert model.position == 77.0
