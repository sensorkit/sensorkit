import asyncio
from types import SimpleNamespace

import pytest

import sensorkit.api as sk
from sensorkit.pwi4.rotator import RotatorDevice, RotatorTelemetryPublisher


class FakeBinding:
    def __init__(self):
        self.published: list = []

    async def publish(self, model):
        self.published.append(model)


class FakeRotatorAPI:
    def __init__(self):
        self.calls = []
        self._status = SimpleNamespace(
            rotator=SimpleNamespace(
                mech_position_degs=45.0,
                is_connected=True,
                is_enabled=True,
            )
        )

    async def status(self):
        self.calls.append(("status", ()))
        return self._status

    async def connect(self):
        self.calls.append(("connect", ()))

    async def disconnect(self):
        self.calls.append(("disconnect", ()))

    async def enable(self):
        self.calls.append(("enable", ()))

    async def disable(self):
        self.calls.append(("disable", ()))

    async def goto_mech(self, pos: float):
        self.calls.append(("goto_mech", (pos,)))

    async def stop(self):
        self.calls.append(("stop", ()))


@pytest.mark.asyncio
async def test_rotator_telemetry_publishes_models():
    api = FakeRotatorAPI()
    binding = FakeBinding()
    pub = RotatorTelemetryPublisher(binding=binding, api=api, frequency=0.01)

    await pub.start()
    await asyncio.sleep(0.03)
    await pub.stop()

    kinds = {type(m).__name__ for m in binding.published}
    assert "RotatorPosition" in kinds
    assert "Connected" in kinds
    assert "Enabled" in kinds


@pytest.mark.asyncio
async def test_rotator_device_lifecycle_and_commands():
    api = FakeRotatorAPI()
    device = RotatorDevice(api=SimpleNamespace(rotator=api))
    device.device.binding = FakeBinding()

    await device.startup()
    assert ("connect", ()) in api.calls
    assert ("enable", ()) in api.calls

    await device.move(sk.ChangeRotatorPosition(position=123.4))
    await device.halt(sk.Stop())
    await device.connect(sk.Connect())
    await device.enable(sk.Enable())
    await device.disable(sk.Disable())
    await device.disconnect(sk.Disconnect())

    assert ("goto_mech", (123.4,)) in api.calls
    assert ("stop", ()) in api.calls
    assert api.calls.count(("connect", ())) >= 2
    assert api.calls.count(("enable", ())) >= 2
    assert ("disable", ()) in api.calls
    assert ("disconnect", ()) in api.calls

    await device.shutdown()
