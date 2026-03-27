import asyncio
from types import SimpleNamespace

import pytest

import sensorkit.api as sk
from sensorkit.pwi4.mirrorcover import MirrorCoverDevice, MirrorCoverTelemetryPublisher, Reset


class FakeBinding:
    def __init__(self):
        self.published: list = []

    async def publish(self, model):
        self.published.append(model)


class FakeMirrorCoverAPI:
    def __init__(self):
        self.calls = []
        self._status = SimpleNamespace(
            mirrorcover=SimpleNamespace(
                is_connected=True,
            )
        )

    async def status(self):
        self.calls.append(("status", ()))
        return self._status

    async def connect(self):
        self.calls.append(("connect", ()))

    async def disconnect(self):
        self.calls.append(("disconnect", ()))

    async def open(self):
        self.calls.append(("open", ()))

    async def close(self):
        self.calls.append(("close", ()))

    async def stop(self):
        self.calls.append(("stop", ()))

    async def reset(self):
        self.calls.append(("reset", ()))


@pytest.mark.asyncio
async def test_mirrorcover_telemetry_publishes_connected():
    api = FakeMirrorCoverAPI()
    binding = FakeBinding()
    pub = MirrorCoverTelemetryPublisher(binding=binding, api=api, frequency=0.01)

    await pub.start()
    await asyncio.sleep(0.03)
    await pub.stop()

    kinds = {type(m).__name__ for m in binding.published}
    assert "Connected" in kinds


@pytest.mark.asyncio
async def test_mirrorcover_device_lifecycle_and_commands():
    api = FakeMirrorCoverAPI()
    device = MirrorCoverDevice(api=SimpleNamespace(mirrorcover=api))
    device.device.binding = FakeBinding()

    await device.init()
    assert ("connect", ()) in api.calls

    await device.open(sk.Open())
    await device.close(sk.Close())
    await device.stop(sk.Close())
    await device.reset(Reset())

    assert ("open", ()) in api.calls
    assert ("close", ()) in api.calls
    assert ("stop", ()) in api.calls
    assert ("reset", ()) in api.calls

    await device.deinit()
    assert ("disconnect", ()) in api.calls
