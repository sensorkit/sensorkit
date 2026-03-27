import asyncio
from types import SimpleNamespace

import pytest

import sensorkit.api as sk
from sensorkit.pwi4.focuser import FocuserDevice, FocuserTelemetryPublisher


class FakeBinding:
    def __init__(self):
        self.published: list = []

    async def publish(self, model):
        self.published.append(model)


class FakeFocuserAPI:
    def __init__(self):
        self.calls = []
        self._status = SimpleNamespace(
            focuser=SimpleNamespace(
                position=123.4,
                is_connected=True,
                is_enabled=True,
            )
        )

    def set_status(self, position=123.4, is_connected=True, is_enabled=True):
        self._status = SimpleNamespace(
            focuser=SimpleNamespace(
                position=position,
                is_connected=is_connected,
                is_enabled=is_enabled,
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

    async def goto(self, position: float):
        self.calls.append(("goto", (position,)))

    async def stop(self):
        self.calls.append(("stop", ()))


@pytest.mark.asyncio
async def test_focuser_telemetry_publishes_models():
    api = FakeFocuserAPI()
    binding = FakeBinding()
    pub = FocuserTelemetryPublisher(binding=binding, api=api, frequency=0.01)

    await pub.start()
    # Allow at least one poll cycle
    await asyncio.sleep(0.03)
    await pub.stop()

    # Expect FocusPosition, Connected, Enabled published at least once
    kinds = {type(m).__name__ for m in binding.published}
    assert "FocusPosition" in kinds
    assert "Connected" in kinds
    assert "Enabled" in kinds


@pytest.mark.asyncio
async def test_focuser_device_lifecycle_and_commands():
    api = FakeFocuserAPI()
    device = FocuserDevice(api=SimpleNamespace(focuser=api))

    # patch in a binding for telemetry
    device.device.binding = FakeBinding()

    await device.startup()
    # Expect connect + enable
    assert ("connect", ()) in api.calls
    assert ("enable", ()) in api.calls

    # Test commands
    await device.move(sk.ChangeFocusPosition(position=200.0))
    await device.halt(sk.Stop())
    await device.connect(sk.Connect())
    await device.enable(sk.Enable())
    await device.disable(sk.Disable())
    await device.disconnect(sk.Disconnect())

    # Ensure calls were forwarded
    assert ("goto", (200.0,)) in api.calls
    assert ("stop", ()) in api.calls
    assert api.calls.count(("connect", ())) >= 2
    assert api.calls.count(("enable", ())) >= 2
    assert ("disable", ()) in api.calls
    assert ("disconnect", ()) in api.calls

    await device.shutdown()
