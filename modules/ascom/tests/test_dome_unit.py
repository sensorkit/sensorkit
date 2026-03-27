import time

from alpaca.dome import ShutterState
import pytest
import sensorkit.api as sk
from sensorkit.ascom.dome import DomeService


class FakeBinding:
    def __init__(self):
        self.published = []

    async def publish(self, model):
        self.published.append(model)


class FakeDome:
    def __init__(self):
        self.Connected = False
        self.Slaved = False
        self.AtHome = False
        self.AtPark = False
        self.Slewing = False
        self.ShutterStatus = ShutterState.shutterClosed
        self.Azimuth = 0.0
        self.Altitude = 0.0

        # Capabilities
        self.CanFindHome = True
        self.CanPark = True
        self.CanSetAltitude = False
        self.CanSetAzimuth = True
        self.CanSetPark = True
        self.CanSetShutter = True
        self.CanSlave = True
        self.CanSyncAzimuth = True

    def OpenShutter(self):
        # This method may be called from a worker thread (service uses to_thread),
        # so avoid asyncio APIs here and simulate progression synchronously.
        self.ShutterStatus = ShutterState.shutterOpening
        time.sleep(0.01)
        self.ShutterStatus = ShutterState.shutterOpen

    def CloseShutter(self):
        self.ShutterStatus = ShutterState.shutterClosing
        time.sleep(0.01)
        self.ShutterStatus = ShutterState.shutterClosed


@pytest.mark.asyncio
async def test_dome_startup_and_connected_publish():
    dome = FakeDome()
    svc = DomeService(device=dome, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    assert dome.Connected is True
    kinds = {type(m).__name__ for m in svc.device.binding.published}
    assert "Connected" in kinds


@pytest.mark.asyncio
async def test_dome_open_and_close_waits_until_complete():
    dome = FakeDome()
    svc = DomeService(device=dome, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()

    await svc.dome_open(sk.Open())
    assert dome.ShutterStatus == ShutterState.shutterOpen

    await svc.dome_close(sk.Close())
    assert dome.ShutterStatus == ShutterState.shutterClosed


@pytest.mark.asyncio
async def test_dome_publishers_opened_and_connected():
    dome = FakeDome()
    svc = DomeService(device=dome, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()

    opened = await svc.opened()
    assert opened.is_open is False

    dome.ShutterStatus = ShutterState.shutterOpen
    opened = await svc.opened()
    assert opened.is_open is True

    connected = await svc.connected()
    assert connected.is_connected is True
