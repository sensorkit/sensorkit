import pytest
from conftest import MockNinaClient

import sensorkit.api as sk
from sensorkit.models.devices import TemperatureUnit
from sensorkit.nina.camera import NinaCameraConfig, NinaCameraState
from sensorkit.std.instrument import (
    Binning,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
)


@pytest.fixture
def client():
    return MockNinaClient(info_response={
        "Connected": True,
        "CameraXSize": 4096,
        "CameraYSize": 4096,
        "BinX": 1,
        "BinY": 1,
        "CCDTemperature": -10.0,
    })


@pytest.fixture
def camera(client):
    config = NinaCameraConfig(device_type="camera")
    c = config.create_device()
    c._client = client
    c.state = NinaCameraState()
    c.device_connected = True
    return c


class TestCameraConnect:
    @pytest.mark.asyncio
    async def test_connect(self, client, camera):
        camera.device_connected = None
        await camera.camera_connect(sk.Connect())
        assert camera.device_connected is True
        reqs = client.find_requests("/equipment/camera/connect")
        assert len(reqs) == 1

    @pytest.mark.asyncio
    async def test_disconnect(self, client, camera):
        await camera.camera_disconnect(sk.Disconnect())
        assert camera.device_connected is False
        reqs = client.find_requests("/equipment/camera/disconnect")
        assert len(reqs) == 1


class TestCameraCommands:
    @pytest.mark.asyncio
    async def test_set_binning(self, client, camera):
        await camera.camera_set_binning(
            ConfigureCameraSensor(binning=Binning(x=2, y=2))
        )
        reqs = client.find_requests("/equipment/camera/set-binning")
        assert len(reqs) == 1
        assert reqs[0][1]["binning"] == "2x2"

    @pytest.mark.asyncio
    async def test_set_binning_none(self, client, camera):
        """ConfigureCameraSensor with no binning should be a no-op."""
        await camera.camera_set_binning(ConfigureCameraSensor())
        reqs = client.find_requests("/equipment/camera/set-binning")
        assert len(reqs) == 0

    @pytest.mark.asyncio
    async def test_set_temperature(self, client, camera):
        await camera.camera_set_temperature(
            ConfigureCameraCooler(
                enable=True,
                setpoint=CameraSensorTemperature(temperature=-20, units=TemperatureUnit.CELSIUS),
            )
        )
        reqs = client.find_requests("/equipment/camera/cool")
        assert len(reqs) == 1
        assert reqs[0][1]["temperature"] == -20

    @pytest.mark.asyncio
    async def test_abort(self, client, camera):
        await camera.camera_abort(sk.Abort())
        reqs = client.find_requests("/equipment/camera/abort-exposure")
        assert len(reqs) == 1
