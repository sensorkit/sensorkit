# SPDX-License-Identifier: Apache-2.0
"""Tests for Alpaca camera device."""

import array

import pytest

from sensorkit.alpaca.camera import (
    AlpacaCameraConfig,
    AlpacaCameraState,
)
from sensorkit.alpaca.testing import FakeAlpacaSDKDevice, Readings
from sensorkit.std import Connect, Disconnect, Stop


class TestCameraCapture:
    def test_do_capture_returns_image_data(self):
        """Test the synchronous capture method with a fake camera."""
        config = AlpacaCameraConfig(host="localhost")
        cam = config.create_device()

        cam.camera = FakeAlpacaSDKDevice(
            ImageReady=True,
            ImageArrayRaw=array.array("H", [100, 200, 300, 400, 500, 600]),
        )

        result = cam._do_capture(1.0, True, 10.0)
        assert len(result) == 6
        assert cam.camera.calls("StartExposure") == [((1.0, True), {})]

    def test_do_capture_polls_until_ready(self):
        """Test that capture polls ImageReady until True."""
        config = AlpacaCameraConfig(host="localhost")
        cam = config.create_device()

        cam.camera = FakeAlpacaSDKDevice(
            ImageReady=Readings(False, False, True),
            ImageArrayRaw=array.array("H", [100]),
        )

        result = cam._do_capture(0.1, True, 10.0)
        assert result == array.array("H", [100])

    def test_do_capture_timeout(self):
        """Test that capture raises RuntimeError on timeout."""
        config = AlpacaCameraConfig(host="localhost")
        cam = config.create_device()

        cam.camera = FakeAlpacaSDKDevice(ImageReady=False)

        with pytest.raises(RuntimeError, match="timed out"):
            cam._do_capture(0.0, True, 0.5)


@pytest.fixture
def camera():
    config = AlpacaCameraConfig(host="localhost", timeout=5.0, status_frequency=0.1)
    cam = config.create_device()
    cam.state = AlpacaCameraState()
    cam.device_name = "Camera"
    device = FakeAlpacaSDKDevice(
        Connected=True,
        Connecting=False,
        ImageReady=False,
        CameraState=0,
        BinX=1,
        BinY=1,
        MaxBinX=4,
        MaxBinY=4,
        CameraXSize=4096,
        CameraYSize=4096,
        CoolerOn=False,
        CCDTemperature=-10.0,
        SetCCDTemperature=-10.0,
        CanAbortExposure=True,
        CanStopExposure=True,
        CanSetCCDTemperature=True,
        CanGetCoolerPower=True,
        CanAsymmetricBin=False,
        CanPulseGuide=False,
        HasShutter=True,
    )
    cam.camera = device
    cam.device_connected = True
    cam._can_abort_exposure = True
    cam._can_stop_exposure = True
    cam._can_set_ccd_temperature = True
    cam._can_get_cooler_power = True
    cam._can_asymmetric_bin = False
    cam._can_pulse_guide = False
    cam._has_shutter = True
    cam._camera_x_size = 4096
    cam._camera_y_size = 4096
    cam._max_bin_x = 4
    cam._max_bin_y = 4
    cam._data_tasks = set()
    return cam


class TestCameraLifecycle:
    @pytest.mark.asyncio
    async def test_camera_connect(self, camera):
        camera.device_connected = False
        camera.camera._properties["Connected"] = False
        await camera.camera_connect(Connect())
        assert camera.device_connected is True

    @pytest.mark.asyncio
    async def test_camera_disconnect(self, camera):
        await camera.camera_disconnect(Disconnect())
        assert camera.device_connected is False

    @pytest.mark.asyncio
    async def test_camera_set_temperature(self, camera):
        from sensorkit.std import TemperatureUnit
        from sensorkit.std.instrument import CameraSensorTemperature, ConfigureCameraCooler

        await camera.camera_set_temperature(
            ConfigureCameraCooler(
                enable=True,
                setpoint=CameraSensorTemperature(
                    temperature=-20.0,
                    units=TemperatureUnit.CELSIUS,
                ),
            )
        )
        assert camera.camera._properties["CoolerOn"] is True
        assert camera.camera._properties["SetCCDTemperature"] == -20.0

    @pytest.mark.asyncio
    async def test_camera_set_binning(self, camera):
        from sensorkit.std.instrument import Binning, ConfigureCameraSensor

        await camera.camera_set_binning(ConfigureCameraSensor(binning=Binning(x=2, y=2)))
        assert camera.camera._properties["BinX"] == 2
        assert camera.camera._properties["BinY"] == 2

    @pytest.mark.asyncio
    async def test_camera_stop(self, camera):
        await camera.camera_stop(Stop())

    @pytest.mark.asyncio
    async def test_camera_abort(self, camera):
        import sensorkit.api as sk

        await camera.camera_abort(sk.Abort())
