"""Tests for NINA service configuration models."""

import pytest

from sensorkit.nina.service import NinaConfig, NinaServerConfig
from sensorkit.nina.camera import NinaCameraConfig
from sensorkit.nina.dome import NinaDomeConfig
from sensorkit.nina.filter_wheel import NinaFilterWheelConfig
from sensorkit.nina.focuser import NinaFocuserConfig
from sensorkit.nina.guider import NinaGuiderConfig
from sensorkit.nina.mount import NinaMountConfig
from sensorkit.nina.rotator import NinaRotatorConfig
from sensorkit.nina.safety_monitor import NinaSafetyMonitorConfig
from sensorkit.nina.switch import NinaSwitchConfig
from sensorkit.nina.weather import NinaWeatherConfig


class TestNinaServerConfig:
    def test_defaults(self):
        config = NinaServerConfig()
        assert config.host == "localhost"
        assert config.port == 1888
        assert config.devices == {}

    def test_propagation(self):
        data = {
            "host": "192.168.1.50",
            "port": 9999,
            "devices": {
                "camera1": {"device_type": "camera"},
                "mount1": {"device_type": "mount"},
            },
        }
        config = NinaServerConfig.model_validate(data)

        cam = config.devices["camera1"]
        assert isinstance(cam, NinaCameraConfig)
        assert cam.host == "192.168.1.50"
        assert cam.port == 9999

        tel = config.devices["mount1"]
        assert isinstance(tel, NinaMountConfig)
        assert tel.host == "192.168.1.50"

    def test_device_override(self):
        data = {
            "host": "192.168.1.50",
            "port": 1888,
            "devices": {
                "camera1": {
                    "device_type": "camera",
                    "host": "10.0.0.1",
                    "port": 2000,
                },
            },
        }
        config = NinaServerConfig.model_validate(data)
        cam = config.devices["camera1"]
        assert cam.host == "10.0.0.1"
        assert cam.port == 2000


class TestNinaConfig:
    def test_empty(self):
        config = NinaConfig()
        assert config.endpoints == []

    def test_full_config(self):
        data = {
            "endpoints": [
                {
                    "host": "localhost",
                    "port": 1888,
                    "devices": {
                        "cam": {"device_type": "camera"},
                        "tel": {"device_type": "mount"},
                        "dome": {"device_type": "dome"},
                        "foc": {"device_type": "focuser"},
                        "fw": {"device_type": "filter_wheel"},
                        "rot": {"device_type": "rotator"},
                        "guide": {"device_type": "guider"},
                        "sw": {"device_type": "switch"},
                        "wx": {"device_type": "weather"},
                        "sm": {"device_type": "safety_monitor"},
                    },
                }
            ]
        }
        config = NinaConfig.model_validate(data)
        assert len(config.endpoints) == 1
        assert len(config.endpoints[0].devices) == 10


class TestDeviceCreation:
    def test_camera(self):
        from sensorkit.nina.camera import NinaCamera
        config = NinaCameraConfig(host="localhost")
        assert isinstance(config.create_device(), NinaCamera)

    def test_mount(self):
        from sensorkit.nina.mount import NinaMount
        config = NinaMountConfig(host="localhost")
        assert isinstance(config.create_device(), NinaMount)

    def test_dome(self):
        from sensorkit.nina.dome import NinaDome
        config = NinaDomeConfig(host="localhost")
        assert isinstance(config.create_device(), NinaDome)

    def test_focuser(self):
        from sensorkit.nina.focuser import NinaFocuser
        config = NinaFocuserConfig(host="localhost")
        assert isinstance(config.create_device(), NinaFocuser)

    def test_filter_wheel(self):
        from sensorkit.nina.filter_wheel import NinaFilterWheel
        config = NinaFilterWheelConfig(host="localhost")
        assert isinstance(config.create_device(), NinaFilterWheel)

    def test_rotator(self):
        from sensorkit.nina.rotator import NinaRotator
        config = NinaRotatorConfig(host="localhost")
        assert isinstance(config.create_device(), NinaRotator)

    def test_guider(self):
        from sensorkit.nina.guider import NinaGuider
        config = NinaGuiderConfig(host="localhost")
        assert isinstance(config.create_device(), NinaGuider)

    def test_switch(self):
        from sensorkit.nina.switch import NinaSwitch
        config = NinaSwitchConfig(host="localhost")
        assert isinstance(config.create_device(), NinaSwitch)

    def test_weather(self):
        from sensorkit.nina.weather import NinaWeather
        config = NinaWeatherConfig(host="localhost")
        assert isinstance(config.create_device(), NinaWeather)

    def test_safety_monitor(self):
        from sensorkit.nina.safety_monitor import NinaSafetyMonitor
        config = NinaSafetyMonitorConfig(host="localhost")
        assert isinstance(config.create_device(), NinaSafetyMonitor)
