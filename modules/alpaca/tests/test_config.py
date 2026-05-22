"""Tests for Alpaca service configuration models."""

from sensorkit.alpaca.camera import AlpacaCameraConfig
from sensorkit.alpaca.cover_calibrator import AlpacaCoverCalibratorConfig
from sensorkit.alpaca.dome import AlpacaDomeConfig
from sensorkit.alpaca.filter_wheel import AlpacaFilterWheelConfig
from sensorkit.alpaca.focuser import AlpacaFocuserConfig
from sensorkit.alpaca.observing_conditions import AlpacaObservingConditionsConfig
from sensorkit.alpaca.rotator import AlpacaRotatorConfig
from sensorkit.alpaca.safety_monitor import AlpacaSafetyMonitorConfig
from sensorkit.alpaca.service import AlpacaConfig, AlpacaServerConfig
from sensorkit.alpaca.switch import AlpacaSwitchConfig
from sensorkit.alpaca.telescope import AlpacaTelescopeConfig


class TestAlpacaServerConfig:
    def test_defaults(self):
        config = AlpacaServerConfig()
        assert config.host == "localhost"
        assert config.port == 11111
        assert config.protocol == "http"
        assert config.devices == {}

    def test_propagation(self):
        data = {
            "host": "192.168.1.50",
            "port": 7654,
            "protocol": "https",
            "devices": {
                "camera1": {
                    "device_type": "camera",
                    "device_number": 0,
                },
                "telescope1": {
                    "device_type": "telescope",
                    "device_number": 0,
                },
            },
        }
        config = AlpacaServerConfig.model_validate(data)

        cam = config.devices["camera1"]
        assert isinstance(cam, AlpacaCameraConfig)
        assert cam.host == "192.168.1.50"
        assert cam.port == 7654
        assert cam.protocol == "https"

        tel = config.devices["telescope1"]
        assert isinstance(tel, AlpacaTelescopeConfig)
        assert tel.host == "192.168.1.50"

    def test_device_override(self):
        """Device-level settings should not be overwritten by server defaults."""
        data = {
            "host": "192.168.1.50",
            "port": 7654,
            "devices": {
                "camera1": {
                    "device_type": "camera",
                    "host": "10.0.0.1",
                    "port": 9999,
                },
            },
        }
        config = AlpacaServerConfig.model_validate(data)
        cam = config.devices["camera1"]
        assert cam.host == "10.0.0.1"
        assert cam.port == 9999


class TestAlpacaConfig:
    def test_empty(self):
        config = AlpacaConfig()
        assert config.endpoints == []

    def test_full_config(self):
        data = {
            "endpoints": [
                {
                    "host": "localhost",
                    "port": 11111,
                    "devices": {
                        "cam": {"device_type": "camera"},
                        "tel": {"device_type": "telescope"},
                        "dome": {"device_type": "dome"},
                        "foc": {"device_type": "focuser"},
                        "fw": {"device_type": "filter_wheel"},
                        "rot": {"device_type": "rotator"},
                        "cc": {"device_type": "cover_calibrator"},
                        "oc": {"device_type": "observing_conditions"},
                        "sm": {"device_type": "safety_monitor"},
                        "sw": {"device_type": "switch"},
                    },
                }
            ]
        }
        config = AlpacaConfig.model_validate(data)
        assert len(config.endpoints) == 1
        assert len(config.endpoints[0].devices) == 10


class TestDeviceConfigs:
    def test_camera_config(self):
        config = AlpacaCameraConfig(host="localhost")
        assert config.device_type == "camera"
        assert config.timeout == 60.0

    def test_telescope_config(self):
        config = AlpacaTelescopeConfig(host="localhost")
        assert config.device_type == "telescope"
        assert config.status_frequency_slow == 1.0
        assert config.status_frequency_fast == 0.1
        assert config.timeout == 300.0

    def test_dome_config(self):
        config = AlpacaDomeConfig(host="localhost")
        assert config.device_type == "dome"
        assert config.timeout == 300.0

    def test_focuser_config(self):
        config = AlpacaFocuserConfig(host="localhost")
        assert config.device_type == "focuser"

    def test_filter_wheel_config(self):
        config = AlpacaFilterWheelConfig(host="localhost")
        assert config.device_type == "filter_wheel"

    def test_rotator_config(self):
        config = AlpacaRotatorConfig(host="localhost")
        assert config.device_type == "rotator"

    def test_cover_calibrator_config(self):
        config = AlpacaCoverCalibratorConfig(host="localhost")
        assert config.device_type == "cover_calibrator"

    def test_observing_conditions_config(self):
        config = AlpacaObservingConditionsConfig(host="localhost")
        assert config.device_type == "observing_conditions"
        assert config.status_frequency == 30.0

    def test_safety_monitor_config(self):
        config = AlpacaSafetyMonitorConfig(host="localhost")
        assert config.device_type == "safety_monitor"

    def test_switch_config(self):
        config = AlpacaSwitchConfig(host="localhost")
        assert config.device_type == "switch"


class TestDeviceCreation:
    """Test that all configs create the correct device type."""

    def test_camera_creates_device(self):
        from sensorkit.alpaca.camera import AlpacaCamera

        config = AlpacaCameraConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaCamera)

    def test_telescope_creates_device(self):
        from sensorkit.alpaca.telescope import AlpacaTelescope

        config = AlpacaTelescopeConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaTelescope)

    def test_dome_creates_device(self):
        from sensorkit.alpaca.dome import AlpacaDome

        config = AlpacaDomeConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaDome)

    def test_focuser_creates_device(self):
        from sensorkit.alpaca.focuser import AlpacaFocuser

        config = AlpacaFocuserConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaFocuser)

    def test_filter_wheel_creates_device(self):
        from sensorkit.alpaca.filter_wheel import AlpacaFilterWheel

        config = AlpacaFilterWheelConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaFilterWheel)

    def test_rotator_creates_device(self):
        from sensorkit.alpaca.rotator import AlpacaRotator

        config = AlpacaRotatorConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaRotator)

    def test_cover_calibrator_creates_device(self):
        from sensorkit.alpaca.cover_calibrator import AlpacaCoverCalibrator

        config = AlpacaCoverCalibratorConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaCoverCalibrator)

    def test_observing_conditions_creates_device(self):
        from sensorkit.alpaca.observing_conditions import AlpacaObservingConditions

        config = AlpacaObservingConditionsConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaObservingConditions)

    def test_safety_monitor_creates_device(self):
        from sensorkit.alpaca.safety_monitor import AlpacaSafetyMonitor

        config = AlpacaSafetyMonitorConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaSafetyMonitor)

    def test_switch_creates_device(self):
        from sensorkit.alpaca.switch import AlpacaSwitch

        config = AlpacaSwitchConfig(host="localhost")
        device = config.create_device()
        assert isinstance(device, AlpacaSwitch)
