"""Config parsing and registration tests for the sdasim module."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from sensorkit.sdasim.camera import SdasimCamera, SdasimCameraConfig, SdasimCameraState
from sensorkit.sdasim.service import SdasimConfig


class TestSdasimCameraConfig:
    def test_defaults(self):
        config = SdasimCameraConfig(sdasim_config="scene.yaml")
        assert config.device_type == "camera"
        assert config.sdasim_config == "scene.yaml"
        assert config.mount_entity is None
        assert config.rotator_entity is None
        assert config.device == "cpu"
        assert config.temperature == -10.0
        assert config.binning == 1
        assert config.rebuild_threshold_deg == 0.25
        assert config.status_frequency == 1.0
        assert config.timeout == 60.0

    def test_sdasim_config_required(self):
        with pytest.raises(ValidationError):
            SdasimCameraConfig()

    def test_create_device_returns_camera(self):
        config = SdasimCameraConfig(sdasim_config="scene.yaml")
        device = config.create_device()
        assert isinstance(device, SdasimCamera)
        assert device.config is config

    def test_state_defaults(self):
        state = SdasimCameraState()
        assert state.device_type == "camera"
        assert state.bin_x == 1
        assert state.bin_y == 1


class TestSdasimConfig:
    def test_parses_devices_mapping(self):
        raw = {
            "devices": {
                "SimulatedCamera": {
                    "device_type": "camera",
                    "sdasim_config": "scene.yaml",
                    "mount_entity": "SimulatedMount",
                    "binning": 2,
                }
            }
        }
        config = SdasimConfig.model_validate(raw)
        camera = config.devices["SimulatedCamera"]
        assert isinstance(camera, SdasimCameraConfig)
        assert camera.mount_entity == "SimulatedMount"
        assert camera.binning == 2

    def test_unified_list_form_ignores_id(self):
        # Mirrors the entity_mapper contract: a list of entries each with an `id`
        # (the `id` is popped for the entity name; extra fields are ignored).
        raw = [
            {
                "id": "Sdasim",
                "devices": {
                    "SimulatedCamera": {
                        "device_type": "camera",
                        "sdasim_config": "scene.yaml",
                    }
                },
            }
        ]
        ids = [elem.pop("id") for elem in raw]
        assert ids == ["Sdasim"]

        parsed = TypeAdapter(list[SdasimConfig]).validate_python(raw)
        assert parsed[0].devices["SimulatedCamera"].sdasim_config == "scene.yaml"


def test_config_section_registered():
    from sensorkit.config.section import get_config_section

    section = get_config_section("sdasim")
    assert section is not None
    assert section.key == "sdasim"
    assert section.service_path == "sensorkit.sdasim.service"
