# SPDX-License-Identifier: Apache-2.0
"""Config parsing and registration tests for the sdasim module."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from sensorkit.sdasim.camera import SdasimCamera, SdasimCameraConfig, SdasimCameraState


class TestSdasimCameraConfig:
    def test_defaults(self):
        config = SdasimCameraConfig(sdasim_config="scene.yaml")
        assert config.sdasim_config == "scene.yaml"
        assert config.mount_entity is None
        assert config.rotator_entity is None
        assert config.device == "cpu"
        assert config.temperature == -10.0
        assert config.binning == 1
        assert config.status_frequency == 1.0

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
        assert state.bin_x == 1
        assert state.bin_y == 1


class TestSdasimSection:
    def test_unified_list_form_names_each_camera(self):
        # Mirrors the by_subkey contract: a flat list of camera entries, each naming its
        # entity with `id`; the entry validates as a camera config with the key still in
        # place. One entry == one camera == one service (delegate entity).
        raw = [
            {
                "id": "sdasimCameraAlpaca",
                "sdasim_config": "scene.yaml",
                "mount_entity": "OmniSimTelescope",
                "rotator_entity": "OmniSimRotator",
                "binning": 2,
            },
            {
                "id": "sdasimCameraPWI4",
                "sdasim_config": "scene.yaml",
                "mount_entity": "PWI4Telescope",
            },
        ]
        ids = [elem["id"] for elem in raw]
        assert ids == ["sdasimCameraAlpaca", "sdasimCameraPWI4"]

        parsed = TypeAdapter(list[SdasimCameraConfig]).validate_python(raw)
        assert parsed[0].mount_entity == "OmniSimTelescope"
        assert parsed[0].rotator_entity == "OmniSimRotator"
        assert parsed[0].binning == 2
        assert parsed[1].mount_entity == "PWI4Telescope"
        assert parsed[1].rotator_entity is None


def test_config_section_registered():
    import sensorkit.sdasim.service  # noqa: F401 -- import registers the config section
    from sensorkit.config.section import get_config_section

    section = get_config_section("sdasim")
    assert section is not None
    assert section.key == "sdasim"
    assert section.service_path == "sensorkit.sdasim.service"
