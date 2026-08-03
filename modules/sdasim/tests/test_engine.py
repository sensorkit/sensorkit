# SPDX-License-Identifier: Apache-2.0
"""Engine and telemetry-bridge tests for the sdasim module."""

from __future__ import annotations

import numpy as np
import pytest

from sensorkit.sdasim.engine import SdasimEngine

# Minimal self-contained sdasim SceneConfig (bins star field, catalog OFF, so no
# external data or Space-Track creds needed) used by the real-render tests.
_SCENE = """
sensor:
  height: 256
  width: 256
  y_fov: 0.5
  x_fov: 0.5
  exposure: 1.0
  num_frames: 1
  is_cmos: false
  a2d_dtype: uint16
stars:
  mode: bins
star_motion:
  temporal_osf: 1
seed: 1
device: cpu
"""


# Same scene with sdasim's geometric defocus model enabled and noise disabled,
# so in-focus vs defocused frames differ only through the optics model.
_SCENE_OPTICS = (
    _SCENE
    + """
enable_shot_noise: false
enable_read_noise: false
optics:
  enabled: true
  f_number: 8.0
  obscuration: 0.35
  pixel_pitch_um: 9.0
  max_render_seconds: 0.5
"""
)


@pytest.fixture
def scene_yaml(tmp_path):
    path = tmp_path / "scene.yaml"
    path.write_text(_SCENE)
    return str(path)


@pytest.fixture
def optics_scene_yaml(tmp_path):
    path = tmp_path / "scene_optics.yaml"
    path.write_text(_SCENE_OPTICS)
    return str(path)


class TestEngineNotInitialized:
    def test_render_before_initialize_raises(self):
        engine = SdasimEngine("scene.yaml")
        with pytest.raises(RuntimeError):
            engine.render_frame(1.0, 0.0, 0.0)

    def test_dimensions_zero_before_initialize(self):
        engine = SdasimEngine("scene.yaml")
        assert engine.sensor_width == 0
        assert engine.sensor_height == 0
        assert not engine.initialized

    def test_missing_config_raises(self):
        engine = SdasimEngine("/nonexistent/scene.yaml")
        with pytest.raises(FileNotFoundError):
            engine.initialize()


class TestApplyBinning:
    def test_no_binning_passthrough(self):
        img = np.arange(16, dtype=np.uint16).reshape(4, 4)
        out = SdasimEngine.apply_binning(img, 1)
        assert out is img

    def test_2x2_sums_blocks(self):
        img = np.ones((4, 4), dtype=np.uint16)
        out = SdasimEngine.apply_binning(img, 2)
        assert out.shape == (2, 2)
        assert np.all(out == 4)  # each 2x2 block sums to 4

    def test_clips_to_uint16(self):
        img = np.full((2, 2), 40000, dtype=np.uint16)
        out = SdasimEngine.apply_binning(img, 2)
        assert out.dtype == np.uint16
        assert out[0, 0] == 65535  # 4*40000 clipped


class TestRealRender:
    """End-to-end render (catalog off); requires the optional sdasim + torch extra."""

    def test_render_static_scene(self, scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(scene_yaml, device="cpu")
        engine.initialize()
        assert (engine.sensor_width, engine.sensor_height) == (256, 256)

        image, meta = engine.render_frame(0.5, 10.0, 20.0)
        assert image.dtype == np.uint16
        assert image.shape == (256, 256)
        assert isinstance(meta, dict)
        # Engine forwards the commanded pointing into render (which re-projects a
        # sky-backed field to it); metadata echoes the center used for this frame.
        assert meta["point_ra"] == 10.0
        assert meta["point_dec"] == 20.0

    def test_render_binned_ccd(self, scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(scene_yaml, device="cpu")
        engine.initialize()
        image, _ = engine.render_frame(0.5, 10.0, 20.0, bin_factor=2)
        assert image.shape == (128, 128)
        assert image.dtype == np.uint16


class TestDefocus:
    """Focuser telemetry -> sdasim optics model pass-through."""

    def test_optics_enabled_property(self, scene_yaml, optics_scene_yaml):
        pytest.importorskip("sdasim")
        plain = SdasimEngine(scene_yaml, device="cpu")
        assert not plain.optics_enabled  # also safe before initialize()
        plain.initialize()
        assert not plain.optics_enabled
        optics = SdasimEngine(optics_scene_yaml, device="cpu")
        optics.initialize()
        assert optics.optics_enabled

    def test_defocus_flows_into_render(self, optics_scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(optics_scene_yaml, device="cpu")
        engine.initialize()
        sharp, meta_sharp = engine.render_frame(0.5, 10.0, 20.0, defocus_um=0.0)
        blurred, meta_blur = engine.render_frame(0.5, 10.0, 20.0, defocus_um=1500.0)
        assert meta_sharp["pupil_samples"] == 1
        assert meta_blur["defocus_um"] == 1500.0
        assert meta_blur["pupil_samples"] > 1
        # Noise is off in this scene, so any difference is the defocus model.
        assert not np.array_equal(sharp, blurred)

    def test_defocus_ignored_without_optics(self, scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(scene_yaml, device="cpu")
        engine.initialize()
        image, _ = engine.render_frame(0.5, 10.0, 20.0, defocus_um=1500.0)
        assert image.shape == (256, 256)  # renders in focus, no error

    def test_scene_reused_across_pointing_rebuilt_on_exposure_change(self, scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(scene_yaml, device="cpu")
        engine.initialize()

        engine.render_frame(0.5, 10.0, 20.0)
        scene1 = engine._scene
        # Any pointing change reuses the same Scene -- the star field re-projects
        # to the commanded pointing inside render(), no rebuild needed.
        engine.render_frame(0.5, 10.05, 20.0)
        assert engine._scene is scene1
        engine.render_frame(0.5, 30.0, 20.0)
        assert engine._scene is scene1
        # Exposure is baked in at construction, so a change there rebuilds.
        engine.render_frame(1.0, 30.0, 20.0)
        assert engine._scene is not scene1
