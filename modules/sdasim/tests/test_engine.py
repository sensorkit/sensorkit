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


@pytest.fixture
def scene_yaml(tmp_path):
    path = tmp_path / "scene.yaml"
    path.write_text(_SCENE)
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


class TestAngularSep:
    def test_zero_separation(self):
        assert SdasimEngine._angular_sep_deg(10.0, 20.0, 10.0, 20.0) == pytest.approx(0.0, abs=1e-9)

    def test_one_degree_in_ra_at_equator(self):
        assert SdasimEngine._angular_sep_deg(0.0, 0.0, 1.0, 0.0) == pytest.approx(1.0, abs=1e-6)

    def test_none_is_infinite(self):
        assert SdasimEngine._angular_sep_deg(0.0, 0.0, None, None) == float("inf")


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

    def test_render_binned_ccd(self, scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(scene_yaml, device="cpu")
        engine.initialize()
        image, _ = engine.render_frame(0.5, 10.0, 20.0, bin_factor=2)
        assert image.shape == (128, 128)
        assert image.dtype == np.uint16

    def test_scene_reused_within_threshold_rebuilt_on_drift(self, scene_yaml):
        pytest.importorskip("sdasim")
        engine = SdasimEngine(scene_yaml, device="cpu", rebuild_threshold_deg=0.25)
        engine.initialize()

        engine.render_frame(0.5, 10.0, 20.0)
        scene1 = engine._scene
        # Small drift within threshold -> same Scene reused.
        engine.render_frame(0.5, 10.05, 20.0)
        assert engine._scene is scene1
        # Large slew past threshold -> Scene rebuilt.
        engine.render_frame(0.5, 30.0, 20.0)
        assert engine._scene is not scene1
