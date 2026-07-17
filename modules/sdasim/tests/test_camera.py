# SPDX-License-Identifier: Apache-2.0
"""Camera command/capture tests for the sdasim module (engine mocked)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest
from conftest import make_data_graph_writer

from sensorkit.astro.common import RADecPointing
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.data.fits import ImageInfo
from sensorkit.models.devices import AxisRate, AxisRates, MountAxis
from sensorkit.sdasim.camera import DeviceConnectionError, SdasimCamera, SdasimCameraConfig
from sensorkit.std import (
    Binning,
    CameraCapture,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
    ExposureInfo,
    FrameType,
    TemperatureUnit,
)


def fake_mount_sub(ra_hours: float, dec_degrees: float, ra_rate: float = 0.0, dec_rate: float = 0.0):
    """A stand-in for a started mount ContextSubscription.

    Exposes the same `.cache.get(KeywordType)` surface the camera reads, holding
    real keyword models so type-keyed lookups behave like the real cache.
    """
    cache = {
        RADecPointing: RADecPointing(
            right_ascension_hours=ra_hours, declination_degrees=dec_degrees
        ),
        AxisRates: AxisRates(
            right_ascension=AxisRate(axis=MountAxis.RIGHT_ASCENSION, velocity=ra_rate),
            declination=AxisRate(axis=MountAxis.DECLINATION, velocity=dec_rate),
        ),
    }
    return SimpleNamespace(cache=cache)


def make_camera(**overrides) -> SdasimCamera:
    """Build a camera with its runtime state wired up but the engine mocked."""
    config = SdasimCameraConfig(sdasim_config="scene.yaml", **overrides)
    camera = SdasimCamera(config)

    engine = MagicMock()
    engine.initialized = True
    engine.catalog_enabled = False
    engine.sensor_width = 64
    engine.sensor_height = 48
    engine.default_point = (0.0, 0.0)
    # render_frame now returns (image, metadata).
    engine.render_frame = MagicMock(
        return_value=(np.ones((48, 64), dtype=np.uint16), {"num_targets": 3})
    )

    camera._engine = engine
    camera._mount_sub = fake_mount_sub(6.0, 20.0)  # -> point_ra 90 deg, sidereal
    camera._rotator_sub = None
    camera._bin_x = camera._bin_y = 1
    camera._temperature = config.temperature
    camera._num_targets = None
    camera._mount_ra_rate = 0.0
    camera._mount_dec_rate = 0.0
    camera._capture_lock = asyncio.Lock()
    camera._capture_task = None
    camera.device_connected = True
    return camera


class TestBinning:
    @pytest.mark.asyncio
    async def test_symmetric_binning_set(self, mock_sk_device):
        camera = make_camera()
        await camera.camera_set_binning(ConfigureCameraSensor(binning=Binning(x=2, y=2)))
        assert camera._bin_x == 2
        assert camera._bin_y == 2

    @pytest.mark.asyncio
    async def test_asymmetric_binning_coerced_to_x(self, mock_sk_device):
        camera = make_camera()
        await camera.camera_set_binning(ConfigureCameraSensor(binning=Binning(x=3, y=1)))
        assert camera._bin_x == 3
        assert camera._bin_y == 3

    @pytest.mark.asyncio
    async def test_no_binning_field_is_noop(self, mock_sk_device):
        camera = make_camera()
        await camera.camera_set_binning(ConfigureCameraSensor())
        assert camera._bin_x == 1


class TestTemperature:
    @pytest.mark.asyncio
    async def test_setpoint_tracked(self, mock_sk_device):
        camera = make_camera()
        await camera.camera_set_temperature(
            ConfigureCameraCooler(
                enable=True,
                setpoint=CameraSensorTemperature(temperature=-20.0, units=TemperatureUnit.CELSIUS),
            )
        )
        assert camera._temperature == -20.0


class TestCapture:
    @pytest.mark.asyncio
    async def test_capture_passes_pointing_and_rate(self, mock_sk_device):
        # render_frame(exposure, point_ra, point_dec, mount_ra_rate, mount_dec_rate, obs_time, bin)
        camera = make_camera()
        await camera.camera_capture(CameraCapture(integration_time=0.0, context=Context()))
        args = camera._engine.render_frame.call_args.args
        assert args[1] == pytest.approx(90.0)  # ra_hours 6.0 * 15
        assert args[2] == pytest.approx(20.0)  # dec
        assert camera._num_targets == 3

    @pytest.mark.asyncio
    async def test_capture_falls_back_to_scene_center_without_mount(self, mock_sk_device):
        camera = make_camera()
        camera._mount_sub = None  # no mount subscription -> scene center, sidereal
        camera._engine.default_point = (123.0, -45.0)
        await camera.camera_capture(CameraCapture(integration_time=0.0, context=Context()))
        args = camera._engine.render_frame.call_args.args
        assert args[1] == pytest.approx(123.0)
        assert args[2] == pytest.approx(-45.0)
        assert args[3] == 0.0 and args[4] == 0.0  # sidereal fallback

    @pytest.mark.asyncio
    async def test_capture_uses_inertial_mount_rate(self, mock_sk_device):
        # A rate track publishes nonzero ICRF axis rates; they pass straight through.
        camera = make_camera()
        camera._mount_sub = fake_mount_sub(6.0, 20.0, ra_rate=0.5, dec_rate=-0.25)
        await camera.camera_capture(CameraCapture(integration_time=0.0, context=Context()))
        args = camera._engine.render_frame.call_args.args
        assert args[3] == pytest.approx(0.5)
        assert args[4] == pytest.approx(-0.25)

    @pytest.mark.asyncio
    async def test_capture_no_datagraph_is_safe(self, mock_sk_device):
        # data_graph returns None by default -> capture renders but discards.
        # integration_time=0 so the exposure wait is a no-op.
        camera = make_camera()
        await camera.camera_capture(CameraCapture(integration_time=0.0, context=Context()))
        camera._engine.render_frame.assert_called_once()

    @pytest.mark.asyncio
    async def test_capture_holds_frame_for_exposure(self, mock_sk_device):
        # The frame must not be yielded before the commanded integration time.
        import time

        camera = make_camera()
        exposure = 0.3
        start = time.perf_counter()
        await camera.camera_capture(
            CameraCapture(integration_time=exposure, context=Context())
        )
        elapsed = time.perf_counter() - start
        assert elapsed >= exposure  # render is instant (mocked), so the wait dominates

    @pytest.mark.asyncio
    async def test_capture_writes_to_datagraph(self, mock_sk_device):
        graph, writer = make_data_graph_writer()
        mock_sk_device.data_graph.return_value = graph

        camera = make_camera()
        cmd = CameraCapture(integration_time=0.0, context=Context())
        await camera.camera_capture(cmd)
        context = cmd.context

        # Rendered 48x64 uint16 -> 48*64*2 bytes written.
        writer.write.assert_called_once()
        written = writer.write.call_args[0][0]
        assert len(written) == 48 * 64 * 2
        writer.drain.assert_awaited_once()
        writer.wait_closed.assert_awaited_once()

        # Context carries the image structure (as ImageInfo) + acquisition metadata for
        # array_to_fits. BITPIX is derived by astropy at write time, not stored here.
        image_info = context.get(ImageInfo)
        assert image_info.array.shape == (48, 64)
        assert image_info.array.dtype == "uint16"
        assert image_info.binning == (1, 1)
        exposure_info = context.get(ExposureInfo)
        assert exposure_info.exposure_time == 0.0
        assert exposure_info.image_type is FrameType.LIGHT
        assert context.get(FileNameTemplate)

    @pytest.mark.asyncio
    async def test_capture_requires_connected(self, mock_sk_device):
        camera = make_camera()
        camera.device_connected = False
        with pytest.raises(DeviceConnectionError):
            await camera.camera_capture(CameraCapture(integration_time=1.0, context=Context()))


class TestLifecycle:
    """Connection guard + status-loop scaffolding (flattened onto the camera)."""

    def _camera(self) -> SdasimCamera:
        return SdasimCamera(SdasimCameraConfig(sdasim_config="scene.yaml"))

    @pytest.mark.asyncio
    async def test_require_connected_returns_when_connected(self):
        camera = self._camera()
        camera.device_connected = True
        await camera.require_connected()  # no raise

    @pytest.mark.asyncio
    async def test_require_connected_raises_when_not_connected(self):
        camera = self._camera()
        with pytest.raises(DeviceConnectionError):
            await camera.require_connected()

    @pytest.mark.asyncio
    async def test_status_loop_start_and_stop(self):
        camera = self._camera()
        ran = asyncio.Event()

        async def loop():
            ran.set()
            await asyncio.sleep(3600)

        camera.start_status_loop(loop())
        await asyncio.wait_for(ran.wait(), timeout=1.0)
        assert camera._status_task is not None

        await camera.stop_status_loop()
        assert camera._status_task is None


class TestArchetype:
    def test_camera_capture_handler_exists(self):
        # CameraCapture handler exists so the StandardCamera archetype is satisfied.
        assert hasattr(SdasimCamera, "camera_capture")
        assert hasattr(SdasimCamera, "_initialize")

    def test_connect_disconnect_handlers_removed(self):
        # Connect/Disconnect are optional on the StandardCamera archetype; the
        # simulated camera publishes connected state at init/deinit instead.
        assert not hasattr(SdasimCamera, "camera_connect")
        assert not hasattr(SdasimCamera, "camera_disconnect")
