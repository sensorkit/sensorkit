import array
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import sensorkit.api as sk
from sensorkit.ascom.camera import (
    CameraAttributes,
    CameraCapabilities,
    CameraLimitations,
    CameraService,
)
from sensorkit.ascom.commands import (
    CameraSequenceCapture,
)
from sensorkit.ascom.commands import (
    ResetROI as CmdResetROI,
)
from sensorkit.ascom.commands import (
    SetGain as CmdSetGain,
)
from sensorkit.ascom.commands import (
    SetOffset as CmdSetOffset,
)
from sensorkit.ascom.commands import (
    SetReadoutMode as CmdSetReadoutMode,
)
from sensorkit.ascom.commands import (
    SetROI as CmdSetROI,
)


class FakeBinding:
    def __init__(self):
        self.published: list = []

    async def publish(self, model):
        self.published.append(model)

    async def data_graph(self):
        return None  # Force camera to drop data without failing


class FakeCamera:
    def __init__(self):
        # Minimal camera properties used by CameraService
        self.Connected = False
        self.CameraXSize = 4000
        self.CameraYSize = 3000
        # Binning limits
        self.MaxBinX = 4
        self.MaxBinY = 4
        self.PixelSizeX = 3.76
        self.PixelSizeY = 3.76
        self.SensorName = "IMX571"
        self.SensorType = "BayerRGGB"
        self.ElectronsPerADU = 0.7
        self.FullWellCapacity = 50000
        self.MaxADU = 65535
        self.BayerOffsetX = 0
        self.BayerOffsetY = 0
        self.ReadoutModes = ["normal", "high-speed"]
        self.Gains = [0, 1, 2]
        self.Offsets = [0, 50, 100]

        self.BinX = 1
        self.BinY = 1
        self.StartX = 0
        self.StartY = 0
        self.NumX = self.CameraXSize
        self.NumY = self.CameraYSize
        self.ReadoutMode = 0
        self.Gain = 0
        self.Offset = 0
        self.FrameType = "Light"
        self.CoolerOn = False
        self.SetCCDTemperature = lambda t: None
        self.CCDTemperature = -10.0
        self.HeatSinkTemperature = 5.0
        self.CoolerPower = 20.0
        self.LastExposureDuration = 0.0
        self.LastExposureStartTime = datetime.now(UTC)
        self.PercentCompleted = 100

        # Capture plumbing
        self._image_ready = False
        self.ImageReady = False
        self.ImageArray = [array.array("H", [0, 1, 2, 3]) for _ in range(2)]

        # Capabilities (some drivers map to properties, here we expose constants)
        self.CanAsymmetricBin = True
        self.CanAbortExposure = True
        self.CanStopExposure = True
        self.CanGetCoolerPower = True
        self.CanSetCCDTemperature = True
        self.CanFastReadout = True
        self.CanPulseGuide = False
        self.CanSetGain = True
        self.CanSetOffset = True
        self.CanSetReadoutMode = True
        self.HasShutter = True

        self.ExposureMin = 0.001
        self.ExposureMax = 600.0
        self.ExposureResolution = 0.001
        self.GainMin = 0
        self.GainMax = 100
        self.GainStep = 1
        self.OffsetMin = 0
        self.OffsetMax = 100
        self.OffsetStep = 10

    def StartExposure(self, exposure_s, light):
        # simulate an exposure finishing immediately
        self.LastExposureDuration = exposure_s
        self.ImageReady = True

    def AbortExposure(self):
        self.ImageReady = False

    def StopExposure(self):
        self.ImageReady = False


@pytest.mark.asyncio
async def test_camera_startup_and_connected_publish():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()
    assert cam.Connected is True
    kinds = {type(m).__name__ for m in svc.device.binding.published}
    assert "Connected" in kinds
    assert isinstance(svc.capabilities, CameraCapabilities)
    assert isinstance(svc.attributes, CameraAttributes)
    assert isinstance(svc.limitations, CameraLimitations)


@pytest.mark.asyncio
async def test_camera_set_binning_and_roi_reset():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.startup()

    # ensure attributes/limitations are populated
    svc.attributes = CameraAttributes.from_device(cam)
    svc.limitations = CameraLimitations.from_device(cam)
    svc.capabilities = CameraCapabilities.from_device(cam)

    await svc.camera_set_binning(sk.SetBinning(x=2, y=2))
    assert cam.BinX == 2 and cam.BinY == 2
    # ROI reset to full-frame in binned pixels
    assert cam.StartX == 0 and cam.StartY == 0
    assert cam.NumX == cam.CameraXSize // 2
    assert cam.NumY == cam.CameraYSize // 2


@pytest.mark.asyncio
async def test_camera_set_and_reset_roi():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    await svc.startup()
    svc.attributes = CameraAttributes.from_device(cam)

    await svc.camera_set_roi(CmdSetROI(start_x=10, start_y=20, num_x=100, num_y=200))
    assert (cam.StartX, cam.StartY, cam.NumX, cam.NumY) == (10, 20, 100, 200)

    await svc.camera_reset_roi(CmdResetROI())
    assert (cam.StartX, cam.StartY) == (0, 0)
    assert cam.NumX == cam.CameraXSize // cam.BinX
    assert cam.NumY == cam.CameraYSize // cam.BinY


@pytest.mark.asyncio
async def test_camera_gain_offset_clamping_and_steps():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    await svc.startup()
    svc.limitations = CameraLimitations.from_device(cam)
    svc.capabilities = CameraCapabilities.from_device(cam)

    await svc.camera_set_gain(CmdSetGain(value=101))  # clamp to max 100
    assert cam.Gain == 100
    await svc.camera_set_offset(CmdSetOffset(value=17))  # step to nearest 10
    assert cam.Offset == 20


@pytest.mark.asyncio
async def test_camera_set_readout_mode_by_name_and_index():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    await svc.startup()
    svc.attributes = CameraAttributes.from_device(cam)
    svc.capabilities = CameraCapabilities.from_device(cam)

    await svc.camera_set_readout_mode(CmdSetReadoutMode(mode="high-speed"))
    assert cam.ReadoutMode == 1
    await svc.camera_set_readout_mode(CmdSetReadoutMode(mode=0))
    assert cam.ReadoutMode == 0


@pytest.mark.asyncio
async def test_camera_capture_calls_start_exposure_and_handles_no_graph():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    await svc.camera_capture(sk.CameraCapture(integration_time=0.01, context={}))
    # exposure finished and data produced; since no graph, just ensure the loop didn't error
    assert cam.ImageReady is True


@pytest.mark.asyncio
async def test_camera_abort_invokes_abort_or_stop():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    svc.capabilities = SimpleNamespace(abort_exposure=True, stop_exposure=False)

    await svc.camera_abort(sk.Abort())
    assert cam.ImageReady is False


@pytest.mark.asyncio
async def test_camera_set_temperature_enables_cooler_and_publishes():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()
    svc.capabilities = SimpleNamespace(set_ccd_temperature=True)

    await svc.camera_set_temperature(sk.SetTemperature(temperature=-15.0, units=sk.TemperatureUnit.CELSIUS))
    assert cam.CoolerOn is True
    kinds = {type(m).__name__ for m in svc.device.binding.published}
    assert "Temperature" in kinds


@pytest.mark.asyncio
async def test_camera_sequence_capture_invokes_camera_capture_multiple_times():
    cam = FakeCamera()
    svc = CameraService(camera=cam, status_frequency=1.0)
    svc.device.binding = FakeBinding()

    # Patch camera_capture to count calls
    calls = {"n": 0}

    async def fake_single(c):
        calls["n"] += 1

    svc.camera_capture = fake_single  # type: ignore

    await svc.camera_sequence_capture(CameraSequenceCapture(integration_time=0.01, count=3, interval_s=0.0, context={}))
    assert calls["n"] == 3
