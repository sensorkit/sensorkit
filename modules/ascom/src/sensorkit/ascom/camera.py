from __future__ import annotations

import array
import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Self

import numpy as np
from alpaca.camera import Camera as AscomCamera
from alpaca.exceptions import NotConnectedException, NotImplementedException
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.ascom.commands import (
    CoolerOn as CmdCoolerOn,
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
from sensorkit.data.fits import ArrayInfo

if TYPE_CHECKING:
    from sensorkit.ascom.service import CameraConfig


_array_typecode_to_dtype = {
    "b": "int8",
    "B": "uint8",
    "h": "int16",
    "H": "uint16",
    "i": "int32",
    "I": "uint32",
    "l": "int64",
    "L": "uint64",
    "f": "float32",
    "d": "float64",
}

_dtype_to_bitpix = {
    "uint8": 8,
    "int8": 8,
    "int16": 16,
    "uint16": 16,
    "int32": 32,
    "uint32": 32,
    "int64": 64,
    "uint64": 64,
    "float32": -32,
    "float64": -64,
}

_dtype_to_bzero = {
    "uint8": 128,
    "uint16": 32768,
    "uint32": 2147483648,
    "uint64": 9223372036854775808,
}

# Safely read Alpaca device properties that may not be implemented by the driver.
def _safe_get(d: AscomCamera, name: str, default=None):
    try:
        return getattr(d, name)
    except Exception:
        return default


@sk.declare_keyword
class CameraCapabilities(BaseModel):
    assymetric_binning: bool | None = None
    abort_exposure: bool | None = None
    stop_exposure: bool | None = None
    retrieve_cooler_power: bool | None = None
    set_ccd_temperature: bool | None = None
    fast_readout: bool | None = None
    pulse_guide: bool | None = None
    set_gain: bool | None = None
    set_offset: bool | None = None
    set_readout_mode: bool | None = None
    has_shutter: bool | None = None
    subframe: bool | None = None

    @classmethod
    def from_device(cls, d: AscomCamera):
        return cls(
            assymetric_binning=_safe_get(d, "CanAsymmetricBin", None),
            abort_exposure=_safe_get(d, "CanAbortExposure", None),
            stop_exposure=_safe_get(d, "CanStopExposure", None),
            retrieve_cooler_power=_safe_get(d, "CanGetCoolerPower", None),
            set_ccd_temperature=_safe_get(d, "CanSetCCDTemperature", None),
            fast_readout=_safe_get(d, "CanFastReadout", None),
            pulse_guide=_safe_get(d, "CanPulseGuide", None),
            set_gain=_safe_get(d, "CanSetGain", None),
            set_offset=_safe_get(d, "CanSetOffset", None),
            set_readout_mode=_safe_get(d, "CanSetReadoutMode", None),
            has_shutter=_safe_get(d, "HasShutter", None),
            subframe=True if all(hasattr(d, n) for n in ("StartX", "StartY", "NumX", "NumY")) else None,
        )


@sk.declare_keyword
class CameraLimitations(BaseModel):
    max_binning_x: int = 1
    max_binning_y: int = 1
    min_exposure: float | None = None
    max_exposure: float | None = None
    exposure_resolution: float | None = None
    gain_min: float | int | None = None
    gain_max: float | int | None = None
    gain_step: float | int | None = None
    offset_min: float | int | None = None
    offset_max: float | int | None = None
    offset_step: float | int | None = None

    @classmethod
    def from_device(cls, d: AscomCamera):
        return cls(
            max_binning_x=_safe_get(d, "MaxBinX", 1),
            max_binning_y=_safe_get(d, "MaxBinY", 1),
            min_exposure=_safe_get(d, "ExposureMin", None),
            max_exposure=_safe_get(d, "ExposureMax", None),
            exposure_resolution=_safe_get(d, "ExposureResolution", None),
            gain_min=_safe_get(d, "GainMin", None),
            gain_max=_safe_get(d, "GainMax", None),
            gain_step=_safe_get(d, "GainStep", None),
            offset_min=_safe_get(d, "OffsetMin", None),
            offset_max=_safe_get(d, "OffsetMax", None),
            offset_step=_safe_get(d, "OffsetStep", None),
        )


@sk.declare_keyword
class CameraAttributes(BaseModel):
    size_x: int = 0
    size_y: int = 0
    pixel_size_x_microns: float | None = None
    pixel_size_y_microns: float | None = None
    sensor_name: str | None = None
    sensor_type: str | None = None
    electrons_per_adu: float | None = None
    full_well_capacity: float | None = None
    max_adu: int | None = None
    bayer_offset_x: int | None = None
    bayer_offset_y: int | None = None
    readout_modes: list[str] | None = None
    gains: list[str] | list[int] | list[float] | None = None
    offsets: list[str] | list[int] | list[float] | None = None

    @classmethod
    def from_device(cls, d: AscomCamera):
        sensor_type_val = _safe_get(d, "SensorType", None)
        return cls(
            size_x=_safe_get(d, "CameraXSize", 0),
            size_y=_safe_get(d, "CameraYSize", 0),
            pixel_size_x_microns=_safe_get(d, "PixelSizeX", None),
            pixel_size_y_microns=_safe_get(d, "PixelSizeY", None),
            sensor_name=_safe_get(d, "SensorName", None),
            sensor_type=str(sensor_type_val) if sensor_type_val is not None else None,
            electrons_per_adu=_safe_get(d, "ElectronsPerADU", None),
            full_well_capacity=_safe_get(d, "FullWellCapacity", None),
            max_adu=_safe_get(d, "MaxADU", None),
            bayer_offset_x=_safe_get(d, "BayerOffsetX", None),
            bayer_offset_y=_safe_get(d, "BayerOffsetY", None),
            readout_modes=list(_safe_get(d, "ReadoutModes", []) or []),
            gains=list(_safe_get(d, "Gains", []) or []),
            offsets=list(_safe_get(d, "Offsets", []) or []),
        )


@sk.declare_keyword
class CameraCurrentSettings(BaseModel):
    binning_x: int | None = None
    binning_y: int | None = None
    roi_start_x: int | None = None
    roi_start_y: int | None = None
    roi_num_x: int | None = None
    roi_num_y: int | None = None
    readout_mode: int | str | None = None
    gain: int | float | None = None
    offset: int | float | None = None
    frame_type: str | None = None
    cooler_on: bool | None = None
    ccd_temperature_setpoint: float | None = None

    @classmethod
    def from_device(cls, d: AscomCamera):
        frame_type_val = _safe_get(d, "FrameType", None)
        return cls(
            binning_x=_safe_get(d, "BinX", None),
            binning_y=_safe_get(d, "BinY", None),
            roi_start_x=_safe_get(d, "StartX", None),
            roi_start_y=_safe_get(d, "StartY", None),
            roi_num_x=_safe_get(d, "NumX", None),
            roi_num_y=_safe_get(d, "NumY", None),
            readout_mode=_safe_get(d, "ReadoutMode", None),
            gain=_safe_get(d, "Gain", None),
            offset=_safe_get(d, "Offset", None),
            frame_type=str(frame_type_val) if frame_type_val is not None else None,
            cooler_on=_safe_get(d, "CoolerOn", None),
            ccd_temperature_setpoint=_safe_get(d, "SetCCDTemperature", None),
        )


@sk.declare_keyword
class CameraTelemetry(BaseModel):
    ccd_temperature: float | None = None
    heatsink_temperature: float | None = None
    cooler_power_percent: float | None = None
    last_exposure_duration: float | None = None
    last_exposure_start_time: datetime | None = None
    percent_completed: float | None = None

    @classmethod
    def from_device(cls, d: AscomCamera):
        return cls(
            ccd_temperature=_safe_get(d, "CCDTemperature", None),
            heatsink_temperature=_safe_get(d, "HeatSinkTemperature", None),
            cooler_power_percent=_safe_get(d, "CoolerPower", None),
            last_exposure_duration=_safe_get(d, "LastExposureDuration", None),
            last_exposure_start_time=_safe_get(d, "LastExposureStartTime", None),
            percent_completed=_safe_get(d, "PercentCompleted", None),
        )

class CameraTelemetryPublisher:
    """Publishes camera telemetry and state on a background loop."""

    def __init__(
        self,
        *,
        binding: sk.DeviceImpl,
        camera: AscomCamera,
        frequency: float = 1.0
    ):
        self.binding = binding
        self.camera = camera
        self.frequency = frequency
        self._task: asyncio.Task | None = None
        self.capabilities: CameraCapabilities | None = None
        self.limitations: CameraLimitations | None = None
        self.attributes: CameraAttributes | None = None

    async def start(self):
        """Start the telemetry publisher background task."""
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        """Stop the telemetry publisher background task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        """Background loop publishing telemetry at the configured frequency."""
        backoff = 1.0
        while True:
            try:
                await asyncio.gather(
                    self.publish_connected(),
                    self.publish_sensor_size(),
                    self.publish_binning(),
                    self.publish_temperature(),
                    self.publish_attributes(),
                    self.publish_capabilities(),
                    self.publish_limitations(),
                    self.publish_current_settings(),
                    self.publish_telemetry(),
                )
                backoff = 1.0
                await asyncio.sleep(self.frequency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Camera telemetry publish failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 2)

    async def publish_connected(self):
        try:
            is_connected = await asyncio.to_thread(lambda: self.camera.Connected)
            await self.binding.publish(
                sk.Connected(is_connected=is_connected)
            )
        except Exception:
            return None

    async def publish_sensor_size(self):
        try:
            x = await asyncio.to_thread(lambda: self.camera.CameraXSize)
            y = await asyncio.to_thread(lambda: self.camera.CameraYSize)
            await self.binding.publish(
                sk.CameraSensorSize(x=x, y=y)
            )
        except Exception:
            return None

    async def publish_binning(self):
        try:
            bin_x = await asyncio.to_thread(lambda: self.camera.BinX)
            bin_y = await asyncio.to_thread(lambda: self.camera.BinY)
            await self.binding.publish(
                sk.Binning(x=bin_x, y=bin_y)
            )
        except Exception:
            return None

    async def publish_temperature(self):
        try:
            temp_c = await asyncio.to_thread(getattr, self.camera, "CCDTemperature", None)
            if temp_c is None:
                return None
            await self.binding.publish(
                sk.Temperature(temperature=float(temp_c), units=sk.TemperatureUnit.CELSIUS)
            )
        except Exception:
            return None

    async def publish_attributes(self):
        try:
            if self.attributes:
                await self.binding.publish(self.attributes)
        except Exception:
            return None

    async def publish_capabilities(self):
        try:
            if self.capabilities:
                await self.binding.publish(self.capabilities)
        except Exception:
            return None

    async def publish_limitations(self):
        try:
            if self.limitations:
                await self.binding.publish(self.limitations)
        except Exception:
            return None

    async def publish_current_settings(self):
        try:
            settings = await asyncio.to_thread(CameraCurrentSettings.from_device, self.camera)
            await self.binding.publish(settings)
        except Exception:
            return None

    async def publish_telemetry(self):
        try:
            telemetry = await asyncio.to_thread(CameraTelemetry.from_device, self.camera)
            await self.binding.publish(telemetry)
        except Exception:
            return None


@sk.declare_device
class CameraService:

    def __init__(
        self,
        camera: AscomCamera,
        config: CameraConfig,
    ):
        self.camera = camera
        self.config = config
        self.telemetry_publisher: CameraTelemetryPublisher | None = None
        self._data_tasks: set[asyncio.Task] = set()

    @property
    def capabilities(self) -> CameraCapabilities | None:
        return self.telemetry_publisher.capabilities if self.telemetry_publisher else None

    @property
    def limitations(self) -> CameraLimitations | None:
        return self.telemetry_publisher.limitations if self.telemetry_publisher else None

    @property
    def attributes(self) -> CameraAttributes | None:
        return self.telemetry_publisher.attributes if self.telemetry_publisher else None

    @classmethod
    async def create(
        cls,
        device_url: str = "localhost:32323",
        config: CameraConfig = None,
    ) -> Self:
        if config is None:
            from sensorkit.ascom.service import CameraConfig
            config = CameraConfig(entity="camera")
        return cls(
            camera=AscomCamera(address=device_url, device_number=config.device_number),
            config=config,
        )

    def _apply_config(self):
        """Apply configuration settings to camera. Called after connection."""
        # Turn on cooler if configured
        if self.config.cooler_on:
            try:
                if hasattr(self.camera, "CoolerOn"):
                    self.camera.CoolerOn = True
                    logger.info("Cooler enabled")
            except Exception:
                logger.debug("CoolerOn toggle not supported or failed")

        # Set CCD temperature setpoint if configured
        if self.config.ccd_temperature_setpoint is not None:
            try:
                self.camera.SetCCDTemperature = self.config.ccd_temperature_setpoint
                logger.info(f"CCD temperature setpoint set to {self.config.ccd_temperature_setpoint}°C")
            except Exception:
                logger.debug("SetCCDTemperature not supported or failed")

        # Set readout mode if configured
        if self.config.readout_mode is not None:
            self._apply_readout_mode(self.config.readout_mode)

    def _apply_readout_mode(self, mode_val: int | str):
        """Apply readout mode setting, supporting both index and name."""
        try:
            if isinstance(mode_val, str):
                modes = _safe_get(self.camera, "ReadoutModes", []) or []
                try:
                    idx = list(modes).index(mode_val)
                    self.camera.ReadoutMode = idx
                    logger.info(f"Readout mode set to '{mode_val}' (index {idx})")
                except ValueError:
                    logger.warning(f"Readout mode '{mode_val}' not found in {list(modes)}")
            else:
                self.camera.ReadoutMode = int(mode_val)
                logger.info(f"Readout mode set to {mode_val}")
        except Exception:
            logger.debug("ReadoutMode not supported or failed")

    def _set_connected(self, connected: bool):
        self.camera.Connected = connected

    @sk.on_attach
    async def startup(self):
        await asyncio.to_thread(self._set_connected, True)
        await sk.device().publish(sk.Connected(is_connected=True))

        self._apply_config()

        try:
            # Initialize telemetry publisher and populate initial state
            self.telemetry_publisher = CameraTelemetryPublisher(
                binding=sk.device(),
                camera=self.camera,
                frequency=self.config.status_frequency
            )

            # Be robust to drivers with unimplemented properties
            self.telemetry_publisher.capabilities = CameraCapabilities.from_device(self.camera)
            self.telemetry_publisher.attributes = CameraAttributes.from_device(self.camera)
            self.telemetry_publisher.limitations = CameraLimitations.from_device(self.camera)

            # Start background telemetry publishing
            await self.telemetry_publisher.start()

        except NotConnectedException as e:
            raise RuntimeError("Not connected") from e
        except NotImplementedException as e:
            # Don't fail startup if some properties are unimplemented
            logger.warning(f"Some device properties are not implemented: {e}")
        except Exception as e:
            # Log and continue; background publishers will refresh what they can
            logger.warning(f"Startup property read error: {e}")

    @sk.on_detach
    async def shutdown(self):
        if self.telemetry_publisher:
            await self.telemetry_publisher.stop()

        # Turn off cooler before disconnecting (if it was enabled)
        if self.config.cooler_on:
            try:
                if hasattr(self.camera, "CoolerOn"):
                    self.camera.CoolerOn = False
                    logger.info("Cooler disabled")
            except Exception:
                logger.debug("CoolerOn toggle not supported or failed during shutdown")

        await asyncio.to_thread(self._set_connected, False)
        await sk.device().publish(sk.Connected(is_connected=False))

    @sk.command_handler
    async def camera_connect(self, cmd: sk.Connect):
        try:
            await asyncio.to_thread(self._set_connected, True)
            await sk.device().publish(sk.Connected(is_connected=True))
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Connect failed")
            raise RuntimeError("Connect failed") from e

    @sk.command_handler
    async def camera_disconnect(self, cmd: sk.Disconnect):
        try:
            await asyncio.to_thread(self._set_connected, False)
            await sk.device().publish(sk.Connected(is_connected=False))
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Disconnect failed")
            raise RuntimeError("Disconnect failed") from e

    @sk.command_handler
    async def camera_set_binning(self, cmd: sk.SetBinning):
        try:
            # Validate inputs
            if cmd.x is None or cmd.y is None or cmd.x <= 0 or cmd.y <= 0:
                raise RuntimeError("Binning must be positive integers")

            # Capability and limits checks (guard for None)
            asym_ok = bool(getattr(self.capabilities, "assymetric_binning", False)) if self.capabilities else False
            if cmd.x != cmd.y and not asym_ok:
                raise RuntimeError("Asymmetric binning requested but not supported by device")

            max_x = getattr(self.limitations, "max_binning_x", 1) if self.limitations else 1
            max_y = getattr(self.limitations, "max_binning_y", 1) if self.limitations else 1
            if cmd.x > max_x or cmd.y > max_y:
                raise RuntimeError(f"Invalid binning for device. Max={max_x}x{max_y}")

            # Apply binning
            try:
                self.camera.BinX = int(cmd.x)
                self.camera.BinY = int(cmd.y)

                # Reset/clamp ROI to full-frame under new binning
                size_x = getattr(self.attributes, "size_x", 0) if self.attributes else 0
                size_y = getattr(self.attributes, "size_y", 0) if self.attributes else 0
                num_x = max(1, size_x // cmd.x) if size_x else getattr(self.camera, "NumX", 0)
                num_y = max(1, size_y // cmd.y) if size_y else getattr(self.camera, "NumY", 0)

                self.camera.StartX = 0
                self.camera.StartY = 0
                self.camera.NumX = num_x
                self.camera.NumY = num_y
                logger.info(f"Binning applied: {cmd.x}x{cmd.y}; ROI reset to {num_x}x{num_y}")
            except NotImplementedException as e:
                logger.warning("Device does not support binning")
                raise e
            except Exception as e:
                logger.exception("SetBinning apply failed")
                raise RuntimeError("SetBinning failed") from e
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("SetBinning failed")
            raise RuntimeError("SetBinning failed") from e

    def _do_capture(
        self,
        exposure_s: float,
        light: bool,
        slow_poll_period: float = 1.0,
        fast_poll_period: float = 0.2,
        fast_poll_switch_delta: float = 3.0,
        timeout: float | None = None,
    ) -> list[array.array]:
        """Run the exposure and download the resulting image data."""
        if timeout is None:
            timeout = exposure_s + 10.0

        start = datetime.now()
        self.camera.StartExposure(exposure_s, light)

        # Detect the end of the exposure. We start by polling slowly, only to check for errors
        # raised by the alpaca API. Once the exposure should be nearing completion, we switch to
        # fast polling to minimize our reaction latency.

        now = datetime.now()
        start += (now - start) / 2
        slow_poll_until = start + timedelta(seconds=min(exposure_s - fast_poll_switch_delta, timeout))
        fast_poll_until = start + timedelta(seconds=timeout)

        while now < slow_poll_until:
            if self.camera.ImageReady:
                # This should not happen!
                logger.warning(
                    f"Requested {exposure_s:.1f} sec exposure, "
                    f"but ASCOM camera returned ImageReady in {now - start:.1f} sec!"
                )
                break

            time.sleep(slow_poll_period)
            now = datetime.now()

        while now < fast_poll_until:
            if self.camera.ImageReady:
                break

            time.sleep(fast_poll_period)
            now = datetime.now()
        else:
            raise RuntimeError("exposure timed out")

        # Download and return the raw data.
        return self.camera.ImageArray

    def _image_to_bytes(self, data: list[array.array], dtype: str):
        # Stack columns into (width, height) array, then transpose to (height, width).
        return np.column_stack(data).astype(dtype, copy=False).tobytes()

    async def _handle_data(self, data: list[array.array], context: sk.Context):
        try:
            # Set image information in the data context. Note ASCOM image data is returned in
            # column-major format.

            dtype = _array_typecode_to_dtype.get(data[0].typecode)
            width = len(data)
            height = len(data[1])
            logger.info(f"Got image with {width}x{height} pixels, dtype {dtype}")

            context.set(
                ArrayInfo(
                    shape=(height, width),
                    dtype=dtype,
                )
            )

            # Set FITS pixel format keywords
            context["bits_per_pixel"] = _dtype_to_bitpix.get(dtype, 16)
            context["zero_value"] = _dtype_to_bzero.get(dtype, 0)

            # Convert the image data to bytes and pass it along to our DataGraph. Presently we
            # transpose the image data to row-major format, however this may be a step better left
            # to the DataGraph itself.

            image_bytes = await asyncio.to_thread(self._image_to_bytes, data, dtype)

            if graph := await sk.device().data_graph():
                source = graph.app_source()
                writer = source.produce(context)
                writer.write(image_bytes)
                logger.debug(f"wrote {len(image_bytes)} bytes to DataGraph")
                await writer.drain()

                writer.close()
                await writer.wait_closed()
            else:
                logger.warning("discarding data since no DataGraph is defined!")
        except Exception:
            logger.exception("Failed to publish image to DataGraph")

    @sk.command_handler
    async def camera_capture(self, cmd: sk.CameraCapture):
        # Enforce exposure constraints and compute timeout
        exp = float(cmd.integration_time)
        if self.limitations is not None:
            if self.limitations.min_exposure is not None:
                exp = max(exp, float(self.limitations.min_exposure))
            if self.limitations.max_exposure is not None:
                exp = min(exp, float(self.limitations.max_exposure))
            if self.limitations.exposure_resolution is not None and self.limitations.exposure_resolution > 0:
                step = float(self.limitations.exposure_resolution)
                # Round to nearest multiple of resolution
                steps = round(exp / step)
                exp = max(step, steps * step)

        if self.config.timeout is not None:
            timeout_s = self.config.timeout
        else:
            timeout_s = max(10.0, exp * 2.0)

        logger.info(f"Requesting {exp:.2f}s capture from ASCOM camera (timeout {timeout_s:.1f}s)")

        try:
            exposure_start_fallback = datetime.now(UTC)
            data: list[array.array] = await asyncio.wait_for(
                asyncio.to_thread(self._do_capture, exp, True, timeout=timeout_s), timeout=timeout_s
            )
        except asyncio.TimeoutError:
            logger.error("Exposure timed out; attempting to abort exposure")
            try:
                if getattr(self.capabilities, "abort_exposure", False):
                    self.camera.AbortExposure()
                elif getattr(self.capabilities, "stop_exposure", False):
                    self.camera.StopExposure()
            except Exception as e:
                logger.warning(f"Abort/StopExposure failed: {e}")
            return
        except asyncio.CancelledError:
            logger.warning("Capture cancelled; attempting to abort exposure")
            try:
                if getattr(self.capabilities, "abort_exposure", False):
                    self.camera.AbortExposure()
                elif getattr(self.capabilities, "stop_exposure", False):
                    self.camera.StopExposure()
            except Exception as e:
                logger.warning(f"Abort/StopExposure failed: {e}")
            return

        if not data or not data[0]:
            logger.error("No data returned from ASCOM camera!")
            return

        # Set capture context info.
        context = cmd.context

        if not context.get("file_name", None):
            context["file_name"] = f"{str(uuid.uuid1())}.fits"

        exposure_start: datetime | None = _safe_get(self.camera, "LastExposureStartTime", None)
        if exposure_start:
            exposure_start = datetime.fromisoformat(exposure_start).replace(tzinfo=UTC)
        else:
            exposure_start = exposure_start_fallback

        context["capture_time"] = str(exposure_start)

        context["instrument_name"] = _safe_get(self.camera, "Name", str(sk.device().entity))
        context["sensor_name"] = _safe_get(self.camera, "SensorName")
        context["etime"] = _safe_get(self.camera, "LastExposureDuration", cmd.integration_time)
        context["binning_x"] = _safe_get(self.camera, "BinX", 1)
        context["binning_y"] = _safe_get(self.camera, "BinY", 1)

        # Handle propagation of the new image data in a background task.
        task = asyncio.create_task(self._handle_data(data, context))
        self._data_tasks.add(task)
        task.add_done_callback(self._data_tasks.discard)

    @sk.command_handler
    async def camera_abort(self, cmd: sk.Abort):
        """Abort or stop an in-flight exposure if supported by the device."""
        try:
            if getattr(self.capabilities, "abort_exposure", False):
                self.camera.AbortExposure()
                logger.info("AbortExposure invoked")
            elif getattr(self.capabilities, "stop_exposure", False):
                self.camera.StopExposure()
                logger.info("StopExposure invoked")
            else:
                logger.warning("Device does not support abort/stop exposure")
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("Abort failed")
            raise RuntimeError("Abort failed") from e

    @sk.command_handler
    async def camera_set_temperature(self, cmd: sk.SetTemperature):
        """Set CCD temperature setpoint and ensure cooler is enabled if supported."""
        try:
            if not getattr(self.capabilities, "set_ccd_temperature", False):
                raise RuntimeError("Device does not support setting CCD temperature")

            # Turn on cooler when possible
            try:
                if hasattr(self.camera, "CoolerOn"):
                    self.camera.CoolerOn = True
            except Exception:
                # Non-fatal if the driver doesn't allow toggling
                logger.debug("CoolerOn toggle not supported or failed")

            self.camera.SetCCDTemperature(cmd.temperature)
            await sk.device().publish(
                sk.Temperature(temperature=cmd.temperature, units=cmd.units)
            )
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("SetTemperature failed")
            raise RuntimeError("SetTemperature failed") from e

    # ----------------------- Local command handlers -----------------------
    @sk.command_handler
    async def camera_set_roi(self, cmd: CmdSetROI):
        try:
            # Compute max ROI in binned pixels
            bin_x = getattr(self.camera, "BinX", 1) or 1
            bin_y = getattr(self.camera, "BinY", 1) or 1
            max_x = (self.attributes.size_x // bin_x) if self.attributes else 0
            max_y = (self.attributes.size_y // bin_y) if self.attributes else 0

            if cmd.num_x <= 0 or cmd.num_y <= 0:
                raise RuntimeError("ROI dimensions must be positive")
            if cmd.start_x < 0 or cmd.start_y < 0:
                raise RuntimeError("ROI start must be non-negative")
            if cmd.start_x + cmd.num_x > max_x or cmd.start_y + cmd.num_y > max_y:
                raise RuntimeError("ROI exceeds sensor bounds for current binning")

            self.camera.StartX = cmd.start_x
            self.camera.StartY = cmd.start_y
            self.camera.NumX = cmd.num_x
            self.camera.NumY = cmd.num_y
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("SetROI failed")
            raise RuntimeError("SetROI failed") from e

    @sk.command_handler
    async def camera_reset_roi(self, cmd: CmdResetROI):
        try:
            bin_x = getattr(self.camera, "BinX", 1) or 1
            bin_y = getattr(self.camera, "BinY", 1) or 1
            max_x = (self.attributes.size_x // bin_x) if self.attributes else 0
            max_y = (self.attributes.size_y // bin_y) if self.attributes else 0
            self.camera.StartX = 0
            self.camera.StartY = 0
            self.camera.NumX = max_x
            self.camera.NumY = max_y
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except Exception as e:
            logger.exception("ResetROI failed")
            raise RuntimeError("ResetROI failed") from e

    @sk.command_handler
    async def camera_set_gain(self, cmd: CmdSetGain):
        try:
            if not getattr(self.capabilities, "set_gain", False):
                raise RuntimeError("Device does not support setting gain")
            val = cmd.value
            if self.limitations:
                gmin = self.limitations.gain_min
                gmax = self.limitations.gain_max
                gstep = self.limitations.gain_step or 0
                if gmin is not None:
                    val = max(val, gmin)
                if gmax is not None:
                    val = min(val, gmax)
                if gstep and gstep > 0:
                    steps = round((val - (gmin or 0)) / gstep)
                    val = (gmin or 0) + steps * gstep
            self.camera.Gain = int(val) if isinstance(val, bool) or val == int(val) else val
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("SetGain failed")
            raise RuntimeError("SetGain failed") from e

    @sk.command_handler
    async def camera_set_offset(self, cmd: CmdSetOffset):
        try:
            if not getattr(self.capabilities, "set_offset", False):
                raise RuntimeError("Device does not support setting offset")
            val = cmd.value
            if self.limitations:
                omin = self.limitations.offset_min
                omax = self.limitations.offset_max
                ostep = self.limitations.offset_step or 0
                if omin is not None:
                    val = max(val, omin)
                if omax is not None:
                    val = min(val, omax)
                if ostep and ostep > 0:
                    steps = round((val - (omin or 0)) / ostep)
                    val = (omin or 0) + steps * ostep
            self.camera.Offset = int(val) if isinstance(val, bool) or val == int(val) else val
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("SetOffset failed")
            raise RuntimeError("SetOffset failed") from e

    @sk.command_handler
    async def camera_set_readout_mode(self, cmd: CmdSetReadoutMode):
        try:
            if not getattr(self.capabilities, "set_readout_mode", False):
                raise RuntimeError("Device does not support setting readout mode")
            mode_val = cmd.mode
            if isinstance(mode_val, str):
                # find index in attributes.readout_modes
                modes = (self.attributes.readout_modes if self.attributes else None) or []
                try:
                    idx = modes.index(mode_val)
                except ValueError:
                    raise RuntimeError("Unknown readout mode name") from None
                self.camera.ReadoutMode = idx
            else:
                self.camera.ReadoutMode = int(mode_val)
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("SetReadoutMode failed")
            raise RuntimeError("SetReadoutMode failed") from e

    @sk.command_handler
    async def camera_cooler_on(self, cmd: CmdCoolerOn):
        try:
            if not hasattr(self.camera, "CoolerOn"):
                raise RuntimeError("Device does not support cooler control")
            self.camera.CoolerOn = bool(cmd.on)
        except NotConnectedException as e:
            raise RuntimeError("Device disconnected") from e
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("CoolerOn failed")
            raise RuntimeError("CoolerOn failed") from e
