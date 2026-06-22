from __future__ import annotations

import array
import asyncio
import time
import uuid
from datetime import UTC, datetime
from typing import Literal, override

import numpy as np
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from alpaca.camera import Camera
from sensorkit.alpaca.device import (
    AlpacaDevice,
    AlpacaDeviceConfig,
    AlpacaDeviceState,
)
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.data.fits import ArrayInfo
from sensorkit.models.devices import Deinit, Init, Stop
from sensorkit.std import (
    Binning,
    CameraCapture,
    CameraSensorSize,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
    Connect,
    Connected,
    Disconnect,
    TemperatureUnit,
)

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
    "uint16": 32768,
    "uint32": 2147483648,
}

_CAMERA_STATES = {
    0: "Idle",
    1: "Waiting",
    2: "Exposing",
    3: "Reading",
    4: "Download",
    5: "Error",
}
_SENSOR_TYPES = {
    0: "Monochrome",
    1: "Color",
    2: "RGGB",
    3: "CMYG",
    4: "CMYG2",
    5: "LRGB",
}


@sk.declare_keyword
class AlpacaCameraStatus(BaseModel):
    """ICameraV4 properties."""

    # State
    camera_state: str = "Idle"
    is_pulse_guiding: bool = False
    percent_completed: int | None = None

    # Sensor
    sensor_name: str | None = None
    sensor_type: str | None = None
    camera_x_size: int | None = None
    camera_y_size: int | None = None
    pixel_size_x: float | None = None
    pixel_size_y: float | None = None
    max_adu: int | None = None
    electrons_per_adu: float | None = None
    full_well_capacity: float | None = None
    bayer_offset_x: int | None = None
    bayer_offset_y: int | None = None
    has_shutter: bool = False

    # Binning
    bin_x: int = 1
    bin_y: int = 1
    max_bin_x: int = 1
    max_bin_y: int = 1
    can_asymmetric_bin: bool = False

    # Subframe
    start_x: int = 0
    start_y: int = 0
    num_x: int | None = None
    num_y: int | None = None

    # Exposure
    min_exposure: float | None = None
    max_exposure: float | None = None
    exposure_resolution: float | None = None
    last_exposure_duration: float | None = None
    last_exposure_start_time: str | None = None

    # Cooler
    cooler_on: bool = False
    ccd_temperature: float | None = None
    set_ccd_temperature: float | None = None
    heat_sink_temperature: float | None = None
    cooler_power: float | None = None
    can_set_ccd_temperature: bool = False
    can_get_cooler_power: bool = False

    # Readout
    readout_mode: int | None = None
    readout_modes: list[str] | None = None

    # Gain / Offset
    gain: int | None = None
    gain_min: int | None = None
    gain_max: int | None = None
    gains: list[str] | None = None
    offset: int | None = None
    offset_min: int | None = None
    offset_max: int | None = None
    offsets: list[str] | None = None

    # Capabilities
    can_abort_exposure: bool = False
    can_stop_exposure: bool = False
    can_pulse_guide: bool = False


@sk.declare_device
class AlpacaCamera(AlpacaDevice):
    """Alpaca Camera implementation."""

    config: AlpacaCameraConfig
    device_name = "Camera"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(AlpacaCameraState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = AlpacaCameraState()

        # Initialize the camera
        await self.camera_init(Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.camera_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def camera_init(self, cmd: Init):
        # Connect to the hardware
        self._reconnect = lambda: self.camera_connect(Connect())
        self.camera = Camera(self.address, self.config.device_number, self.config.protocol)
        await self.camera_connect(Connect())

        self._data_tasks: set[asyncio.Task] = set()
        self._camera_x_size: int = 0
        self._camera_y_size: int = 0

        c = self.camera

        # Read capabilities
        self._can_abort_exposure = await self.get(c, "CanAbortExposure", False)
        self._can_stop_exposure = await self.get(c, "CanStopExposure", False)
        self._can_asymmetric_bin = await self.get(c, "CanAsymmetricBin", False)
        self._can_set_ccd_temperature = await self.get(c, "CanSetCCDTemperature", False)
        self._can_get_cooler_power = await self.get(c, "CanGetCoolerPower", False)
        self._can_pulse_guide = await self.get(c, "CanPulseGuide", False)
        self._has_shutter = await self.get(c, "HasShutter", False)

        # Read static sensor properties
        self._camera_x_size = await self.get(c, "CameraXSize", 0)
        self._camera_y_size = await self.get(c, "CameraYSize", 0)
        self._max_bin_x = await self.get(c, "MaxBinX", 1)
        self._max_bin_y = await self.get(c, "MaxBinY", 1)
        self._sensor_name = await self.get(c, "SensorName", None)
        self._sensor_type = _SENSOR_TYPES.get(await self.get(c, "SensorType", 0), "Unknown")
        self._pixel_size_x = await self.get(c, "PixelSizeX", None)
        self._pixel_size_y = await self.get(c, "PixelSizeY", None)
        self._max_adu = await self.get(c, "MaxADU", None)
        self._electrons_per_adu = await self.get(c, "ElectronsPerADU", None)
        self._full_well_capacity = await self.get(c, "FullWellCapacity", None)
        self._bayer_offset_x = await self.get(c, "BayerOffsetX", None)
        self._bayer_offset_y = await self.get(c, "BayerOffsetY", None)

        # Read exposure limits
        self._min_exposure = await self.get(c, "ExposureMin", None)
        self._max_exposure = await self.get(c, "ExposureMax", None)
        self._exposure_resolution = await self.get(c, "ExposureResolution", None)

        # Read readout/gain/offset metadata
        self._gains = await self.get(c, "Gains", None)
        self._gain_min = await self.get(c, "GainMin", None)
        self._gain_max = await self.get(c, "GainMax", None)
        self._offsets = await self.get(c, "Offsets", None)
        self._offset_min = await self.get(c, "OffsetMin", None)
        self._offset_max = await self.get(c, "OffsetMax", None)
        self._readout_modes = await self.get(c, "ReadoutModes", None)

        # Set the camera temperature
        await self.camera_set_temperature(
            ConfigureCameraCooler(
                enable=True,
                setpoint=CameraSensorTemperature(
                    temperature=self.config.temperature, units=TemperatureUnit.CELSIUS
                ),
            )
        )

    @sk.command_handler
    async def camera_deinit(self, cmd: Deinit):
        await self.camera_abort(sk.Abort())
        if self._data_tasks:
            await asyncio.gather(*self._data_tasks, return_exceptions=True)

    @sk.command_handler
    async def camera_connect(self, cmd: Connect):
        await self.connect(self.camera, timeout=self.config.timeout)
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def camera_disconnect(self, cmd: Disconnect):
        await self.disconnect(self.camera)
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def camera_stop(self, cmd: Stop):
        await self.camera_abort(sk.Abort())

    @sk.command_handler
    async def camera_abort(self, cmd: sk.Abort):
        await self.require_connected()
        logger.debug("aborting exposure")
        await self._try_abort()
        logger.debug("aborted exposure")

    async def _try_abort(self):
        try:
            if self._can_abort_exposure:
                await asyncio.to_thread(self.camera.AbortExposure)
                logger.debug("aborted exposure")
            elif self._can_stop_exposure:
                await asyncio.to_thread(self.camera.StopExposure)
                logger.debug("stopped exposure")
            else:
                logger.warning("Cannot abort/stop exposure")
        except Exception as e:
            logger.warning(f"Abort/stop exposure failed: {e}")

    @sk.command_handler
    async def camera_set_temperature(self, cmd: ConfigureCameraCooler):
        await self.require_connected()
        logger.debug(f"setting camera temperature to {cmd.setpoint.temperature} °C")

        if not self._can_set_ccd_temperature:
            logger.warning("Cannot set CCD temperature")
            return

        target = cmd.setpoint.temperature

        try:
            await self.put(self.camera, "CoolerOn", cmd.enable)
        except Exception:
            logger.warning("Cannot set CoolerOn but can set SetCCDTemperature")

        await self.put(self.camera, "SetCCDTemperature", target)

        set_ccd_temperature = await self.get(self.camera, "SetCCDTemperature", None)
        if set_ccd_temperature != target:
            logger.warning(f"Requested camera temperature of {target} C, got {set_ccd_temperature} C")
        else:
            logger.debug(f"set camera temperature to {target} C")

    @sk.command_handler
    async def camera_set_binning(self, cmd: ConfigureCameraSensor):
        await self.require_connected()

        if cmd.binning is None:
            return
        bin_x, bin_y = int(cmd.binning.x), int(cmd.binning.y)
        logger.debug(f"setting camera binning to {bin_x}x{bin_y}")

        if not self._can_asymmetric_bin and bin_x != bin_y:
            logger.warning("Cannot asymmetric bin, using BinX")
            bin_y = bin_x

        bin_x = min(bin_x, self._max_bin_x)
        bin_y = min(bin_y, self._max_bin_y)

        await self.put(self.camera, "BinX", bin_x)
        await self.put(self.camera, "BinY", bin_y)

        actual_x = await self.get(self.camera, "BinX", None)
        actual_y = await self.get(self.camera, "BinY", None)
        if actual_x != bin_x or actual_y != bin_y:
            raise RuntimeError(
                f"Requested camera binning of ({bin_x}, {bin_y}), "
                f"got ({actual_x}, {actual_y})"
            )

        # Reset subframe to full frame under new binning
        await self.put(self.camera, "StartX", 0)
        await self.put(self.camera, "StartY", 0)
        await self.put(self.camera, "NumX", self._camera_x_size // bin_x)
        await self.put(self.camera, "NumY", self._camera_y_size // bin_y)

        logger.debug(f"set camera binning to ({bin_x}, {bin_y})")

    @sk.command_handler
    async def camera_capture(self, cmd: CameraCapture):
        await self.require_connected()
        logger.info(f"Requesting {cmd.integration_time:.1f} sec capture from camera")
        logger.debug("starting camera capture")

        exposure_seconds = float(cmd.integration_time)

        # Enforce exposure constraints
        if self._min_exposure is not None and exposure_seconds < self._min_exposure:
            logger.warning(
                f"Requested exposure time ({exposure_seconds:.3f} s) is too short; attempting abort"
            )
            await self._try_abort()
            return
        if self._max_exposure is not None and exposure_seconds > self._max_exposure:
            logger.warning(
                f"Requested exposure time ({exposure_seconds:.3f} s) is too long; attempting abort"
            )
            await self._try_abort()
            return
        if self._exposure_resolution is not None and self._exposure_resolution > 0:
            step = self._exposure_resolution
            exposure_seconds = max(step, round(exposure_seconds / step) * step)

        timeout_seconds = max(self.config.timeout, exposure_seconds * 3.0)

        # Start the exposure
        try:
            date_obs_fallback = datetime.now(UTC)
            data: array.array = await asyncio.wait_for(
                asyncio.to_thread(self._do_capture, exposure_seconds, True, timeout_seconds),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.error("Exposure timed out; attempting abort")
            await self._try_abort()
            raise
        except asyncio.CancelledError:
            logger.warning("Capture cancelled; attempting abort")
            await self._try_abort()
            raise

        if not data:
            logger.error("No data returned from camera!")
            raise RuntimeError("Camera returned no data")

        # Build data context
        # ImageArrayRaw returns a flat array; dimensions come from ImageArrayInfo.
        # Alpaca dimension convention: Dimension1 = columns (X), Dimension2 = rows (Y).
        info = self.camera.ImageArrayInfo
        context = cmd.context
        dtype = _array_typecode_to_dtype.get(data.typecode, "uint16")
        width = info.Dimension1
        height = info.Dimension2

        context.set(ArrayInfo(shape=(height, width), dtype=dtype))
        max_adu = await self.get(self.camera, "MaxADU", None)
        default_bitpix = (max_adu.bit_length() + 7) // 8 * 8 if max_adu else 16
        context["max_adu"] = max_adu
        context["bitpix"] = _dtype_to_bitpix.get(dtype, default_bitpix)
        context["bscale"] = 1
        context["bzero"] = _dtype_to_bzero.get(dtype, 0)

        last_exposure_start_time = await self.get(self.camera, "LastExposureStartTime", None)
        if last_exposure_start_time:
            context["date_obs"] = last_exposure_start_time
        else:
            context["date_obs"] = str(date_obs_fallback)

        last_exposure_duration = await self.get(self.camera, "LastExposureDuration", None)
        context["exptime"] = (
            last_exposure_duration if last_exposure_duration is not None else exposure_seconds
        )

        context["instrume"] = await self.get(self.camera, "SensorName", "")
        context["readoutm"] = await self.get(self.camera, "ReadoutMode", None)
        context["ccdtemp"] = await self.get(self.camera, "CCDTemperature", None)
        context["xbinning"] = await self.get(self.camera, "BinX", 1)
        context["ybinning"] = await self.get(self.camera, "BinY", 1)

        # GPS/timing metadata defaults (None = keyword omitted from FITS header)
        context["time_src"] = None
        context["date_end"] = None
        context["gps_seqn"] = None
        context["gps_lat"] = None
        context["gps_lon"] = None

        # Fetch GPS/timing metadata (non-standard extension; graceful if absent)
        try:
            gps_meta = await asyncio.to_thread(self.camera._get, "gpsmetadata")
            if isinstance(gps_meta, dict):
                context["time_src"] = gps_meta.get("TIME-SRC")
                context["date_end"] = gps_meta.get("DATE-END")
                if gps_meta.get("TIME-SRC") == "GPS":
                    context["gps_seqn"] = gps_meta.get("GPS-SEQN")
                    context["gps_lat"] = gps_meta.get("GPS-LAT")
                    context["gps_lon"] = gps_meta.get("GPS-LON")
        except Exception:
            pass  # Camera doesn't support this endpoint — defaults remain None

        if not context.get("file_name", None):
            context["file_name"] = f"{uuid.uuid1()}.fits"
            
        if not context.get(FileNameTemplate):
            context.set(FileNameTemplate(template=f"{uuid.uuid1()}.fits"))

        # Reshape the array and write to the DataGraph
        task = asyncio.create_task(self._process_image(data, dtype, width, height, context))
        self._data_tasks.add(task)
        task.add_done_callback(self._data_tasks.discard)

    def _do_capture(self, exposure_seconds: float, light: bool, timeout: float) -> array.array:
        """Execute a capture synchronously (runs in thread)."""

        self.camera.StartExposure(exposure_seconds, light)

        deadline = time.monotonic() + timeout
        slow_until = time.monotonic() + max(0, exposure_seconds - 3.0)

        while time.monotonic() < slow_until:
            if self.camera.ImageReady:
                return self.camera.ImageArrayRaw
            time.sleep(1.0)

        while time.monotonic() < deadline:
            if self.camera.ImageReady:
                return self.camera.ImageArrayRaw
            time.sleep(0.1)

        raise RuntimeError("Exposure timed out waiting for ImageReady")

    async def _process_image(
        self, data: array.array, dtype: str, width: int, height: int, context: sk.Context
    ):
        try:
            # The flat buffer from ImageArrayRaw is in row-major order with
            # shape (width, height) per Alpaca convention (Dim1=X, Dim2=Y).
            # Reshape and transpose to (height, width) for FITS row-major.
            image_bytes = await asyncio.to_thread(
                lambda: np.ascontiguousarray(
                    np.frombuffer(data, dtype=dtype).reshape(width, height).T
                ).tobytes()
            )

            if graph := await sk.device().data_graph():
                source = graph.app_source()
                writer = source.produce(context)
                writer.write(image_bytes)
                logger.debug(f"wrote {len(image_bytes)} bytes to DataGraph")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            else:
                logger.warning("No DataGraph defined; discarding data")
        except Exception:
            logger.exception("Failed to publish image to DataGraph")

    async def status_publish(self):
        while True:
            try:
                c = self.camera
                connected = await self.get(c, "Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    ccd_temperature = await self.get(c, "CCDTemperature", None)
                    bin_x = await self.get(c, "BinX", 1)
                    bin_y = await self.get(c, "BinY", 1)

                    await device.publish(Binning(x=bin_x, y=bin_y))
                    if self._camera_x_size and self._camera_y_size:
                        await device.publish(
                            CameraSensorSize(x=self._camera_x_size, y=self._camera_y_size)
                        )
                    if ccd_temperature is not None:
                        await device.publish(
                            CameraSensorTemperature(
                                temperature=ccd_temperature,
                                units=TemperatureUnit.CELSIUS,
                            )
                        )

                    # Full ICameraV4 status — only include properties the camera supports
                    camera_state = await self.get(c, "CameraState", 0)
                    properties: dict = {
                        "camera_state": _CAMERA_STATES.get(camera_state, "Unknown"),
                        "bin_x": bin_x,
                        "bin_y": bin_y,
                        "max_bin_x": self._max_bin_x,
                        "max_bin_y": self._max_bin_y,
                        "cooler_on": await self.get(c, "CoolerOn", False),
                    }

                    # Sensor
                    for attr, key in (
                        ("_sensor_name", "sensor_name"),
                        ("_sensor_type", "sensor_type"),
                        ("_pixel_size_x", "pixel_size_x"),
                        ("_pixel_size_y", "pixel_size_y"),
                        ("_max_adu", "max_adu"),
                        ("_electrons_per_adu", "electrons_per_adu"),
                        ("_full_well_capacity", "full_well_capacity"),
                        ("_bayer_offset_x", "bayer_offset_x"),
                        ("_bayer_offset_y", "bayer_offset_y"),
                    ):
                        val = getattr(self, attr, None)
                        if val is not None:
                            properties[key] = val

                    if self._camera_x_size:
                        properties["camera_x_size"] = self._camera_x_size
                    if self._camera_y_size:
                        properties["camera_y_size"] = self._camera_y_size
                    if self._has_shutter:
                        properties["has_shutter"] = True
                    if self._can_asymmetric_bin:
                        properties["can_asymmetric_bin"] = True

                    # Subframe
                    start_x = await self.get(c, "StartX", None)
                    start_y = await self.get(c, "StartY", None)
                    num_x = await self.get(c, "NumX", None)
                    num_y = await self.get(c, "NumY", None)
                    if start_x is not None:
                        properties["start_x"] = start_x
                    if start_y is not None:
                        properties["start_y"] = start_y
                    if num_x is not None:
                        properties["num_x"] = num_x
                    if num_y is not None:
                        properties["num_y"] = num_y

                    # Exposure limits
                    if self._min_exposure is not None:
                        properties["min_exposure"] = self._min_exposure
                    if self._max_exposure is not None:
                        properties["max_exposure"] = self._max_exposure
                    if self._exposure_resolution is not None:
                        properties["exposure_resolution"] = self._exposure_resolution

                    # Dynamic exposure info
                    last_exposure_duration = await self.get(c, "LastExposureDuration", None)
                    last_exposure_start_time = await self.get(c, "LastExposureStartTime", None)
                    pct = await self.get(c, "PercentCompleted", None)
                    if last_exposure_duration is not None:
                        properties["last_exposure_duration"] = last_exposure_duration
                    if last_exposure_start_time is not None:
                        properties["last_exposure_start_time"] = last_exposure_start_time
                    if pct is not None:
                        properties["percent_completed"] = pct

                    # Cooler
                    if ccd_temperature is not None:
                        properties["ccd_temperature"] = ccd_temperature
                    if self._can_set_ccd_temperature:
                        properties["can_set_ccd_temperature"] = True
                        set_ccd_temperature = await self.get(c, "SetCCDTemperature", None)
                        if set_ccd_temperature is not None:
                            properties["set_ccd_temperature"] = set_ccd_temperature
                    heat_sink_temperature = await self.get(c, "HeatSinkTemperature", None)
                    if heat_sink_temperature is not None:
                        properties["heat_sink_temperature"] = heat_sink_temperature
                    if self._can_get_cooler_power:
                        properties["can_get_cooler_power"] = True
                        cooler_power = await self.get(c, "CoolerPower", None)
                        if cooler_power is not None:
                            properties["cooler_power"] = cooler_power

                    # Readout
                    if self._readout_modes is not None:
                        properties["readout_modes"] = self._readout_modes
                        readout_mode = await self.get(c, "ReadoutMode", None)
                        if readout_mode is not None:
                            properties["readout_mode"] = readout_mode

                    # Gain
                    if self._gain_min is not None or self._gains is not None:
                        gain = await self.get(c, "Gain", None)
                        if gain is not None:
                            properties["gain"] = gain
                        if self._gain_min is not None:
                            properties["gain_min"] = self._gain_min
                        if self._gain_max is not None:
                            properties["gain_max"] = self._gain_max
                        if self._gains is not None:
                            properties["gains"] = self._gains

                    # Offset
                    if self._offset_min is not None or self._offsets is not None:
                        offset = await self.get(c, "Offset", None)
                        if offset is not None:
                            properties["offset"] = offset
                        if self._offset_min is not None:
                            properties["offset_min"] = self._offset_min
                        if self._offset_max is not None:
                            properties["offset_max"] = self._offset_max
                        if self._offsets is not None:
                            properties["offsets"] = self._offsets

                    # Capabilities
                    if self._can_abort_exposure:
                        properties["can_abort_exposure"] = True
                    if self._can_stop_exposure:
                        properties["can_stop_exposure"] = True
                    if self._can_pulse_guide:
                        properties["can_pulse_guide"] = True
                        properties["is_pulse_guiding"] = await self.get(c, "IsPulseGuiding", False)

                    # properties_str = ", ".join(f"{k}={v}" for k, v in properties.items())
                    # logger.debug(
                    #     f"Alpaca camera status: connected={connected}, ccd_temperature={ccd_temperature}, "
                    #     f"bin_x={bin_x}, bin_y={bin_y}, {properties_str}"
                    # )

                    await device.publish(AlpacaCameraStatus(**properties))
            except Exception as e:
                logger.exception(f"Error in camera status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class AlpacaCameraConfig(AlpacaDeviceConfig[AlpacaCamera]):
    device_type: Literal["camera"] = "camera"
    temperature: float = -10.0
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return AlpacaCamera(self)


class AlpacaCameraState(AlpacaDeviceState):
    device_type: Literal["camera"] = "camera"
