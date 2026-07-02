from __future__ import annotations

import asyncio
import base64
import io
import uuid
from datetime import UTC, datetime
from typing import Literal, override

from astropy.io import fits
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.data.fits import ArrayInfo
from sensorkit.nina.device import (
    NinaDevice,
    NinaDeviceConfig,
    NinaDeviceState,
)
from sensorkit.models.devices import Stop
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


@sk.declare_keyword
class NinaCameraStatus(BaseModel):
    """NINA camera status from /equipment/camera/info."""

    # Identity
    name: str | None = None
    display_name: str | None = None
    device_id: str | None = None
    connected: bool = False

    # State
    camera_state: str = "NoState"
    is_exposing: bool = False
    exposure_end_time: str | None = None
    last_download_time: float | None = None

    # Sensor
    sensor_type: str | None = None
    x_size: int | None = None
    y_size: int | None = None
    pixel_size: float | None = None
    bit_depth: int | None = None
    bayer_offset_x: int | None = None
    bayer_offset_y: int | None = None
    electrons_per_adu: float | None = None

    # Binning
    bin_x: int = 1
    bin_y: int = 1
    binning_modes: list[dict] | None = None

    # Temperature
    temperature: float | None = None
    target_temp: float | None = None
    temperature_set_point: float | None = None
    at_target_temp: bool | None = None
    can_set_temperature: bool = False
    cooler_on: bool = False
    cooler_power: float | None = None
    has_dew_heater: bool = False
    dew_heater_on: bool = False

    # Gain / Offset
    gain: int | None = None
    default_gain: int | None = None
    gain_min: int | None = None
    gain_max: int | None = None
    can_set_gain: bool = False
    can_get_gain: bool = False
    gains: list[int] | None = None
    offset: int | None = None
    default_offset: int | None = None
    offset_min: int | None = None
    offset_max: int | None = None
    can_set_offset: bool = False

    # USB
    usb_limit: int | None = None
    usb_limit_min: int | None = None
    usb_limit_max: int | None = None
    can_set_usb_limit: bool = False

    # Readout
    readout_mode: int | None = None
    readout_modes: list[str] | None = None
    readout_mode_for_snap_images: int | None = None
    readout_mode_for_normal_images: int | None = None

    # Exposure limits
    exposure_max: float | None = None
    exposure_min: float | None = None

    # Subsampling
    can_sub_sample: bool = False
    is_sub_sample_enabled: bool = False
    sub_sample_x: int | None = None
    sub_sample_y: int | None = None
    sub_sample_width: int | None = None
    sub_sample_height: int | None = None

    # Misc
    has_shutter: bool = False
    battery: float | None = None
    live_view_enabled: bool = False
    can_show_live_view: bool = False
    supported_actions: list[str] | None = None


@sk.declare_device
class NinaCamera(NinaDevice):
    """NINA Camera implementation."""

    config: NinaCameraConfig
    device_name = "Camera"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(NinaCameraState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = NinaCameraState()

        # Initialize the camera
        await self._initialize()
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self.camera_disconnect(Disconnect())
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Connect to the hardware
        self._reconnect = lambda: self.camera_connect(Connect())
        await self.camera_connect(Connect())

        # Read initial info for sensor dimensions
        info = await self.info("camera")
        self._x_size = info.get("XSize", 0)
        self._y_size = info.get("YSize", 0)

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
    async def camera_connect(self, cmd: Connect):
        await self.connect("camera")
        await sk.device().publish(Connected(is_connected=True))

    @sk.command_handler
    async def camera_disconnect(self, cmd: Disconnect):
        await self.disconnect("camera")
        await sk.device().publish(Connected(is_connected=False))

    @sk.command_handler
    async def camera_stop(self, cmd: Stop):
        await self.camera_abort(sk.Abort())

    @sk.command_handler
    async def camera_abort(self, cmd: sk.Abort):
        await self.require_connected()
        logger.debug("aborting exposure")
        await self.client.get("/equipment/camera/abort-exposure")
        logger.debug("aborted exposure")

    @sk.command_handler
    async def camera_set_temperature(self, cmd: ConfigureCameraCooler):
        await self.require_connected()
        logger.debug(f"setting camera temperature to {cmd.setpoint.temperature} °C")

        target = cmd.setpoint.temperature
        await self.client.get(
            "/equipment/camera/cool",
            temperature=target,
            minutes=5.0,
        )

        info = await self.info("camera")
        set_point = info.get("TemperatureSetPoint")
        if set_point != target:
            logger.warning(f"Requested camera temperature of {target} C, got {set_point} C")
        else:
            logger.debug(f"set camera temperature to {target} C")

    @sk.command_handler
    async def camera_set_binning(self, cmd: ConfigureCameraSensor):
        if cmd.binning is None:
            return
        await self.require_connected()
        bin_x, bin_y = int(cmd.binning.x), int(cmd.binning.y)
        binning = f"{bin_x}x{bin_y}"
        logger.debug(f"setting camera binning to ({bin_x}, {bin_y})")
        await self.client.get("/equipment/camera/set-binning", binning=binning)

        info = await self.info("camera")
        actual_x = info.get("BinX")
        actual_y = info.get("BinY")
        if actual_x != bin_x or actual_y != bin_y:
            raise RuntimeError(
                f"Requested camera binning of ({bin_x}, {bin_y}), "
                f"got ({actual_x}, {actual_y})"
            )
        else:
            logger.debug(f"set camera binning to ({bin_x}, {bin_y})")

    @sk.command_handler
    async def camera_capture(self, cmd: CameraCapture):
        await self.require_connected()
        exposure_seconds = float(cmd.integration_time)
        logger.info(f"Requesting {exposure_seconds:.1f} sec capture from camera")

        # Get image count before capture so we can detect when the new image is saved
        count_before = await self.client.get("/image-history", count=True)

        # Capture and save raw FITS (NINA saves asynchronously after capture completes)
        exposure_start = datetime.now(UTC)
        await self.client.get(
            "/equipment/camera/capture",
            duration=exposure_seconds,
            imageType="LIGHT",
            save=True,
            waitForResult=True,
            omitImage=True,
        )

        # Poll until the image appears in NINA's history
        async with asyncio.timeout(30.0):
            while True:
                count_after = await self.client.get("/image-history", count=True)
                if count_after > count_before:
                    break
                await asyncio.sleep(0.5)

        # Retrieve the raw FITS data (base64-encoded)
        index = count_after - 1
        logger.debug(f"retrieving FITS image at index {index}")
        fits_b64 = await self.client.get(f"/image/{index}", raw_fits=True)
        fits_bytes = await asyncio.to_thread(base64.b64decode, fits_b64)

        # Parse the FITS to extract pixel data and metadata
        hdul = await asyncio.to_thread(fits.open, io.BytesIO(fits_bytes))
        try:
            hdr = hdul[0].header
            data = hdul[0].data
            height, width = data.shape
            dtype = str(data.dtype)
            image_bytes = await asyncio.to_thread(data.tobytes)

            # Get image history metadata
            history_resp = await self.client.get("/image-history", index=index)
            history = history_resp[0] if isinstance(history_resp, list) else history_resp

            # Build context
            context = cmd.context
            context.set(ArrayInfo(shape=(height, width), dtype=dtype))
            context["bitpix"] = int(hdr.get("BITPIX", 16))
            context["bscale"] = float(hdr.get("BSCALE", 1))
            context["bzero"] = float(hdr.get("BZERO", 0))
            context["date_obs"] = str(exposure_start)
            context["exptime"] = exposure_seconds
            context["instrume"] = history.get("CameraName", str(sk.device().entity))
            context["xbinning"] = await self._get_binning_x()
            context["ybinning"] = await self._get_binning_y()

            if not context.get(FileNameTemplate):
                context.set(
                    FileNameTemplate(template=history.get("Filename", f"{uuid.uuid1()}.fits"))
                )

            # Write to DataGraph
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

        finally:
            hdul.close()

    async def _get_binning_x(self) -> int:
        info = await self.info("camera")
        return info.get("BinX", 1)

    async def _get_binning_y(self) -> int:
        info = await self.info("camera")
        return info.get("BinY", 1)

    async def status_publish(self):
        while True:
            try:
                info = await self.info("camera")
                connected = info.get("Connected", False)
                self.device_connected = connected

                device = sk.device()
                await device.publish(Connected(is_connected=connected))

                if connected:
                    temperature = info.get("Temperature")
                    bin_x = info.get("BinX", 1)
                    bin_y = info.get("BinY", 1)
                    x_size = info.get("XSize", 0)
                    y_size = info.get("YSize", 0)

                    await device.publish(Binning(x=bin_x, y=bin_y))

                    if x_size and y_size:
                        await device.publish(CameraSensorSize(x=x_size, y=y_size))

                    if temperature is not None:
                        await device.publish(
                            CameraSensorTemperature(
                                temperature=temperature,
                                units=TemperatureUnit.CELSIUS,
                            )
                        )

                    # Map all NINA camera info fields to NinaCameraStatus
                    _FIELD_MAP = {
                        "name": "Name",
                        "display_name": "DisplayName",
                        "device_id": "DeviceId",
                        "camera_state": "CameraState",
                        "is_exposing": "IsExposing",
                        "exposure_end_time": "ExposureEndTime",
                        "last_download_time": "LastDownloadTime",
                        "sensor_type": "SensorType",
                        "x_size": "XSize",
                        "y_size": "YSize",
                        "pixel_size": "PixelSize",
                        "bit_depth": "BitDepth",
                        "bayer_offset_x": "BayerOffsetX",
                        "bayer_offset_y": "BayerOffsetY",
                        "electrons_per_adu": "ElectronsPerADU",
                        "bin_x": "BinX",
                        "bin_y": "BinY",
                        "binning_modes": "BinningModes",
                        "temperature": "Temperature",
                        "target_temp": "TargetTemp",
                        "temperature_set_point": "TemperatureSetPoint",
                        "at_target_temp": "AtTargetTemp",
                        "can_set_temperature": "CanSetTemperature",
                        "cooler_on": "CoolerOn",
                        "cooler_power": "CoolerPower",
                        "has_dew_heater": "HasDewHeater",
                        "dew_heater_on": "DewHeaterOn",
                        "gain": "Gain",
                        "default_gain": "DefaultGain",
                        "gain_min": "GainMin",
                        "gain_max": "GainMax",
                        "can_set_gain": "CanSetGain",
                        "can_get_gain": "CanGetGain",
                        "gains": "Gains",
                        "offset": "Offset",
                        "default_offset": "DefaultOffset",
                        "offset_min": "OffsetMin",
                        "offset_max": "OffsetMax",
                        "can_set_offset": "CanSetOffset",
                        "usb_limit": "USBLimit",
                        "usb_limit_min": "USBLimitMin",
                        "usb_limit_max": "USBLimitMax",
                        "can_set_usb_limit": "CanSetUSBLimit",
                        "readout_mode": "ReadoutMode",
                        "readout_modes": "ReadoutModes",
                        "readout_mode_for_snap_images": "ReadoutModeForSnapImages",
                        "readout_mode_for_normal_images": "ReadoutModeForNormalImages",
                        "exposure_max": "ExposureMax",
                        "exposure_min": "ExposureMin",
                        "can_sub_sample": "CanSubSample",
                        "is_sub_sample_enabled": "IsSubSampleEnabled",
                        "sub_sample_x": "SubSampleX",
                        "sub_sample_y": "SubSampleY",
                        "sub_sample_width": "SubSampleWidth",
                        "sub_sample_height": "SubSampleHeight",
                        "has_shutter": "HasShutter",
                        "battery": "Battery",
                        "live_view_enabled": "LiveViewEnabled",
                        "can_show_live_view": "CanShowLiveView",
                        "supported_actions": "SupportedActions",
                    }

                    fields: dict = {"connected": True}
                    for field_name, info_key in _FIELD_MAP.items():
                        val = info.get(info_key)
                        if val is not None:
                            fields[field_name] = val

                    fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())
                    # logger.debug(f"NINA camera status: {fields_str}")

                    await device.publish(NinaCameraStatus(**fields))
            except Exception as e:
                logger.exception(f"Error in camera status publish: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            await asyncio.sleep(self.config.status_frequency)


class NinaCameraConfig(NinaDeviceConfig[NinaCamera]):
    device_type: Literal["camera"] = "camera"
    temperature: float = -10.0
    status_frequency: float = 1.0
    timeout: float = 60.0

    @override
    def create_device(self):
        return NinaCamera(self)


class NinaCameraState(NinaDeviceState):
    device_type: Literal["camera"] = "camera"
