from __future__ import annotations

import array
import asyncio
import uuid
from typing import Literal, override

from astropy.time import Time
from loguru import logger

import sensorkit.api as sk
from sensorkit.data.fits import ArrayInfo
from sensorkit.models.devices import (
    CameraSensorSize,
    Connected,
    TemperatureUnit,
)
from sensorkit.std.instrument import (
    Binning,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
)
from sensorkit.thesky.device import (
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
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


@sk.declare_device
class TheSkyCamera(TheSkyDevice):
    """TheSky Camera implementation."""

    config: TheSkyCameraConfig
    device_name = "Camera"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        try:
            self.state = await device.kv_get_model(TheSkyCameraState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyCameraState()

        await self.camera_init(sk.Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await self.stop_status_loop()
        await self.camera_deinit(sk.Deinit())
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def camera_init(self, cmd: sk.Init):
        self._reconnect = lambda: self.camera_connect(sk.Connect())
        await self.camera_connect(sk.Connect())

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
    async def camera_deinit(self, cmd: sk.Deinit):
        await self.camera_abort(sk.Abort())
        await self.camera_disconnect(sk.Disconnect())

    @sk.command_handler
    async def camera_connect(self, cmd: sk.Connect):
        logger.debug("connecting to Camera")

        await self.execute(
            """
            ccdsoftCamera.Asynchronous = 1;
            ccdsoftCamera.ShutDownTemperatureRegulationOnDisconnect = 1;
            ccdsoftCamera.Connect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Ready")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to Camera")

    @sk.command_handler
    async def camera_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from Camera")

        await self.execute(
            """
            ccdsoftCamera.Disconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Not Connected")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from Camera")

    @sk.command_handler
    async def camera_stop(self, cmd: sk.Stop):
        await self.camera_abort(sk.Abort())

    @sk.command_handler
    async def camera_abort(self, cmd: sk.Abort):
        await self.require_connected()
        logger.debug("aborting camera capture")

        await self.execute(
            """
            ccdsoftCamera.Abort();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Ready")

        logger.debug("aborted camera capture")

    @sk.command_handler
    async def camera_set_temperature(self, cmd: ConfigureCameraCooler):
        await self.require_connected()
        logger.debug(f"setting camera temperature to {cmd.setpoint.temperature}")

        target = cmd.setpoint.temperature
        await self.execute(
            f"""
            ccdsoftCamera.TemperatureSetPoint = {target};
            ccdsoftCamera.RegulateTemperature = {1 if cmd.enable else 0};
            """
        )

        resp = await self.execute(
            """
            ccdsoftCamera.TemperatureSetPoint;
            """
        )
        temp = float(resp)
        if temp != target:
            logger.warning(f"Requested camera temperature of {target} C, got {temp} C")
        else:
            logger.debug(f"set camera temperature to {target} C")

    @sk.command_handler
    async def camera_set_binning(self, cmd: ConfigureCameraSensor):
        await self.require_connected()

        if cmd.binning is None:
            return
        bin_x, bin_y = int(cmd.binning.x), int(cmd.binning.y)
        logger.debug(f"setting camera binning to ({bin_x}, {bin_y})")
        await self.execute(
            f"""
            ccdsoftCamera.BinX = {bin_x};
            ccdsoftCamera.BinY = {bin_y};
            """
        )

        resp = await self.execute(
            """
            var Out;
            Out = [
                ccdsoftCamera.BinX,
                ccdsoftCamera.BinY
            ];
            """
        )
        actual_x, actual_y = [int(x) for x in resp.split(",")]
        if actual_x != bin_x or actual_y != bin_y:
            logger.warning(
                f"Requested camera binning of ({bin_x}, {bin_y}), got ({actual_x}, {actual_y})"
            )
        else:
            logger.debug(f"set camera binning to ({actual_x}, {actual_y})")

    @sk.command_handler
    async def camera_capture(self, cmd: sk.CameraCapture):
        await self.require_connected()
        logger.info(f"Requesting {cmd.integration_time:.1f} sec capture from camera")
        logger.debug("starting camera capture")

        frame_type = 1  # 1=Light, 2=Bias, 3=Dark, 4=Flat Field
        await self.execute(
            f"""
            ccdsoftCamera.Frame = {frame_type};
            ccdsoftCamera.ExposureTime = {cmd.integration_time};
            ccdsoftCamera.TakeImage();
            """
        )

        async with asyncio.timeout(cmd.integration_time + self.config.timeout):
            await self.poll("""ccdsoftCamera.IsExposureComplete;""", "1", interval=0.01)

        resp = await self.execute(
            """
            ccdsoftCameraImage.AttachToActiveImager();
            var H = ccdsoftCameraImage.HeightInPixels;
            var Out = "";
            for (var y = 0; y < H; y++) {
                if (y > 0) Out += ";";
                Out += ccdsoftCameraImage.scanLine(y);
            }
            """
        )

        # Convert it to list[array.array] - this is CPU-bound, so background it
        data: list[array.array] = await asyncio.to_thread(
            self._pixels_from_csv_block, resp, "uint16"
        )

        if not data or not data[0]:
            logger.error("No data returned from camera!")
            return

        dtype = _array_typecode_to_dtype.get(data[0].typecode)
        logger.debug(f"got image array with {len(data)} rows, {len(data[0])} cols, dtype {dtype}")

        # Set context
        resp = await self.execute(
            """
            var Out;
            Out = [
                ccdsoftCameraImage.JulianDay,
                ccdsoftCameraImage.FITSKeyword("BITPIX"),
                ccdsoftCamera.BinX,
                ccdsoftCamera.BinY                                
            ];
            """
        )
        jd, bpp, binx, biny = [float(x) for x in resp.split(",")]
        cmd.context["date_obs"] = Time(jd, format="jd", scale="utc").isot
        cmd.context["exptime"] = cmd.integration_time
        cmd.context["bitpix"] = _dtype_to_bitpix.get(dtype, int(bpp))
        cmd.context["bscale"] = 1
        cmd.context["bzero"] = _dtype_to_bzero.get(dtype, 0)

        if graph := await sk.device().data_graph():
            source = graph.app_source()

            cmd.context.set(
                ArrayInfo(
                    shape=(len(data), len(data[0])),
                    dtype=dtype,
                )
            )

            instrume = await self.execute("""SelectedHardware.cameraModel;""")
            cmd.context["instrume"] = instrume
            cmd.context["xbinning"] = int(binx)
            cmd.context["ybinning"] = int(biny)

            if not cmd.context.get("file_name", None):
                cmd.context["file_name"] = f"{str(uuid.uuid1())}.fits"

            writer = source.produce(cmd.context)
            n = 0

            for row in data:
                row_bytes = row.tobytes()
                writer.write(row_bytes)
                n += len(row_bytes)

            logger.debug(f"wrote {n} bytes to DataGraph")
            await writer.drain()

            writer.close()
            await writer.wait_closed()

        else:
            logger.warning("discarding data since no DataGraph is defined!")

        logger.debug("completed camera capture")

    def _pixels_from_csv_block(self, csv_block: str, dtype: str = "uint16") -> list[array.array]:
        """
        Convert a single CSV block string like:
            "1,2,3;10,20,30;100,200,300"
        into a list of array.array rows.
        """

        # Reverse mapping: dtype string to typecode
        dtype_to_typecode = {v: k for k, v in _array_typecode_to_dtype.items()}
        typecode = dtype_to_typecode[dtype]

        img = []
        for row_str in csv_block.strip().split(";"):
            if not row_str:
                continue
            nums = list(map(int, row_str.split(",")))
            img.append(array.array(typecode, nums))
        return img

    async def status_publish(self):
        while True:
            try:
                resp = await self.execute(
                    """
                    var Out;
                    Out = [
                        ccdsoftCamera.Status,
                        ccdsoftCamera.Temperature,
                        ccdsoftCamera.SubframeLeft,
                        ccdsoftCamera.SubframeTop,
                        ccdsoftCamera.SubframeRight,
                        ccdsoftCamera.SubframeBottom,
                        ccdsoftCamera.BinX,
                        ccdsoftCamera.BinY
                    ];
                    """
                )
            except Exception as e:
                logger.exception(f"Error in status_publish execute: {e}")
                await asyncio.sleep(self.config.status_frequency)
                continue

            try:
                parts = resp.split(",")
                temp, left, top, right, bottom, binx, biny = map(float, parts[1:])

                connected = parts[0] != "Not Connected"
                self.device_connected = connected

                width = int(right - left)
                height = int(bottom - top)

                # logger.debug(
                #     f"TheSky camera status: connected={connected}, temperature={temp}, width={width}, height={height}, binx={int(binx)}, biny={int(biny)}"
                # )

                device = sk.device()
                await device.publish(Connected(is_connected=connected))
                await device.publish(
                    CameraSensorTemperature(temperature=temp, units=TemperatureUnit.CELSIUS)
                )
                await device.publish(CameraSensorSize(x=width, y=height))
                await device.publish(Binning(x=int(binx), y=int(biny)))

            except Exception as e:
                logger.warning(f"Failed to update TheSky camera status ({e})")
                await asyncio.sleep(self.config.status_frequency)
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyCameraConfig(TheSkyDeviceConfig[TheSkyCamera]):
    """TheSky Camera configuration."""

    device_type: Literal["camera"] = "camera"
    temperature: float = -10.0
    status_frequency: float = 1.0
    timeout: float

    @override
    def create_device(self):
        return TheSkyCamera(self)


class TheSkyCameraState(TheSkyDeviceState):
    """TheSky Camera state."""

    device_type: Literal["camera"] = "camera"
