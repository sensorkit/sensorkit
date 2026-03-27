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
    Binning,
    CameraSensorSize,
    Connected,
    TemperatureUnit,
)
from sensorkit.std.instrument import CameraSensorTemperature
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


@sk.declare_device
class TheSkyCamera(TheSkyDevice):
    """TheSky Camera implementation."""
    config: TheSkyCameraConfig
    device_name = "Camera"

    @sk.on_attach
    async def entity_init(self):
        """Restore last known state."""
        device = sk.device()

        # Restore last known state
        try:
            self.state = await device.kv_get_model(TheSkyCameraState)
            logger.debug(f"restoring state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyCameraState()

        # Initialize the camera
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.camera_init(sk.Init())

    @sk.command_handler
    async def camera_init(self, cmd: sk.Init):
        """Connect to the hardware, start publishing status, start cooling the camera."""
        # Connect to the hardware
        await self.camera_connect(sk.Connect())

        # Start camera status publishing
        # TODO: Use service context ThreadGroup.
        logger.debug("starting thesky camera status loop")
        self._status_task = asyncio.create_task(self.status_publish())

        # Set the camera temperature
        await self.set_temperature(sk.SetTemperature(
            temperature=self.config.temperature,
            units=TemperatureUnit.CELSIUS
        ))

    @sk.command_handler
    async def camera_deinit(self, cmd: sk.Deinit):
        """Stop publishing status, disconnect from the hardware."""
        # Stop camera status publishing
        logger.debug("stopping thesky camera status loop")
        if hasattr(self, "_status_task"):
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        # Disconnect from the hardware
        await self.camera_disconnect(sk.Disconnect())

    @sk.on_detach
    async def entity_deinit(self):
        """Save current state."""
        await sk.device().kv_put_model(self.state)

        # De-initialize the camera
        # FIXME: this is temporary, while awaiting updates to the standard controller
        await self.camera_deinit(sk.Deinit())

    @sk.command_handler
    async def camera_connect(self, cmd: sk.Connect):
        """Establish connection, start cooling, start publishing status."""
        logger.debug("connecting to thesky camera")
        await self.execute(
            f"""
            ccdsoftCamera.Asynchronous = 1;
            ccdsoftCamera.ShutDownTemperatureRegulationOnDisconnect = 1;
            ccdsoftCamera.Connect();
            """
        )

        # Wait for the camera to connect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Ready")

        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        logger.debug("connected to thesky camera")

    @sk.command_handler
    async def camera_disconnect(self, cmd: sk.Disconnect):
        logger.debug("disconnecting from thesky camera")
        await self.execute(
            """
            ccdsoftCamera.Disconnect();
            """
        )

        # Wait for the camera to disconnect
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Not Connected")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from thesky camera")

    @sk.command_handler
    async def set_temperature(self, cmd: sk.SetTemperature):
        self.require_connected()
        logger.debug(f"setting thesky camera temperature to {cmd.temperature}")
        await self.execute(
            f"""
            ccdsoftCamera.TemperatureSetPoint = {cmd.temperature};
            ccdsoftCamera.RegulateTemperature = 1;
            """
        )

        # Confirm the setpoint was accepted
        resp = await self.execute(
            """
            ccdsoftCamera.TemperatureSetPoint;
            """
        )
        temp = float(resp)
        if temp != cmd.temperature:
            logger.warning(f"Requested camera temperature of {cmd.temperature} C, got {temp} C")
        else:
            logger.debug(f"set thesky camera temperature to {cmd.temperature} C")

    @sk.command_handler
    async def set_binning(self, cmd: sk.SetBinning):
        self.require_connected()
        logger.debug(f"setting thesky camera binning to ({cmd.x}, {cmd.y})")
        await self.execute(
            f"""
            ccdsoftCamera.BinX = {cmd.x};
            ccdsoftCamera.BinY = {cmd.y};
            """
        )

        # Confirm the binning values were accepted
        resp = await self.execute(
            """
            var Out;
            Out = [
                ccdsoftCamera.BinX,
                ccdsoftCamera.BinY
            ];
            """
        )
        binx, biny = [int(x) for x in resp.split(',')]
        if binx != cmd.x or biny != cmd.y:
            logger.warning(f"Requested camera binning of ({cmd.x}, {cmd.y}), got ({binx}, {biny})")
        else:
            logger.debug(f"set thesky camera binning to ({binx}, {biny})")

    @sk.command_handler
    async def abort(self, cmd: sk.Abort):
        self.require_connected()
        logger.debug("aborting thesky camera capture")
        await self.execute(
            """
            ccdsoftCamera.Abort();
            """
        )

        # Wait for the camera to abort
        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Ready")
        logger.debug("aborted thesky camera capture")

    @sk.command_handler
    async def camera_capture(self, cmd: sk.CameraCapture):
        self.require_connected()
        logger.info(f"Requesting {cmd.integration_time:.1f} sec capture from TheSky camera")

        # Start the capture
        logger.debug("starting thesky camera capture")
        frame_type = 1  # 1=Light, 2=Bias, 3=Dark, 4=Flat Field
        await self.execute(
            f"""
            ccdsoftCamera.Frame = {frame_type};
            ccdsoftCamera.ExposureTime = {cmd.integration_time};
            ccdsoftCamera.TakeImage();
            """
        )

        # Wait for it to finish
        async with asyncio.timeout(cmd.integration_time + self.config.timeout):
            await self.poll("""ccdsoftCamera.IsExposureComplete;""", "1", interval=0.01)

        # Retrieve the image
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
            logger.error("No data returned from TheSky camera!")
            return

        dtype = _array_typecode_to_dtype.get(data[0].typecode)
        logger.info(f"Got TheSky image array with {len(data)} rows, {len(data[0])} cols, dtype {dtype}")

        # Set context
        resp = await self.execute(
            """
            var Out;
            Out = [
                ccdsoftCameraImage.JulianDay,
                ccdsoftCameraImage.FITSKeyword("BITPIX")
            ];
            """
        )
        jd, bpp = [float(x) for x in resp.split(',')]
        cmd.context["date_obs"] = Time(jd, format='jd', scale='utc').isot
        cmd.context["etime"] = cmd.integration_time
        cmd.context["bits_per_pixel"] = _dtype_to_bitpix.get(dtype, 16)  #int(bpp)

        if graph := await sk.device().data_graph():
            source = graph.app_source()

            cmd.context.set(
                ArrayInfo(
                    shape=(len(data), len(data[0])),
                    dtype=dtype,
                )
            )

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

        logger.debug("completed thesky camera capture")

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
                parts = resp.split(',')
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
                await device.publish(
                    CameraSensorSize(x=width, y=height)
                )
                await device.publish(
                    Binning(x=int(binx), y=int(biny))
                )

            except Exception as e:
                logger.warning(f"Failed to update TheSky camera status ({e})")
                continue

            # FIXME: Account for query time
            await asyncio.sleep(self.config.status_frequency)


class TheSkyCameraConfig(TheSkyDeviceConfig[TheSkyCamera]):
    """TheSky Camera configuration."""
    device_type: Literal["camera"] = "camera"
    temperature: float
    timeout: float
    status_frequency: float = 1.0

    @override
    def create_device(self):
        return TheSkyCamera(self)


class TheSkyCameraState(TheSkyDeviceState):
    """TheSky Camera state."""
    device_type: Literal["camera"] = "camera"
    # Add camera-specific state fields here as needed in the future