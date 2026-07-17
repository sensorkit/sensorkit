# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import array
import asyncio
import uuid
from typing import Literal, override

import numpy as np
from astropy.time import Time
from loguru import logger

import sensorkit.api as sk
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.data.fits import ArrayInfo, ImageInfo
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
    ExposureInfo,
    FrameType,
    TemperatureUnit,
)
from sensorkit.thesky.device import (
    ProcessAbortedError,
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyDeviceState,
)


@sk.declare_device
class TheSkyCamera(TheSkyDevice):
    """TheSky Camera implementation."""

    config: TheSkyCameraConfig
    device_name = "Camera"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(TheSkyCameraState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = TheSkyCameraState()

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
        logger.debug("connecting to camera")

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

        logger.debug("connected to camera")

    @sk.command_handler
    async def camera_disconnect(self, cmd: Disconnect):
        logger.debug("disconnecting from camera")

        await self.execute(
            """
            ccdsoftCamera.Disconnect();
            """
        )

        async with asyncio.timeout(self.config.timeout):
            await self.poll("""ccdsoftCamera.Status;""", "Not Connected")

        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))

        logger.debug("disconnected from camera")

    @sk.command_handler
    async def camera_stop(self, cmd: Stop):
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
        temperature_set_point = float(resp)
        if temperature_set_point != target:
            logger.warning(f"Requested camera temperature of {target} C, got {temperature_set_point} C")
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
            raise RuntimeError(
                f"Requested camera binning of ({bin_x}, {bin_y}), "
                f"got ({actual_x}, {actual_y})"
            )
        else:
            logger.debug(f"set camera binning to ({actual_x}, {actual_y})")

    @sk.command_handler
    async def camera_capture(self, cmd: CameraCapture):
        await self.require_connected()
        logger.info(f"Requesting {cmd.integration_time:.1f} sec capture from camera")
        logger.debug("starting camera capture")

        # Start the exposure
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

        array_info = ArrayInfo.from_array(data[0], shape=(len(data), len(data[0])))
        logger.debug(f"got image array with {len(data)} rows, {len(data[0])} cols, dtype {array_info.dtype}")

        # Build data context
        resp = await self.execute(
            """
            var Out;
            Out = [
                ccdsoftCameraImage.JulianDay,
                ccdsoftCameraImage.Temperature,
                ccdsoftCamera.BinX,
                ccdsoftCamera.BinY
            ];
            """
        )
        jd, temp, binx, biny = [float(x) for x in resp.split(",")]

        # Write it to the DataGraph
        if graph := await sk.device().data_graph():
            source = graph.app_source()

            instrume = await self.execute("""SelectedHardware.cameraModel;""")
            cmd.context.set(
                ImageInfo(array=array_info, binning=(int(binx), int(biny))),
                ExposureInfo(
                    date_obs=Time(jd, format="jd", scale="utc").isot,
                    exposure_time=cmd.integration_time,
                    instrument=instrume,
                    image_type=FrameType.LIGHT,
                    ccd_temperature=temp,
                ),
            )

            if not cmd.context.get(FileNameTemplate):
                cmd.context.set(FileNameTemplate(template=f"{str(uuid.uuid1())}.fits"))

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

        # `array.array` typecode for the target dtype (e.g. uint16 -> "H").
        typecode = np.dtype(dtype).char

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
            except ProcessAbortedError:
                # TheSky returns 212 transiently after an `abort`; camera is fine
                pass
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
