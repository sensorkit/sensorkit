from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Literal, override

import numpy as np
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.data.fits import ArrayInfo
from sensorkit.models.devices import Deinit, Init, Stop
from sensorkit.sdasim.device import SdasimDevice, SdasimDeviceConfig, SdasimDeviceState
from sensorkit.sdasim.engine import SdasimEngine, SensorKitBridge
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

# FITS BITPIX / BZERO for the dtypes sdasim can produce. FITS has no native
# unsigned-integer type, so unsigned data uses a signed BITPIX with a BZERO
# offset (astropy follows the same convention).
_DTYPE_TO_BITPIX = {"uint8": 8, "int16": 16, "uint16": 16, "int32": 32, "uint32": 32}
_DTYPE_TO_BZERO = {"uint8": 0, "int16": 0, "uint16": 32768, "int32": 0, "uint32": 2147483648}


@sk.declare_keyword
class SdasimCameraStatus(BaseModel):
    """Status of the simulated sdasim camera."""

    connected: bool = False
    camera_state: str = "idle"  # "idle" | "exposing"
    sensor_width: int | None = None
    sensor_height: int | None = None
    bin_x: int = 1
    bin_y: int = 1
    temperature: float | None = None
    mount_ra_rate: float = 0.0  # inertial RA rate fed to sdasim (deg/s); 0 == sidereal
    mount_dec_rate: float = 0.0  # inertial Dec rate (deg/s)
    num_targets: int | None = None  # satellites rendered in the last frame
    catalog_enabled: bool = False
    mount_connected: bool = False
    rotator_connected: bool = False


@sk.declare_device
class SdasimCamera(SdasimDevice):
    """Simulated camera backed by the sdasim renderer."""

    config: SdasimCameraConfig
    device_name = "Camera"

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(SdasimCameraState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = SdasimCameraState()

        # Runtime state
        self._engine = SdasimEngine(
            self.config.sdasim_config,
            device=self.config.device,
            rebuild_threshold_deg=self.config.rebuild_threshold_deg,
        )
        self._bridge = SensorKitBridge(self.config.mount_entity, self.config.rotator_entity)
        self._bin_x = self.state.bin_x or self.config.binning
        self._bin_y = self.state.bin_y or self.config.binning
        self._temperature = self.config.temperature
        self._num_targets: int | None = None
        self._mount_ra_rate = 0.0
        self._mount_dec_rate = 0.0
        self._capture_lock = asyncio.Lock()
        self._capture_task: asyncio.Task | None = None

        await self.camera_init(Init())
        self.start_status_loop(self.status_publish())

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.stop_status_loop()
        await self._bridge.stop()
        self.state.bin_x = self._bin_x
        self.state.bin_y = self._bin_y
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def camera_init(self, cmd: Init):
        self._reconnect = lambda: self.camera_connect(Connect())

        # Load the sdasim scene config (CPU-bound; off the event loop).
        await asyncio.to_thread(self._engine.initialize)

        await self.camera_connect(Connect())

        # Start telemetry subscriptions for live pointing.
        await self._bridge.start(sk.device().sensorkit())

        await sk.device().publish(
            CameraSensorSize(x=self._engine.sensor_width, y=self._engine.sensor_height)
        )

    @sk.command_handler
    async def camera_deinit(self, cmd: Deinit):
        await self.camera_abort(sk.Abort())

    @sk.command_handler
    async def camera_connect(self, cmd: Connect):
        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))
        logger.debug("connected to simulated camera")

    @sk.command_handler
    async def camera_disconnect(self, cmd: Disconnect):
        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))
        logger.debug("disconnected from simulated camera")

    @sk.command_handler
    async def camera_stop(self, cmd: Stop):
        await self.camera_abort(sk.Abort())

    @sk.command_handler
    async def camera_abort(self, cmd: sk.Abort):
        if self._capture_task is not None and not self._capture_task.done():
            logger.debug("aborting in-flight capture")
            self._capture_task.cancel()

    @sk.command_handler
    async def camera_set_temperature(self, cmd: ConfigureCameraCooler):
        # No physical cooler -- track the requested setpoint for status/telemetry.
        self._temperature = cmd.setpoint.temperature
        logger.debug(f"simulated camera setpoint -> {self._temperature} °C")

    @sk.command_handler
    async def camera_set_binning(self, cmd: ConfigureCameraSensor):
        if cmd.binning is None:
            return
        bin_x, bin_y = int(cmd.binning.x), int(cmd.binning.y)
        if bin_x != bin_y:
            logger.warning(
                f"sdasim only supports symmetric binning; using {bin_x}x{bin_x} (requested {bin_x}x{bin_y})"
            )
            bin_y = bin_x
        self._bin_x = self._bin_y = bin_x
        logger.debug(f"set simulated camera binning to {bin_x}x{bin_y}")

    @sk.command_handler
    async def camera_capture(self, cmd: CameraCapture):
        await self.require_connected()
        exposure_seconds = float(cmd.integration_time)
        bin_factor = max(1, int(self._bin_x))
        logger.info(f"Rendering {exposure_seconds:.1f} sec capture")

        # Live pointing + inertial (ICRF) mount rate drive the render. sdasim's
        # apparent_rate = object_rate - mount_rate model handles sidereal (rate
        # 0,0 -> sharp stars, streaking satellites) and rate track uniformly, so
        # we just pass the mount rate through -- no sidereal/rate classification.
        sk_state = self._bridge.get_state()
        if sk_state.mount.connected:
            point_ra = sk_state.mount.ra_hours * 15.0
            point_dec = sk_state.mount.dec_degrees
            self._mount_ra_rate = sk_state.mount.ra_rate_dps
            self._mount_dec_rate = sk_state.mount.dec_rate_dps
        else:
            logger.warning("no live mount pointing; using scene center, sidereal")
            point_ra, point_dec = self._engine.default_point
            self._mount_ra_rate = self._mount_dec_rate = 0.0

        exposure_start = datetime.now(UTC)
        obs_time = exposure_start.isoformat()

        # Integrate for the commanded exposure: render the frame, then hold it
        # until the full integration time has elapsed so the frame is not yielded
        # early. Render time is absorbed into the exposure window (wall-clock per
        # capture ~= integration_time). Wrapped in the tracked task so an abort
        # cancels it and status reads "exposing" for the whole duration.
        loop = asyncio.get_running_loop()
        t0 = loop.time()

        async def _expose():
            # sdasim/torch render is CPU-bound -> run off the event loop.
            frame, meta = await asyncio.to_thread(
                self._engine.render_frame,
                exposure_seconds,
                point_ra,
                point_dec,
                self._mount_ra_rate,
                self._mount_dec_rate,
                obs_time,
                bin_factor,
            )
            remaining = exposure_seconds - (loop.time() - t0)
            if remaining > 0:
                await asyncio.sleep(remaining)
            return frame, meta

        async with self._capture_lock:
            self._capture_task = asyncio.ensure_future(_expose())
            try:
                image, meta = await self._capture_task
            finally:
                self._capture_task = None

        self._num_targets = meta.get("num_targets")

        height, width = image.shape
        dtype = str(image.dtype)
        image_bytes = await asyncio.to_thread(np.ascontiguousarray(image).tobytes)

        # Build the data context consumed by the DataGraph (array_to_fits, etc.).
        context = cmd.context
        context.set(ArrayInfo(shape=(height, width), dtype=dtype))
        context["bitpix"] = _DTYPE_TO_BITPIX.get(dtype, 16)
        context["bscale"] = 1
        context["bzero"] = _DTYPE_TO_BZERO.get(dtype, 32768)
        context["date_obs"] = str(exposure_start)
        context["exptime"] = exposure_seconds
        context["instrume"] = str(sk.device().entity)
        context["ccdtemp"] = self._temperature
        context["readoutm"] = self.config.readout_mode
        context["xbinning"] = bin_factor
        context["ybinning"] = bin_factor

        if not context.get("file_name", None):
            context["file_name"] = f"{uuid.uuid1()}.fits"

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

    async def status_publish(self):
        while True:
            try:
                device = sk.device()
                await device.publish(Connected(is_connected=bool(self.device_connected)))

                if self.device_connected:
                    await device.publish(Binning(x=self._bin_x, y=self._bin_y))

                    if self._engine.initialized:
                        await device.publish(
                            CameraSensorSize(
                                x=self._engine.sensor_width, y=self._engine.sensor_height
                            )
                        )

                    await device.publish(
                        CameraSensorTemperature(
                            temperature=self._temperature, units=TemperatureUnit.CELSIUS
                        )
                    )

                    state = self._bridge.get_state()
                    await device.publish(
                        SdasimCameraStatus(
                            connected=True,
                            camera_state="exposing"
                            if (self._capture_task is not None and not self._capture_task.done())
                            else "idle",
                            sensor_width=self._engine.sensor_width or None,
                            sensor_height=self._engine.sensor_height or None,
                            bin_x=self._bin_x,
                            bin_y=self._bin_y,
                            temperature=self._temperature,
                            mount_ra_rate=self._mount_ra_rate,
                            mount_dec_rate=self._mount_dec_rate,
                            num_targets=self._num_targets,
                            catalog_enabled=self._engine.catalog_enabled,
                            mount_connected=state.mount.connected,
                            rotator_connected=state.rotator.connected,
                        )
                    )
            except Exception as e:
                logger.exception(f"Error in sdasim camera status publish: {e}")

            await asyncio.sleep(self.config.status_frequency)


class SdasimCameraConfig(SdasimDeviceConfig[SdasimCamera]):
    """Configuration for a simulated sdasim camera."""

    device_type: Literal["camera"] = "camera"
    sdasim_config: str  # path to the sdasim scene YAML (required)
    mount_entity: str | None = None
    rotator_entity: str | None = None
    device: str = "cpu"  # torch device for sdasim ("cpu", "cuda", "mps", "auto")
    temperature: float = -10.0  # simulated cooler setpoint (°C)
    binning: int = 1  # default symmetric binning factor
    readout_mode: int = 0  # reported as FITS READOUTM (single simulated mode)
    rebuild_threshold_deg: float = 0.25  # rebuild the Scene when pointing drifts past this

    @override
    def create_device(self):
        return SdasimCamera(self)


class SdasimCameraState(SdasimDeviceState):
    """Persistent state for a simulated sdasim camera."""

    device_type: Literal["camera"] = "camera"
    bin_x: int = 1
    bin_y: int = 1
