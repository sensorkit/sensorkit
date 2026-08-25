# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import ClassVar

import numpy as np
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.common import RADecPointing
from sensorkit.common.aio import AsyncLoop
from sensorkit.data.context import ContextSubscription
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.data.fits import ArrayInfo, ImageInfo
from sensorkit.sdasim.engine import SdasimEngine
from sensorkit.std import (
    AxisRates,
    Binning,
    CameraCapture,
    CameraSensorSize,
    CameraSensorTemperature,
    ConfigureCameraCooler,
    ConfigureCameraSensor,
    Connected,
    ExposureInfo,
    FocusPosition,
    FrameType,
    RotatorPosition,
    Stop,
    TemperatureUnit,
)


class SdasimError(Exception):
    """Base exception for sdasim device errors."""


class DeviceConnectionError(SdasimError):
    """Device is not connected."""


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
    focuser_connected: bool = False
    focus_position: float | None = None  # focuser steps consumed for the last frame
    defocus_um: float | None = None  # focal shift fed to sdasim's optics model


@sk.declare_device
class SdasimCamera:
    """Simulated camera backed by the sdasim renderer.

    sdasim only ever exposes a camera, so the device is self-contained: the
    lifecycle scaffolding (connection tracking, background status loop) lives
    here rather than in a shared base class. There is no remote client to talk
    to -- the "hardware" is the in-process sdasim renderer.
    """

    device_name: ClassVar[str] = "Camera"

    def __init__(self, config: SdasimCameraConfig):
        self.config = config
        self.device_connected: bool | None = None

    # --- lifecycle scaffolding --------------------------------------------

    async def require_connected(self):
        """Raise if the simulated camera is not connected."""
        if not self.device_connected:
            raise DeviceConnectionError(f"{self.device_name} not connected")

    # --- entity lifecycle -------------------------------------------------

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
        )
        self._mount_sub: ContextSubscription | None = None
        self._rotator_sub: ContextSubscription | None = None
        self._focuser_sub: ContextSubscription | None = None
        self._bin_x = self.state.bin_x or self.config.binning
        self._bin_y = self.state.bin_y or self.config.binning
        self._temperature = self.config.temperature
        self._readout_mode = self.config.readout_mode
        self._num_targets: int | None = None
        self._mount_ra_rate = 0.0
        self._mount_dec_rate = 0.0
        self._focus_position: float | None = None
        self._defocus_um: float | None = None
        self._capture_lock = asyncio.Lock()
        self._capture_task: asyncio.Task | None = None

        await self._initialize()
        self.status_loop = AsyncLoop(
            self.status_publish, interval=self.config.status_frequency, log=True
        ).start()

    @sk.on_detach
    async def entity_deinit(self):
        await asyncio.sleep(self.config.status_frequency)
        await self.status_loop.stop()
        await self._stop_subscriptions()
        self.device_connected = False
        await sk.device().publish(Connected(is_connected=False))
        self.state.bin_x = self._bin_x
        self.state.bin_y = self._bin_y
        await sk.device().kv_put_model(self.state)

    async def _initialize(self):
        # Load the sdasim scene config (CPU-bound; off the event loop).
        await asyncio.to_thread(self._engine.initialize)

        # The in-process renderer is always reachable once initialized; there is
        # no Connect/Disconnect handshake (those commands are optional on the
        # StandardCamera archetype), so we publish connected state directly.
        self.device_connected = True
        await sk.device().publish(Connected(is_connected=True))

        # Start telemetry subscriptions for live pointing.
        await self._start_subscriptions()

        await sk.device().publish(
            CameraSensorSize(x=self._engine.sensor_width, y=self._engine.sensor_height)
        )

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

    async def camera_set_readout_mode(self, cmd: ConfigureCameraSensor):
        if cmd.readout_mode is None:
            return
        self._readout_mode = cmd.readout_mode
        logger.debug(f"set simulated camera readout mode to {self._readout_mode}")

    async def camera_set_gain(self, cmd: ConfigureCameraSensor):
        if cmd.gain is None:
            return
        logger.warning("sdasim camera has no gain model; ignoring gain")

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
    async def camera_configure_sensor(self, cmd: ConfigureCameraSensor):
        await self.camera_set_readout_mode(cmd)
        await self.camera_set_gain(cmd)
        await self.camera_set_binning(cmd)

    # --- SensorKit telemetry subscriptions --------------------------------

    async def _start_subscriptions(self):
        """Subscribe to the configured mount/rotator entities for live pointing.

        `ContextSubscription` caches each entity's latest keywords; the camera
        reads them at capture time. Mount RA/Dec rates are consumed as published:
        with an ICRF `AxisRates` producer (the alpaca telescope) a sidereal
        track reports (0, 0) and a rate track reports the tracked object's
        apparent rate -- exactly what sdasim's `apparent_rate = object_rate -
        mount_rate` model wants, no conversion.
        """
        kit = sk.device().sensorkit()

        if self.config.mount_entity:
            sub = ContextSubscription(kit.entity(self.config.mount_entity))
            sub.add(RADecPointing)
            sub.add(AxisRates)
            await sub.start()
            self._mount_sub = sub
            logger.debug(f"sdasim: subscribed to mount '{self.config.mount_entity}'")

        if self.config.rotator_entity:
            sub = ContextSubscription(kit.entity(self.config.rotator_entity))
            sub.add(RotatorPosition)
            await sub.start()
            self._rotator_sub = sub
            logger.debug(f"sdasim: subscribed to rotator '{self.config.rotator_entity}'")

        if self.config.focuser_entity:
            sub = ContextSubscription(kit.entity(self.config.focuser_entity))
            sub.add(FocusPosition)
            await sub.start()
            self._focuser_sub = sub
            logger.debug(f"sdasim: subscribed to focuser '{self.config.focuser_entity}'")

    async def _stop_subscriptions(self):
        for sub in (self._mount_sub, self._rotator_sub, self._focuser_sub):
            if sub is not None:
                await sub.stop()
        self._mount_sub = self._rotator_sub = self._focuser_sub = None

    @property
    def _mount_connected(self) -> bool:
        return self._mount_sub is not None and self._mount_sub.cache.get(RADecPointing) is not None

    @property
    def _rotator_connected(self) -> bool:
        return (
            self._rotator_sub is not None
            and self._rotator_sub.cache.get(RotatorPosition) is not None
        )

    @property
    def _focuser_connected(self) -> bool:
        return (
            self._focuser_sub is not None
            and self._focuser_sub.cache.get(FocusPosition) is not None
        )

    @staticmethod
    def _axis_velocity(rates: AxisRates | None, axis: str) -> float:
        """Inertial velocity (deg/s) for a mount axis, or 0.0 if unavailable."""
        rate = getattr(rates, axis, None) if rates is not None else None
        if rate is None or rate.velocity is None:
            return 0.0
        return rate.velocity

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
        pointing = self._mount_sub.cache.get(RADecPointing) if self._mount_sub else None
        if pointing is not None:
            point_ra = pointing.right_ascension_hours * 15.0
            point_dec = pointing.declination_degrees
            rates = self._mount_sub.cache.get(AxisRates)
            self._mount_ra_rate = self._axis_velocity(rates, "right_ascension")
            self._mount_dec_rate = self._axis_velocity(rates, "declination")
        else:
            logger.warning("no live mount pointing; using scene center, sidereal")
            point_ra, point_dec = self._engine.default_point
            self._mount_ra_rate = self._mount_dec_rate = 0.0

        # Live focuser position (if configured) -> commanded defocus in microns
        # for sdasim's optics model. Same passive-telemetry pattern as the mount:
        # whatever device implements the focuser (OmniSim, TheSky, real hardware)
        # publishes FocusPosition; the camera just reads the latest value.
        self._focus_position = None
        self._defocus_um = None
        focus = self._focuser_sub.cache.get(FocusPosition) if self._focuser_sub else None
        if focus is not None:
            self._focus_position = focus.current_position
            self._defocus_um = (
                focus.current_position - self.config.best_focus_position
            ) * self.config.microns_per_step

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
                self._defocus_um,
                cmd.frame_type,
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

        image_bytes = await asyncio.to_thread(np.ascontiguousarray(image).tobytes)

        # Build the data context consumed by the DataGraph (array_to_fits, etc.).
        context = cmd.context
        context.set(
            ImageInfo(array=ArrayInfo.from_array(image), binning=(bin_factor, bin_factor)),
            ExposureInfo(
                date_obs=exposure_start,
                exposure_time=exposure_seconds,
                instrument=str(sk.device().entity),
                image_type=FrameType(meta["frame_type"]),
                readout_mode=self._readout_mode,
                ccd_temperature=self._temperature,
                set_temperature=self._temperature,
            ),
        )

        if not context.get(FileNameTemplate):
            context.set(FileNameTemplate(template=f"{uuid.uuid1()}.fits"))

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
        device = sk.device()
        await device.publish(Connected(is_connected=bool(self.device_connected)))

        if self.device_connected:
            await device.publish(Binning(x=self._bin_x, y=self._bin_y))

            if self._engine.initialized:
                await device.publish(
                    CameraSensorSize(x=self._engine.sensor_width, y=self._engine.sensor_height)
                )

            await device.publish(
                CameraSensorTemperature(
                    temperature=self._temperature, units=TemperatureUnit.CELSIUS
                )
            )

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
                    mount_connected=self._mount_connected,
                    rotator_connected=self._rotator_connected,
                    focuser_connected=self._focuser_connected,
                    focus_position=self._focus_position,
                    defocus_um=self._defocus_um,
                )
            )


class SdasimCameraConfig(BaseModel):
    """Configuration for a simulated sdasim camera."""

    sdasim_config: str  # path to the sdasim scene YAML (required)
    mount_entity: str | None = None
    rotator_entity: str | None = None
    focuser_entity: str | None = None  # entity publishing FocusPosition (steps)
    microns_per_step: float = 1.0  # focuser mechanism scale: steps -> microns of focal shift
    best_focus_position: float = 0.0  # focuser position (steps) at best focus
    device: str = "cpu"  # torch device for sdasim ("cpu", "cuda", "mps", "auto")
    temperature: float = -10.0  # simulated cooler setpoint (°C)
    binning: int = 1  # default symmetric binning factor
    readout_mode: int = 0  # reported as FITS READOUTM (single simulated mode)
    status_frequency: float = 1.0  # telemetry publish cadence (seconds)

    def create_device(self) -> SdasimCamera:
        return SdasimCamera(self)


class SdasimCameraState(BaseModel):
    """Persistent state for a simulated sdasim camera."""

    bin_x: int = 1
    bin_y: int = 1
