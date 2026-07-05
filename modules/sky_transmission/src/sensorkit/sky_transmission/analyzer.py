# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from fnmatch import fnmatch
import pathlib
import time

from loguru import logger
import numpy as np

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing
from sensorkit.common.filewatch import FileEventKind, watch_dir
from sensorkit.sky_transmission.models import (
    SkyTransmissionConfig,
    FrameState,
    SkyTransmission,
    SkyTransmissionMap,
)
from sensorkit.sky_transmission.pipeline import AllClearPipeline, MovieBuilder
from sensorkit.sky_transmission.server import start_image_server
from sensorkit.std.instrument import CameraCapture
from sensorkit.std.sensor import Capabilities


@sk.declare_entity
class SkyTransmissionAnalyzer:
    """Continuously processes all-sky camera frames via allclear and publishes sky transmission."""

    def __init__(self, config: SkyTransmissionConfig):
        self.config = config

        self._pipeline = AllClearPipeline(self.config.allclear)
        self._movie_builder = MovieBuilder()

        self.state = FrameState()

        self._pointings: dict[str, tuple[float, float]] = {}

        self._entity = None
        self._tasks: list[asyncio.Task] = []

    @sk.on_attach
    async def entity_init(self):
        self._entity = sk.entity()
        self._sk = self._entity.sensorkit()
        logger.info("starting SkyTransmissionAnalyzer")

        for mount in await self._discover_mounts():
            self._tasks.append(asyncio.create_task(self._monitor_pointing(mount)))

        acquisition = self.config.acquisition
        if acquisition.mode not in ("watch_directory", "alpaca"):
            raise RuntimeError(f"unknown acquisition mode {acquisition.mode}")

        # Both modes consume frames from watch_path via the directory watcher.
        # In "alpaca" mode we additionally trigger the camera; its DataGraph
        # producer writes frames into watch_path for the watcher to pick up.
        self._tasks.append(asyncio.create_task(self._watch_directory_loop()))
        if acquisition.mode == "alpaca":
            if not acquisition.camera:
                raise RuntimeError("alpaca acquisition mode requires acquisition.camera")
            self._tasks.append(asyncio.create_task(self._capture_trigger_loop()))

        self._tasks.append(
            asyncio.create_task(
                start_image_server(
                    self.config.server.host,
                    self.config.server.port,
                    self.state,
                )
            )
        )

    @sk.on_detach
    async def entity_deinit(self):
        logger.info("stopping SkyTransmissionAnalyzer")
        for task in self._tasks:
            task.cancel()

    async def _discover_mounts(self) -> set[str]:
        """Mount entities controlled by any configured controller, for overlay.

        Each controller publishes its device map (Capabilities). We don't need
        an explicit `mounts` list — we derive it from `controllers`.
        """
        mounts: set[str] = set()
        for controller in self.config.controllers:
            try:
                caps = await self._sk.controller(controller).kv_get_model(Capabilities)
            except Exception:
                logger.warning(f"could not get devices for controller '{controller}'")
                continue
            if caps.devices.mount:
                mounts.add(caps.devices.mount)
        return mounts

    async def _monitor_pointing(self, mount: str):
        try:
            client = self._sk.entity(mount)
            async for _, pointing in await client.monitor(AltAzPointing):
                self._pointings[mount] = (
                    pointing.azimuth_degrees,
                    pointing.altitude_degrees,
                )
        except Exception:
            logger.warning(f"pointing monitor failed for {mount}")

    async def _watch_directory_loop(self):
        acquisition = self.config.acquisition
        watch_path = pathlib.Path(acquisition.watch_path)
        patterns = (
            acquisition.watch_pattern
            if isinstance(acquisition.watch_pattern, list)
            else [acquisition.watch_pattern]
        )

        await asyncio.to_thread(watch_path.mkdir, parents=True, exist_ok=True)

        events = await watch_dir(
            watch_path,
            recursive=acquisition.watch_recursive,
            kinds=(FileEventKind.CREATED, FileEventKind.MOVED),
        )

        logger.debug(
            f"watching {watch_path} for {patterns} "
            f"(recursive={acquisition.watch_recursive})"
        )

        try:
            async for event in events:
                if event.is_directory:
                    continue
                path = event.path
                if not any(fnmatch(path.name, p) for p in patterns):
                    continue
                if not await self._wait_for_stable_file(path):
                    logger.warning(f"giving up on unstable file {path}")
                    continue
                try:
                    await self._process_frame(image_path=str(path))
                except Exception:
                    logger.exception(f"frame processing error for {path}")
        finally:
            await events.aclose()

    async def _wait_for_stable_file(
        self,
        path: pathlib.Path,
        *,
        poll_interval: float = 0.5,
        stable_for: float = 1.0,
        timeout: float = 60.0,
    ) -> bool:
        """Wait until *path* exists, is non-empty, and its size has been unchanged
        for *stable_for* seconds (so the writer has flushed and closed). Returns
        False if the file never stabilises within *timeout*."""
        deadline = time.monotonic() + timeout
        last_size = -1
        stable_since: float | None = None

        while time.monotonic() < deadline:
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                size = -1

            now = time.monotonic()
            if size > 0 and size == last_size:
                stable_since = stable_since or now
                if now - stable_since >= stable_for:
                    return True
            else:
                stable_since = None
                last_size = size

            await asyncio.sleep(poll_interval)

        return False

    async def _capture_trigger_loop(self):
        """Periodically trigger the all-sky camera (alpaca acquisition mode).

        The camera is a SensorKit alpaca-module entity whose DataGraph producer
        (app_source -> array_to_fits -> write_file) writes each frame into
        watch_path; _watch_directory_loop then consumes it. We only issue the
        capture command here, on the configured interval.
        """
        acquisition = self.config.acquisition
        camera = self._sk.device(acquisition.camera)
        backoff = 1.0

        while True:
            t0 = time.monotonic()
            try:
                # context=None: a free-running all-sky capture has no tasking
                # context, so the camera's producer graph must supply FITS
                # metadata (DATE-OBS/EXPTIME) itself rather than reading tasking
                # keys. Verify end-to-end against the real camera + data_flow.
                await camera.command(
                    CameraCapture(
                        integration_time=acquisition.exposure_time_seconds,
                        context=None,
                    )
                )
                backoff = 1.0
            except Exception:
                logger.warning(
                    f"all-sky capture trigger failed for '{acquisition.camera}'; "
                    f"retrying in {backoff:.0f}s"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue

            elapsed = time.monotonic() - t0
            sleep_time = max(0.0, acquisition.interval_seconds - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    async def _process_frame(self, image_path: str):
        logger.debug(f"processing frame: {image_path}")
        t0 = time.monotonic()

        pointings = dict(self._pointings) if self._pointings else None

        result = await asyncio.to_thread(
            self._pipeline.process_frame,
            image_path,
            pointings=pointings,
            output_dir=self.config.output.directory,
        )

        elapsed = time.monotonic() - t0
        logger.debug(
            f"frame processed in {elapsed:.1f}s: "
            f"clear={result.clear_fraction:.0%}, "
            f"matched={result.n_matched}/{result.n_expected}, "
            f"status={result.status}"
        )

        # Metrics are published via the SkyTransmission keyword below; the
        # server only needs the latest annotated JPEG for the stream.
        if result.annotated_jpeg:
            self.state.latest_jpeg = result.annotated_jpeg
            self.state.frame_id += 1

        await self._entity.publish(
            SkyTransmission(
                clear_fraction=round(result.clear_fraction, 2),
                n_matched=result.n_matched,
                n_expected=result.n_expected,
                threshold=self.config.allclear.clear_threshold,
                status=result.status,
            )
        )

        if result.sky_result is not None:
            tmap = result.sky_result.transmission_map
            await self._entity.publish(
                SkyTransmissionMap(
                    alt_grid=tmap.alt_grid.tolist(),
                    az_grid=tmap.az_grid.tolist(),
                    transmission=np.where(
                        np.isfinite(tmap.transmission), tmap.transmission, -1.0
                    ).tolist(),
                    obs_time=str(result.sky_result.obs_time),
                    site_lat=result.sky_result.site_lat,
                    site_lon=result.sky_result.site_lon,
                )
            )

        if result.annotated_path:
            movie_path = str(
                pathlib.Path(self.config.output.directory) / "latest.mp4"
            )
            await asyncio.to_thread(
                self._movie_builder.build_movie,
                self.config.output.directory,
                movie_path,
                self.config.output.movie_lookback_hours,
            )

        await asyncio.to_thread(self._prune_old_frames_sync)

    def _prune_old_frames_sync(self):
        cutoff = time.time() - self.config.output.retention_hours * 3600

        out_dir = pathlib.Path(self.config.output.directory)
        if out_dir.exists():
            for p in out_dir.glob("*_transmission.png"):
                if p.stat().st_mtime < cutoff:
                    p.unlink(missing_ok=True)

        watch_path = self.config.acquisition.watch_path
        if watch_path:
            wp = pathlib.Path(watch_path)
            if wp.exists():
                patterns = self.config.acquisition.watch_pattern
                if isinstance(patterns, str):
                    patterns = [patterns]
                for pat in patterns:
                    for p in wp.glob(pat):
                        if p.stat().st_mtime < cutoff:
                            p.unlink(missing_ok=True)
