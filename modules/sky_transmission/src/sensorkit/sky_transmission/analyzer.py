from __future__ import annotations

import asyncio
from fnmatch import fnmatch
import pathlib
import time
import uuid

from loguru import logger
import numpy as np
from watchdog.events import FileCreatedEvent, FileMovedEvent, FileSystemEventHandler
from watchdog.observers import Observer

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition, AltAzPointing
from sensorkit.sky_transmission.models import (
    SkyTransmissionConfig,
    FrameState,
    SkyTransmission,
    SkyTransmissionMap,
)
from sensorkit.sky_transmission.pipeline import AllClearPipeline, MovieBuilder
from sensorkit.sky_transmission.server import start_image_server


@sk.declare_entity
class SkyTransmissionAnalyzer:
    """Continuously processes all-sky camera frames via allclear and publishes sky transmission."""

    def __init__(self, config: SkyTransmissionConfig):
        self.config = config
        self._site_position: SitePosition | None = None

        self._pipeline = AllClearPipeline(self.config.allclear)
        self._movie_builder = MovieBuilder()

        self.state = FrameState(output_dir=self.config.output.directory)

        self._pointings: dict[str, tuple[float, float]] = {}

        self._entity = None
        self._tasks: list[asyncio.Task] = []

    @sk.on_attach
    async def entity_init(self):
        self._entity = sk.entity()
        self._sk = self._entity.sensorkit()
        logger.info(f"starting SkyTransmissionAnalyzer for {self.config.entity}")

        try:
            self._site_position = await self._sk.controller(
                self.config.controller
            ).kv_get_model(SitePosition)
        except Exception:
            logger.warning(
                f"could not get SitePosition from '{self.config.controller}'"
            )

        for mount in self.config.mounts:
            self._tasks.append(
                asyncio.create_task(self._monitor_pointing(mount))
            )

        if self.config.acquisition.mode == "watch_directory":
            self._tasks.append(asyncio.create_task(self._watch_directory_loop()))
        elif self.config.acquisition.mode == "alpaca":
            self._tasks.append(asyncio.create_task(self._alpaca_loop()))
        else:
            raise RuntimeError(
                f"unknown acquisition mode {self.config.acquisition.mode}"
            )

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
        logger.info(f"stopping SkyTransmissionAnalyzer for {self.config.entity}")
        for task in self._tasks:
            task.cancel()

    async def _monitor_pointing(self, mount: str):
        try:
            client = self._sk.entity(mount)
            async for _, pointing in await client.monitor(AltAzPointing):
                self._pointings[mount] = (
                    pointing.azimuth_degrees,
                    pointing.altitude_degrees,
                )
        except asyncio.CancelledError:
            raise
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

        queue: asyncio.Queue[pathlib.Path] = asyncio.Queue()
        handler = _FileHandler(queue, asyncio.get_running_loop())
        observer = Observer()
        observer.schedule(handler, str(watch_path), recursive=acquisition.watch_recursive)
        observer.start()

        logger.debug(
            f"watching {watch_path} for {patterns} "
            f"(recursive={acquisition.watch_recursive})"
        )

        try:
            while True:
                path = await queue.get()
                if not any(fnmatch(path.name, p) for p in patterns):
                    continue
                if not await self._wait_for_stable_file(path):
                    logger.warning(f"giving up on unstable file {path}")
                    continue
                try:
                    await self._process_frame(image_path=str(path))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(f"frame processing error for {path}")
        finally:
            observer.stop()

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

    async def _alpaca_loop(self):
        from alpyca.camera import Camera

        acquisition = self.config.acquisition
        url = f"http://{acquisition.alpaca_host}:{acquisition.alpaca_port}"
        backoff = 1.0

        while True:
            try:
                camera = Camera(url, acquisition.alpaca_device_number)
                camera.Connected = True
                logger.debug(
                    f"connected to Alpaca camera at {url} "
                    f"(device {acquisition.alpaca_device_number})"
                )
                backoff = 1.0

                while True:
                    t0 = time.monotonic()
                    image_path = await asyncio.to_thread(
                        self._alpaca_capture,
                        camera,
                        acquisition.exposure_time_seconds,
                    )
                    await self._process_frame(image_path)

                    elapsed = time.monotonic() - t0
                    sleep_time = max(0.0, acquisition.interval_seconds - elapsed)
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    f"Alpaca acquisition error; retrying in {backoff:.0f}s"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    def _alpaca_capture(self, camera, exposure_seconds: float) -> str:
        from astropy.io import fits

        camera.StartExposure(exposure_seconds, Light=True)
        while not camera.ImageReady:
            time.sleep(0.5)

        data = np.column_stack(camera.ImageArray).astype(np.float64)
        if data.ndim == 3:
            data = np.mean(data, axis=2)

        hdr = fits.Header()
        hdr["DATE-OBS"] = camera.LastExposureStartTime
        hdr["EXPTIME"] = camera.LastExposureDuration
        if self._site_position is not None:
            hdr["SITELAT"] = self._site_position.latitude_degrees
            hdr["SITELONG"] = self._site_position.longitude_degrees
        else:
            hdr["SITELAT"] = self._pipeline.instrument_model.site_lat
            hdr["SITELONG"] = self._pipeline.instrument_model.site_lon

        output_dir = pathlib.Path(self.config.output.directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = str(output_dir / f"{uuid.uuid4()}.fits")
        fits.PrimaryHDU(data=data, header=hdr).writeto(filename)

        return filename

    async def _process_frame(self, image_path: str):
        logger.debug(f"processing frame: {image_path}")
        t0 = time.monotonic()

        pointings = dict(self._pointings) if self._pointings else None

        site_location = None
        if self._site_position is not None:
            site_location = (
                self._site_position.latitude_degrees,
                self._site_position.longitude_degrees,
            )

        result = await asyncio.to_thread(
            self._pipeline.process_frame,
            image_path,
            pointings=pointings,
            output_dir=self.config.output.directory,
            site_location=site_location,
        )

        elapsed = time.monotonic() - t0
        logger.debug(
            f"frame processed in {elapsed:.1f}s: "
            f"clear={result.clear_fraction:.0%}, "
            f"matched={result.n_matched}/{result.n_expected}, "
            f"status={result.status}"
        )

        self.state.clear_fraction = result.clear_fraction
        self.state.n_matched = result.n_matched
        self.state.n_expected = result.n_expected
        self.state.solve_status = result.status
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


class _FileHandler(FileSystemEventHandler):
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self._queue = queue
        self._loop = loop

    def on_created(self, event: FileCreatedEvent):
        if not event.is_directory:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait, pathlib.Path(event.src_path)
            )

    def on_moved(self, event: FileMovedEvent):
        if not event.is_directory:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait, pathlib.Path(event.dest_path)
            )
