from __future__ import annotations

import asyncio
import time

from loguru import logger

import sensorkit.api as sk
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.models import SenpaiConfig
from sensorkit.senpai.pipeline import SenpaiPipeline


@sk.declare_entity
class SenpaiAnalyzer:
    """Continuously processes frames via SENPAI and publishes SenpaiResults."""

    def __init__(self, config: SenpaiConfig):
        self.config = config

        self._pipeline = SenpaiPipeline(self.config.senpai_config, self.config.senpai_output_dir)

        self._entity = None
        self._tasks: list[asyncio.Task] = []

    @sk.on_attach
    async def entity_init(self):
        self._entity = sk.entity()
        logger.info(f"Starting SenpaiAnalyzer for {self.config.entity}")
        self._tasks.append(asyncio.create_task(self._process_frames()))

    @sk.on_detach
    async def entity_deinit(self):
        logger.info(f"Stopping SenpaiAnalyzer for {self.config.entity}")
        for task in self._tasks:
            task.cancel()

    async def _process_frames(self):
        """Consume FITS files from the DataGraph, run the pipeline, publish results."""
        try:
            graph = await self._entity.data_graph()
            if graph is None:
                logger.warning("No DataGraph configured for senpai analyzer")
                return

            sink = graph.app_sink()
            async for context, data in sink.consume():
                try:
                    info = context.get(FileInfo)
                    file_path = info.path if info else ""
                    logger.debug(f"processing frame: {file_path}")
                    t0 = time.monotonic()

                    # Spawn the frame processor
                    result = await asyncio.to_thread(
                        self._pipeline.process_frame,
                        data,
                        file_path,
                    )

                    elapsed = time.monotonic() - t0
                    fwhm_str = (
                        f"{result.median_fwhm_arcsec:.2f}"
                        if result.median_fwhm_arcsec is not None
                        else "None"
                    )
                    logger.debug(
                        f"frame processed in {elapsed:.1f}s: n_sources={result.n_sources}, "
                        f"median_fwhm_arcsec={fwhm_str}, solved={result.solved}"
                    )

                    # Publish results back to SensorKit
                    await self._entity.publish(result)
                except Exception:
                    logger.exception("Frame processing error")
        except Exception:
            logger.exception("DataGraph consumer failed")
