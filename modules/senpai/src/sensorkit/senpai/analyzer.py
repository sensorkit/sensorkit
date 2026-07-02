from __future__ import annotations

import asyncio
import logging
import time
from logging.handlers import RotatingFileHandler

from loguru import logger

import sensorkit.api as sk
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.models import SenpaiConfig
from sensorkit.senpai.pipeline import SenpaiPipeline

# Default destination for the SENPAI engine's (stdlib `logging`) output.
DEFAULT_ENGINE_LOG_PATH = "/opt/sk/senpai.log"

# Libraries that bolt their *own* stderr handler on import (so they bypass the
# root logger). We import them eagerly to strip those handlers.
_OWN_HANDLER_LIBS = ("astropy", "astroquery")


def redirect_engine_logging(path: str = DEFAULT_ENGINE_LOG_PATH) -> None:
    """Route the SENPAI engine's stdlib logging — and its astro deps — to a file.

    Importing `senpai` runs its `setup_logging()`, which attaches a console
    handler to the *root* logger; `astropy`/`astroquery` additionally bolt
    their own stderr handlers on import. The net effect is that every stdlib
    record — `senpai.*`, `astroeasy.*`, `astroquery`, and `astropy`
    WCS/fit warnings — echoes onto the console beside SensorKit's loguru output.
    Funnel all of it into a dedicated rotating file and leave the console to
    loguru. SensorKit's own logging is loguru and is untouched. Idempotent.
    """
    import senpai  # noqa: F401  -- ensure the engine's setup_logging() has run

    file_handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
        )
    )

    def _drop_console(lg: logging.Logger) -> None:
        # Remove console (stream→tty) handlers but keep file handlers, which may
        # be shared with other loggers (e.g. the engine's uvicorn config), so we
        # detach without closing them.
        for h in list(lg.handlers):
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                lg.removeHandler(h)

    # Own the root logger: drop the engine's console + site-packages file handler
    # and funnel everything that propagates here into our file.
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(file_handler)
    root.setLevel(logging.INFO)

    # astropy/astroquery keep their own stderr handler regardless of the root;
    # strip it so their warnings flow (via propagation) to our file instead.
    for name in _OWN_HANDLER_LIBS:
        try:
            __import__(name)
        except Exception:
            continue
        _drop_console(logging.getLogger(name))

    # Make sure the senpai namespace flows to the root's file handler.
    senpai_logger = logging.getLogger("senpai")
    _drop_console(senpai_logger)
    senpai_logger.propagate = True


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
        redirect_engine_logging()
        logger.info("Starting SenpaiAnalyzer")
        self._tasks.append(asyncio.create_task(self._process_frames()))

    @sk.on_detach
    async def entity_deinit(self):
        logger.info("Stopping SenpaiAnalyzer")
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
