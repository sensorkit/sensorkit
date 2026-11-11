# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Buffer
from logging.handlers import RotatingFileHandler
from pathlib import Path

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.common.keyword import get_keyword_info
from sensorkit.data.context import Context
from sensorkit.senpai import PRE_IMPORT_ROOT_HANDLERS
from sensorkit.senpai.models import SenpaiConfig, SenpaiResult
from sensorkit.senpai.pipeline import SenpaiPipeline
from sensorkit.senpai.processors import FocusSweepProcessor, StandardProcessor
from sensorkit.std.focus import FocusAnalysis, FocusSweepAnalysis, FocusSweepContext

# Libraries that bolt their *own* stderr handler on import (so they bypass the
# root logger). We import them eagerly to strip those handlers.
_OWN_HANDLER_LIBS = ("astropy", "astroquery")


def _drop_console(lg: logging.Logger) -> None:
    # Remove console (stream→tty) handlers but keep file handlers, which may
    # be shared with other loggers (e.g. the engine's uvicorn config), so we
    # detach without closing them.
    for h in list(lg.handlers):
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
            lg.removeHandler(h)


def quiet_engine_logging() -> None:
    """Undo the stdlib logging handlers the SENPAI engine installs on import.

    Importing `senpai` runs its `setup_logging()`, which attaches a console
    handler (and a rotating file inside site-packages) to the *root* logger;
    `astropy`/`astroquery` additionally bolt their own stderr handlers on. The
    net effect is that every stdlib record in the process — `senpai.*`,
    `astroeasy.*`, `httpx`, `matplotlib`, `astropy` WCS/fit warnings — echoes
    onto the console beside SensorKit's loguru output. Put the root logger back
    as the import found it and leave the console to loguru. SensorKit's own
    logging is loguru and is untouched. Idempotent.
    """
    root = logging.getLogger()
    for h in list(root.handlers):
        if h not in PRE_IMPORT_ROOT_HANDLERS:
            root.removeHandler(h)

    # That setup_logging() *replaces* the root's handlers rather than adding to
    # them, so anything an embedding application had installed is gone by now.
    for handler in PRE_IMPORT_ROOT_HANDLERS:
        if handler not in root.handlers:
            root.addHandler(handler)

    # astropy/astroquery keep their own stderr handler regardless of the root.
    for name in _OWN_HANDLER_LIBS:
        try:
            __import__(name)
        except Exception:
            continue
        _drop_console(logging.getLogger(name))

    _drop_console(logging.getLogger("senpai"))


def redirect_engine_logging(path: str) -> None:
    """Route the SENPAI engine's stdlib logging — and its astro deps — to a file.

    Every importer of this module already drops those records (see
    `quiet_engine_logging`, and the namespace muted in `sensorkit.senpai`); the
    process that actually runs the analyzer wants to keep them instead. Funnel
    everything propagating to the root into a dedicated rotating file, and
    re-open the engine namespace so its records reach it. Idempotent.
    """
    quiet_engine_logging()

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
        )
    )

    root = logging.getLogger()
    root.addHandler(file_handler)
    root.setLevel(logging.INFO)

    # The package import mutes `senpai` for processes that merely import the
    # module; this one is the analyzer, so let its records through to the file.
    logging.getLogger("senpai").propagate = True


# `senpai` is imported (via .pipeline, above) by the time this module body runs,
# so the handlers it bolted onto the root logger exist and can be taken back off.
# This runs in every process that imports the module — i.e. every process listing
# `sensorkit.senpai.service` in `sensorkit.imports`, not just the analyzer's.
quiet_engine_logging()


@sk.declare_entity
class SenpaiAnalyzer:
    """Analyze science frames and focus sweeps with SENPAI.

    Publishes `SenpaiResult` for science frames, `FocusAnalysis` for each sweep
    frame, and `FocusSweepAnalysis` for each completed or failed sweep.
    """

    def __init__(self, config: SenpaiConfig):
        self.config = config

        pipeline = SenpaiPipeline(self.config.senpai_config, self.config.senpai_output_dir)
        self._standard = StandardProcessor(pipeline, sequence=self.config.process_sequence)
        self._sweeps = FocusSweepProcessor(pipeline)
        self._processors = (self._sweeps, self._standard)

        self._entity = None
        self._tasks: list[asyncio.Task] = []

    @sk.on_attach
    async def entity_init(self):
        self._entity = sk.entity()

        for keyword in (SenpaiResult, FocusAnalysis, FocusSweepAnalysis):
            self._entity.declare_published_keyword(get_keyword_info(keyword).key)

        redirect_engine_logging(str(Path(self.config.senpai_output_dir) / "senpai_engine.log"))
        logger.info("Starting SenpaiAnalyzer")
        self._tasks.append(asyncio.create_task(self._process_frames()))

    @sk.on_detach
    async def entity_deinit(self):
        logger.info("Stopping SenpaiAnalyzer")
        for task in self._tasks:
            task.cancel()

    async def _process_frames(self):
        """Analyze DataGraph frames and publish image and sweep measurements."""
        pending: asyncio.Task | None = None
        try:
            graph = await self._entity.data_graph()
            if graph is None:
                logger.warning("No DataGraph configured for senpai analyzer")
                return

            frames = aiter(graph.app_sink().consume())
            while True:
                if pending is None:
                    pending = asyncio.ensure_future(anext(frames, None))

                done, _ = await asyncio.wait({pending}, timeout=self._wait_timeout())

                if done:
                    item = pending.result()
                    pending = None
                    if item is None:
                        break
                    try:
                        await self._handle_frame(item)
                    except Exception:
                        logger.exception("Frame processing error")
                    continue

                # Evaluate inactivity deadlines after draining queued frames,
                # since engine processing can delay frame consumption.
                await self._flush_stalled()
        except Exception:
            logger.exception("DataGraph consumer failed")
        finally:
            if pending is not None:
                pending.cancel()

    def _wait_timeout(self) -> float | None:
        """Return seconds until the next stall deadline, or None with no open groups."""
        deadlines = [
            deadline
            for processor in self._processors
            if (deadline := processor.deadline()) is not None
        ]

        if not deadlines:
            return None

        return max(min(deadlines) - time.monotonic(), 0.05)

    async def _handle_frame(self, item: tuple[Context, Buffer]) -> None:
        """Analyze one DataGraph frame and publish its measurements."""
        context, data = item

        # Sweep membership takes precedence over frame type and collect metadata.
        is_sweep = context.get(FocusSweepContext) is not None
        processor = self._sweeps if is_sweep else self._standard

        await self._publish_all(processor.handle(context, data))

    async def _flush_stalled(self) -> None:
        """Analyze partial collects and publish failures for stalled sweeps."""
        for processor in self._processors:
            await self._publish_all(processor.flush_stalled())

    async def _publish_all(self, results: AsyncIterator[BaseModel]) -> None:
        """Publish each analysis, continuing after individual publication failures."""
        async with contextlib.aclosing(results):
            async for result in results:
                try:
                    await self._entity.publish(result)
                except Exception:
                    logger.exception(f"Failed to publish {type(result).__name__}")
