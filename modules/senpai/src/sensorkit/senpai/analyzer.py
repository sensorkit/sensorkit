# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import time
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from pathlib import Path

from loguru import logger

import sensorkit.api as sk
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai import PRE_IMPORT_ROOT_HANDLERS
from sensorkit.senpai.models import SenpaiConfig
from sensorkit.senpai.pipeline import FrameInput, SenpaiPipeline

# Default destination for the SENPAI engine's (stdlib `logging`) output.
DEFAULT_ENGINE_LOG_PATH = "/opt/sk/senpai.log"

# Libraries that bolt their *own* stderr handler on import (so they bypass the
# root logger). We import them eagerly to strip those handlers.
_OWN_HANDLER_LIBS = ("astropy", "astroquery")

# A sequence is stalled once no new frame has arrived for its exposure time
# plus this margin (readout, dither, file-write latency) — the next frame of a
# healthy collect always lands within that. Stalled sequences (e.g. a dropped
# exposure) are processed partially rather than parked forever.
_STALL_MARGIN_S = 60.0

# Stall limit when the context carries no exposure time.
_STALL_DEFAULT_S = 120.0

# How many finished collects to remember, so that a frame redelivered after its
# batch has run is dropped rather than opening a fresh batch that only stalls.
# Collects are short-lived, so this only has to outlive the redelivery window.
_DONE_MEMORY = 256


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


def redirect_engine_logging(path: str = DEFAULT_ENGINE_LOG_PATH) -> None:
    """Route the SENPAI engine's stdlib logging — and its astro deps — to a file.

    Every importer of this module already drops those records (see
    `quiet_engine_logging`, and the namespace muted in `sensorkit.senpai`); the
    process that actually runs the analyzer wants to keep them instead. Funnel
    everything propagating to the root into a dedicated rotating file, and
    re-open the engine namespace so its records reach it. Idempotent.
    """
    quiet_engine_logging()

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


@dataclass
class _Batch:
    """Frames accumulated for one collect sequence, keyed by task_id."""

    expected: int
    stall_after_s: float
    inputs: list[FrameInput] = field(default_factory=list)
    last_added: float = field(default_factory=time.monotonic)

    def holds(self, inp: FrameInput) -> bool:
        """Whether this frame is already in the batch (identified by its file)."""
        return bool(inp.file_path) and any(i.file_path == inp.file_path for i in self.inputs)


@sk.declare_entity
class SenpaiAnalyzer:
    """Continuously processes frames via SENPAI and publishes SenpaiResults."""

    def __init__(self, config: SenpaiConfig):
        self.config = config

        self._pipeline = SenpaiPipeline(
            self.config.senpai_config,
            self.config.senpai_output_dir,
        )

        self._entity = None
        self._tasks: list[asyncio.Task] = []

        # Open sequence batches keyed by task_id (process_sequence only).
        self._batches: dict[str, _Batch] = {}

        # Task ids whose batch has already run, oldest first (see _mark_done).
        self._done: dict[str, None] = {}

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
        """Consume FITS files from the DataGraph, run the pipeline, publish results.

        With `process_sequence` on, frames carrying a collect identity (task_id
        + frame_count in the context) accumulate into a batch that runs as one
        SENPAI collect once frame_count frames have arrived; everything else
        runs per frame. The wait below wakes either on the next frame or on the
        earliest open batch going stale, so stalled sequences flush without any
        background timer task.
        """
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

                timeout = None
                if self._batches:
                    deadline = min(
                        batch.last_added + batch.stall_after_s
                        for batch in self._batches.values()
                    )
                    timeout = max(deadline - time.monotonic(), 0.05)
                done, _ = await asyncio.wait({pending}, timeout=timeout)

                if done:
                    item = pending.result()
                    pending = None
                    if item is None:
                        break
                    context, data = item
                    try:
                        task_id = context.get("task_id")
                        info = context.get(FileInfo)
                        inp = FrameInput(
                            data=data,
                            file_path=str(info.path) if info else "",
                            task_id=str(task_id) if task_id is not None else None,
                            frame_num=context.get("frame_num"),
                            frame_count=context.get("frame_count"),
                        )

                        if not (self.config.process_sequence and inp.task_id and inp.frame_count):
                            await self._process([inp])
                        elif inp.task_id in self._done:
                            # Redelivered after its collect already ran (below).
                            logger.debug(
                                f"sequence ({inp.task_id}): ignoring {inp.file_path}, "
                                f"the collect has already been processed"
                            )
                        else:
                            batch = self._batches.get(inp.task_id)
                            if batch is None:
                                try:
                                    stall_after = float(context.get("exptime")) + _STALL_MARGIN_S
                                except (TypeError, ValueError):
                                    stall_after = _STALL_DEFAULT_S
                                batch = self._batches[inp.task_id] = _Batch(
                                    expected=int(inp.frame_count),
                                    stall_after_s=stall_after,
                                )

                            # One delivery per frame is not guaranteed: a file can be
                            # announced twice (a Docker bind mount raises `created`
                            # both when the file appears and when its data lands), and
                            # the second copy would fill the batch with a frame
                            # identical to one already in it — same timestamp, so
                            # SENPAI derives a zero frame gap, an infinite track rate,
                            # and dies on int(inf). It also completes the batch early,
                            # before the real frame arrives. Keep the first sighting.
                            if batch.holds(inp):
                                logger.debug(
                                    f"sequence ({inp.task_id}): ignoring duplicate "
                                    f"delivery of {inp.file_path}"
                                )
                            else:
                                batch.inputs.append(inp)
                                batch.last_added = time.monotonic()

                                if len(batch.inputs) >= batch.expected:
                                    del self._batches[inp.task_id]
                                    self._mark_done(inp.task_id)
                                    await self._process(batch.inputs, from_sequence=True)
                    except Exception:
                        logger.exception("Frame processing error")
                    continue

                # The wait timed out with no frame ready, so the sink is fully
                # drained: a batch past its deadline is genuinely stalled — not
                # merely unread while a long SENPAI run blocked this loop
                # (frames arriving during processing land above, refreshing
                # last_added, before this sweep can run). Flush the stalled.
                now = time.monotonic()
                stalled = [
                    task_id
                    for task_id, batch in self._batches.items()
                    if now - batch.last_added > batch.stall_after_s
                ]
                for task_id in stalled:
                    batch = self._batches.pop(task_id)
                    self._mark_done(task_id)
                    logger.warning(
                        f"sequence ({task_id}): no new frame in {batch.stall_after_s:.0f}s "
                        f"({len(batch.inputs)}/{batch.expected} frames); processing partial batch"
                    )
                    try:
                        await self._process(batch.inputs, from_sequence=True)
                    except Exception:
                        logger.exception("Frame processing error")
        except Exception:
            logger.exception("DataGraph consumer failed")
        finally:
            if pending is not None:
                pending.cancel()

    def _mark_done(self, task_id: str) -> None:
        """Remember a finished collect, dropping the oldest to stay bounded."""
        self._done[task_id] = None
        while len(self._done) > _DONE_MEMORY:
            del self._done[next(iter(self._done))]

    async def _process(self, inputs: list[FrameInput], from_sequence: bool = False):
        """Run SENPAI over the inputs and publish one SenpaiResult per frame."""
        if len(inputs) == 1:
            logger.debug(f"processing frame: {inputs[0].file_path}")
        else:
            logger.debug(f"processing sequence ({inputs[0].task_id}): {len(inputs)} frames")
        t0 = time.monotonic()

        # SENPAI's engine prints noise straight to stdout (e.g. "Found N negative
        # pixels, setting to max value ..."), bypassing the stdlib logging that
        # redirect_engine_logging() funnels to the log file. Swallow that stdout here;
        # SensorKit's console is on stderr (see common.logging), so it is unaffected.
        def _run_pipeline() -> list:
            with contextlib.redirect_stdout(io.StringIO()):
                return self._pipeline.process_frames(inputs, from_sequence)

        results = await asyncio.to_thread(_run_pipeline)

        elapsed = time.monotonic() - t0
        logger.debug(f"processed {len(inputs)} frame(s) in {elapsed:.1f}s")
        for result in results:
            fwhm_str = (
                f"{result.median_fwhm_arcsec:.2f}"
                if result.median_fwhm_arcsec is not None
                else "None"
            )
            logger.debug(
                f"{Path(result.file_path).name}: n_sources={result.n_sources}, "
                f"median_fwhm_arcsec={fwhm_str}, solved={result.solved}, "
                f"from_sequence={result.from_sequence}"
            )

            # Publish results back to SensorKit; one failed publish (e.g. a
            # payload exceeding the transport limit) must not drop the batch's
            # remaining results.
            try:
                await self._entity.publish(result)
            except Exception:
                logger.exception(f"Failed to publish result for {result.file_path}")
