# SPDX-License-Identifier: Apache-2.0
"""Process science frames and focus sweeps from the DataGraph."""

from __future__ import annotations

import io
import time
from collections.abc import AsyncIterator, Buffer, Iterator
from pathlib import Path
from uuid import UUID

from astropy.io import fits
from loguru import logger

import sensorkit.api as sk
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.focus import FocusSweep
from sensorkit.senpai.models import SenpaiResult
from sensorkit.senpai.pipeline import FrameInput, SenpaiPipeline, run_engine
from sensorkit.std.focus import FocusAnalysis, FocusPosition, FocusSweepAnalysis, FocusSweepContext
from sensorkit.std.instrument import ExposureInfo

STALL_MARGIN_S = 60.0
"""Additional seconds allowed between frames before a collect or sweep stalls.

Covers readout, dither, and file-write latency beyond the exposure time.
"""

STALL_DEFAULT_S = 120.0
"""Stall limit of a collect sequence whose frames carry no exposure time."""

MAX_CLOSED_GROUPS = 256
"""Number of closed collect and sweep keys retained to reject late deliveries."""

CALIBRATION_FRAME_TYPES = {"dark", "bias", "flat"}
"""Frame types excluded from science analysis."""

type SweepKey = tuple[str, str, UUID]
"""Controller, instrument, and sweep ID of a focus sweep."""


class FrameGroups[K, G]:
    """Collect sequences or focus sweeps awaiting more frames, keyed by identity.

    A group stalls when no frame arrives within its stall limit.
    Closed group keys are retained to identify late deliveries.
    """

    def __init__(self):
        self._open: dict[K, G] = {}
        self._stall_after: dict[K, float] = {}
        self._deadlines: dict[K, float] = {}

        # Keys of closed groups, oldest first.
        self._done: dict[K, None] = {}

    def __len__(self) -> int:
        """Return the number of open groups."""
        return len(self._open)

    def get(self, key: K) -> G | None:
        """Return the open group for this key, if any."""
        return self._open.get(key)

    def is_done(self, key: K) -> bool:
        """Return whether this key belongs to a recently closed group."""
        return key in self._done

    def open(self, key: K, group: G, stall_after_s: float) -> G:
        """Open a group and start its stall limit."""
        self._open[key] = group
        self._stall_after[key] = stall_after_s
        self.touch(key)

        return group

    def touch(self, key: K) -> None:
        """Restart the stall limit of an open group after a new frame."""
        self._deadlines[key] = time.monotonic() + self._stall_after[key]

    def close(self, key: K) -> G:
        """Close an open group and remember its key.

        Raises:
            KeyError: If the key has no open group.
        """
        group = self._open.pop(key)
        del self._stall_after[key]
        del self._deadlines[key]
        self._done[key] = None

        while len(self._done) > MAX_CLOSED_GROUPS:
            del self._done[next(iter(self._done))]

        return group

    def deadline(self) -> float | None:
        """Return the earliest monotonic stall deadline, or None with no open groups."""
        return min(self._deadlines.values(), default=None)

    def pop_stalled(self) -> Iterator[tuple[K, G]]:
        """Close and yield each stalled group.

        A group is closed as it is yielded, so the rest stay open if iteration stops early.
        """
        now = time.monotonic()

        for key in [key for key, deadline in self._deadlines.items() if now > deadline]:
            yield key, self.close(key)


def sweep_context(context: Context, data: Buffer) -> Context | None:
    """Restore focus capture metadata, or return None for a frame outside a sweep.

    A typed `FocusSweepContext` is authoritative. Otherwise a FOCSWEEP card marks a sweep
    frame, and its capture metadata is read from the cards of that FITS header.

    Raises:
        KeyError: If a marked header lacks a required card.
        ValueError: If a marked header has an invalid card value.
    """
    if context.get(FocusSweepContext) is not None:
        return context

    header = _sweep_header(data)

    if header is None:
        return None

    restored = Context(context)
    restored.set(
        FocusSweepContext.from_fits_header(header),
        sk.InstrumentInfo(name=header["INSTRUME"]),
    )

    if "SKCTRL" in header:
        restored.set_value("controller_name", header["SKCTRL"])

    for keyword in (ExposureInfo.from_fits_header(header), FocusPosition.from_fits_header(header)):
        if keyword is not None:
            restored.set(keyword)

    return restored


def _sweep_header(data: Buffer) -> fits.Header | None:
    """Find the first FITS header containing FOCSWEEP across all HDUs."""
    try:
        with fits.open(io.BytesIO(data)) as frames:
            return next((hdu.header.copy() for hdu in frames if "FOCSWEEP" in hdu.header), None)
    except Exception:
        return None


def _frame_type(data: Buffer) -> str | None:
    """IMAGETYP of a FITS payload, lowercased; None when absent or unreadable."""
    try:
        value = fits.getheader(io.BytesIO(data)).get("IMAGETYP")
    except Exception:
        return None

    return str(value).strip().lower() if value is not None else None


class StandardProcessor:
    """Process science frames and yield one `SenpaiResult` per frame.

    With sequence processing on, frames that carry a collect identity are held
    until the whole collect has arrived and then run as one sequence.
    A stalled collect is analyzed with the frames received so far.
    Frames without a collect identity are analyzed individually.
    Calibration frames are skipped.

    Attributes:
        batches: Frames of the open collects, by task ID.
    """

    def __init__(self, pipeline: SenpaiPipeline, sequence: bool):
        self.batches: FrameGroups[str, list[FrameInput]] = FrameGroups()
        self._pipeline = pipeline
        self._sequence = sequence

    def deadline(self) -> float | None:
        """Return the monotonic time at which the next open collect stalls, if any is open."""
        return self.batches.deadline()

    async def handle(self, context: Context, data: Buffer) -> AsyncIterator[SenpaiResult]:
        """Analyze one frame or add it to a collect awaiting more frames."""
        info = context.get(FileInfo)
        frame_type = _frame_type(data)

        if frame_type in CALIBRATION_FRAME_TYPES:
            logger.debug(f"skipping {frame_type} frame {info.path if info else ''}")
            return

        controller = context.get("controller_name")
        task_id = context.get("task_id")
        inp = FrameInput(
            data=data,
            file_path=str(info.path) if info else "",
            task_id=str(task_id) if task_id is not None else None,
            frame_num=context.get("frame_num"),
            frame_count=context.get("frame_count"),
            controller_name=str(controller) if controller is not None else None,
        )

        if not (self._sequence and inp.task_id and inp.frame_count):
            for result in await self._run([inp]):
                yield result

            return

        if self.batches.is_done(inp.task_id):
            logger.debug(
                f"sequence ({inp.task_id}): ignoring {inp.file_path}, "
                f"the collect has already been processed"
            )
            return

        batch = self.batches.get(inp.task_id)

        if batch is None:
            try:
                stall_after = float(context.get("exptime")) + STALL_MARGIN_S
            except (TypeError, ValueError):
                stall_after = STALL_DEFAULT_S

            batch = self.batches.open(inp.task_id, [], stall_after)

        # Duplicate file events can complete a batch early and produce a zero
        # frame interval in SENPAI's track-rate calculation.
        if inp.file_path and any(held.file_path == inp.file_path for held in batch):
            logger.debug(
                f"sequence ({inp.task_id}): ignoring duplicate delivery of {inp.file_path}"
            )
            return

        batch.append(inp)
        self.batches.touch(inp.task_id)

        if len(batch) >= int(inp.frame_count):
            self.batches.close(inp.task_id)

            for result in await self._run(batch, from_sequence=True):
                yield result

    async def flush_stalled(self) -> AsyncIterator[SenpaiResult]:
        """Run each stalled collect with the frames it received."""
        for task_id, batch in self.batches.pop_stalled():
            logger.warning(
                f"sequence ({task_id}): stalled with {len(batch)} frame(s); "
                f"processing partial batch"
            )

            try:
                results = await self._run(batch, from_sequence=True)
            except Exception:
                logger.exception("Frame processing error")
                continue

            for result in results:
                yield result

    async def _run(
        self, inputs: list[FrameInput], from_sequence: bool = False
    ) -> list[SenpaiResult]:
        if len(inputs) == 1:
            logger.debug(f"processing frame: {inputs[0].file_path}")
        else:
            logger.debug(f"processing sequence ({inputs[0].task_id}): {len(inputs)} frames")

        t0 = time.monotonic()
        results = await run_engine(self._pipeline.process_frames, inputs, from_sequence)
        logger.debug(f"processed {len(inputs)} frame(s) in {time.monotonic() - t0:.1f}s")

        for result in results:
            fwhm = result.median_fwhm_arcsec
            logger.debug(
                f"{Path(result.file_path).name}: n_sources={result.n_sources}, "
                f"median_fwhm_arcsec={f'{fwhm:.2f}' if fwhm is not None else 'None'}, "
                f"solved={result.solved}, from_sequence={result.from_sequence}"
            )

        return results


class FocusSweepProcessor:
    """Measure focus frames and fit completed sweeps.

    Each frame is measured on arrival and yields a `FocusAnalysis`.
    A sweep yields one `FocusSweepAnalysis` when all samples arrive,
    when sample metadata conflicts, or when the sweep stalls.

    Attributes:
        sweeps: Measurements of the open sweeps.
    """

    def __init__(self, pipeline: SenpaiPipeline):
        self.sweeps: FrameGroups[SweepKey, FocusSweep] = FrameGroups()
        self._pipeline = pipeline

    def deadline(self) -> float | None:
        """Return the monotonic time at which the next open sweep stalls, if any is open."""
        return self.sweeps.deadline()

    async def handle(
        self, context: Context, data: Buffer
    ) -> AsyncIterator[FocusAnalysis | FocusSweepAnalysis]:
        """Measure one sweep frame and close its sweep once every sample has arrived.

        Raises:
            KeyError: If sweep, controller, or instrument metadata is missing.
        """
        membership = context[FocusSweepContext]
        task = context.get(sk.TaskInfo)
        controller = str(task.controller_id if task is not None else context["controller_name"])
        instrument = context[sk.InstrumentInfo].name
        exposure = context.get(ExposureInfo)
        key = (controller, instrument, membership.sweep_id)
        index = membership.sample_index

        if self.sweeps.is_done(key):
            logger.debug(f"focus sweep ({membership.sweep_id}): ignoring late sample {index}")
            return

        sweep = self.sweeps.get(key)

        if sweep is None:
            sweep = self.sweeps.open(
                key,
                FocusSweep(
                    controller_id=sk.ControllerRef(controller),
                    instrument=instrument,
                    sweep_id=membership.sweep_id,
                    sample_count=membership.sample_count,
                ),
                (exposure.exposure_time if exposure else 0.0) + STALL_MARGIN_S,
            )

        reason = (
            f"sample {index} has no exposure metadata"
            if exposure is None
            else sweep.sample_conflict_reason(membership)
        )

        if reason is not None:
            self.sweeps.close(key)
            yield self._log_result(sweep.failure(reason))
            return

        if sweep.has_sample(membership):
            logger.debug(f"focus sweep ({membership.sweep_id}): ignoring duplicate sample {index}")
            return

        info = context.get(FileInfo)
        file_path = str(info.path) if info else ""
        position = context.get(FocusPosition)
        measurement = await run_engine(self._pipeline.measure_focus, data, file_path)
        sample = FocusAnalysis(
            controller_id=sweep.controller_id,
            instrument=instrument,
            image_id=file_path or f"{membership.sweep_id}/{index}",
            exposure=exposure,
            reported_position=position.position if position is not None else None,
            sweep=membership,
            median_fwhm_pixels=measurement.median_fwhm_pixels,
            measurement_count=measurement.measurement_count,
            valid=measurement.reason is None,
            reason=measurement.reason,
        )

        sweep.add(sample)
        self.sweeps.touch(key)
        yield sample

        if sweep.complete:
            self.sweeps.close(key)
            yield self._log_result(sweep.fit())

    async def flush_stalled(self) -> AsyncIterator[FocusSweepAnalysis]:
        """Fail each stalled sweep with the samples it received."""
        for _, sweep in self.sweeps.pop_stalled():
            received = f"{len(sweep.samples)} of {sweep.sample_count} samples"
            yield self._log_result(sweep.failure(f"sweep stalled with {received}"))

    def _log_result(self, result: FocusSweepAnalysis) -> FocusSweepAnalysis:
        if result.outcome == "success":
            logger.info(
                f"focus sweep ({result.sweep_id}): best focus {result.disposition} "
                f"{result.position:.1f}, rms={result.rms_residual_pixels:.2f} px"
            )
        else:
            logger.warning(f"focus sweep ({result.sweep_id}): {result.reason}")

        return result
