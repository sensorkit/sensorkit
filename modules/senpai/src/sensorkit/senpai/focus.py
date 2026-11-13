# SPDX-License-Identifier: Apache-2.0
"""Focus sweep assembly and fitting."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from uuid import UUID

import numpy as np

import sensorkit.api as sk
from sensorkit.std.focus import FocusAnalysis, FocusSweepAnalysis, FocusSweepContext

MIN_FIT_POSITIONS = 5
"""Distinct usable positions required to fit a sweep."""

MIN_VARIATION_RMS_RATIO = 2.0
"""Fitted width change across the sweep, in RMS residuals, required to locate focus."""


@dataclass
class FocusSweep:
    """Measurements received for one sweep, by sample index.

    Attributes:
        controller_id: The controller that owns the instrument.
        instrument: Name of the controller instrument that takes the sweep.
        sweep_id: Unique sweep identifier.
        sample_count: Number of samples required to complete the sweep.
        samples: Measurements received so far, keyed by sample index.
    """

    controller_id: sk.ControllerRef
    instrument: str
    sweep_id: UUID
    sample_count: int
    samples: dict[int, FocusAnalysis] = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        """Whether every expected sample has been measured."""
        return len(self.samples) >= self.sample_count

    def has_sample(self, context: FocusSweepContext) -> bool:
        """Return whether the sample's index has already been measured."""
        return context.sample_index in self.samples

    def sample_conflict_reason(self, context: FocusSweepContext) -> str | None:
        """Describe a conflicting sample count or repeated position, if any."""
        index = context.sample_index

        if context.sample_count != self.sample_count:
            return (
                f"sample {index} reports {context.sample_count} samples, "
                f"expected {self.sample_count}"
            )

        held = self.samples.get(index)

        if held is not None and held.sweep.commanded_position != context.commanded_position:
            return f"sample {index} repeats with a different commanded position"

        return None

    def add(self, sample: FocusAnalysis) -> None:
        """Record the measurement of a new sample."""
        self.samples[sample.sweep.sample_index] = sample

    def ordered_samples(self) -> list[FocusAnalysis]:
        """Return the measurements ordered by sample index."""
        return [self.samples[index] for index in sorted(self.samples)]

    def failure(self, reason: str) -> FocusSweepAnalysis:
        """Build a failed sweep analysis with the measurements received so far."""
        return FocusSweepAnalysis(
            controller_id=self.controller_id,
            instrument=self.instrument,
            sweep_id=self.sweep_id,
            outcome="failure",
            reason=reason,
            samples=self.ordered_samples(),
        )

    def fit(self) -> FocusSweepAnalysis:
        """Locate best focus from the measurements received.

        Fits a quadratic to the median FWHM at each commanded position.
        A minimum inside the sampled positions is the estimated best focus.
        A fit that only rises or falls across the sweep places best focus beyond
        the end sample with the smaller fitted width.
        Fits fail with fewer than five usable positions, an interior width peak,
        a nonpositive minimum, or insufficient width variation.

        Returns:
            The focus position, direction, or failure, with the sample measurements.
        """
        samples = self.ordered_samples()
        usable = [sample for sample in samples if sample.valid]
        widths: dict[float, list[float]] = {}

        for sample in usable:
            widths.setdefault(sample.sweep.commanded_position, []).append(sample.median_fwhm_pixels)

        def result(**fields) -> FocusSweepAnalysis:
            return FocusSweepAnalysis(
                controller_id=self.controller_id,
                instrument=self.instrument,
                sweep_id=self.sweep_id,
                samples=samples,
                **fields,
            )

        if len(widths) < MIN_FIT_POSITIONS:
            return result(
                outcome="failure",
                reason=(
                    f"fit requires {MIN_FIT_POSITIONS} usable distinct positions, got {len(widths)}"
                ),
            )

        # Map the sampled interval onto [-1, 1] to keep the fit well conditioned.
        positions = sorted(widths)
        low = positions[0]
        high = positions[-1]
        center = (low + high) / 2
        scale = (high - low) / 2

        # Repeat exposures at one position count once.
        medians = [statistics.median(widths[position]) for position in positions]
        fit = np.polyfit((np.array(positions) - center) / scale, medians, 2)

        if not np.all(np.isfinite(fit)):
            return result(outcome="failure", reason="fit did not produce finite coefficients")

        curvature, slope, _ = fit.tolist()

        def fitted(x: float) -> float:
            return float(np.polyval(fit, x))

        rms = math.sqrt(
            statistics.fmean(
                (
                    sample.median_fwhm_pixels
                    - fitted((sample.sweep.commanded_position - center) / scale)
                )
                ** 2
                for sample in usable
            )
        )
        turning = -slope / (2 * curvature) if curvature != 0 else math.inf
        inside = -1 < turning < 1
        ends = (fitted(-1), fitted(1))

        if inside and curvature < 0:
            return result(
                outcome="failure",
                rms_residual_pixels=rms,
                reason="widths peak inside the sweep",
            )

        # A turning point inside the sweep is a minimum from here on.
        variation = max(ends) - fitted(turning) if inside else abs(ends[0] - ends[1])
        roundoff_tolerance = 64 * math.ulp(max(medians))
        minimum_variation = max(MIN_VARIATION_RMS_RATIO * rms, roundoff_tolerance)

        if variation <= minimum_variation:
            return result(
                outcome="failure",
                rms_residual_pixels=rms,
                reason="widths vary too little across the sweep to locate focus",
            )

        if not inside:
            # Best focus lies beyond the end with the smaller fitted width.
            _, position, disposition = min(
                (ends[0], low, "less_than"),
                (ends[1], high, "greater_than"),
            )

            return result(
                outcome="success",
                position=position,
                disposition=disposition,
                rms_residual_pixels=rms,
            )

        if fitted(turning) <= 0:
            return result(
                outcome="failure",
                rms_residual_pixels=rms,
                reason="fit predicts a nonpositive minimum width",
            )

        return result(
            outcome="success",
            position=center + turning * scale,
            disposition="equals",
            rms_residual_pixels=rms,
        )
