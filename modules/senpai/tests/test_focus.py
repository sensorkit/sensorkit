# SPDX-License-Identifier: Apache-2.0
"""Focus sweep assembly and fitting."""

from datetime import UTC, datetime
from uuid import UUID

import pytest

import sensorkit.api as sk
from sensorkit.senpai.focus import FocusSweep, fit_focus_sweep
from sensorkit.std.focus import FocusAnalysis, FocusSweepContext
from sensorkit.std.instrument import ExposureInfo

SWEEP_ID = UUID("6f1c2b7e-3d4a-4c1b-9e2f-0a1b2c3d4e5f")
POSITIONS = [1000.0, 1050.0, 1100.0, 1150.0, 1200.0, 1250.0, 1300.0]


def v_curve(position: float, best: float = 1140.0) -> float:
    """Width of a star at a focuser position, narrowest at `best`."""
    return 2.5 + ((position - best) / 60.0) ** 2


def context(index: int, position: float, count: int = len(POSITIONS)) -> FocusSweepContext:
    return FocusSweepContext(
        sweep_id=SWEEP_ID,
        sample_index=index,
        sample_count=count,
        commanded_position=position,
    )


def sample(index: int, position: float, width: float | None, count: int = len(POSITIONS)):
    """A measurement at one sweep position, unusable when `width` is None."""
    return FocusAnalysis(
        controller_id="controller",
        instrument="camera",
        image_id=f"frame-{index}.fits",
        exposure=ExposureInfo(
            date_obs=datetime(2026, 11, 10, 8, 30, tzinfo=UTC),
            exposure_time=2.0,
            instrument="QHY600M",
        ),
        sweep=context(index, position, count),
        median_fwhm_pixels=width,
        source_count=0 if width is None else 1,
        valid=width is not None,
        reason="no stars" if width is None else None,
    )


def sweep_of(widths: list[float | None], positions: list[float] = POSITIONS):
    return [
        sample(index, position, width, count=len(positions))
        for index, (position, width) in enumerate(zip(positions, widths, strict=True))
    ]


class TestFit:
    def test_interior_minimum(self):
        result = fit_focus_sweep(sweep_of([v_curve(p) for p in POSITIONS]))

        assert (result.outcome, result.disposition) == ("success", "equals")
        assert result.position == pytest.approx(1140.0)
        assert result.rms_residual_pixels == pytest.approx(0.0, abs=1e-9)

    def test_noisy_minimum(self):
        noise = [0.04, -0.03, 0.02, -0.04, 0.03, -0.02, 0.04]
        widths = [v_curve(p) + n for p, n in zip(POSITIONS, noise, strict=True)]

        result = fit_focus_sweep(sweep_of(widths))

        assert result.disposition == "equals"
        assert result.position == pytest.approx(1140.0, abs=5.0)
        assert 0 < result.rms_residual_pixels < 0.1

    def test_arrival_order_does_not_change_fit(self):
        samples = sweep_of([v_curve(p) for p in POSITIONS])

        assert fit_focus_sweep(samples[::-1]).position == fit_focus_sweep(samples).position

    def test_repeated_positions_count_once(self):
        positions = [*POSITIONS, 1000.0, 1000.0, 1000.0]
        widths = [v_curve(p) for p in POSITIONS] + [9.0, v_curve(1000.0), 1.0]

        result = fit_focus_sweep(sweep_of(widths, positions))

        # The median of the repeats is the true width, so the outliers do not move the fit.
        assert result.position == pytest.approx(1140.0)
        assert len(result.samples) == 10
        assert result.rms_residual_pixels > 1.0

    def test_unusable_samples_are_kept_as_evidence(self):
        positions = [*POSITIONS, 1350.0]
        widths = [*(v_curve(p) for p in POSITIONS), None]

        result = fit_focus_sweep(sweep_of(widths, positions))

        assert result.position == pytest.approx(1140.0)
        assert [s.valid for s in result.samples] == [True] * 7 + [False]

    @pytest.mark.parametrize(
        "best, disposition, position",
        [
            (1500.0, "greater_than", 1300.0),
            (800.0, "less_than", 1000.0),
        ],
    )
    def test_minimum_outside_sweep_gives_direction(self, best, disposition, position):
        result = fit_focus_sweep(sweep_of([v_curve(p, best) for p in POSITIONS]))

        assert (result.outcome, result.disposition) == ("success", disposition)
        assert result.position == position

    @pytest.mark.parametrize(
        "slope, disposition, position",
        [
            (-0.01, "greater_than", 1300.0),
            (0.01, "less_than", 1000.0),
        ],
    )
    def test_linear_trend_gives_direction(self, slope, disposition, position):
        noise = [0.02, -0.02, 0.01, -0.01, 0.02, -0.02, 0.01]
        widths = [8.0 + slope * (p - 1150.0) + n for p, n in zip(POSITIONS, noise, strict=True)]

        result = fit_focus_sweep(sweep_of(widths))

        assert (result.disposition, result.position) == (disposition, position)
        assert result.rms_residual_pixels > 0

    def test_flat_widths_fail(self):
        widths = [3.05, 2.95, 3.05, 2.95, 3.05, 2.95, 3.05]

        result = fit_focus_sweep(sweep_of(widths))

        assert (result.outcome, result.position, result.disposition) == ("failure", None, None)
        assert "too little" in result.reason
        assert result.rms_residual_pixels > 0

    @pytest.mark.parametrize("count", [5, 7, 8, 10, 11])
    @pytest.mark.parametrize("width", [0.1, 3.0, 4.0, 100.0])
    def test_constant_widths_fail(self, count, width):
        positions = [1000.0 + 50.0 * index for index in range(count)]

        result = fit_focus_sweep(sweep_of([width] * count, positions))

        assert (result.outcome, result.position, result.disposition) == ("failure", None, None)

    def test_peak_inside_sweep_fails(self):
        result = fit_focus_sweep(sweep_of([10.0 - v_curve(p) for p in POSITIONS]))

        assert result.outcome == "failure"
        assert "peak" in result.reason

    def test_too_few_usable_positions_fail(self):
        widths = [v_curve(p) for p in POSITIONS[:4]] + [None, None, None]

        result = fit_focus_sweep(sweep_of(widths))

        assert result.outcome == "failure"
        assert "got 4" in result.reason
        assert result.rms_residual_pixels is None
        assert len(result.samples) == 7

    def test_repeats_do_not_count_as_distinct_positions(self):
        positions = [1000.0, 1000.0, 1050.0, 1100.0, 1150.0, 1150.0]

        result = fit_focus_sweep(sweep_of([v_curve(p) for p in positions], positions))

        assert "got 4" in result.reason


class TestFocusSweep:
    def make(self, count: int = 3) -> FocusSweep:
        return FocusSweep(
            controller_id="controller",
            instrument="camera",
            sweep_id=SWEEP_ID,
            sample_count=count,
        )

    def test_completes_on_sample_count(self):
        sweep = self.make()

        for index in (2, 0):
            sweep.add(sample(index, 1000.0 + index, 3.0, count=3))

        assert not sweep.complete

        sweep.add(sample(1, 1001.0, 3.0, count=3))

        assert sweep.complete
        assert [s.sweep.sample_index for s in sweep.evidence()] == [0, 1, 2]

    def test_holds_measured_index(self):
        sweep = self.make()
        sweep.add(sample(1, 1001.0, 3.0, count=3))

        assert sweep.holds(context(1, 1001.0, count=3))
        assert not sweep.holds(context(2, 1001.0, count=3))

    def test_same_position_at_another_index_is_no_conflict(self):
        sweep = self.make()
        sweep.add(sample(0, 1000.0, 3.0, count=3))

        assert sweep.conflict(context(1, 1000.0, count=3)) is None
        assert sweep.conflict(context(0, 1000.0, count=3)) is None

    def test_conflicting_sample_count(self):
        sweep = self.make()

        assert "expected 3" in sweep.conflict(context(0, 1000.0, count=5))

    def test_conflicting_position_for_measured_index(self):
        sweep = self.make()
        sweep.add(sample(0, 1000.0, 3.0, count=3))

        assert "different commanded position" in sweep.conflict(context(0, 1010.0, count=3))

    def test_failure_keeps_ordered_evidence(self):
        sweep = self.make()
        sweep.add(sample(2, 1002.0, 3.0, count=3))
        sweep.add(sample(0, 1000.0, None, count=3))

        result = sweep.failure("sweep stalled")

        assert (result.outcome, result.reason) == ("failure", "sweep stalled")
        assert result.controller_id == sk.ControllerRef("controller")
        assert result.instrument == "camera"
        assert [s.sweep.sample_index for s in result.samples] == [0, 2]
