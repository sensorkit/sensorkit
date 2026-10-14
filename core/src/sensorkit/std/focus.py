# SPDX-License-Identifier: Apache-2.0
"""Focuser control and focus analysis contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, Field, FiniteFloat, model_validator

import sensorkit.api as sk
from sensorkit.std.instrument import Binning, ExposureInfo


@sk.declare_keyword
class FocusPosition(BaseModel):
    """Current position of the focuser."""

    position: float


class ChangeFocusPosition(sk.DeviceCommand):
    """Move the focuser to an absolute position."""

    position: float


StandardFocuser = sk.declare_archetype(
    "focuser",
    required_commands=(ChangeFocusPosition,),
    required_keywords=("FocusPosition",),
)
"""A device that reports and adjusts a focus position."""


class AutoFocus(sk.Task):
    """Run a fixed focus sweep for one instrument.

    The sweep visits each position in the given order and takes one light frame at each.
    Positions may repeat. Current pointing and tracking are preserved.
    The task schema is beta.

    Attributes:
        instrument: Entity name of the camera that takes the sweep.
        positions: Focuser positions to visit, one exposure per entry.
        integration_time_seconds: Exposure time of each frame.
        binning: Camera binning, or None to keep the current setting.
        gain: Camera gain, or None to keep the current setting.
        readout_mode: Camera readout mode, or None to keep the current setting.
        filter_name: Filter to select, or None to keep the current filter.
    """

    task_type: Literal["autofocus"] = "autofocus"
    instrument: str = Field(min_length=1)
    positions: list[FiniteFloat] = Field(min_length=1)
    integration_time_seconds: FiniteFloat = Field(gt=0)
    binning: Binning | None = None
    gain: FiniteFloat | None = None
    readout_mode: int | None = None
    filter_name: str | None = None


@sk.declare_keyword(kind="config")
class FocusAnalysisSource(BaseModel):
    """Analysis provider selected by a controller.

    Attributes:
        entity: The entity that publishes focus analysis results.
    """

    entity: sk.EntityRef


@sk.declare_keyword
class FocusSweepContext(BaseModel):
    """Membership and commanded position for one sweep sample.

    Attributes:
        instrument: Camera entity name, written as FOCINST.
        sweep_id: Sweep identity, written as FOCSWEEP.
        sample_index: Zero-based sample index, written as FOCSAMP.
        sample_count: Requested sample count, written as FOCCOUNT.
        commanded_position: Requested focuser position, written as FOCCMD.
    """

    instrument: str = Field(min_length=1)
    sweep_id: UUID
    sample_index: int = Field(ge=0)
    sample_count: int = Field(gt=0)
    commanded_position: FiniteFloat

    @model_validator(mode="after")
    def _check_index(self) -> Self:
        if self.sample_index >= self.sample_count:
            raise ValueError("sample_index: must be less than sample_count")

        return self

    def get_fits_cards(self):
        """Yield FITS cards identifying this sweep sample."""
        yield "FOCINST", (self.instrument, "Focus instrument")
        yield "FOCSWEEP", (str(self.sweep_id), "Focus sweep ID")
        yield "FOCSAMP", (self.sample_index, "Zero-based focus sample index")
        yield "FOCCOUNT", (self.sample_count, "Requested focus sample count")
        yield "FOCCMD", (self.commanded_position, "Commanded focus position")

    @classmethod
    def from_fits_header(cls, header: Mapping[str, object]) -> Self | None:
        """Read sweep context from FITS header values.

        Args:
            header: Header card values, such as an Astropy Header.

        Returns:
            The sweep context, or None if the header has no sweep marker.

        Raises:
            KeyError: If the header has a sweep marker but lacks another sweep card.
            ValidationError: If a sweep card value is invalid.
        """
        if "FOCSWEEP" not in header:
            return None

        return cls.model_validate(
            {
                "instrument": header["FOCINST"],
                "sweep_id": header["FOCSWEEP"],
                "sample_index": header["FOCSAMP"],
                "sample_count": header["FOCCOUNT"],
                "commanded_position": header["FOCCMD"],
            }
        )


@sk.declare_keyword
class FocusAnalysis(BaseModel):
    """Focus measurement and capture metadata for one image.

    Invalid measurements include a reason and may retain diagnostic evidence.

    Attributes:
        controller_id: The controller that owns the instrument.
        instrument: Camera entity name, independent of the camera-reported
            `exposure.instrument` label.
        image_id: Provider identifier of the analyzed image.
        exposure: Observed acquisition metadata of the image.
        reported_position: Focuser position reported at capture, if known.
        sweep: Sweep membership, or None for an image outside a sweep.
        median_fwhm_pixels: Median point-source FWHM in image pixels.
        source_count: Number of sources contributing to the measurement.
        valid: Whether the measurement is usable.
        reason: Why the measurement is unusable.
    """

    controller_id: sk.ControllerRef
    instrument: str = Field(min_length=1)
    image_id: str = Field(min_length=1)
    exposure: ExposureInfo
    reported_position: FiniteFloat | None = None
    sweep: FocusSweepContext | None = None
    median_fwhm_pixels: FiniteFloat | None = Field(default=None, gt=0)
    source_count: int = Field(ge=0)
    valid: bool
    reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_measurement(self) -> Self:
        match self:
            case FocusAnalysis(valid=True, reason=str()):
                raise ValueError("reason: a valid measurement has no failure reason")
            case FocusAnalysis(valid=True, median_fwhm_pixels=None):
                raise ValueError("median_fwhm_pixels: a valid measurement requires a FWHM")
            case FocusAnalysis(valid=True, source_count=0):
                raise ValueError("source_count: a valid measurement requires a source")
            case FocusAnalysis(valid=False, reason=None):
                raise ValueError("reason: an invalid measurement requires a reason")

        return self

    @model_validator(mode="after")
    def _check_sweep(self) -> Self:
        if self.sweep is not None and self.sweep.instrument != self.instrument:
            raise ValueError("sweep: instrument must match the analysis instrument")

        return self


@sk.declare_keyword
class FocusSweepAnalysis(BaseModel):
    """Terminal fit outcome and image evidence for one sweep.

    Success proposes a focus position for the controller to apply.
    Failure includes a reason and may retain partial evidence and a rejected fit residual.

    Attributes:
        controller_id: The controller that owns the instrument.
        instrument: Entity name of the camera that took the sweep.
        sweep_id: Identity of the sweep.
        outcome: Whether the fit produced a proposed position.
        best_position: Proposed focuser position.
        rms_residual_pixels: Root mean square residual of the fitted FWHM values, in pixels.
        reason: Why the fit failed.
        samples: Per-image evidence available to the fit.
    """

    controller_id: sk.ControllerRef
    instrument: str = Field(min_length=1)
    sweep_id: UUID
    outcome: Literal["success", "failure"]
    best_position: FiniteFloat | None = None
    rms_residual_pixels: FiniteFloat | None = Field(default=None, ge=0)
    reason: str | None = Field(default=None, min_length=1)
    samples: list[FocusAnalysis] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_outcome(self) -> Self:
        match self:
            case FocusSweepAnalysis(outcome="success", reason=str()):
                raise ValueError("reason: a successful sweep has no failure reason")
            case FocusSweepAnalysis(outcome="success", best_position=None):
                raise ValueError("best_position: a successful sweep requires a position")
            case FocusSweepAnalysis(outcome="success", rms_residual_pixels=None):
                raise ValueError("rms_residual_pixels: a successful sweep requires a residual")
            case FocusSweepAnalysis(outcome="success") if not any(s.valid for s in self.samples):
                raise ValueError("samples: a successful sweep requires a valid sample")
            case FocusSweepAnalysis(outcome="failure", reason=None):
                raise ValueError("reason: a failed sweep requires a reason")
            case FocusSweepAnalysis(outcome="failure") if self.best_position is not None:
                raise ValueError("best_position: a failed sweep has no position")

        return self

    @model_validator(mode="after")
    def _check_samples(self) -> Self:
        for sample in self.samples:
            if sample.sweep is None or sample.sweep.sweep_id != self.sweep_id:
                raise ValueError("samples: each sample requires context from this sweep")

            if sample.controller_id != self.controller_id:
                raise ValueError("samples: controller must match the sweep controller")

            if sample.instrument != self.instrument:
                raise ValueError("samples: instrument must match the sweep instrument")

        return self


FocusProvider = sk.declare_trait(
    "focus_provider",
    required_keywords=("FocusAnalysis", "FocusSweepAnalysis"),
)
"""An entity that publishes per-image and per-sweep focus analysis."""
