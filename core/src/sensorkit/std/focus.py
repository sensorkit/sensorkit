# SPDX-License-Identifier: Apache-2.0
"""Focuser commands and focus analysis schemas."""

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

    def get_fits_cards(self):
        """Yield the reported focuser position as FOCPOS."""
        yield "FOCPOS", (self.position, "Reported focus position")

    @classmethod
    def from_fits_header(cls, header: Mapping[str, object]) -> Self | None:
        """Read the reported focuser position, or None if the header has no FOCPOS card.

        Raises:
            ValidationError: If the card value is invalid.
        """
        if "FOCPOS" not in header:
            return None

        return cls.model_validate({"position": header["FOCPOS"]})


class ChangeFocusPosition(sk.DeviceCommand):
    """Move the focuser to an absolute position.

    Drivers may quantize the requested position to their supported resolution.
    Positions outside the device's travel limits fail.

    A successful response confirms that motion has ended and the final
    position matches the target within the device's supported resolution.
    Publish that position as
    `FocusPosition` before responding successfully.

    The command fails if the final position cannot be verified or differs
    from the target.
    """

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
        instrument: Name of the controller instrument that takes the sweep.
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
    """Focus analysis provider selected by a controller.

    Attributes:
        entity: The entity that publishes focus analysis results.
    """

    entity: sk.EntityRef


@sk.declare_keyword(key="FocusSweep")
class FocusSweepContext(BaseModel):
    """Sweep identity and commanded position for one sample, available in context as `FocusSweep`.

    Attributes:
        sweep_id: Unique sweep identifier, written as FOCSWEEP.
        sample_index: Zero-based sample index, written as FOCSAMP.
        sample_count: Requested sample count, written as FOCCOUNT.
        commanded_position: Requested focuser position, written as FOCCMD.
    """

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
                "sweep_id": header["FOCSWEEP"],
                "sample_index": header["FOCSAMP"],
                "sample_count": header["FOCCOUNT"],
                "commanded_position": header["FOCCMD"],
            }
        )


@sk.declare_keyword
class FocusAnalysis(BaseModel):
    """Focus measurement and capture metadata for one image.

    Invalid measurements include a reason and may retain a measured FWHM and measurement count.

    Attributes:
        controller_id: The controller that owns the instrument.
        instrument: Name of the controller instrument that took the image.
        image_id: Provider identifier of the analyzed image.
        exposure: Exposure metadata recorded at capture.
        reported_position: Focuser position reported at capture, if known.
        sweep: Sweep membership, or None for an image outside a sweep.
        median_fwhm_pixels: Median point-source FWHM in image pixels.
        measurement_count: Number of FWHM measurements represented by the result.
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
    measurement_count: int = Field(ge=0)
    valid: bool
    reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_measurement(self) -> Self:
        match self:
            case FocusAnalysis(valid=True, reason=str()):
                raise ValueError("reason: a valid measurement has no failure reason")
            case FocusAnalysis(valid=True, median_fwhm_pixels=None):
                raise ValueError("median_fwhm_pixels: a valid measurement requires a FWHM")
            case FocusAnalysis(valid=True, measurement_count=0):
                raise ValueError("measurement_count: a valid measurement requires a measurement")
            case FocusAnalysis(valid=False, reason=None):
                raise ValueError("reason: an invalid measurement requires a reason")

        return self


@sk.declare_keyword
class FocusSweepAnalysis(BaseModel):
    """Focus fit outcome and sample measurements for one sweep.

    A successful fit estimates best focus or its direction beyond the sampled range.
    A failed fit includes a reason and may retain sample measurements and a fit residual.

    Attributes:
        controller_id: The controller that owns the instrument.
        instrument: Name of the controller instrument that took the sweep.
        sweep_id: Unique sweep identifier.
        outcome: Whether the fit estimated a focus position or direction.
        position: Estimated focus position or sampled boundary, depending on disposition.
        disposition: Relationship of best focus to position. `equals` gives an
            estimated minimum. `greater_than` and `less_than` indicate best focus
            beyond the corresponding boundary of the sampled range.
        rms_residual_pixels: Root mean square residual of the fitted FWHM values, in pixels.
        reason: Why the fit failed.
        samples: Per-image measurements used to assess the sweep.
    """

    controller_id: sk.ControllerRef
    instrument: str = Field(min_length=1)
    sweep_id: UUID
    outcome: Literal["success", "failure"]
    position: FiniteFloat | None = None
    disposition: Literal["equals", "greater_than", "less_than"] | None = None
    rms_residual_pixels: FiniteFloat | None = Field(default=None, ge=0)
    reason: str | None = Field(default=None, min_length=1)
    samples: list[FocusAnalysis] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_outcome(self) -> Self:
        match self:
            case FocusSweepAnalysis(outcome="success", reason=str()):
                raise ValueError("reason: a successful sweep has no failure reason")
            case FocusSweepAnalysis(outcome="success", position=None):
                raise ValueError("position: a successful sweep requires a position")
            case FocusSweepAnalysis(outcome="success", disposition=None):
                raise ValueError("disposition: a successful sweep requires a disposition")
            case FocusSweepAnalysis(outcome="success", rms_residual_pixels=None):
                raise ValueError("rms_residual_pixels: a successful sweep requires a residual")
            case FocusSweepAnalysis(outcome="success") if not any(s.valid for s in self.samples):
                raise ValueError("samples: a successful sweep requires a valid sample")
            case FocusSweepAnalysis(outcome="failure", reason=None):
                raise ValueError("reason: a failed sweep requires a reason")
            case FocusSweepAnalysis(outcome="failure", position=float()):
                raise ValueError("position: a failed sweep has no position")
            case FocusSweepAnalysis(outcome="failure", disposition=str()):
                raise ValueError("disposition: a failed sweep has no disposition")

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
