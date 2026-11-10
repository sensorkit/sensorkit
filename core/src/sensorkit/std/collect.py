# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, Self

from pydantic import BaseModel, Field, model_validator

import sensorkit.api as sk
from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import (
    AltAzTarget,
    EphemerisTarget,
    FrameTarget,
    ICRSTarget,
    RateTarget,
    StateVectorTarget,
    Target,
    TLETarget,
)
from sensorkit.std.instrument import FrameType


class CameraParameterSet(BaseModel):
    """Exposure settings and frame count for a camera sequence.

    Attributes:
        instrument: Controller instrument name, or None for automatic selection.
            The controller must use a named instrument for the whole sequence
            or reject the task.
    """

    instrument: Optional[str] = Field(default=None, min_length=1)
    frame_type: FrameType = Field(default=FrameType.LIGHT)
    integration_time_seconds: float = Field(...)
    frame_count: int = Field(...)
    filter_name: Optional[str] = Field(default=None)
    readout_mode: Optional[int] = Field(default=None)
    gain: Optional[float] = Field(default=None)
    binning_x: Optional[int] = Field(default=None)
    binning_y: Optional[int] = Field(default=None)


@sk.declare_keyword
class Collect(BaseModel):
    """Requested target, camera settings, and frame number for a collect.

    The sensor adds this keyword to each capture's context. Device telemetry in
    that context records observed values separately from these requested settings.

    Attributes:
        target: Target requested for this frame's tracking mode.
        target_id: Optional target name or identifier.
        params: Requested camera settings and frame count.
        frame_number: Zero-based frame number on the assigned instrument.
    """

    target: Target
    target_id: str | None = None
    params: CameraParameterSet
    frame_number: int = 0

    @property
    def track_mode(self) -> Literal["fixed", "sidereal", "rate", "unknown"]:
        match self.target:
            case TLETarget() | RateTarget() | EphemerisTarget() | StateVectorTarget():
                return "rate"
            case ICRSTarget() | FrameTarget(frame=ReferenceFrame.ICRF):
                return "sidereal"
            case AltAzTarget() | FrameTarget(frame=ReferenceFrame.ALTAZ):
                return "fixed"
            case FrameTarget():
                return "rate"

        return "unknown"

    def get_fits_cards(self):
        yield "TARGET", (self.target_id, "Target ID")
        yield "TRKMODE", (self.track_mode, "Tracking mode")
        yield "FRAMENUM", (self.frame_number, "Frame number")


class StandardCollectTask(sk.CollectTask):
    """Request camera frame sequences while following a target.

    The sensor controller matches each parameter set to an instrument and plans
    pointing, camera configuration, capture, and cleanup. Shared devices determine
    which sequences can run concurrently. Incompatible settings are placed in
    separate configuration epochs.

    Each capture receives the execution context, requested collect metadata, and
    available telemetry from its instrument's device chain.

    Attributes:
        task_type: Discriminator for a standard collect task.
        target: Target to follow.
        camera_params: Settings for one sequence, or a list of sequences.
        end_time: Legacy advisory end time. Use execution expiry to enforce a deadline.
        sidereal_frames: Zero-based indices within each sequence that use sidereal
            tracking. Instruments capturing concurrently switch together.
        target_id: Optional target identifier. The sensor infers the NORAD ID for a
            TLE target or the object name for a catalog target when omitted.
    """

    task_type: Literal["standard_collect"] = "standard_collect"
    target: Target
    camera_params: CameraParameterSet | list[CameraParameterSet]
    end_time: datetime | None = None
    sidereal_frames: list[int] = Field(default_factory=list)
    target_id: str | None = None

    @property
    def exposures(self) -> tuple[CameraParameterSet, ...]:
        """Return the requested frame sequences as a tuple."""
        if isinstance(self.camera_params, CameraParameterSet):
            return (self.camera_params,)

        return tuple(self.camera_params)

    @model_validator(mode="after")
    def _check_exposures(self) -> Self:
        if not self.exposures:
            raise ValueError("camera_params: a collect takes at least one exposure")

        return self
