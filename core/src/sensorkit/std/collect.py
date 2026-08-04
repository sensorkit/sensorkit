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
    """Camera Parameter Set"""

    integration_time_seconds: float = Field(...)
    frame_count: int = Field(...)
    binning_x: Optional[int] = Field(default=None)
    binning_y: Optional[int] = Field(default=None)
    gain: Optional[float] = Field(default=None)
    frame_type: Optional[FrameType] = Field(default=FrameType.LIGHT)
    filter_name: Optional[str] = Field(default=None)


@sk.declare_keyword
class Collect(BaseModel):
    """Collect context information snapshot.

    This keyword represents a snapshot of the complete collect state at a specific point
    in time during collection—for example, before an individual frame is captured. It
    carries the target being observed, camera parameters, and the current position in
    the frame sequence. It is primarily used with the standard collect task and other
    compatible observation tasks.

    Attributes:
        target: The astronomical target to observe (can be various target types like TLE,
            catalog object, ephemeris, etc.)
        target_id: Optional name or identifier of the object being observed.
        params: Camera configuration parameters including integration time, frame count,
            binning, gain, frame type, and filter
        frame_number: Current frame number in the collection sequence (default: 0)
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
    """Task to collect one or more camera frames while tracking a target.

    This task executes a standard observation by configuring a camera to capture a sequence
    of frames while a mount tracks a specified target. It handles the complete observation
    workflow including target tracking, camera configuration, auxiliary device sequencing,
    and frame acquisition.

    Implementations must build and propagate context data to any operations that require it.
    The base context must be taken from the `TaskExecution`. (FIXME: do this internally)

    Mandatory context keywords:
      - Collect: Snapshot of the current collect state, prior to each operation
      - AltAzPointing
      - RADecPointing
      - AxisRates
      - SitePosition

    Several exposures may be asked for at once, as a list. Whether they are taken
    together or in turn is not the task's to say: each is matched to an instrument
    that can take it, and a sensor with more than one takes them concurrently while
    a sensor with one takes them in sequence.

    Attributes:
        task_type: Always "standard_collect" for this task type
        target: The astronomical target to observe (e.g., TLE, catalog object, ephemeris,
            ICRS coordinates, alt-az position, etc.)
        camera_params: Camera configuration including integration time, frame count,
            binning, gain, frame type, and filter selection. A list asks for one
            exposure per element
        end_time: *Advisory* deadline for task completion. Deprecated and slated for removal
        sidereal_frames: Frame numbers that should use sidereal tracking mode, counted
            within each exposure. Every instrument exposing at once switches together,
            since the tracking mode is the mount's rather than a camera's. Defaults to
            empty list
        target_id: Optional name or identifier of the object being observed. If not supplied,
            the handler may infer an identifier (e.g., NORAD ID for TLE targets, catalog name
            for catalog targets) on a per-target basis
    """

    task_type: Literal["standard_collect"] = "standard_collect"
    target: Target
    camera_params: CameraParameterSet | list[CameraParameterSet]
    end_time: datetime | None = None
    sidereal_frames: list[int] = Field(default_factory=list)
    target_id: str | None = None

    @property
    def exposures(self) -> tuple[CameraParameterSet, ...]:
        """Every exposure asked for, however the task wrote them."""
        if isinstance(self.camera_params, CameraParameterSet):
            return (self.camera_params,)

        return tuple(self.camera_params)

    @model_validator(mode="after")
    def _check_exposures(self) -> Self:
        if not self.exposures:
            raise ValueError("camera_params: a collect takes at least one exposure")

        return self
