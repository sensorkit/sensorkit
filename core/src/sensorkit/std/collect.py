# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

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

    Attributes:
        task_type: Always "standard_collect" for this task type
        target: The astronomical target to observe (e.g., TLE, catalog object, ephemeris,
            ICRS coordinates, alt-az position, etc.)
        camera_params: Camera configuration including integration time, frame count,
            binning, gain, frame type, and filter selection
        end_time: *Advisory* deadline for task completion. Deprecated and slated for removal
        sidereal_frames: List of frame numbers that should use sidereal tracking mode.
            Defaults to empty list
        target_id: Optional name or identifier of the object being observed. If not supplied,
            the handler may infer an identifier (e.g., NORAD ID for TLE targets, catalog name
            for catalog targets) on a per-target basis
        focus_position: Optional focuser position (steps), and the switch between manual and
            managed focus. When SET, the handler drives the focuser exactly here and applies
            nothing else — no filter offset, no FocusCorrection — so a caller stating a focus
            (a manual/interactive capture preserving the operator's focus, or one step of an
            autofocus V-curve sweep) gets precisely what it asked for. When None, the capture is
            managed: the handler drives base + the active filter's offset + the standing
            FocusCorrection.
    """

    task_type: Literal["standard_collect"] = "standard_collect"
    target: Target
    camera_params: CameraParameterSet
    end_time: datetime | None = None
    sidereal_frames: list[int] = Field(default_factory=list)
    target_id: str | None = None
    focus_position: float | None = None
