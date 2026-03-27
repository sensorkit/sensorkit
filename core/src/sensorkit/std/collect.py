from datetime import datetime
from enum import StrEnum
from typing import Literal, Optional

from pydantic import BaseModel, Field

from sensorkit.astro.target import Target
from sensorkit.core.task import CollectTask


class FrameType(StrEnum):
    """Astronomical frame type for a camera capture."""
    LIGHT = "light"
    BIAS = "bias"
    DARK = "dark"
    FLAT = "flat"


class CameraParameterSet(BaseModel):
    """Camera Parameter Set"""
    integration_time_seconds: float = Field(...)
    frame_count: int = Field(...)
    binning_x: Optional[int] = Field(default=None)
    binning_y: Optional[int] = Field(default=None)
    gain: Optional[float] = Field(default=None)
    frame_type: Optional[FrameType] = Field(default=FrameType.LIGHT)
    filter_name: Optional[str] = Field(default=None)


class StandardCollectTask(CollectTask):
    """Task to collect one or more camera frames while tracking a target."""
    task_type: Literal['standard_collect'] = "standard_collect"
    target: Target
    camera_params: CameraParameterSet
    end_time: datetime
    sidereal_track_from_frame: int | None = None
