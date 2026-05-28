import enum
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, Field

from sensorkit.astro.common import RADecPointing
from sensorkit.astro.coords import Coordinates
from sensorkit.astro.target import Target
from sensorkit.common.keyword import declare_keyword
from sensorkit.core.device import DeviceCommand

COMPAT = True

if COMPAT:
    # Re-exports for compat.
    from sensorkit.std.traits import (
        Connect,
        Connected,
        Disable,
        Disconnect,
        Enable,
        Enabled,
        Temperature,
        TemperatureUnit,
    )
    from sensorkit.std.instrument import (
        Binning,
        CameraCapture,
        CameraSensorSize,
        ChangeRotatorPosition,
        ConfigureCameraCooler,
        RotatorPosition,
    )
    from sensorkit.std.optics import (
        ChangeFocusPosition,
        Filter,
        Filters,
        FocusPosition,
        SetFilter,
    )
    BaseCommand = DeviceCommand


# TODO: Move to a device-specific state keyword.
@declare_keyword
class Opened(BaseModel):
    """Keyword reporting whether a device (e.g., dome, mirror cover) is currently open."""
    is_open: bool


# TODO: Overhaul the remaining interfaces below and move to mount.py

class MountAxis(enum.StrEnum):
    """Identifies a physical axis of a telescope mount."""
    ALTITUDE="altitude"
    AZIMUTH="azimuth"
    RIGHT_ASCENSION="right_ascension"
    DECLINATION="declination"

class EnableAxis(BaseCommand):
    command_id: Literal["EnableAxis"] = "EnableAxis"
    axis: MountAxis

class DisableAxis(BaseCommand):
    command_id: Literal["DisableAxis"] = "DisableAxis"
    axis: MountAxis

@declare_keyword
class AxisEnabled(BaseModel):
    """Keyword reporting whether a specific mount axis is enabled."""
    enabled: bool
    axis: MountAxis

@declare_keyword
class AxisRate(BaseModel):
    """Keyword reporting position, velocity, and acceleration for a single mount axis."""
    axis: MountAxis

    mechanical_position: float | None = Field(None, description="Current Mechanical position (degrees)")
    min_mechanical_position: float | None = Field(None, description="Maximum mechanical position (degrees)")
    max_mechanical_position: float | None = Field(None, description="Maximum mechanical position (degrees)")

    velocity: float | None = Field(None, description="Current Velocity (Degrees per Second)")
    max_velocity: float | None = Field(None, description="Max Velocity (Degrees per Second)")

    acceleration: float | None = Field(None, description="Current Acceleration (Degrees per Second ^2)")
    max_acceleration: float | None = Field(None, description="Max Acceleration (Degrees per Second ^2)")

    measured_current: float | None = Field(None, description="Measured current (Amps)")


@declare_keyword
class AxisRates(BaseModel):
    """Aggregated AxisRate data for all mount axes."""
    azimuth: AxisRate | None = None
    altitude: AxisRate | None = None
    right_ascension: AxisRate | None = None
    declination: AxisRate | None = None


@declare_keyword
class AxisTargetDistance(BaseModel):
    """Distance between current and target axis position, in arcseconds."""
    distance_arcseconds: float
    rms_error_arcseconds: float | None = None
    axis: MountAxis


@declare_keyword
class Slewing(BaseModel):
    """Indicates whether a mount is slewing."""

    is_slewing: bool


@declare_keyword
class Tracking(BaseModel):
    """Indicates whether a mount is tracking."""

    is_tracking: bool


@dataclass
class AltAzArcseconds:
    """An angular offset expressed in altitude and azimuth arcseconds."""
    azimuth_arcseconds: float
    altitude_arcseconds: float

@dataclass
class RADecArcseconds:
    """An angular offset expressed in right-ascension and declination arcseconds."""
    right_ascension_arcseconds: float
    declination_arcseconds: float

class ApplyOffset(BaseCommand):
    command_id: Literal["ApplyOffset"] = "ApplyOffset"
    offset: AltAzArcseconds | RADecArcseconds

class FollowTarget(BaseCommand):
    command_id: Literal["FollowTarget"] = "FollowTarget"
    target: Target

class SetParkPosition(BaseCommand):
    command_id: Literal["SetParkPosition"] = "SetParkPosition"
    position: Coordinates

class MoveToPark(BaseCommand):
    command_id: Literal["MoveToPark"] = "MoveToPark"


class Init(BaseCommand):
    command_id: Literal["Init"] = "Init"

class Deinit(BaseCommand):
    command_id: Literal["Deinit"] = "Deinit"

class Home(BaseCommand):
    command_id: Literal["Home"] = "Home"

class Stop(BaseCommand):
    command_id: Literal["Stop"] = "Stop"


@dataclass
class AzimuthRange:
    """A range of azimuth values defining a wrap constraint."""
    min: float
    max: float | None = None

@declare_keyword
class AzimuthWrapRange(AzimuthRange):
    """Keyword reporting the configured azimuth wrap range for the mount."""
    ...

class SetAzimuthWrapRangeMin(BaseCommand, AzimuthWrapRange):
    command_id: Literal["SetAzimuthWrapRangeMin"] = "SetAzimuthWrapRangeMin"

# Model Commands
class ModelAddPoint(BaseCommand):
    command_id: Literal["ModelAddPoint"] = "ModelAddPoint"
    point: RADecPointing

class ModelDeletePoint(BaseCommand):
    command_id: Literal["ModelDeletePoint"] = "ModelDeletePoint"
    indexes: list[int]

class ModelEnablePoint(DeviceCommand):
    command_id: Literal["ModelEnablePoint"] = "ModelEnablePoint"
    indexes: list[int]

class ModelDisablePoint(BaseCommand):
    command_id: Literal["ModelDisablePoint"] = "ModelDisablePoint"
    indexes: list[int]

class ModelClearPoints(BaseCommand):
    command_id: Literal["ModelClearPoints"] = "ModelClearPoints"

class ModelSaveAsDefault(BaseCommand):
    command_id: Literal["ModelSaveAsDefault"] = "ModelSaveAsDefault"

class ModelSave(BaseCommand):
    command_id: Literal["ModelSave"] = "ModelSave"
    filename: str

class ModelLoad(BaseCommand):
    command_id: Literal["ModelLoad"] = "ModelLoad"
    filename: str
