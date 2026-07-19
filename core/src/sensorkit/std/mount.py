# SPDX-License-Identifier: Apache-2.0
"""Standard mount commands, keywords, traits, and archetype."""

import enum
from dataclasses import dataclass

from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import RADecPointing
from sensorkit.astro.target import Target
from sensorkit.std.traits import Deinit, Home, Init, MoveToPark, SetParkPosition, Stop


class MountAxis(enum.StrEnum):
    """Identifies a physical axis of a telescope mount."""

    ALTITUDE = "altitude"
    AZIMUTH = "azimuth"
    RIGHT_ASCENSION = "right_ascension"
    DECLINATION = "declination"


class EnableAxis(sk.DeviceCommand):
    """Enable motion control for a mount axis."""

    axis: MountAxis


class DisableAxis(sk.DeviceCommand):
    """Disable motion control for a mount axis."""

    axis: MountAxis


@sk.declare_keyword
class AxisEnabled(BaseModel):
    """Keyword reporting whether a specific mount axis is enabled."""

    enabled: bool
    axis: MountAxis


@sk.declare_keyword
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


@sk.declare_keyword
class AxisRates(BaseModel):
    """Aggregated AxisRate data for all mount axes."""

    azimuth: AxisRate | None = None
    altitude: AxisRate | None = None
    right_ascension: AxisRate | None = None
    declination: AxisRate | None = None


@sk.declare_keyword
class AxisTargetDistance(BaseModel):
    """Distance between current and target axis position, in arcseconds."""

    distance_arcseconds: float
    rms_error_arcseconds: float | None = None
    axis: MountAxis


@sk.declare_keyword
class Slewing(BaseModel):
    """Indicates whether a mount is slewing."""

    is_slewing: bool


@sk.declare_keyword
class Tracking(BaseModel):
    """Indicates whether a mount is tracking."""

    is_tracking: bool


@sk.declare_keyword
class MountStatus:
    """Mount status keyword."""
    is_moving: bool
    target_acquired: bool
    target: Target


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


class ApplyOffset(sk.DeviceCommand):
    """Apply an angular offset to the mount's current pointing."""

    offset: AltAzArcseconds | RADecArcseconds


class FollowTarget(sk.DeviceCommand):
    """Slew to a target and continuously track it."""

    target: Target


@sk.declare_keyword
class AzimuthWrapRange(BaseModel):
    """Keyword reporting the configured azimuth wrap range for the mount."""

    min: float
    max: float | None = None


class SetAzimuthWrapRangeMin(sk.DeviceCommand):
    """Set the minimum azimuth of the mount's wrap range."""

    min: float


class ModelAddPoint(sk.DeviceCommand):
    """Add a calibration point to the mount's pointing model."""

    point: RADecPointing


class ModelDeletePoint(sk.DeviceCommand):
    """Delete pointing-model points by index."""

    indexes: list[int]


class ModelEnablePoint(sk.DeviceCommand):
    """Enable pointing-model points by index."""

    indexes: list[int]


class ModelDisablePoint(sk.DeviceCommand):
    """Disable pointing-model points by index."""

    indexes: list[int]


class ModelClearPoints(sk.DeviceCommand):
    """Remove all points from the mount's pointing model."""


class ModelSave(sk.DeviceCommand):
    """Save the mount's pointing model to a named file."""

    filename: str


class ModelLoad(sk.DeviceCommand):
    """Load a mount pointing model from a named file."""

    filename: str


HasPointingModel = sk.declare_trait(
    "HasPointingModel",
    required_commands=(
        ModelAddPoint,
        ModelDeletePoint,
        ModelEnablePoint,
        ModelDisablePoint,
        ModelClearPoints,
        ModelSave,
        ModelLoad,
    ),
)
"""A mount that maintains a persistent pointing model."""


StandardMount = sk.declare_archetype(
    "mount",
    required_commands=(
        Init,
        Deinit,
        MoveToPark,
        SetParkPosition,
        Stop,
        Home,
        FollowTarget,
    ),
    optional_commands=(
        EnableAxis,
        DisableAxis,
        ApplyOffset,
    ),
)
"""A telescope mount that can follow targets and park."""
