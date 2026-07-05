# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field

import sensorkit.api as sk


@sk.declare_keyword
class Filter(BaseModel):
    """Represents an optical filter with its properties."""

    name: str
    position: int | None = None
    wavelength: float | None = None


@sk.declare_keyword
class Filters(BaseModel):
    """Collection of optical filters available in a filter changer."""

    filters: list[Filter] = Field(default_factory=list)


class SetFilter(sk.DeviceCommand):
    """Set the active optical filter by name or position."""

    filter: str | int


StandardFilterChanger = sk.declare_archetype(
    "filter_changer",
    required_commands=(SetFilter,),
)
"""A device that can change optical filters by name or position."""


@sk.declare_keyword
class FocusPosition(BaseModel):
    """Current position of the focuser."""

    position: float


class ChangeFocusPosition(sk.DeviceCommand):
    """Command to change the focus position to a specified value."""

    position: float


StandardFocuser = sk.declare_archetype(
    "focuser",
    required_commands=(ChangeFocusPosition,),
)
"""A device that can adjust a focus position."""


class OpenMirrorCover(sk.DeviceCommand):
    """Command to open the mirror cover."""


class CloseMirrorCover(sk.DeviceCommand):
    """Command to close the mirror cover."""


StandardMirrorCover = sk.declare_archetype(
    "mirror_cover",
    required_commands=(OpenMirrorCover,CloseMirrorCover,),
)
"""A device representing a mirror cover that can be opened and closed."""
