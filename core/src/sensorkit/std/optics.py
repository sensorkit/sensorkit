# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field

import sensorkit.api as sk


@sk.declare_keyword
class Filter(BaseModel):
    """Represents an optical filter with its properties."""

    name: str
    position: int | None = None
    wavelength: float | None = None
    focus_offset: float | None = None

    def get_fits_cards(self):
        yield "FILTER", (self.name, "Active optical filter")


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

    # The focuser's configured default/home position, established at device init — the anchor
    # that per-filter offsets and FocusCorrection are relative to. None until the device module
    # publishes it (see e.g. the alpaca focuser's `default_position`).
    base_position: float | None = None
    current_position: float

    def get_fits_cards(self):
        yield "FOCUSPOS", (self.current_position, "Focuser position [steps]")


class ChangeFocusPosition(sk.DeviceCommand):
    """Command to change the focus position to a specified value."""

    position: float


@sk.declare_keyword
class FocusCorrection(BaseModel):
    """Standing focus correction from an advisor (e.g. the autofocus analyzer).

    `position` is the RESIDUAL the advisor has learned, relative to the focuser's base position
    plus the active filter's offset. It is posted to the focuser entity's KV rather than commanded
    at the device: at every capture whose task carries no explicit `focus_position`,
    `sensor_collect` drives the focuser to `adapt(...)`. The correction is therefore a persistent
    calibration term, not a one-shot command — restarts and manual moves simply converge back to
    the corrected focus at the next capture. (Disabling the ADVISOR — e.g. the autofocus
    analyzer's enable flag — stops the residual being updated; base + filter offset are always
    applied regardless.)
    """

    position: float = 0.0

    def adapt(self, focus: FocusPosition | None, filter: Filter | None = None) -> float | None:
        """Absolute focuser target for a capture: base + per-filter offset + this correction.

        Returns None when the focuser has not published a base position (device not yet
        initialized, or a device module that predates `default_position`).
        """
        if focus is None or focus.base_position is None:
            return None
        offset = (filter.focus_offset or 0.0) if filter is not None else 0.0
        return focus.base_position + offset + self.position


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
    required_commands=(
        OpenMirrorCover,
        CloseMirrorCover,
    ),
)
"""A device representing a mirror cover that can be opened and closed."""
