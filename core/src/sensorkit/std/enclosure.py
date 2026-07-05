# SPDX-License-Identifier: Apache-2.0
from typing import Literal

from pydantic import BaseModel

import sensorkit.api as sk


class EnclosureAperture(BaseModel):
    """Configuration for enclosure aperture opening dimensions.

    Specifies the width (azimuth) and height (altitude) of the aperture opening to accommodate
    the field of view of a camera or other instrument while blocking out unwanted light sources
    such as the moon or other bright objects.

    Attributes:
        min_azimuth_span: Minimum azimuth span (width) of the aperture opening in degrees.
            Defaults to 0.0 (fully closed).
        min_altitude_span: Minimum altitude span (height) of the aperture opening in degrees.
            Defaults to 0.0 (fully closed).
        bounds_policy: Policy for handling out-of-bounds aperture requests. "clip" will adjust
            the aperture to valid bounds, while "error" will raise an error. Defaults to "error".
    """

    min_azimuth_span: float = 0.0
    min_altitude_span: float = 0.0
    bounds_policy: Literal["clip", "error"] = "error"


class ControlEnclosureAperture(sk.DeviceCommand):
    """Move the enclosure aperture to a specified state.

    This is the full-control command for enclosure aperture management. Only enclosures that allow
    custom aperture configurations will implement this command for advanced aperture control scenarios.

    All enclosures must minimally implement OpenEnclosure and CloseEnclosure, which are
    limited specializations of this command for basic open/close operations.

    Args:
        state: Either a predefined state ("full_open", "full_close") or a custom
            EnclosureAperture configuration specifying azimuth/altitude radius and bounds policy.
    """

    state: Literal["full_open", "full_close"] | EnclosureAperture = "full_open"


class OpenEnclosure(ControlEnclosureAperture):
    """Fully open the enclosure aperture."""

    state: Literal["full_open"] = "full_open"


class CloseEnclosure(ControlEnclosureAperture):
    """Fully close the enclosure aperture."""

    state: Literal["full_close"] = "full_close"


class MoveEnclosure(sk.DeviceCommand):
    """Rotate the enclosure and adjust its aperture to accommodate a target.

    This command rotates the enclosure to the specified azimuth position and, if the
    `ControlEnclosureAperture` command is supported, centers the aperture to frame the target.
    This is typically used to synchronize the enclosure position with a telescope mount's pointing
    direction.

    Attributes:
        target_azimuth: target azimuth position in degrees
        target_altitude: target altitude position in degrees, or None to leave unchanged
    """

    target_azimuth: float
    target_altitude: float | None


StandardEnclosure = sk.declare_archetype(
    "enclosure",
    required_commands=(OpenEnclosure, CloseEnclosure),
    optional_commands=(MoveEnclosure, ControlEnclosureAperture),
)
"""Standard enclosure archetype for observatory enclosures and domes.

A StandardEnclosure provides basic open/close functionality and optional advanced features
like rotation and aperture control. All enclosures must support opening and closing operations,
while movement to specific positions and custom aperture configurations are optional capabilities
for more sophisticated enclosure systems.
"""
