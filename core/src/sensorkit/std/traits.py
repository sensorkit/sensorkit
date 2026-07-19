# SPDX-License-Identifier: Apache-2.0
import enum

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.astro.coords import Coordinates


class TemperatureUnit(enum.StrEnum):
    """Unit of measurement for temperature readings."""
    FAHRENHEIT = "f"
    CELSIUS = "c"
    KELVIN = "k"


@sk.declare_keyword
class Temperature(BaseModel):
    """Current temperature reading with its associated unit."""
    temperature: float
    units: TemperatureUnit


@sk.declare_keyword
class Connected(BaseModel):
    """Connection status keyword for a device."""
    is_connected: bool


class Connect(sk.DeviceCommand):
    """Establish a connection to a device."""


class Disconnect(sk.DeviceCommand):
    """Terminate an existing connection to a device."""


MustConnect = sk.declare_trait(
    "MustConnect",
    required_commands=(Connect, Disconnect),
)


@sk.declare_keyword
class Enabled(BaseModel):
    is_enabled: bool


class Enable(sk.DeviceCommand):
    """Command to enable a device or feature."""


class Disable(sk.DeviceCommand):
    """Command to disable a device or feature."""


MustEnable = sk.declare_trait(
    "MustEnable",
    required_commands=(Enable, Disable),
)


class Init(sk.DeviceCommand):
    """Initialize a device, bringing it to an operational state."""


class Deinit(sk.DeviceCommand):
    """Deinitialize a device, returning it to a safe idle state."""


class Home(sk.DeviceCommand):
    """Move a device to its home position."""


class Stop(sk.DeviceCommand):
    """Stop all device motion or activity."""


class MoveToPark(sk.DeviceCommand):
    """Move a device to its park position."""


class SetParkPosition(sk.DeviceCommand):
    """Set the position a device moves to when parked."""

    position: Coordinates


@sk.declare_keyword
class Opened(BaseModel):
    """Keyword reporting whether a device (e.g., dome, mirror cover) is currently open."""

    is_open: bool
