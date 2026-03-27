import enum

from pydantic import BaseModel

import sensorkit.api as sk


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
