"""Standard weather telemetry keyword and archetype."""

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.common.keyword import declare_keyword


@declare_keyword
class BasicWeather(BaseModel):
    """Ambient weather conditions keyword, with all fields optional."""
    temperature: float | None = None
    humidity: float | None = None
    pressure: float | None = None
    cloud_cover: float | None = None
    dew_point: float | None = None
    rain_rate: float | None = None
    wind_direction: float | None = None
    wind_speed: float | None = None


StandardWeather = sk.declare_archetype(
    "weather",
    required_keywords=(BasicWeather,),
)
"""Standard archetype for ambient weather telemetry providers.

A StandardWeather entity publishes ``BasicWeather``. It's a capability tag for discovery —
weather providers are telemetry sources, not actuators, so the archetype asserts no
required commands.
"""
