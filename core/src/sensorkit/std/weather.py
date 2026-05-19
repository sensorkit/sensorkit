from pydantic import BaseModel

import sensorkit.api as sk


@sk.declare_keyword
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


WeatherProvider = sk.declare_trait(
    "WeatherProvider",
    required_keywords=("BasicWeather",),
)

StandardWeather = sk.declare_archetype(
    "weather",
    required_traits=(WeatherProvider,),
)
"""Standard archetype for ambient weather telemetry providers."""
