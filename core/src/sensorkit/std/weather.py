from pydantic import BaseModel

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
