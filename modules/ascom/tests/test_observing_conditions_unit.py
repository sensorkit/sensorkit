import pytest

from sensorkit.ascom.observing_conditions import ObservingConditionsService


class FakeObservingConditions:
    def __init__(self):
        self.Connected = False
        self.Temperature = 10.0
        self.Humidity = 50.0
        self.Pressure = 1013.25
        self.CloudCover = 0.1
        self.DewPoint = 5.0
        self.RainRate = 0.0
        self.WindSpeed = 2.5


@pytest.mark.asyncio
async def test_observing_conditions_weather_publisher_handles_all_fields():
    oc = FakeObservingConditions()
    svc = ObservingConditionsService(device=oc, status_frequency=1.0)

    await svc.startup()
    model = await svc.weather()
    assert model.temperature == 10.0
    assert model.humidity == 50.0
    assert model.pressure == 1013.25
    assert model.cloud_cover == 0.1
    assert model.dew_point == 5.0
    assert model.rain_rate == 0.0
    assert model.wind_speed == 2.5
