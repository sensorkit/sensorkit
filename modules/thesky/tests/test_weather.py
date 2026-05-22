import pytest

from sensorkit.thesky.weather import TheSkyWeatherConfig


@pytest.fixture
def weather(simulator):
    host, port = simulator
    config = TheSkyWeatherConfig(
        device_type="weather",
        host=host,
        port=port,
        timeout=5.0,
        status_frequency=0.1,
    )
    return config.create_device()


@pytest.mark.asyncio
async def test_weather_connect(weather):
    import sensorkit.api as sk

    await weather.weather_connect(sk.Connect())
    assert weather.device_connected is True


@pytest.mark.asyncio
async def test_weather_disconnect(weather):
    import sensorkit.api as sk

    await weather.weather_connect(sk.Connect())
    await weather.weather_disconnect(sk.Disconnect())
    assert weather.device_connected is False


@pytest.mark.asyncio
async def test_weather_connect_sets_flags(weather):
    """Verify that connect sets autoStartup, autoShutdown, and scriptedObjectsNoGoAware."""
    import sensorkit.api as sk

    await weather.weather_connect(sk.Connect())

    resp = await weather.execute(
        """
        var Out;
        Out = [
            WeatherUtil.autoStartup,
            WeatherUtil.autoShutdown,
            WeatherUtil.scriptedObjectsNoGoAware
        ];
        """
    )
    values = [int(float(x)) for x in resp.split(",")]
    assert values == [1, 1, 1]
