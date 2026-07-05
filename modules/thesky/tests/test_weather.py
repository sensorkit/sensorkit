# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.std import Connect, Disconnect
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
    await weather.weather_connect(Connect())
    assert weather.device_connected is True


@pytest.mark.asyncio
async def test_weather_disconnect(weather):
    await weather.weather_connect(Connect())
    await weather.weather_disconnect(Disconnect())
    assert weather.device_connected is False


@pytest.mark.asyncio
async def test_weather_connect_sets_flags(weather):
    """Verify that connect sets autoStartup, autoShutdown, and scriptedObjectsNoGoAware."""
    await weather.weather_connect(Connect())

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
