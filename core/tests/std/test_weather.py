import asyncio
import contextlib

import pytest
import pytest_asyncio

from sensorkit.auto.constraint import ConstraintEvaluator
from sensorkit.backend.base import Backend, Entity
from sensorkit.common.keyword import dump_keyword_json, get_keyword_info
from sensorkit.core.client import SensorKit
from sensorkit.std.weather import BasicWeather, WeatherConstraint


async def _publish_keyword(backend: Backend, entity_name: str, model):
    stream = backend.stream(Entity.at(entity_name))
    await stream.publish(get_keyword_info(model).key, dump_keyword_json(model))


@pytest_asyncio.fixture
async def ev_timeout():
    async with asyncio.timeout(None) as t:
        yield t


def test_weather_constraint():
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=50.0,
        humidity_deadband=5.0,
        wind_max=1e9,
        wind_deadband=0.0,
        rain_max=1e9,
        rain_deadband=0.0,
    )

    # No data -> becomes active
    w = BasicWeather(humidity=None, wind_speed=None, rain_rate=None)
    errors = c.check_weather(w)
    assert errors
    assert any("missing" in e for e in errors)

    # Move to good range -> should clear (wind/rain use 0.0, clearly below their large maxes)
    w = BasicWeather(humidity=0.0, wind_speed=0.0, rain_rate=0.0)
    errors = c.check_weather(w)
    assert not errors

    # Above max -> becomes active
    w_high = BasicWeather(humidity=60.0, wind_speed=0.0, rain_rate=0.0)
    errors = c.check_weather(w_high)
    assert errors
    assert any("humidity" in e for e in errors)

    # Move into the deadband (between 45 and 50) -> should keep previous state (remain active)
    w_deadband = BasicWeather(humidity=47.0, wind_speed=0.0, rain_rate=0.0)
    errors = c.check_weather(w_deadband)
    assert errors

    # Drop below max - deadband (i.e., < 45) -> should clear
    w_clear = BasicWeather(humidity=40.0, wind_speed=0.0, rain_rate=0.0)
    errors = c.check_weather(w_clear)
    assert not errors

    # Back into deadband -> should keep previous state (remain cleared)
    w_deadband_again = BasicWeather(humidity=48.0, wind_speed=0.0, rain_rate=0.0)
    errors = c.check_weather(w_deadband_again)
    assert not errors

    # Exceed max again -> becomes active
    w_exceed = BasicWeather(humidity=51.0, wind_speed=0.0, rain_rate=0.0)
    errors = c.check_weather(w_exceed)
    assert errors
    assert any("humidity" in e for e in errors)


def test_weather_constraint_wind():
    # Use 0.0 for humidity and rain so they're clearly below their maxes and don't block clearing.
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=50.0,
        wind_max=30.0,
        wind_deadband=5.0,
        rain_max=10.0,
    )

    # Below threshold → safe
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=20.0, rain_rate=0.0))
    assert not errors

    # Above max → active
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=35.0, rain_rate=0.0))
    assert errors
    assert any("wind" in e for e in errors)

    # Inside deadband (between 25 and 30) → stays active
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=27.0, rain_rate=0.0))
    assert errors

    # Below deadband (<25) → clears
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=20.0, rain_rate=0.0))
    assert not errors


def test_weather_constraint_rain():
    # Use 0.0 for humidity and wind so they're clearly below their maxes and don't block clearing.
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=50.0,
        wind_max=30.0,
        rain_max=5.0,
        rain_deadband=2.0,
    )

    # Below threshold → safe
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=0.0, rain_rate=1.0))
    assert not errors

    # Above max → active
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=0.0, rain_rate=6.0))
    assert errors
    assert any("rain" in e for e in errors)

    # Inside deadband (between 3 and 5) → stays active
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=0.0, rain_rate=4.0))
    assert errors

    # Below deadband (<3) → clears
    errors = c.check_weather(BasicWeather(humidity=0.0, wind_speed=0.0, rain_rate=2.0))
    assert not errors


def test_weather_constraint_partial_none_data():
    """A single None field is enough to set the constraint active."""
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=100.0,
        wind_max=100.0,
        rain_max=100.0,
    )

    # Only humidity missing
    errors = c.check_weather(BasicWeather(humidity=None, wind_speed=5.0, rain_rate=0.0))
    assert errors
    assert any("humidity" in e for e in errors)

    # Only wind missing
    errors = c.check_weather(BasicWeather(humidity=50.0, wind_speed=None, rain_rate=0.0))
    assert errors
    assert any("wind_speed" in e for e in errors)

    # Only rain missing
    errors = c.check_weather(BasicWeather(humidity=50.0, wind_speed=5.0, rain_rate=None))
    assert errors
    assert any("rain_rate" in e for e in errors)


def test_weather_constraint_no_deadband():
    """With zero deadband, constraint clears as soon as value drops below max."""
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=50.0,
        humidity_deadband=0.0,
        wind_max=1e9,
        rain_max=1e9,
    )

    # Above → active
    errors = c.check_weather(BasicWeather(humidity=55.0, wind_speed=0.0, rain_rate=0.0))
    assert errors

    # Just below max → clears immediately (no deadband)
    errors = c.check_weather(BasicWeather(humidity=49.0, wind_speed=0.0, rain_rate=0.0))
    assert not errors


def test_weather_constraint_defaults():
    c = WeatherConstraint(provider="w1")
    assert c.humidity_max is None
    assert c.wind_max is None
    assert c.rain_max is None
    assert c.humidity_deadband == 0.0
    assert c.wind_deadband == 0.0
    assert c.rain_deadband == 0.0


@pytest.mark.asyncio
async def test_weather_check_task_stream_data(backend, ev_timeout):
    """check_task processes weather records from the stream and sets ready."""
    c = WeatherConstraint(
        provider="weather-svc",
        humidity_max=80.0,
        wind_max=1e9,
        rain_max=1e9,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Good reading then bad reading.
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=50.0, wind_speed=0.0, rain_rate=0.0))
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=90.0, wind_speed=0.0, rain_rate=0.0))

        async with asyncio.timeout(2.0):
            await ev.wait_ready()

        # After the second reading (humidity=90 > 80), constraint should be active.
        await asyncio.sleep(0.05)
        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_weather_check_task_clears_on_good_data(backend, ev_timeout):
    """check_task clears constraint when weather improves."""
    c = WeatherConstraint(
        provider="weather-svc",
        humidity_max=80.0,
        wind_max=1e9,
        rain_max=1e9,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Bad reading then good reading.
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=90.0, wind_speed=0.0, rain_rate=0.0))
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=50.0, wind_speed=0.0, rain_rate=0.0))

        async with asyncio.timeout(2.0):
            await ev.wait_ready()

        await asyncio.sleep(0.05)
        assert not ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
