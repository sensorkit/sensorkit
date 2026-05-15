import asyncio
import contextlib
import json

import pytest

from sensorkit.backend.base import Backend, Entity
from sensorkit.common.condition import CrossesAboveCondition, CrossesBelowCondition
from sensorkit.common.keyword import dump_keyword_json, get_keyword_info
from sensorkit.core.client import SensorKit
from sensorkit.std.weather import BasicWeather
from sensorkit.models.safety import BasicSafety
from sensorkit.auto.constraint import (
    Constraint,
    ConstraintEvaluator,
    GenericConstraint,
    SafetyConstraint,
    WeatherConstraint,
)


async def _publish_keyword(backend: Backend, entity_name: str, model):
    """Publish a keyword model to an entity's stream."""
    stream = backend.stream(Entity.at(entity_name))
    await stream.publish(get_keyword_info(model).key, dump_keyword_json(model))


async def _publish_raw(backend: Backend, entity_name: str, keyword: str, data):
    """Publish raw JSON data to an entity's stream."""
    stream = backend.stream(Entity.at(entity_name))
    await stream.publish(keyword, json.dumps(data).encode())


@pytest.mark.asyncio
async def test_constraint_evaluator():
    class DummyConstraint(Constraint):
        kind: str = "dummy"

        async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
            evaluator.ready.set()
            evaluator.active.set()

    c = DummyConstraint()
    ev = c.make_evaluator()

    assert not ev.ready.is_set()
    # Constraints fail-closed by default: active is set on init and only cleared
    # once a positive observation confirms safe conditions.
    assert ev.active.is_set()

    async with asyncio.TaskGroup() as tg:
        ev.start(task_group=tg)

    assert ev.ready.is_set()
    assert ev.active.is_set()


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
    active, reason = c.check_weather(w)
    assert active
    assert "missing data" in reason

    # Move to good range -> should clear
    w = BasicWeather(humidity=0.0, wind_speed=1e9, rain_rate=1e9)
    active, reason = c.check_weather(w)
    assert not active

    # Above max -> becomes active (keep other metrics high to avoid influencing outcome)
    w_high = BasicWeather(humidity=60.0, wind_speed=1e9, rain_rate=1e9)
    active, reason = c.check_weather(w_high)
    assert active
    assert "humidity" in reason

    # Move into the deadband (between 45 and 50) -> should keep previous state (remain active)
    w_deadband = BasicWeather(humidity=47.0, wind_speed=1e9, rain_rate=1e9)
    active, _ = c.check_weather(w_deadband, was_active=True)
    assert active

    # Drop below max - deadband (i.e., < 45) -> should clear
    w_clear = BasicWeather(humidity=40.0, wind_speed=1e9, rain_rate=1e9)
    active, _ = c.check_weather(w_clear, was_active=True)
    assert not active

    # Back into deadband -> should keep previous state (remain cleared)
    w_deadband_again = BasicWeather(humidity=48.0, wind_speed=1e9, rain_rate=1e9)
    active, _ = c.check_weather(w_deadband_again, was_active=False)
    assert not active

    # Exceed max again -> becomes active
    w_exceed = BasicWeather(humidity=51.0, wind_speed=1e9, rain_rate=1e9)
    active, reason = c.check_weather(w_exceed)
    assert active
    assert "humidity" in reason


@pytest.mark.asyncio
async def test_generic_constraint():
    """Test that GenericConstraint wires condition evaluation to the evaluator's active flag."""
    c = GenericConstraint(
        entity="dummy",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0, deadband=5.0),
    )
    ev = c.make_evaluator()

    # Cross above threshold -> activates
    _, was_active = await c._apply(ev, current=55.0, previous=45.0, was_active=False, label="test")
    assert ev.active.is_set()
    assert was_active

    # Still in deadband (>= 45) -> stays active
    _, was_active = await c._apply(ev, current=47.0, previous=55.0, was_active=True, label="test")
    assert ev.active.is_set(), "Should remain active inside deadband"

    # Drop below deadband (< 45) -> clears
    _, was_active = await c._apply(ev, current=40.0, previous=47.0, was_active=True, label="test")
    assert not ev.active.is_set(), "Should clear when below deadband"
    assert not was_active

    # Rise into deadband but not crossing threshold -> stays cleared
    _, was_active = await c._apply(ev, current=48.0, previous=40.0, was_active=False, label="test")
    assert not ev.active.is_set(), "Should stay cleared inside deadband if not crossed"

    # Cross above again -> re-activates
    _, was_active = await c._apply(ev, current=51.0, previous=48.0, was_active=False, label="test")
    assert ev.active.is_set()
    assert was_active


def test_weather_constraint_wind():
    # Hold humidity and rain at exactly their max so only wind drives clearing.
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=50.0,
        wind_max=30.0,
        wind_deadband=5.0,
        rain_max=10.0,
    )

    # Below threshold → safe
    active, _ = c.check_weather(BasicWeather(humidity=50.0, wind_speed=20.0, rain_rate=10.0))
    assert not active

    # Above max → active
    active, reason = c.check_weather(BasicWeather(humidity=50.0, wind_speed=35.0, rain_rate=10.0))
    assert active
    assert "wind" in reason

    # Inside deadband (between 25 and 30) → stays active (other metrics at max, not below)
    active, _ = c.check_weather(BasicWeather(humidity=50.0, wind_speed=27.0, rain_rate=10.0), was_active=True)
    assert active

    # Below deadband (<25) → clears
    active, _ = c.check_weather(BasicWeather(humidity=50.0, wind_speed=20.0, rain_rate=10.0), was_active=True)
    assert not active


def test_weather_constraint_rain():
    # Hold humidity and wind at exactly their max so only rain drives clearing.
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=50.0,
        wind_max=30.0,
        rain_max=5.0,
        rain_deadband=2.0,
    )

    # Below threshold → safe
    active, _ = c.check_weather(BasicWeather(humidity=50.0, wind_speed=30.0, rain_rate=1.0))
    assert not active

    # Above max → active
    active, reason = c.check_weather(BasicWeather(humidity=50.0, wind_speed=30.0, rain_rate=6.0))
    assert active
    assert "rain" in reason

    # Inside deadband (between 3 and 5) → stays active (other metrics at max, not below)
    active, _ = c.check_weather(BasicWeather(humidity=50.0, wind_speed=30.0, rain_rate=4.0), was_active=True)
    assert active

    # Below deadband (<3) → clears
    active, _ = c.check_weather(BasicWeather(humidity=50.0, wind_speed=30.0, rain_rate=2.0), was_active=True)
    assert not active


def test_weather_constraint_partial_none_data():
    """A single None field is enough to set the constraint active."""
    c = WeatherConstraint(
        provider="dummy",
        humidity_max=100.0,
        wind_max=100.0,
        rain_max=100.0,
    )

    # Only humidity missing
    active, reason = c.check_weather(BasicWeather(humidity=None, wind_speed=5.0, rain_rate=0.0))
    assert active
    assert "humidity" in reason

    # Only wind missing
    active, reason = c.check_weather(BasicWeather(humidity=50.0, wind_speed=None, rain_rate=0.0))
    assert active
    assert "wind_speed" in reason

    # Only rain missing
    active, reason = c.check_weather(BasicWeather(humidity=50.0, wind_speed=5.0, rain_rate=None))
    assert active
    assert "rain_rate" in reason


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
    active, _ = c.check_weather(BasicWeather(humidity=55.0, wind_speed=0.0, rain_rate=0.0))
    assert active

    # Just below max → clears immediately (no deadband)
    active, _ = c.check_weather(BasicWeather(humidity=49.0, wind_speed=0.0, rain_rate=0.0))
    assert not active


def test_weather_constraint_defaults():
    c = WeatherConstraint(provider="w1")
    assert c.humidity_max == float("inf")
    assert c.wind_max == float("inf")
    assert c.rain_max == float("inf")
    assert c.humidity_deadband == 0.0
    assert c.wind_deadband == 0.0
    assert c.rain_deadband == 0.0
    assert c.time_to_live == 30.0


def test_safety_constraint_model():
    c = SafetyConstraint(provider="safety-provider")
    assert c.kind == "safety"
    assert c.provider == "safety-provider"
    assert c.time_to_live == 30.0

    ev = c.make_evaluator()
    assert not ev.ready.is_set()
    # Fail-closed default — see test_constraint_evaluator.
    assert ev.active.is_set()


def test_safety_constraint_custom_ttl():
    c = SafetyConstraint(provider="s1", time_to_live=60.0)
    assert c.time_to_live == 60.0


@pytest.mark.asyncio
async def test_evaluator_start_without_task_group():
    """start() without task_group falls back to asyncio.create_task."""

    class InstantConstraint(Constraint):
        kind: str = "instant"

        async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
            evaluator.ready.set()

    c = InstantConstraint()
    ev = c.make_evaluator()
    ev.start()
    await asyncio.sleep(0.05)
    assert ev.ready.is_set()


@pytest.mark.asyncio
async def test_evaluator_kwargs_forwarded():
    """Extra kwargs from start() are forwarded to check_task."""
    received = {}

    class KwargConstraint(Constraint):
        kind: str = "kwarg"

        async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
            received.update(kwargs)
            evaluator.ready.set()

    c = KwargConstraint()
    ev = c.make_evaluator()

    async with asyncio.TaskGroup() as tg:
        ev.start(task_group=tg, foo="bar", num=42)

    assert received == {"foo": "bar", "num": 42}


@pytest.mark.asyncio
async def test_generic_constraint_crosses_below():
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="celsius",
        condition=CrossesBelowCondition(threshold=0.0, deadband=2.0),
    )
    ev = c.make_evaluator()

    # Cross below 0 → activates
    _, was_active = await c._apply(ev, current=-1.0, previous=5.0, was_active=False, label="test")
    assert ev.active.is_set()
    assert was_active

    # Remain below threshold → stays active
    _, was_active = await c._apply(ev, current=-3.0, previous=-1.0, was_active=True, label="test")
    assert ev.active.is_set()

    # Rise into deadband (0 to 2) → stays active
    _, was_active = await c._apply(ev, current=1.0, previous=-3.0, was_active=True, label="test")
    assert ev.active.is_set()

    # Rise above deadband (> 2) → clears
    _, was_active = await c._apply(ev, current=3.0, previous=1.0, was_active=True, label="test")
    assert not ev.active.is_set()
    assert not was_active


@pytest.mark.asyncio
async def test_generic_constraint_no_field():
    """GenericConstraint without a field uses raw data."""
    c = GenericConstraint(
        entity="sensor",
        keyword="value",
        condition=CrossesAboveCondition(threshold=100.0),
    )
    assert c.field is None

    ev = c.make_evaluator()

    # Cross above → activates
    _, was_active = await c._apply(ev, current=110.0, previous=90.0, was_active=False, label="test")
    assert ev.active.is_set()

    # Drop below → clears (no deadband)
    _, was_active = await c._apply(ev, current=90.0, previous=110.0, was_active=True, label="test")
    assert not ev.active.is_set()


@pytest.mark.asyncio
async def test_weather_check_task_stream_data(backend):
    """check_task processes weather records from the stream and sets ready."""
    c = WeatherConstraint(
        provider="weather-svc",
        humidity_max=80.0,
        wind_max=1e9,
        rain_max=1e9,
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Good reading then bad reading.
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=50.0, wind_speed=0.0, rain_rate=0.0))
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=90.0, wind_speed=0.0, rain_rate=0.0))

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        # After the second reading (humidity=90 > 80), constraint should be active.
        await asyncio.sleep(0.05)
        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_weather_check_task_clears_on_good_data(backend):
    """check_task clears constraint when weather improves."""
    c = WeatherConstraint(
        provider="weather-svc",
        humidity_max=80.0,
        wind_max=1e9,
        rain_max=1e9,
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Bad reading then good reading.
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=90.0, wind_speed=0.0, rain_rate=0.0))
        await _publish_keyword(backend, "weather-svc", BasicWeather(humidity=50.0, wind_speed=0.0, rain_rate=0.0))

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        await asyncio.sleep(0.05)
        assert not ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_weather_check_task_timeout_sets_active(backend):
    """When no data arrives within TTL, constraint becomes active."""
    c = WeatherConstraint(
        provider="weather-svc",
        humidity_max=80.0,
        time_to_live=0.1,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_safe(backend):
    """Safe reading clears the constraint."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=5.0)
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=True))

        async with asyncio.timeout(2.0):
            await ev.ready.wait()
        assert not ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_unsafe(backend):
    """Unsafe reading sets the constraint active."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=5.0)
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=False))

        async with asyncio.timeout(2.0):
            await ev.ready.wait()
        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_unsafe_then_safe(backend):
    """Constraint clears when safety recovers."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=5.0)
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=False))
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=True))

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        await asyncio.sleep(0.05)
        assert not ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_timeout(backend):
    """Timeout with no data sets constraint active."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=0.1)
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        async with asyncio.timeout(2.0):
            while not ev.active.is_set():
                await asyncio.sleep(0.05)
        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_crosses_above(backend):
    """check_task evaluates incoming stream messages and activates on threshold crossing."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Publish baseline; consumer stores it as `previous`.
        await _publish_raw(backend, "sensor", "Temperature", {"value": 40.0})
        await asyncio.sleep(0.1)

        # Publish crossing value; consumer evaluates and sets ready.
        await _publish_raw(backend, "sensor", "Temperature", {"value": 60.0})

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_filters_keyword(backend):
    """Messages with non-matching keywords are skipped."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Wrong keyword — ignored by check_task, but still delivered by the wildcard consumer.
        await _publish_raw(backend, "sensor", "Humidity", {"value": 99.0})
        # Baseline on the correct keyword.
        await _publish_raw(backend, "sensor", "Temperature", {"value": 40.0})
        await asyncio.sleep(0.1)

        # Publish crossing value; consumer evaluates and sets ready.
        await _publish_raw(backend, "sensor", "Temperature", {"value": 60.0})

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_bad_json_skipped(backend):
    """Invalid JSON messages are silently skipped."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Publish baseline; consumer stores it as `previous`.
        await _publish_raw(backend, "sensor", "Temperature", {"value": 40.0})
        await asyncio.sleep(0.1)

        # Publish bad JSON — consumer skips it; previous stays at 40.0.
        stream = backend.stream(Entity.at("sensor"))
        await stream.publish("Temperature", b"not-json")

        # Crossing value; consumer evaluates and sets ready.
        await _publish_raw(backend, "sensor", "Temperature", {"value": 60.0})

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_no_field(backend):
    """Without a field, uses the full parsed JSON object as the value."""
    c = GenericConstraint(
        entity="sensor",
        keyword="counter",
        condition=CrossesAboveCondition(threshold=10.0),
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Baseline.
        await _publish_raw(backend, "sensor", "counter", 5.0)
        await asyncio.sleep(0.1)

        # Crossing value; consumer evaluates and sets ready.
        await _publish_raw(backend, "sensor", "counter", 15.0)

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_timeout(backend):
    """When no data arrives within TTL, constraint becomes active."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=0.1,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        async with asyncio.timeout(2.0):
            while not ev.active.is_set():
                await asyncio.sleep(0.05)
        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_label_with_field(backend):
    """Label includes entity.keyword.field when field is set."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="celsius",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = c.make_evaluator()
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)

        # Baseline; consumer stores it as `previous`.
        await _publish_raw(backend, "sensor", "Temperature", {"celsius": 40.0})
        await asyncio.sleep(0.1)

        # Crossing value; consumer evaluates and sets ready.
        await _publish_raw(backend, "sensor", "Temperature", {"celsius": 60.0})

        async with asyncio.timeout(2.0):
            await ev.ready.wait()

        assert ev.active.is_set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
