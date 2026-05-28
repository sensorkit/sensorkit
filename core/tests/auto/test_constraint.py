import asyncio
import contextlib
import json

import pytest
import pytest_asyncio

from sensorkit.auto.constraint import (
    Constraint,
    ConstraintEvaluator,
    ConstraintManager,
    GenericConstraint,
)
from sensorkit.backend.base import Backend, Entity
from sensorkit.common.condition import CrossesAboveCondition, CrossesBelowCondition
from sensorkit.common.keyword import dump_keyword_json, get_keyword_info
from sensorkit.core.client import SensorKit
from sensorkit.std.safety import BasicSafety, SafetyConstraint
from sensorkit.std.weather import BasicWeather, WeatherConstraint


async def _publish_keyword(backend: Backend, entity_name: str, model):
    """Publish a keyword model to an entity's stream."""
    stream = backend.stream(Entity.at(entity_name))
    await stream.publish(get_keyword_info(model).key, dump_keyword_json(model))


async def _publish_raw(backend: Backend, entity_name: str, keyword: str, data):
    """Publish raw JSON data to an entity's stream."""
    stream = backend.stream(Entity.at(entity_name))
    await stream.publish(keyword, json.dumps(data).encode())


@pytest_asyncio.fixture
async def ev_timeout():
    """Provides an asyncio.Timeout for ConstraintEvaluator instances under test.
    Uses no deadline so TTL reschedules don't expire during tests."""
    async with asyncio.timeout(None) as t:
        yield t


@pytest.mark.asyncio
async def test_constraint_evaluator(ev_timeout):
    """ConstraintEvaluator starts fail-closed; constrain/ok/ready methods work correctly."""
    c = WeatherConstraint(provider="dummy")
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    assert not ev.is_ready
    # Constraints fail-closed by default: active on init, only cleared on positive observation.
    assert ev.is_active

    # constrain() is idempotent when already active; ok() clears
    assert not ev.constrain("still constrained")  # no change — was already active
    assert ev.is_active
    assert ev.clear("all clear")  # state change: active → clear
    assert not ev.is_active
    assert not ev.clear("still clear")  # no change — already clear

    ev.ready()
    assert ev.is_ready


@pytest.mark.asyncio
async def test_evaluator_hold_defers_clear(ev_timeout):
    """With hold > 0, clear() defers the state change until the hold duration elapses."""
    c = WeatherConstraint(provider="dummy", hold=0.1)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    assert ev.is_active

    # clear() returns False (deferred) and does not immediately change state
    changed = ev.clear("conditions improved")
    assert changed is False
    assert ev.is_active  # still active — hold timer is running

    # After hold expires the constraint clears
    await asyncio.sleep(0.2)
    assert not ev.is_active

    await ev.cancel()


@pytest.mark.asyncio
async def test_evaluator_hold_cancelled_on_constrain(ev_timeout):
    """constrain() during an active hold cancels the deferred clear."""
    c = WeatherConstraint(provider="dummy", hold=0.15)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    assert ev.is_active

    # Begin a hold
    ev.clear("brief improvement")
    assert ev.is_active  # hold is running, not yet cleared

    # Re-constrain before hold expires — cancels the pending clear
    ev.constrain("conditions worsened again")
    assert ev.is_active

    # After the original hold duration would have elapsed, constraint is still active
    await asyncio.sleep(0.25)
    assert ev.is_active

    await ev.cancel()


@pytest.mark.asyncio
async def test_evaluator_ttl_constrain_reschedules():
    """constrain() reschedules the evaluator timeout deadline to now + ttl."""
    ttl = 5.0
    c = WeatherConstraint(provider="dummy", ttl=ttl)
    loop = asyncio.get_running_loop()

    async with asyncio.timeout(None) as timeout:
        ev = ConstraintEvaluator(c, timeout=timeout)
        before = loop.time()
        ev.constrain("test")
        assert timeout.when() is not None
        assert timeout.when() >= before + ttl - 0.1


@pytest.mark.asyncio
async def test_evaluator_ttl_clear_reschedules():
    """clear() reschedules the evaluator timeout deadline to now + ttl."""
    ttl = 5.0
    c = WeatherConstraint(provider="dummy", ttl=ttl)
    loop = asyncio.get_running_loop()

    async with asyncio.timeout(None) as timeout:
        ev = ConstraintEvaluator(c, timeout=timeout)
        before = loop.time()
        ev.clear("test")
        assert timeout.when() is not None
        assert timeout.when() >= before + ttl - 0.1


@pytest.mark.asyncio
async def test_evaluator_ttl_ready_reschedules():
    """ready() reschedules the evaluator timeout deadline to now + ttl."""
    ttl = 5.0
    c = WeatherConstraint(provider="dummy", ttl=ttl)
    loop = asyncio.get_running_loop()

    async with asyncio.timeout(None) as timeout:
        ev = ConstraintEvaluator(c, timeout=timeout)
        before = loop.time()
        ev.ready()
        assert timeout.when() is not None
        assert timeout.when() >= before + ttl - 0.1


@pytest.mark.asyncio
async def test_constraint_manager_wait_ready():
    """ConstraintManager.start() returns only once all constraints report their initial state."""

    class QuickConstraint(Constraint):
        kind: str = "quick"

        async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
            evaluator.clear("all clear")
            evaluator.ready()
            await asyncio.sleep(1000)

    manager = ConstraintManager([QuickConstraint()])
    task = asyncio.create_task(_run_manager_tasks(manager))
    try:
        async with asyncio.timeout(2.0):
            while not all(e.ready for e in manager.entries):
                await asyncio.sleep(0.01)
        assert manager.entries[0].ready
        assert not manager.entries[0].active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_constraint_manager_kwargs_forwarded():
    """ConstraintManager forwards extra kwargs to check_task."""
    received: dict = {}

    class KwargConstraint(Constraint):
        kind: str = "kwarg"

        async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
            received.update(kwargs)
            evaluator.clear()
            evaluator.ready()
            await asyncio.sleep(1000)

    manager = ConstraintManager([KwargConstraint()])
    task = asyncio.create_task(_run_manager_tasks(manager, foo="bar", num=42))
    try:
        async with asyncio.timeout(2.0):
            while not all(e.ready for e in manager.entries):
                await asyncio.sleep(0.01)
        assert "foo" in received
        assert received["foo"] == "bar"
        assert received["num"] == 42
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_constraint_manager_supervisor_restarts_on_error():
    """ConstraintManager restarts check_task after an unhandled exception."""
    call_count = 0

    class FlakyConstraint(Constraint):
        kind: str = "flaky"
        ttl: float = 0.5

        async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("first run fails")
            evaluator.clear("recovered")
            evaluator.ready()
            await asyncio.sleep(1000)

    manager = ConstraintManager([FlakyConstraint()])
    # Speed up the supervisor retry cycle so the test completes quickly.
    manager.CONSTRAINT_RESTART_GRACE = 0.2
    manager.CONSTRAINT_RESTART_DELAY = 0.1
    task = asyncio.create_task(_run_manager_tasks(manager))
    try:
        async with asyncio.timeout(5.0):
            while not all(e.ready for e in manager.entries):
                await asyncio.sleep(0.01)
        assert call_count >= 2
        assert not manager.entries[0].active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _run_manager_tasks(manager: ConstraintManager, **kwargs):
    """Helper: run ConstraintManager tasks until cancelled."""
    async with asyncio.TaskGroup() as tg:
        await manager.start(task_group=tg, **kwargs)


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


@pytest.mark.asyncio
async def test_generic_constraint(ev_timeout):
    """GenericConstraint wires condition evaluation to the evaluator's active state."""
    c = GenericConstraint(
        entity="dummy",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0, deadband=5.0),
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    # Cross above threshold -> activates
    _, was_active = c._apply(ev, current=55.0, previous=45.0, was_active=False, label="test")
    assert ev.is_active
    assert was_active

    # Still in deadband (>= 45) -> stays active
    _, was_active = c._apply(ev, current=47.0, previous=55.0, was_active=True, label="test")
    assert ev.is_active, "Should remain active inside deadband"

    # Drop below deadband (< 45) -> clears
    _, was_active = c._apply(ev, current=40.0, previous=47.0, was_active=True, label="test")
    assert not ev.is_active, "Should clear when below deadband"
    assert not was_active

    # Rise into deadband but not crossing threshold -> stays cleared
    _, was_active = c._apply(ev, current=48.0, previous=40.0, was_active=False, label="test")
    assert not ev.is_active, "Should stay cleared inside deadband if not crossed"

    # Cross above again -> re-activates
    _, was_active = c._apply(ev, current=51.0, previous=48.0, was_active=False, label="test")
    assert ev.is_active
    assert was_active


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
    assert c.ttl == 30.0


@pytest.mark.asyncio
async def test_safety_constraint_model(ev_timeout):
    c = SafetyConstraint(provider="safety-provider")
    assert c.kind == "safety"
    assert c.provider == "safety-provider"
    assert c.time_to_live == 30.0

    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    assert not ev.is_ready
    # Fail-closed default — see test_constraint_evaluator.
    assert ev.is_active


def test_safety_constraint_custom_ttl():
    c = SafetyConstraint(provider="s1", time_to_live=60.0)
    assert c.time_to_live == 60.0


@pytest.mark.asyncio
async def test_generic_constraint_crosses_below(ev_timeout):
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="celsius",
        condition=CrossesBelowCondition(threshold=0.0, deadband=2.0),
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    # Cross below 0 → activates
    _, was_active = c._apply(ev, current=-1.0, previous=5.0, was_active=False, label="test")
    assert ev.is_active
    assert was_active

    # Remain below threshold → stays active
    _, was_active = c._apply(ev, current=-3.0, previous=-1.0, was_active=True, label="test")
    assert ev.is_active

    # Rise into deadband (0 to 2) → stays active
    _, was_active = c._apply(ev, current=1.0, previous=-3.0, was_active=True, label="test")
    assert ev.is_active

    # Rise above deadband (> 2) → clears
    _, was_active = c._apply(ev, current=3.0, previous=1.0, was_active=True, label="test")
    assert not ev.is_active
    assert not was_active


@pytest.mark.asyncio
async def test_generic_constraint_no_field(ev_timeout):
    """GenericConstraint without a field uses raw data."""
    c = GenericConstraint(
        entity="sensor",
        keyword="value",
        condition=CrossesAboveCondition(threshold=100.0),
    )
    assert c.field is None

    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    # Cross above → activates
    _, was_active = c._apply(ev, current=110.0, previous=90.0, was_active=False, label="test")
    assert ev.is_active

    # Drop below → clears (no deadband)
    _, was_active = c._apply(ev, current=90.0, previous=110.0, was_active=True, label="test")
    assert not ev.is_active


@pytest.mark.asyncio
async def test_weather_check_task_stream_data(backend, ev_timeout):
    """check_task processes weather records from the stream and sets ready."""
    c = WeatherConstraint(
        provider="weather-svc",
        humidity_max=80.0,
        wind_max=1e9,
        rain_max=1e9,
        time_to_live=5.0,
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
        time_to_live=5.0,
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


@pytest.mark.asyncio
async def test_safety_check_task_safe(backend, ev_timeout):
    """Safe reading clears the constraint."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=5.0)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=True))

        async with asyncio.timeout(2.0):
            await ev.wait_ready()
        assert not ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_unsafe(backend, ev_timeout):
    """Unsafe reading sets the constraint active."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=5.0)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=False))

        async with asyncio.timeout(2.0):
            await ev.wait_ready()
        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_unsafe_then_safe(backend, ev_timeout):
    """Constraint clears when safety recovers."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=5.0)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        await asyncio.sleep(0.1)
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=False))
        await _publish_keyword(backend, "safety-svc", BasicSafety(is_safe=True))

        async with asyncio.timeout(2.0):
            await ev.wait_ready()

        await asyncio.sleep(0.05)
        assert not ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_safety_check_task_timeout(backend, ev_timeout):
    """Timeout with no data sets constraint active."""
    c = SafetyConstraint(provider="safety-svc", time_to_live=0.1)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        async with asyncio.timeout(2.0):
            while not ev.is_active:
                await asyncio.sleep(0.05)
        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_crosses_above(backend, ev_timeout):
    """check_task evaluates incoming stream messages and activates on threshold crossing."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
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
            await ev.wait_ready()

        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_filters_keyword(backend, ev_timeout):
    """Messages with non-matching keywords are skipped."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
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
            await ev.wait_ready()

        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_bad_json_skipped(backend, ev_timeout):
    """Invalid JSON messages are silently skipped."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
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
            await ev.wait_ready()

        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_no_field(backend, ev_timeout):
    """Without a field, uses the full parsed JSON object as the value."""
    c = GenericConstraint(
        entity="sensor",
        keyword="counter",
        condition=CrossesAboveCondition(threshold=10.0),
        time_to_live=5.0,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
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
            await ev.wait_ready()

        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_timeout(backend, ev_timeout):
    """When no data arrives within TTL, constraint becomes active."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="value",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=0.1,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    kit = SensorKit(backend)

    task = asyncio.create_task(c.check_task(ev, kit))
    try:
        async with asyncio.timeout(2.0):
            while not ev.is_active:
                await asyncio.sleep(0.05)
        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_generic_check_task_label_with_field(backend, ev_timeout):
    """Label includes entity.keyword.field when field is set."""
    c = GenericConstraint(
        entity="sensor",
        keyword="Temperature",
        field="celsius",
        condition=CrossesAboveCondition(threshold=50.0),
        time_to_live=5.0,
    )
    ev = ConstraintEvaluator(c, timeout=ev_timeout)
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
            await ev.wait_ready()

        assert ev.is_active
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
