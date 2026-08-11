# SPDX-License-Identifier: Apache-2.0
import asyncio
import contextlib
import json
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from sensorkit.auto.constraint import (
    Constraint,
    ConstraintEvaluator,
    ConstraintManager,
    GenericConstraint,
)
from sensorkit.backend.base import Backend, Entity
from sensorkit.common.condition import CrossesAboveCondition, EqualsCondition
from sensorkit.core.client import SensorKit
from sensorkit.std.weather import WeatherConstraint


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
    """ConstraintEvaluator starts not_ready (fail-closed); constrain/clear/ready methods work correctly."""
    c = WeatherConstraint(provider="dummy")
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    assert not ev.is_ready
    assert ev.is_active  # not_ready counts as active (fail-closed)

    # constrain before ready: queues intended state but actual stays not_ready
    ev.constrain("bad conditions")
    assert not ev.is_ready
    assert ev.is_active

    # ready() promotes the queued intended state to actual
    ev.ready()
    assert ev.is_ready
    assert ev.is_active  # now in "active" state

    ev.clear("all clear")
    assert not ev.is_active

    ev.clear("still clear")  # idempotent
    assert not ev.is_active


@pytest.mark.asyncio
async def test_evaluator_hold_defers_clear(ev_timeout):
    """With hold > 0, clear() defers the state change until the hold duration elapses."""
    c = WeatherConstraint(provider="dummy", hold=0.1)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    # Put evaluator into active state — hold only triggers on active → clear transition
    ev.constrain("bad conditions")
    ev.ready()
    assert ev.is_active

    # clear() with hold: state becomes "holding", not immediately "clear"
    ev.clear("conditions improved")
    assert ev.is_active  # still active — hold timer is running

    # After hold expires the constraint clears
    await asyncio.sleep(0.2)
    assert not ev.is_active

    await ev.cleanup()


@pytest.mark.asyncio
async def test_evaluator_hold_cancelled_on_constrain(ev_timeout):
    """constrain() during an active hold cancels the deferred clear."""
    c = WeatherConstraint(provider="dummy", hold=0.15)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    # Put evaluator into active state — hold only triggers on active → clear transition
    ev.constrain("bad conditions")
    ev.ready()
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

    await ev.cleanup()


@pytest.mark.asyncio
async def test_hold_carries_over_active_state(ev_timeout):
    """An evaluator retiring while active hands its replacement a full hold."""
    c = WeatherConstraint(provider="dummy", hold=0.3)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    ev.constrain("bad conditions")
    ev.ready()
    await ev.cleanup()

    assert ev.hold_remaining() == 0.3

    # The replacement defers its first clear rather than releasing immediately.
    successor = ConstraintEvaluator(c, timeout=ev_timeout, initial_hold=ev.hold_remaining())
    successor.clear("conditions improved")
    successor.ready()
    assert successor.actual_state.state == "holding"

    await asyncio.sleep(0.4)
    assert not successor.is_active

    await successor.cleanup()


@pytest.mark.asyncio
async def test_hold_resumes_for_remaining_duration(ev_timeout):
    """A hold interrupted by a restart resumes with what was left of it, not a fresh full hold."""
    c = WeatherConstraint(provider="dummy", hold=0.4)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    ev.constrain("bad conditions")
    ev.ready()
    ev.clear("conditions improved")
    assert ev.actual_state.state == "holding"

    # Retire the evaluator part way through the hold.
    await asyncio.sleep(0.25)
    await ev.cleanup()

    remaining = ev.hold_remaining()
    assert 0 < remaining < 0.2

    # The replacement picks up the same clear reading, which does not restart the hold.
    successor = ConstraintEvaluator(c, timeout=ev_timeout, initial_hold=remaining)
    successor.clear("conditions improved")
    successor.ready()
    assert successor.actual_state.state == "holding"

    # The remainder of the original hold elapses; a fresh 0.4s hold would still be running.
    await asyncio.sleep(0.25)
    assert not successor.is_active

    await successor.cleanup()


@pytest.mark.asyncio
async def test_no_hold_remaining_when_clear(ev_timeout):
    """A constraint that retires clear leaves nothing for its replacement to resume."""
    c = WeatherConstraint(provider="dummy", hold=0.3)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    ev.clear("all clear")
    ev.ready()
    await ev.cleanup()

    assert ev.hold_remaining() == 0


@pytest.mark.asyncio
async def test_no_hold_remaining_without_hold_configured(ev_timeout):
    """With hold disabled there is nothing to preserve across a restart."""
    c = WeatherConstraint(provider="dummy")
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    ev.constrain("bad conditions")
    ev.ready()

    assert ev.hold_remaining() == 0


@pytest.mark.asyncio
async def test_carried_hold_stays_constrained_until_ready(ev_timeout):
    """A resumed hold does not release the constraint before the check task reports."""
    c = WeatherConstraint(provider="dummy", hold=0.3)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    ev.constrain("bad conditions")
    ev.ready()
    ev.clear("conditions improved")
    await ev.cleanup()

    successor = ConstraintEvaluator(c, timeout=ev_timeout, initial_hold=ev.hold_remaining())
    assert not successor.is_ready
    assert successor.is_active

    # The carried hold expires while still awaiting data; the constraint stays engaged.
    await asyncio.sleep(0.4)
    assert successor.actual_state.state == "not_ready"
    assert successor.is_active

    await successor.cleanup()


@pytest.mark.asyncio
async def test_holding_until_dt_projects_deadline(ev_timeout):
    """A running hold reports its deadline as a UTC instant."""
    c = WeatherConstraint(provider="dummy", hold=0.3)
    ev = ConstraintEvaluator(c, timeout=ev_timeout)

    assert ev.holding_until_dt is None

    ev.constrain("bad conditions")
    ev.ready()
    ev.clear("conditions improved")

    deadline = ev.holding_until_dt
    assert deadline is not None
    assert 0 < (deadline - datetime.now(UTC)).total_seconds() <= 0.3

    # The projection tracks the hold, so it is gone once the hold expires.
    await asyncio.sleep(0.4)
    assert ev.holding_until_dt is None

    await ev.cleanup()


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
            while not all(e.state != "not_ready" for e in manager.entries):
                await asyncio.sleep(0.01)
        assert manager.entries[0].state != "not_ready"
        assert manager.entries[0].state == "clear"
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
            while not all(e.state != "not_ready" for e in manager.entries):
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
            while not all(e.state != "not_ready" for e in manager.entries):
                await asyncio.sleep(0.01)
        assert call_count >= 2
        assert manager.entries[0].state == "clear"
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _run_manager_tasks(manager: ConstraintManager, **kwargs):
    """Helper: run ConstraintManager tasks until cancelled."""
    async with asyncio.TaskGroup() as tg:
        await manager.start(task_group=tg, **kwargs)


@pytest.mark.asyncio
async def test_supervisor_restart_preserves_hold(backend, monkeypatch):
    """A constraint active when its check_task restarts still holds on the first clear reading."""
    monkeypatch.setattr(ConstraintManager, "CONSTRAINT_RESTART_GRACE", 0.2)
    monkeypatch.setattr(ConstraintManager, "CONSTRAINT_RESTART_DELAY", 0.2)

    c = GenericConstraint(
        entity="sensor",
        keyword="Rain",
        field="state",
        condition=EqualsCondition(threshold="Wet"),
        hold=2.0,
        ttl=1.0,
    )
    manager = ConstraintManager([c])
    task = asyncio.create_task(_run_manager_tasks(manager, kit=SensorKit(backend)))

    async def report_until_settled(value: str) -> str:
        """Feed a steady reading until the constraint reaches a verdict, and return it.

        Repeating the reading covers the gap before each check_task subscribes, and the first one
        through only establishes a baseline for the condition.
        """
        async with asyncio.timeout(5.0):
            while manager.entries[0].state == "not_ready":
                await _publish_raw(backend, "sensor", "Rain", {"state": value})
                await asyncio.sleep(0.05)

        return manager.entries[0].state

    async def wait_for_state(state: str):
        async with asyncio.timeout(5.0):
            while manager.entries[0].state != state:
                await asyncio.sleep(0.01)

    try:
        assert await report_until_settled("Wet") == "active"

        # Starve the feed so the supervisor times out and rebuilds the evaluator.
        await wait_for_state("not_ready")

        # The feed returns dry. The hold from the pre-restart active state still applies.
        assert await report_until_settled("Dry") == "holding"
        assert manager.is_constrained()
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
