# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import math

import pytest
import pytest_asyncio
from pydantic import ValidationError

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerMonitor,
    ControllerOperatingState,
    ControllerState,
    ControllerStateUnknown,
    InternalControllerState,
)
from sensorkit.core.task import InitTask

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY
ERROR = InternalControllerState.ERROR
UNKNOWN = InternalControllerState.UNKNOWN
TRANSITION = InternalControllerState.TRANSITION

ABSENT = "absentcontroller"
"""A controller that has not published state."""


@pytest_asyncio.fixture
async def monitor(kit, controller_impl):
    monitor = ControllerMonitor(kit.controller(controller_impl.entity))
    await monitor.start(timeout=1.0)
    yield monitor
    await monitor.stop()


def lifecycle_input(dome: str) -> KeywordDict:
    context = KeywordDict()
    context.set_value("dome", dome)
    return context


async def next_report(changes: asyncio.Queue[ControllerOperatingState]):
    async with asyncio.timeout(1.0):
        report = await changes.get()

    changes.task_done()
    return report


def assert_unavailable(monitor: ControllerMonitor) -> ControllerStateUnknown:
    """Assert every healthy accessor raises and return the state accessor's exception."""
    assert not monitor.healthy()

    with pytest.raises(ControllerStateUnknown):
        _ = monitor.operating_state

    with pytest.raises(ControllerStateUnknown), monitor.state_changes():
        pass

    with pytest.raises(ControllerStateUnknown) as unknown:
        _ = monitor.state

    return unknown.value


# Readiness


@pytest.mark.asyncio
async def test_start_receives_current_state(kit, controller_impl):
    client = kit.controller(controller_impl.entity)
    monitor = ControllerMonitor(client)

    assert_unavailable(monitor)
    assert monitor.last_state is None

    await monitor.start(timeout=1.0)

    try:
        stored = await client.kv_get_model(ControllerState)

        # UNKNOWN is a valid controller state.
        assert monitor.healthy()
        assert monitor.operating_state.current == UNKNOWN
        assert monitor.state.model_dump() == stored.model_dump()
        assert monitor.last_state.model_dump() == stored.model_dump()
        assert monitor.error is None
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_start_waits_for_creation(kit, service_context):
    monitor = ControllerMonitor(kit.controller(ABSENT))
    starting = asyncio.create_task(monitor.start(timeout=2.0))
    await asyncio.sleep(0.1)

    assert not starting.done()

    try:
        await service_context.register_controller(ABSENT)
        await starting

        assert monitor.operating_state.current == UNKNOWN
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_start_timeout_fails_start(kit):
    monitor = ControllerMonitor(kit.controller(ABSENT))

    with pytest.raises(TimeoutError) as timeout:
        await monitor.start(timeout=0.05)

    assert monitor.error is timeout.value
    assert assert_unavailable(monitor).__cause__ is timeout.value

    with pytest.raises(RuntimeError):
        await monitor.start(timeout=1.0)

    await monitor.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0.0, -1.0, math.inf, math.nan])
async def test_invalid_timeout_does_not_start(kit, controller_impl, timeout):
    monitor = ControllerMonitor(kit.controller(controller_impl.entity))

    with pytest.raises(ValueError):
        await monitor.start(timeout=timeout)

    # Neither a rejected start nor an early stop uses up the monitor.
    await monitor.stop()
    await monitor.start(timeout=1.0)
    await monitor.stop()


@pytest.mark.asyncio
async def test_repeated_start_is_rejected(kit, controller_impl):
    monitor = ControllerMonitor(kit.controller(controller_impl.entity))
    starting = asyncio.create_task(monitor.start(timeout=1.0))
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError):
        await monitor.start(timeout=1.0)

    await starting

    with pytest.raises(RuntimeError):
        await monitor.start(timeout=1.0)

    await monitor.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"not json", b"{}"])
async def test_invalid_initial_state_fails_start(kit, payload):
    client = kit.controller(ABSENT)
    await kit.backend.key_value(client.entity).create("ControllerState", payload)
    monitor = ControllerMonitor(client)

    with pytest.raises(ControllerStateUnknown) as unknown:
        await monitor.start(timeout=1.0)

    assert isinstance(unknown.value.__cause__, ValidationError)
    assert monitor.error is unknown.value.__cause__
    assert monitor.last_state is None

    await monitor.stop()


# Semantic notification


@pytest.mark.asyncio
async def test_short_lived_changes_keep_order(controller_impl, monitor):
    with monitor.state_changes() as changes:
        assert (await next_report(changes)).current == UNKNOWN

        for state in (OPERATE, UNKNOWN, OPERATE):
            await controller_impl.set_internal_state(state)

        reports = [(await next_report(changes)).current for _ in range(3)]

    assert reports == [OPERATE, UNKNOWN, OPERATE]


@pytest.mark.asyncio
async def test_lifecycle_input_changes_notify(kit, controller_impl, monitor):
    @controller_impl.task_handler(InitTask)
    async def init(task: InitTask):
        pass

    with monitor.state_changes(initial_value=False) as changes:
        async with asyncio.timeout(1.0):
            await kit.controller(controller_impl.entity).execute_task(
                InitTask(), context=lifecycle_input("open")
            )

        await controller_impl.set_internal_state(OPERATE, context=lifecycle_input("closed"))
        await controller_impl.set_internal_state(ERROR)
        await controller_impl.set_internal_state(ERROR, context=lifecycle_input("open"))

        reports = [await next_report(changes) for _ in range(5)]

    assert [(r.current, r.target, r.context) for r in reports] == [
        (TRANSITION, OPERATE, lifecycle_input("open")),
        (OPERATE, None, lifecycle_input("open")),
        (OPERATE, None, lifecycle_input("closed")),
        (ERROR, None, lifecycle_input("closed")),
        (ERROR, None, lifecycle_input("open")),
    ]


@pytest.mark.asyncio
async def test_bookkeeping_refreshes_state_without_notifying(kit, controller_impl, monitor, until):
    with monitor.state_changes() as changes:
        await next_report(changes)

        await kit.controller(controller_impl.entity).disable()
        await until(lambda: not monitor.state.enable_state.enabled, timeout=1.0)

        assert changes.empty()

        # A new report of the same state, target and input.
        reported = monitor.operating_state.event_id
        await controller_impl.set_internal_state(UNKNOWN)
        await until(lambda: monitor.operating_state.event_id != reported, timeout=1.0)

        assert changes.empty()

        await controller_impl.set_internal_state(STANDBY)

        assert (await next_report(changes)).current == STANDBY


@pytest.mark.asyncio
async def test_deletion_retains_state(kit, controller_impl, monitor):
    with monitor.state_changes() as changes:
        await next_report(changes)
        await kit.backend.key_value(controller_impl.entity).delete("ControllerState")

        assert monitor.operating_state.current == UNKNOWN

        await controller_impl.set_internal_state(OPERATE)

        assert (await next_report(changes)).current == OPERATE
        assert monitor.error is None


@pytest.mark.asyncio
async def test_new_subscriber_starts_at_latest_report(controller_impl, monitor):
    with monitor.state_changes() as first:
        await next_report(first)
        await controller_impl.set_internal_state(OPERATE)
        await controller_impl.set_internal_state(STANDBY)

        assert (await next_report(first)).current == OPERATE
        assert (await next_report(first)).current == STANDBY

        with monitor.state_changes() as second:
            assert (await next_report(second)).current == STANDBY
            assert second.empty()


@pytest.mark.asyncio
async def test_overflow_closes_only_slow_subscriber(controller_impl, monitor):
    with monitor.state_changes(maxsize=1) as slow, monitor.state_changes() as other:
        await controller_impl.set_internal_state(OPERATE)

        assert (await next_report(other)).current == UNKNOWN
        assert (await next_report(other)).current == OPERATE

        with pytest.raises(asyncio.QueueShutDown):
            slow.get_nowait()

        assert monitor.operating_state.current == OPERATE
        assert monitor.error is None


# Ownership


@pytest.mark.asyncio
async def test_state_copies_are_isolated(controller_impl, monitor, until):
    await controller_impl.set_internal_state(OPERATE, context=lifecycle_input("closed"))
    await until(lambda: monitor.operating_state.current == OPERATE, timeout=1.0)

    monitor.state.operating_state.context.set_value("dome", "open")
    monitor.operating_state.context.set_value("dome", "open")
    monitor.last_state.operating_state.context.set_value("dome", "open")

    assert monitor.operating_state.context == lifecycle_input("closed")
    assert monitor.last_state.operating_state.context == lifecycle_input("closed")


@pytest.mark.asyncio
async def test_reports_are_shared_apart_from_state(controller_impl, monitor, until):
    with monitor.state_changes() as first, monitor.state_changes() as second:
        report = await next_report(first)

        assert await next_report(second) is report

        # A consumer violating the read-only contract cannot reach the cache.
        report.context.set_value("dome", "open")

        assert monitor.operating_state.context == KeywordDict()

        reported = monitor.operating_state.event_id
        await controller_impl.set_internal_state(UNKNOWN)
        await until(lambda: monitor.operating_state.event_id != reported, timeout=1.0)

        assert first.empty()


# Failure and stop


@pytest.mark.asyncio
async def test_invalid_update_fails_monitoring(kit, controller_impl, monitor):
    with monitor.state_changes() as changes:
        await next_report(changes)
        await kit.backend.key_value(controller_impl.entity).update("ControllerState", b"{}")

        with pytest.raises(asyncio.QueueShutDown):
            await next_report(changes)

    assert isinstance(monitor.error, ValidationError)
    assert assert_unavailable(monitor).__cause__ is monitor.error
    assert monitor.last_state.operating_state.current == UNKNOWN

    # Stopping keeps the failure.
    error = monitor.error
    await monitor.stop()

    assert monitor.error is error


@pytest.mark.asyncio
async def test_stop_wakes_subscribers(monitor):
    with monitor.state_changes() as changes:
        await next_report(changes)
        await asyncio.gather(monitor.stop(), monitor.stop())
        await monitor.stop()

        with pytest.raises(asyncio.QueueShutDown):
            await next_report(changes)

    assert monitor.error is None
    assert monitor.last_state is not None
    assert assert_unavailable(monitor).__cause__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [0.0, 0.1], ids=["before-reader", "awaiting-state"])
async def test_stop_during_readiness(kit, delay):
    monitor = ControllerMonitor(kit.controller(ABSENT))
    starting = asyncio.create_task(monitor.start(timeout=10.0))
    await asyncio.sleep(delay)
    await monitor.stop()

    async with asyncio.timeout(1.0):
        with pytest.raises(ControllerStateUnknown):
            await starting

    assert monitor.error is None

    with pytest.raises(RuntimeError):
        await monitor.start(timeout=1.0)


@pytest.mark.asyncio
async def test_start_cancellation_stops_monitoring(kit):
    monitor = ControllerMonitor(kit.controller(ABSENT))
    starting = asyncio.create_task(monitor.start(timeout=10.0))
    await asyncio.sleep(0.1)
    starting.cancel()

    async with asyncio.timeout(1.0):
        with pytest.raises(asyncio.CancelledError):
            await starting

    assert monitor.error is None
    assert_unavailable(monitor)
