# SPDX-License-Identifier: Apache-2.0
"""Tests for published agent runtime state."""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.auto.agent import (
    AgentConfig,
    AgentConfigureRequest,
    AgentControllerInfo,
    AgentOperatingState,
    AgentRuntimeState,
    AgentSchedulerState,
    AgentState,
    ControllerRuntimeState,
    agent_service,
)
from sensorkit.auto.constraint import (
    Constraint,
    ConstraintDetails,
    ConstraintEvaluator,
    ConstraintManager,
)
from sensorkit.auto.failure import FailureState, FailureStateSnapshot
from sensorkit.auto.lifecycle import ControllerLifecycle, LifecycleState
from sensorkit.auto.operator import ControllerConfig, VirtualOperator
from sensorkit.auto.procedures import ControllerFailure, LifecycleStep
from sensorkit.backend.base import Entity
from sensorkit.core.client import SensorKit

UTC_NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


class StatusProbeDetails(ConstraintDetails):
    kind: Literal["status-probe"] = "status-probe"
    reading: int
    labels: list[str]


class StatusProbeConstraint(Constraint):
    """Constrain with structured details once monitoring starts."""

    kind: str = "status-probe"
    reading: int = 0

    async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
        details = StatusProbeDetails(reading=self.reading, labels=["sampled"])
        evaluator.constrain(f"reading {self.reading}", details=details)
        evaluator.ready()
        await asyncio.Event().wait()


class ClearProbeConstraint(Constraint):
    kind: str = "clear-probe"

    async def check_task(self, evaluator: ConstraintEvaluator, /, **kwargs):
        evaluator.clear("all clear")
        evaluator.ready()
        await asyncio.Event().wait()


def controller_config(name: str = "ctrl1", constraints: list[Constraint] | None = None):
    config = ControllerConfig(constraints=constraints or [])
    config.name = name
    return config


def agent_state(runtime_state: AgentRuntimeState | None = None):
    return AgentState(
        operating_state=AgentOperatingState(
            global_control_enabled=False,
            controllers={"ctrl1": AgentControllerInfo.default()},
        ),
        scheduler_state=AgentSchedulerState(scheduling_enabled=False),
        runtime_state=runtime_state or AgentRuntimeState(),
    )


def stale_runtime_state():
    """A runtime state observed by a previous process with failures pending recovery."""
    return AgentRuntimeState(
        controllers={
            "ctrl1": ControllerRuntimeState(
                lifecycle=LifecycleState(
                    step=LifecycleStep.RECOVER,
                    failure_state=FailureStateSnapshot(
                        failures=5,
                        attempts=3,
                        retry_at=UTC_NOW + timedelta(hours=1),
                        failure="ControllerFailure: old",
                    ),
                    last_reactor_error="RuntimeError: old",
                ),
                constrained=True,
                constraints=[],
            )
        }
    )


@contextlib.asynccontextmanager
async def monitoring(manager: ConstraintManager):
    """Run constraint monitoring until the context exits."""
    ready = asyncio.Event()

    async def run():
        async with asyncio.TaskGroup() as group:
            assert await manager.start(task_group=group, ready_timeout=2.0)
            ready.set()

    task = asyncio.create_task(run())

    try:
        async with asyncio.timeout(5.0):
            await ready.wait()

        yield
    finally:
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task


# --- Failure snapshots ---


def test_snapshot_projects_timing_without_changing_bookkeeping():
    state = FailureState()
    error = ControllerFailure("lost")
    utc_before = datetime.now(UTC)
    monotonic = time.monotonic()
    state.record_failure(monotonic - 3.0, error)
    state.set_settled(True, monotonic - 2.0)
    before = state.model_copy()

    snapshot = state.snapshot(monotonic)
    utc_after = datetime.now(UTC)

    assert snapshot.failures == 1
    assert snapshot.recover_after_failures == 2
    assert snapshot.attempts == 0
    assert snapshot.recovery_attempts == 6
    assert not snapshot.recovery_due
    assert not snapshot.emergency_due
    assert utc_before + timedelta(seconds=1.9) <= snapshot.retry_at
    assert snapshot.retry_at <= utc_after + timedelta(seconds=2)
    assert snapshot.retry_at - snapshot.settled_at == timedelta(seconds=4)
    assert snapshot.stable_period == 900.0
    assert snapshot.failure == "ControllerFailure: lost"
    assert snapshot.error is None
    assert snapshot.waiting(monotonic) is None

    assert state == before
    assert state.error is error
    assert state.waiting(monotonic) == monotonic + 2.0


def test_snapshot_omits_passed_retry_deadline():
    state = FailureState()
    state.delay_retry(10.0)

    assert state.snapshot(20.0).retry_at is None
    assert state.snapshot(12.0).retry_at is not None
    assert state.waiting(12.0) == 15.0


def test_snapshot_reference_does_not_move_bookkeeping():
    state = FailureState()
    monotonic = time.monotonic()
    state.record_failure(monotonic, ControllerFailure("lost"))
    state.set_settled(True, monotonic + 1.0)
    before = state.model_copy()

    first = state.snapshot(monotonic)
    earlier = state.snapshot(monotonic - 3600.0)

    assert abs(earlier.retry_at - first.retry_at) < timedelta(milliseconds=100)
    assert abs(earlier.settled_at - first.settled_at) < timedelta(milliseconds=100)
    assert state.waiting(monotonic) == monotonic + 5.0
    assert state == before


def test_snapshot_does_not_apply_an_earned_reset():
    state = FailureState()
    state.record_failure(0.0, ControllerFailure("lost"))
    state.set_settled(True, 10.0)

    snapshot = state.snapshot(2000.0)

    assert snapshot.failures == 1
    assert snapshot.settled_at is not None
    assert state.failures == 1

    # The reset remains available to the next unsettled observation.
    state.set_settled(False, 2000.0)
    assert state.failures == 0


def test_snapshot_reports_escalation_flags():
    state = FailureState()
    state.record_failure(0.0, ControllerFailure("first"))
    state.begin_procedure()
    state.record_failure(1.0, ControllerFailure("second"))

    for _ in range(state.recovery_attempts):
        state.record_failed_attempt(2.0)

    snapshot = state.snapshot(3.0)

    assert snapshot.recovery_due
    assert snapshot.emergency_due
    assert snapshot.attempts == 6


def test_snapshot_round_trips_through_json():
    state = FailureState()
    state.record_failure(0.0, ControllerFailure("lost"))
    state.set_settled(True, 1.0)
    snapshot = state.snapshot(2.0)

    restored = FailureStateSnapshot.model_validate_json(snapshot.model_dump_json())

    assert restored == snapshot
    assert restored.retry_at.utcoffset() == timedelta(0)
    assert '"recovery_due":false' in snapshot.model_dump_json()


# --- Lifecycle status ---


def test_lifecycle_status_separates_procedure_failure_from_reactor_error():
    lifecycle = ControllerLifecycle()
    error = ControllerFailure("procedure")
    lifecycle.failure_state.record_failure(0.0, error)
    lifecycle.last_error = RuntimeError("reactor")

    status = lifecycle.status(1.0)

    assert status.step == LifecycleStep.WAIT
    assert status.failure_state.failure == "ControllerFailure: procedure"
    assert status.last_reactor_error == "RuntimeError: reactor"
    assert lifecycle.failure_state.error is error

    lifecycle.failure_state.begin_procedure()
    status = lifecycle.status(1.0)

    assert status.failure_state.failure is None
    assert status.failure_state.failures == 1
    assert status.last_reactor_error == "RuntimeError: reactor"


# --- Runtime samples ---


@pytest.mark.asyncio
async def test_sample_reports_constraints_in_config_order():
    config = controller_config(
        constraints=[
            StatusProbeConstraint(reading=1),
            ClearProbeConstraint(),
            StatusProbeConstraint(reading=2),
        ]
    )
    operator = VirtualOperator([config])
    manager = operator.drivers["ctrl1"].constraints

    # The manager is not constrained before monitoring starts.
    unmonitored = AgentRuntimeState.sample(operator).controllers["ctrl1"]

    assert not unmonitored.constrained
    assert [c.state for c in unmonitored.constraints] == ["not_ready"] * 3

    async with monitoring(manager):
        sampled = AgentRuntimeState.sample(operator)

        # Later changes to live constraint details do not reach the sample.
        manager.entries[0].details.labels.append("later")

    controller = sampled.controllers["ctrl1"]

    assert controller.constrained
    assert [(c.kind, c.state, c.reason) for c in controller.constraints] == [
        ("status-probe", "active", "reading 1"),
        ("clear-probe", "clear", "all clear"),
        ("status-probe", "active", "reading 2"),
    ]
    assert controller.constraints[0].details == StatusProbeDetails(reading=1, labels=["sampled"])

    restored = AgentState.model_validate_json(agent_state(sampled).model_dump_json())
    constraints = restored.runtime_state.controllers["ctrl1"].constraints

    assert restored.runtime_state == sampled
    assert [type(c.details) for c in constraints] == [
        StatusProbeDetails,
        type(None),
        StatusProbeDetails,
    ]
    assert constraints[2].details.reading == 2


@pytest.mark.asyncio
async def test_sample_reports_fresh_lifecycle_state():
    operator = VirtualOperator([controller_config("ctrl1"), controller_config("ctrl2")])
    operator.drivers["ctrl2"].lifecycle.failure_state.delay_retry(
        asyncio.get_running_loop().time()
    )

    sampled = AgentRuntimeState.sample(operator)

    assert list(sampled.controllers) == ["ctrl1", "ctrl2"]
    assert sampled.controllers["ctrl1"].lifecycle.step == LifecycleStep.WAIT
    assert sampled.controllers["ctrl1"].lifecycle.failure_state.failures == 0
    assert sampled.controllers["ctrl1"].lifecycle.failure_state.retry_at is None

    # The retry deadline follows the event time by the retry delay.
    retry_at = sampled.controllers["ctrl2"].lifecycle.failure_state.retry_at
    event_time = datetime.fromtimestamp(sampled.timestamp() / 1000, UTC)

    assert abs((retry_at - event_time).total_seconds() - 5.0) < 0.5


# --- Agent state ---


@pytest.mark.asyncio
async def test_stored_state_without_runtime_state_loads(backend, kit):
    old = agent_state().model_dump_json(exclude={"runtime_state"})
    await backend.key_value(Entity.at("agent")).update("AgentState", old.encode())

    state = await AgentState.recover(kit.entity("agent"))

    assert state.runtime_state.controllers == {}


@pytest.mark.asyncio
async def test_runtime_state_is_stored_and_streamed(kit, entity_impl):
    operator = VirtualOperator([controller_config()])
    state = agent_state()
    first = AgentRuntimeState.sample(operator)
    await state.update(entity_impl, first)

    client = kit.entity(entity_impl.entity)
    assert (await client.kv_get_model(AgentState)).runtime_state == first

    stream = AgentState.event_stream(client, AgentRuntimeState)

    try:
        async with asyncio.timeout(5.0):
            assert await anext(stream) == first

            second = AgentRuntimeState.sample(operator)
            await state.update(entity_impl, second)

            assert await anext(stream) == second
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_control_requests_do_not_apply_runtime_state(entity_impl):
    config = controller_config()
    operator = VirtualOperator([config])
    stale = stale_runtime_state()
    state = agent_state(stale)
    request = AgentConfigureRequest(global_control_enabled=True, scheduling_enabled=True)

    snapshot = await state.update(
        entity_impl,
        state.scheduler_state.derive_for_request(request),
        state.operating_state.derive_for_request(request),
        return_snapshot=True,
    )
    await snapshot.apply_to_operator(operator, {"ctrl1": config})

    assert snapshot.runtime_state == stale
    assert operator.drivers["ctrl1"].lifecycle.failure_state == FailureState()

    sampled = AgentRuntimeState.sample(operator).controllers["ctrl1"]
    assert sampled.lifecycle.failure_state.failures == 0
    assert sampled.lifecycle.last_reactor_error is None


@pytest.mark.asyncio
async def test_agent_startup_replaces_stored_runtime_state(backend, kit, service_context):
    agent = kit.entity("agent")
    stale = stale_runtime_state()
    await agent.kv_put_model(AgentConfig(controllers={"ctrl1": ControllerConfig()}))
    await agent.kv_put_model(agent_state(stale))

    stream = AgentState.event_stream(agent, AgentRuntimeState)
    service = sk.Service("agent", sk.VERSION)
    service.client = SensorKit(backend)
    task = None

    try:
        async with asyncio.timeout(10.0):
            assert await anext(stream) == stale

            task = asyncio.create_task(agent_service._func(service))

            # The controller has no site position yet, so operator startup is still waiting.
            initial = await anext(stream)

            assert not service.running.done()
            assert (await agent.kv_get_model(AgentState)).runtime_state == initial

            lifecycle = initial.controllers["ctrl1"].lifecycle
            assert lifecycle.step == LifecycleStep.WAIT
            assert lifecycle.failure_state == FailureStateSnapshot()
            assert lifecycle.last_reactor_error is None

            controller = await service_context.register_controller("ctrl1")
            await controller.kv_put_model(
                SitePosition(latitude_degrees=42.0, longitude_degrees=123.0, altitude_km=3.0)
            )

            # Startup completes and the publication loop resumes sampling.
            published = await anext(stream)

            assert published.event_id.int > initial.event_id.int
            assert list(published.controllers) == ["ctrl1"]
    finally:
        await stream.aclose()

        if task is not None:
            task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await task

        if service.context is not None:
            with contextlib.suppress(Exception):
                await service.context.shutdown()
