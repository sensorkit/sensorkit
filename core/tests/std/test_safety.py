import asyncio
import contextlib

import pytest
import pytest_asyncio

from sensorkit.auto.constraint import ConstraintEvaluator
from sensorkit.backend.base import Backend, Entity
from sensorkit.common.keyword import dump_keyword_json, get_keyword_info
from sensorkit.core.client import SensorKit
from sensorkit.std.safety import BasicSafety, SafetyConstraint


async def _publish_keyword(backend: Backend, entity_name: str, model):
    stream = backend.stream(Entity.at(entity_name))
    await stream.publish(get_keyword_info(model).key, dump_keyword_json(model))


@pytest_asyncio.fixture
async def ev_timeout():
    async with asyncio.timeout(None) as t:
        yield t


@pytest.mark.asyncio
async def test_safety_constraint_model(ev_timeout):
    c = SafetyConstraint(provider="safety-provider")
    assert c.kind == "safety"
    assert c.provider == "safety-provider"

    ev = ConstraintEvaluator(c, timeout=ev_timeout)
    assert not ev.is_ready
    # Fail-closed default — see test_constraint_evaluator.
    assert ev.is_active


@pytest.mark.asyncio
async def test_safety_check_task_safe(backend, ev_timeout):
    """Safe reading clears the constraint."""
    c = SafetyConstraint(provider="safety-svc")
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
    c = SafetyConstraint(provider="safety-svc")
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
    c = SafetyConstraint(provider="safety-svc")
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
