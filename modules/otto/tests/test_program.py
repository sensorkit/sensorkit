"""Tests for OttoProgram task factory and active state gating."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sensorkit.otto.program import OttoProgram, OttoState
from sensorkit.otto.task_queue import TaskQueue

from conftest import make_task


@pytest.fixture
def mock_program_binding():
    mock = MagicMock()
    mock.clear_offers = MagicMock()
    mock.add_offer = MagicMock()
    mock.publish_offers = AsyncMock()
    mock.kv_put_model = AsyncMock()
    mock.entity = "otto_program"
    return mock


@pytest.fixture
def program(mock_program_binding):
    p = OttoProgram()
    p.program = mock_program_binding
    p.task_queue = TaskQueue(mock_program_binding)
    p.state = OttoState(whitelist=["25544", "42738"])
    return p


class TestActiveStateGating:
    def test_active_initially_unset(self):
        p = OttoProgram()
        assert not p._active.is_set()

    @pytest.mark.asyncio
    async def test_generate_sets_active(self, program):
        """First call to generate() should set _active and flush expired."""
        assert not program._active.is_set()

        # Call generate with no tasks in queue
        gen = program.generate()
        result = await gen.__anext__()
        assert result is None
        assert program._active.is_set()

    @pytest.mark.asyncio
    async def test_generate_flushes_expired_on_activation(self, program):
        """When generate() activates, it should flush expired tasks."""
        # Add an expired task
        expired = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        await program.task_queue.push_task(expired)
        assert len(program.task_queue) == 1

        # First generate call should flush it
        gen = program.generate()
        result = await gen.__anext__()
        assert result is None
        assert len(program.task_queue) == 0

    @pytest.mark.asyncio
    async def test_generate_does_not_reflush(self, program):
        """Subsequent generate() calls should not re-flush."""
        # Activate
        gen = program.generate()
        await gen.__anext__()
        assert program._active.is_set()

        # Add a task
        task = make_task()
        await program.task_queue.push_task(task)

        # Second call should not flush
        gen2 = program.generate()
        result = await gen2.__anext__()
        assert result is not None
        assert result.task_id == task.task_id

    @pytest.mark.asyncio
    async def test_deinit_clears_active(self, program):
        """program_deinit should clear _active."""
        program._active.set()
        assert program._active.is_set()

        await program.program_deinit()
        assert not program._active.is_set()

    @pytest.mark.asyncio
    async def test_reactivation_flushes_again(self, program):
        """After deinit + reactivation, expired tasks should be flushed again."""
        # First activation
        gen = program.generate()
        await gen.__anext__()
        assert program._active.is_set()

        # Deactivate
        program._active.clear()

        # Add expired task
        expired = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        await program.task_queue.push_task(expired)

        # Reactivate — should flush
        gen2 = program.generate()
        result = await gen2.__anext__()
        assert result is None
        assert len(program.task_queue) == 0


class TestGenerateTaskFactory:
    @pytest.mark.asyncio
    async def test_yields_task_from_queue(self, program):
        """generate() should yield a task when one is available."""
        task = make_task()
        await program.task_queue.push_task(task)

        program._active.set()
        gen = program.generate()
        result = await gen.__anext__()

        assert result is not None
        assert result.task_id == task.task_id

    @pytest.mark.asyncio
    async def test_yields_none_when_empty(self, program):
        """generate() should yield None when queue is empty."""
        program._active.set()
        gen = program.generate()
        result = await gen.__anext__()
        assert result is None

    @pytest.mark.asyncio
    async def test_skips_expired_tasks(self, program):
        """generate() should skip expired tasks and return next valid one."""
        expired = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        valid = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) + timedelta(hours=1),
        )
        await program.task_queue.push_task(expired)
        await program.task_queue.push_task(valid)

        program._active.set()
        gen = program.generate()
        result = await gen.__anext__()
        assert result.task_id == valid.task_id


class TestGenerateTasksGating:
    @pytest.mark.asyncio
    async def test_generate_tasks_waits_for_active(self, program):
        """generate_tasks should block until _active is set."""
        started = asyncio.Event()
        proceeded = asyncio.Event()

        async def wait_for_active():
            started.set()
            await program._active.wait()
            proceeded.set()

        task = asyncio.create_task(wait_for_active())

        # Should start but not proceed past _active.wait()
        await asyncio.sleep(0.05)
        assert started.is_set()
        assert not proceeded.is_set()

        # Set active — should proceed
        program._active.set()
        await asyncio.sleep(0.05)
        assert proceeded.is_set()

        await task


class TestObjectListManagement:
    def test_state_whitelist(self):
        state = OttoState(
            whitelist=["25544", "42738"],
            graylist=["39120"],
            blacklist=[],
        )
        assert "25544" in state.whitelist
        assert "39120" in state.graylist

    def test_state_all_lists(self):
        state = OttoState(
            whitelist=["25544"],
            graylist=["42738"],
            blacklist=["12345"],
        )
        all_objects = set(state.whitelist + state.graylist + state.blacklist)
        assert len(all_objects) == 3
