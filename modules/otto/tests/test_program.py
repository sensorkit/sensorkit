"""Tests for OttoProgram task factory."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_task

from sensorkit.otto.program import OttoProgram, OttoState
from sensorkit.otto.task_queue import TaskQueue


def _resolved_execution():
    """An already-resolved awaitable standing in for the dispatched execution.

    The task factory resumes with `await (yield ...)`, awaiting the execution the tasking loop
    sends back; in these unit tests we drive the generator directly and feed it a settled future.
    """
    fut = asyncio.get_running_loop().create_future()
    fut.set_result(None)
    return fut


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
    p.config = MagicMock()
    p.config.task.end_time_deadband_seconds = 60
    return p


class TestGenerateTaskFactory:
    @pytest.mark.asyncio
    async def test_yields_task_from_queue(self, program):
        """generate() should yield a task when one is available."""
        task = make_task()
        await program.task_queue.push_task(task)

        gen = program.generate()
        result = await gen.__anext__()

        assert result is not None
        assert result.task is task

    @pytest.mark.asyncio
    async def test_yields_none_when_empty(self, program):
        """generate() should yield None when queue is empty."""
        gen = program.generate()
        result = await gen.__anext__()
        assert result is None

    @pytest.mark.asyncio
    async def test_skips_expired_tasks(self, program):
        """generate() should skip expired tasks and return next valid one."""
        expired = make_task(end_time=datetime.now(UTC) - timedelta(hours=1))
        valid = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))
        await program.task_queue.push_task(expired)
        await program.task_queue.push_task(valid)

        gen = program.generate()
        result = await gen.__anext__()
        assert result.task is valid

    @pytest.mark.asyncio
    async def test_pops_in_end_time_order(self, program):
        """Tasks should be popped in end_time order (soonest first)."""
        later = make_task(end_time=datetime.now(UTC) + timedelta(hours=2))
        sooner = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))
        await program.task_queue.push_task(later)
        await program.task_queue.push_task(sooner)

        gen = program.generate()
        result = await gen.__anext__()
        assert result.task is sooner


class TestEndTimeRefresh:
    @pytest.mark.asyncio
    async def test_end_time_refreshed_on_pop(self, program):
        """generate() should recalculate end_time based on current time."""
        # Create a task with a stale end_time (as if generated 30 min ago)
        stale_end = datetime.now(UTC) + timedelta(seconds=30)
        task = make_task(
            end_time=stale_end,
            integration_time=10.0,
            frame_count=3,
        )
        await program.task_queue.push_task(task)

        # Mock config for deadband
        program.config = MagicMock()
        program.config.task.end_time_deadband_seconds = 60

        before = datetime.now(UTC)
        gen = program.generate()
        result = await gen.__anext__()
        after = datetime.now(UTC)

        assert result is not None
        # end_time should be ~now + (10*3) + 60 = now + 90 seconds
        expected_min = before + timedelta(seconds=90)
        expected_max = after + timedelta(seconds=90)
        assert expected_min <= result.task.end_time <= expected_max

    @pytest.mark.asyncio
    async def test_end_time_not_stale(self, program):
        """Even tasks generated hours ago should get a fresh end_time."""
        old_end = datetime.now(UTC) - timedelta(hours=1)  # Already expired
        # But it's still in queue (not yet popped — pop_task removes expired)
        task = make_task(
            end_time=datetime.now(UTC) + timedelta(seconds=1),  # barely valid
            integration_time=5.0,
            frame_count=2,
        )
        await program.task_queue.push_task(task)

        program.config = MagicMock()
        program.config.task.end_time_deadband_seconds = 30

        gen = program.generate()
        result = await gen.__anext__()

        # Should have a fresh end_time: ~now + (5*2) + 30 = now + 40s
        assert result.task.end_time > datetime.now(UTC) + timedelta(seconds=35)


class TestInterTaskDelay:
    @pytest.mark.asyncio
    async def test_sleeps_after_task_when_delay_configured(self, program):
        """A configured delay should sleep after a task completes."""
        program.config = MagicMock()
        program.config.task.end_time_deadband_seconds = 60
        program.config.task.inter_task_delay_seconds = 5.0
        await program.task_queue.push_task(make_task())

        gen = program.generate()
        result = await gen.__anext__()
        assert result is not None

        # Resume past the yield so the post-task delay runs. The factory awaits the value sent
        # back (the execution), so feed it an already-resolved awaitable.
        with patch(
            "sensorkit.otto.program.asyncio.sleep", new_callable=AsyncMock
        ) as mock_sleep:
            with pytest.raises(StopAsyncIteration):
                await gen.asend(_resolved_execution())
            mock_sleep.assert_awaited_once_with(5.0)

    @pytest.mark.asyncio
    async def test_no_sleep_when_delay_zero(self, program):
        """The default (0) delay should not sleep between tasks."""
        program.config = MagicMock()
        program.config.task.end_time_deadband_seconds = 60
        program.config.task.inter_task_delay_seconds = 0.0
        await program.task_queue.push_task(make_task())

        gen = program.generate()
        await gen.__anext__()

        with patch(
            "sensorkit.otto.program.asyncio.sleep", new_callable=AsyncMock
        ) as mock_sleep:
            with pytest.raises(StopAsyncIteration):
                await gen.asend(_resolved_execution())
            mock_sleep.assert_not_awaited()


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


class TestProgramDeinit:
    @pytest.mark.asyncio
    async def test_deinit_saves_state(self, program):
        """program_deinit should save state."""
        await program.program_deinit()
        program.program.kv_put_model.assert_awaited()
