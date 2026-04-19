"""Tests for Otto's TaskQueue."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from sensorkit.otto.task_queue import TaskQueue

from conftest import make_task


@pytest.fixture
def queue(mock_program_binding):
    return TaskQueue(mock_program_binding)


class TestPushTask:
    @pytest.mark.asyncio
    async def test_push_single(self, queue):
        task = make_task()
        await queue.push_task(task)
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_push_sorted_by_end_time(self, queue):
        later = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) + timedelta(minutes=20),
        )
        sooner = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) + timedelta(minutes=5),
        )

        await queue.push_task(later)
        await queue.push_task(sooner)

        popped = await queue.pop_task()
        assert popped.task_id == sooner.task_id

    @pytest.mark.asyncio
    async def test_push_updates_offers(self, queue, mock_program_binding):
        task = make_task()
        await queue.push_task(task)

        mock_program_binding.clear_offers.assert_called()
        mock_program_binding.add_offer.assert_called_once()
        mock_program_binding.publish_offers.assert_awaited()


class TestPopTask:
    @pytest.mark.asyncio
    async def test_pop_returns_task(self, queue):
        task = make_task()
        await queue.push_task(task)

        result = await queue.pop_task()
        assert result is not None
        assert result.task_id == task.task_id
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_pop_empty_returns_none(self, queue):
        result = await queue.pop_task()
        assert result is None

    @pytest.mark.asyncio
    async def test_pop_removes_expired(self, queue):
        expired = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        valid = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) + timedelta(hours=1),
        )

        await queue.push_task(expired)
        await queue.push_task(valid)

        result = await queue.pop_task()
        assert result.task_id == valid.task_id


class TestPeekTask:
    @pytest.mark.asyncio
    async def test_peek_does_not_remove(self, queue):
        task = make_task()
        await queue.push_task(task)

        result = await queue.peek_task()
        assert result is not None
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_peek_empty(self, queue):
        result = await queue.peek_task()
        assert result is None

    @pytest.mark.asyncio
    async def test_peek_removes_expired(self, queue):
        expired = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        await queue.push_task(expired)

        result = await queue.peek_task()
        assert result is None
        assert len(queue) == 0


class TestRemoveTask:
    @pytest.mark.asyncio
    async def test_remove_existing(self, queue):
        task = make_task()
        await queue.push_task(task)

        removed = await queue.remove_task(task.task_id)
        assert removed is True
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self, queue):
        removed = await queue.remove_task(uuid.uuid1())
        assert removed is False


class TestFlushExpired:
    @pytest.mark.asyncio
    async def test_flush_removes_expired(self, queue):
        expired1 = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=2),
        )
        expired2 = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        valid = make_task(
            task_id=uuid.uuid1(),
            end_time=datetime.now(UTC) + timedelta(hours=1),
        )

        await queue.push_task(expired1)
        await queue.push_task(expired2)
        await queue.push_task(valid)

        removed = await queue.flush_expired()
        assert removed == 2
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_flush_empty_queue(self, queue):
        removed = await queue.flush_expired()
        assert removed == 0

    @pytest.mark.asyncio
    async def test_flush_no_expired(self, queue):
        task = make_task(
            end_time=datetime.now(UTC) + timedelta(hours=1),
        )
        await queue.push_task(task)

        removed = await queue.flush_expired()
        assert removed == 0
        assert len(queue) == 1
