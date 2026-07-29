# SPDX-License-Identifier: Apache-2.0
"""Tests for Otto's TaskQueue."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from .data import make_task
from sensorkit.core.program import ProgramOffering
from sensorkit.otto.task_queue import TaskQueue


@pytest.fixture
def queue(program_impl):
    return TaskQueue(program_impl)


class TestPushTask:
    @pytest.mark.asyncio
    async def test_push_single(self, queue):
        task = make_task()
        await queue.push_task(task)
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_push_sorted_by_end_time(self, queue):
        later = make_task(end_time=datetime.now(UTC) + timedelta(minutes=20))
        sooner = make_task(end_time=datetime.now(UTC) + timedelta(minutes=5))

        await queue.push_task(later)
        queued_sooner = await queue.push_task(sooner)

        popped = await queue.pop_task()
        assert popped.id == queued_sooner.id
        assert popped.task is sooner

    @pytest.mark.asyncio
    async def test_push_updates_offers(self, queue, program_impl, recorder):
        published = await recorder()
        task = make_task()

        await queue.push_task(task)

        (offer,) = program_impl.get_offers()
        assert offer.end == task.end_time

        # The queued task's id rides along as interval data locally, but is not part of the
        # published window, so compare the bounds.
        offering = await published.wait_for(ProgramOffering)
        (window,) = offering.offer_windows
        assert (window.begin, window.end) == (offer.begin, offer.end)


class TestPopTask:
    @pytest.mark.asyncio
    async def test_pop_returns_task(self, queue):
        task = make_task()
        queued = await queue.push_task(task)

        result = await queue.pop_task()
        assert result is not None
        assert result.id == queued.id
        assert result.task is task
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_pop_empty_returns_none(self, queue):
        result = await queue.pop_task()
        assert result is None

    @pytest.mark.asyncio
    async def test_pop_removes_expired(self, queue):
        expired = make_task(end_time=datetime.now(UTC) - timedelta(hours=1))
        valid = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))

        await queue.push_task(expired)
        queued_valid = await queue.push_task(valid)

        result = await queue.pop_task()
        assert result.id == queued_valid.id


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
        expired = make_task(end_time=datetime.now(UTC) - timedelta(hours=1))
        await queue.push_task(expired)

        result = await queue.peek_task()
        assert result is None
        assert len(queue) == 0


class TestRemoveTask:
    @pytest.mark.asyncio
    async def test_remove_existing(self, queue):
        task = make_task()
        queued = await queue.push_task(task)

        removed = await queue.remove_task(queued.id)
        assert removed is True
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self, queue):
        removed = await queue.remove_task(uuid.uuid4())
        assert removed is False


class TestFlushExpired:
    @pytest.mark.asyncio
    async def test_flush_removes_expired(self, queue):
        expired1 = make_task(end_time=datetime.now(UTC) - timedelta(hours=2))
        expired2 = make_task(end_time=datetime.now(UTC) - timedelta(hours=1))
        valid = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))

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
        task = make_task(end_time=datetime.now(UTC) + timedelta(hours=1))
        await queue.push_task(task)

        removed = await queue.flush_expired()
        assert removed == 0
        assert len(queue) == 1
