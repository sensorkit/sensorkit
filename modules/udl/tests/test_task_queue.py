# SPDX-License-Identifier: Apache-2.0
"""Tests for the UDL CollectRequest queue and the offer windows it publishes."""

from datetime import UTC, datetime, timedelta

import pytest

from sensorkit.core.program import ProgramOffering
from sensorkit.udl.task_queue import TaskQueue

from .fakes import tle_request


@pytest.fixture
def queue(program_impl):
    return TaskQueue(program_impl)


class TestTaskQueuePush:
    @pytest.mark.asyncio
    async def test_push_single(self, queue):
        await queue.push_task(tle_request())
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_push_sorted_by_start_time(self, queue):
        now = datetime.now(UTC)

        later = tle_request(
            id="later",
            start_time=now + timedelta(minutes=10),
            end_time=now + timedelta(minutes=20),
        )
        sooner = tle_request(
            id="sooner",
            start_time=now + timedelta(minutes=1),
            end_time=now + timedelta(minutes=11),
        )

        await queue.push_task(later)
        await queue.push_task(sooner)

        tasks = list(queue.iter())
        assert tasks[0].id == "sooner"
        assert tasks[1].id == "later"

    @pytest.mark.asyncio
    async def test_push_updates_offers(self, queue, program_impl, recorder):
        published = await recorder()
        request = tle_request()

        await queue.push_task(request)

        (offer,) = program_impl.get_offers()
        assert (offer.begin, offer.end) == (request.start_time, request.end_time)

        # The request id rides along as interval data locally, but is not part of the published
        # window, so compare the bounds.
        offering = await published.wait_for(ProgramOffering)
        (window,) = offering.offer_windows
        assert (window.begin, window.end) == (offer.begin, offer.end)


class TestTaskQueuePop:
    @pytest.mark.asyncio
    async def test_pop_returns_task(self, queue):
        await queue.push_task(tle_request())

        result = await queue.pop_task()
        assert result is not None
        assert result.id == "test-request-001"
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_pop_empty_returns_none(self, queue):
        assert await queue.pop_task() is None

    @pytest.mark.asyncio
    async def test_pop_removes_expired(self, queue):
        now = datetime.now(UTC)
        expired = tle_request(
            id="expired",
            start_time=now - timedelta(hours=2),
            end_time=now - timedelta(hours=1),
        )
        valid = tle_request(id="valid", start_time=now, end_time=now + timedelta(hours=1))

        await queue.push_task(expired)
        await queue.push_task(valid)

        result = await queue.pop_task()
        assert result.id == "valid"

    @pytest.mark.asyncio
    async def test_pop_holds_request_until_window_opens(self, queue, program_impl):
        now = datetime.now(UTC)
        request = tle_request(
            start_time=now + timedelta(minutes=5),
            end_time=now + timedelta(minutes=6),
        )

        await queue.push_task(request)

        assert await queue.pop_task() is None
        assert len(queue) == 1

        (offer,) = program_impl.get_offers()
        assert offer.begin == request.start_time

    @pytest.mark.asyncio
    async def test_pop_returns_opened_window_before_later_one(self, queue):
        now = datetime.now(UTC)
        opened = tle_request(id="opened", start_time=now - timedelta(seconds=1))
        later = tle_request(id="later", start_time=now + timedelta(minutes=5))

        await queue.push_task(later)
        await queue.push_task(opened)

        assert (await queue.pop_task()).id == "opened"
        assert await queue.pop_task() is None
        assert [task.id for task in queue.iter()] == ["later"]

    @pytest.mark.asyncio
    async def test_pop_after_held_request_removed(self, queue, program_impl):
        now = datetime.now(UTC)
        await queue.push_task(tle_request(id="held", start_time=now + timedelta(minutes=5)))

        assert await queue.pop_task() is None
        assert await queue.remove_task("held") is True

        assert await queue.pop_task() is None
        assert program_impl.get_offers() == []

    @pytest.mark.asyncio
    async def test_pop_request_scheduled_ahead_of_held_one(self, queue):
        now = datetime.now(UTC)
        await queue.push_task(tle_request(id="held", start_time=now + timedelta(minutes=5)))

        assert await queue.pop_task() is None

        await queue.push_task(tle_request(id="ahead", start_time=now - timedelta(seconds=1)))

        assert (await queue.pop_task()).id == "ahead"
        assert [task.id for task in queue.iter()] == ["held"]


class TestTaskQueueRemove:
    @pytest.mark.asyncio
    async def test_remove_existing(self, queue):
        await queue.push_task(tle_request(id="to-remove"))

        assert await queue.remove_task("to-remove") is True
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self, queue):
        assert await queue.remove_task("nonexistent") is False


class TestTaskQueuePeek:
    @pytest.mark.asyncio
    async def test_peek_does_not_remove(self, queue):
        await queue.push_task(tle_request())

        assert await queue.peek_task() is not None
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_peek_empty(self, queue):
        assert await queue.peek_task() is None
