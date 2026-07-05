# SPDX-License-Identifier: Apache-2.0
import pytest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from sensorkit.udl.task_queue import TaskQueue

from conftest import MockCollectRequest


@pytest.fixture
def binding():
    mock = MagicMock()
    mock.clear_offers = MagicMock()
    mock.add_offer = MagicMock()
    mock.publish_offers = AsyncMock()
    return mock


@pytest.fixture
def queue(binding):
    return TaskQueue(binding)


class TestTaskQueuePush:
    @pytest.mark.asyncio
    async def test_push_single(self, queue):
        request = MockCollectRequest.with_tle()
        await queue.push_task(request)
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_push_sorted_by_start_time(self, queue):
        now = datetime.now(UTC)

        r1 = MockCollectRequest.with_tle(
            id="later",
            start_time=now + timedelta(minutes=10),
            end_time=now + timedelta(minutes=20),
        )
        r2 = MockCollectRequest.with_tle(
            id="sooner",
            start_time=now + timedelta(minutes=1),
            end_time=now + timedelta(minutes=11),
        )

        await queue.push_task(r1)
        await queue.push_task(r2)

        tasks = list(queue.iter())
        assert tasks[0].id == "sooner"
        assert tasks[1].id == "later"

    @pytest.mark.asyncio
    async def test_push_updates_offers(self, queue, binding):
        request = MockCollectRequest.with_tle()
        await queue.push_task(request)

        binding.clear_offers.assert_called()
        binding.add_offer.assert_called_once()
        binding.publish_offers.assert_awaited()


class TestTaskQueuePop:
    @pytest.mark.asyncio
    async def test_pop_returns_task(self, queue):
        request = MockCollectRequest.with_tle()
        await queue.push_task(request)

        result = await queue.pop_task()
        assert result is not None
        assert result.id == "test-request-001"
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_pop_empty_returns_none(self, queue):
        result = await queue.pop_task()
        assert result is None

    @pytest.mark.asyncio
    async def test_pop_removes_expired(self, queue):
        expired = MockCollectRequest.with_tle(
            id="expired",
            start_time=datetime.now(UTC) - timedelta(hours=2),
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        valid = MockCollectRequest.with_tle(
            id="valid",
            start_time=datetime.now(UTC),
            end_time=datetime.now(UTC) + timedelta(hours=1),
        )

        await queue.push_task(expired)
        await queue.push_task(valid)

        result = await queue.pop_task()
        assert result.id == "valid"


class TestTaskQueueRemove:
    @pytest.mark.asyncio
    async def test_remove_existing(self, queue):
        request = MockCollectRequest.with_tle(id="to-remove")
        await queue.push_task(request)

        removed = await queue.remove_task("to-remove")
        assert removed is True
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent(self, queue):
        removed = await queue.remove_task("nonexistent")
        assert removed is False


class TestTaskQueuePeek:
    @pytest.mark.asyncio
    async def test_peek_does_not_remove(self, queue):
        request = MockCollectRequest.with_tle()
        await queue.push_task(request)

        result = await queue.peek_task()
        assert result is not None
        assert len(queue) == 1

    @pytest.mark.asyncio
    async def test_peek_empty(self, queue):
        result = await queue.peek_task()
        assert result is None
