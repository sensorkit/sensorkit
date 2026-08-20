# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from sensorkit.common.keyword import declare_keyword
from sensorkit.webapi.forwarder import KeyValueForwarder, SKRecord, StreamForwarder


@pytest.mark.asyncio
async def test_kv_forwarder_caches_updates(kit, service_context):
    async with asyncio.timeout(2.0):
        dev = await service_context.register_device("mydevice")

    targets: set[asyncio.Queue[SKRecord]] = set()
    forwarder = KeyValueForwarder(kit, targets=targets)

    async with asyncio.TaskGroup() as tg:
        await forwarder.start(task_group=tg)

        try:
            await dev.publish_entity_info()

            # Wait until the EntityInfo record lands in the cache.
            for _ in range(20):
                if "mydevice" in forwarder.cache:
                    break
                await asyncio.sleep(0.05)

            assert "mydevice" in forwarder.cache
            assert "EntityInfo" in forwarder.cache["mydevice"]
            record = forwarder.cache["mydevice"]["EntityInfo"]
            assert record.kind == "state"
            assert record.payload is not None
        finally:
            await forwarder.stop()


@pytest.mark.asyncio
async def test_kv_forwarder_broadcasts_to_queues(kit, service_context):
    async with asyncio.timeout(2.0):
        dev = await service_context.register_device("mydevice")

    queue: asyncio.Queue[SKRecord] = asyncio.Queue()
    targets = {queue}
    forwarder = KeyValueForwarder(kit, targets=targets)

    async with asyncio.TaskGroup() as tg:
        await forwarder.start(task_group=tg)

        try:
            await dev.publish_entity_info()

            # Drain queue until we find the EntityInfo record.
            entity_info_record = None
            async with asyncio.timeout(2.0):
                while entity_info_record is None:
                    record = await queue.get()
                    if record.subject.prop == "EntityInfo":
                        entity_info_record = record

            assert entity_info_record.kind == "state"
            assert entity_info_record.subject.prop == "EntityInfo"
        finally:
            await forwarder.stop()


@pytest.mark.asyncio
async def test_kv_forwarder_snapshot(kit, service_context):
    async with asyncio.timeout(2.0):
        dev = await service_context.register_device("mydevice")

    targets: set[asyncio.Queue[SKRecord]] = set()
    forwarder = KeyValueForwarder(kit, targets=targets)

    async with asyncio.TaskGroup() as tg:
        await forwarder.start(task_group=tg)

        try:
            await dev.publish_entity_info()

            for _ in range(20):
                if "mydevice" in forwarder.cache:
                    break
                await asyncio.sleep(0.05)

            snapshot = forwarder.snapshot()
            assert len(snapshot) >= 1
            kinds = {r.kind for r in snapshot}
            assert "state" in kinds
        finally:
            await forwarder.stop()


@pytest.mark.asyncio
async def test_stream_forwarder_caches_updates(kit, service_context):
    @declare_keyword
    class ForwarderStreamKeyword(BaseModel):
        value: int

    async with asyncio.timeout(2.0):
        dev = await service_context.register_device("mydevice")

    targets: set[asyncio.Queue[SKRecord]] = set()
    forwarder = StreamForwarder(kit, targets=targets)

    async with asyncio.TaskGroup() as tg:
        await forwarder.start(task_group=tg)

        try:
            await dev.publish(ForwarderStreamKeyword(value=42))

            for _ in range(20):
                if "mydevice" in forwarder.cache:
                    break
                await asyncio.sleep(0.05)

            assert "mydevice" in forwarder.cache
            record = forwarder.cache["mydevice"]["ForwarderStreamKeyword"]
            assert record.kind == "stream"
            assert record.payload["value"] == 42
        finally:
            await forwarder.stop()


@pytest.mark.asyncio
async def test_stream_forwarder_snapshot(kit, service_context):
    @declare_keyword
    class ForwarderSnapshotKeyword(BaseModel):
        reading: float

    async with asyncio.timeout(2.0):
        dev = await service_context.register_device("mydevice")

    targets: set[asyncio.Queue[SKRecord]] = set()
    forwarder = StreamForwarder(kit, targets=targets)

    async with asyncio.TaskGroup() as tg:
        await forwarder.start(task_group=tg)

        try:
            await dev.publish(ForwarderSnapshotKeyword(reading=3.14))

            for _ in range(20):
                if forwarder.cache:
                    break
                await asyncio.sleep(0.05)

            snapshot = forwarder.snapshot()
            assert any(r.payload and r.payload.get("reading") == 3.14 for r in snapshot)
        finally:
            await forwarder.stop()


@pytest.mark.asyncio
async def test_backlogged_subscriber_is_dropped(kit, service_context):
    @declare_keyword
    class ForwarderBacklogKeyword(BaseModel):
        reading: float

    async with asyncio.timeout(2.0):
        dev = await service_context.register_device("mydevice")

    # A subscriber that never drains, with only enough room for one record.
    queue: asyncio.Queue[SKRecord | None] = asyncio.Queue(1)
    targets = {queue}
    forwarder = StreamForwarder(kit, targets=targets)

    async with asyncio.TaskGroup() as tg:
        await forwarder.start(task_group=tg)

        try:
            async with asyncio.timeout(5.0):
                while queue in targets:
                    await dev.publish(ForwarderBacklogKeyword(reading=1.0))
                    await asyncio.sleep(0.05)

            # The stream ends with the sentinel that wakes a parked consumer, so a
            # client that stopped reading is released rather than left holding records.
            last = None

            while not queue.empty():
                last = queue.get_nowait()

            assert last is None
        finally:
            await forwarder.stop()


@pytest.mark.asyncio
async def test_stopping_a_forwarder_that_never_started(kit):
    """An attach that fails partway still gets detached, so stopping is always allowed."""
    forwarder = KeyValueForwarder(kit, targets=set())

    await forwarder.stop()

