# SPDX-License-Identifier: Apache-2.0
"""Tests for NATS backend specifics not covered by the generic backend suite."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

import nats
import nats.js.errors
import pytest
import pytest_asyncio

from sensorkit.backend.base import BackendError, Subject
from sensorkit.backend.nats import KV_BUCKET, STREAM_NAME, NATSBackendImpl


@dataclass
class Entry:
    """Stands in for a nats.js KeyValue.Entry, carrying what `_kv_entry` reads off one."""

    key: str
    value: bytes
    revision: int = 1
    operation: str | None = None  # None means "not a delete"


class FakeKeyWatcher:
    """Simulates nats.js KeyValue.KeyWatcher yielding entries then a None sentinel.

    Entries are yielded in order: initial entries, None (end-of-initial sentinel),
    then live entries. After live entries are exhausted, __anext__ blocks forever
    (like a real watcher waiting for updates).
    """

    def __init__(self, initial: list, live: list | None = None):
        self._items = list(initial) + [None] + list(live or [])
        self._index = 0
        self._blocked = asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index < len(self._items):
            item = self._items[self._index]
            self._index += 1
            return item

        # Simulate blocking while waiting for more live updates.
        await self._blocked.wait()
        raise StopAsyncIteration

    async def stop(self):
        self._blocked.set()


@pytest.mark.asyncio
async def test_kv_iterator_separates_initial_from_live():
    """_kv_iterator must yield initial entries as one batch, then live entries individually.

    Regression: a previous implementation used an async list-comprehension
    `[_kv_entry(e) async for e in watcher]` that never checked for the
    None end-of-initial sentinel, causing either an AttributeError (on
    None.key) or an infinite hang.
    """
    initial = [
        Entry("sensorkit.kv.device.key_a", b"value_a", revision=1),
        Entry("sensorkit.kv.device.key_b", b"value_b", revision=2),
    ]
    live = [
        Entry("sensorkit.kv.device.key_c", b"value_c", revision=3),
    ]

    watcher = FakeKeyWatcher(initial, live)

    batches = []
    async with asyncio.timeout(2.0):
        async for batch in NATSBackendImpl._kv_iterator(watcher):
            batches.append(batch)
            if len(batches) == 2:
                break

    # First batch contains all initial entries.
    assert len(batches[0]) == 2
    assert batches[0][0].value == b"value_a"
    assert batches[0][1].value == b"value_b"

    # Second batch is a single live update.
    assert len(batches[1]) == 1
    assert batches[1][0].value == b"value_c"


@pytest.mark.asyncio
async def test_kv_iterator_empty_initial():
    """_kv_iterator handles the case where no keys exist yet (sentinel immediately)."""
    live = [
        Entry("sensorkit.kv.device.key_a", b"first", revision=1),
    ]

    watcher = FakeKeyWatcher(initial=[], live=live)

    batches = []
    async with asyncio.timeout(2.0):
        async for batch in NATSBackendImpl._kv_iterator(watcher):
            batches.append(batch)
            if len(batches) == 2:
                break

    # First batch is empty (no pre-existing keys).
    assert batches[0] == []

    # Second batch is the live update.
    assert len(batches[1]) == 1
    assert batches[1][0].value == b"first"


# --- Startup against a live server ---


@contextlib.contextmanager
def start_nats_server(*, jetstream: bool):
    """Start a NATS server container and return its URI."""
    from testcontainers.nats import NatsContainer

    with NatsContainer(jetstream=jetstream) as container:
        yield container.nats_uri().replace("localhost", "127.0.0.1")


@pytest.fixture
def nats_server():
    """Return the URI of a fresh NATS server with JetStream enabled."""
    with start_nats_server(jetstream=True) as uri:
        yield uri


@pytest_asyncio.fixture
async def operator(nats_server):
    """Return a JetStream context on a connection separate from the backend under test."""
    nc = await nats.connect(nats_server)

    yield nc.jetstream()

    await nc.close()


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_makes_missing_stream_and_bucket(nats_server, operator):
    backend = await NATSBackendImpl.create(nats_server)
    await backend.stream_publish(Subject(("device",), "status"), b"ready")
    await backend.kv_update(Subject(("device",), "config"), b"{}")

    stream = await operator.stream_info(STREAM_NAME)
    assert stream.config.subjects == ["sensorkit.stream.>"]
    assert stream.state.messages == 1

    kv = await operator.key_value(KV_BUCKET)
    assert (await kv.get("sensorkit.kv.device.config")).value == b"{}"


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_preserves_operator_limits(nats_server, operator):
    """Startup reuses a stream and bucket whose limits differ from the defaults."""
    await operator.add_stream(name=STREAM_NAME, subjects=["sensorkit.stream.>"], max_age=3600)
    await operator.create_key_value(bucket=KV_BUCKET, max_bytes=1 << 20)

    backend = await NATSBackendImpl.create(nats_server)
    await backend.stream_publish(Subject(("device",), "status"), b"ready")

    stream = await operator.stream_info(STREAM_NAME)
    assert stream.config.max_age == 3600
    assert stream.state.messages == 1

    kv_stream = await operator.stream_info(f"KV_{KV_BUCKET}")
    assert kv_stream.config.max_bytes == 1 << 20


@pytest.mark.docker
@pytest.mark.asyncio
async def test_concurrent_create_shares_one_stream(nats_server, operator):
    async with asyncio.timeout(10):
        await asyncio.gather(*(NATSBackendImpl.create(nats_server) for _ in range(4)))

    names = sorted(info.config.name for info in await operator.streams_info())
    assert names == [f"KV_{KV_BUCKET}", STREAM_NAME]


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_rejects_stream_without_event_subjects(nats_server, operator):
    await operator.add_stream(name=STREAM_NAME, subjects=["elsewhere.>"])

    with pytest.raises(BackendError):
        await NATSBackendImpl.create(nats_server)


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_without_jetstream_raises():
    """A JetStream failure other than a missing stream propagates instead of creating one."""
    with (
        start_nats_server(jetstream=False) as uri,
        pytest.raises(nats.js.errors.ServiceUnavailableError),
    ):
        await NATSBackendImpl.create(uri)
