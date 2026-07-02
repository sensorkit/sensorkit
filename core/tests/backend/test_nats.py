"""Tests for NATS backend specifics not covered by the generic backend suite.

Integration tests require Docker (via testcontainers) and are skipped when
`ENV` is not set to `local`. Unit tests (mocked KeyWatcher) always run.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from sensorkit.backend.nats import NATSBackendImpl


def _mock_entry(key: str, value: bytes, revision: int = 1):
    """Create a mock KeyValue.Entry with the attributes _kv_entry expects."""
    entry = MagicMock()
    entry.key = key
    entry.value = value
    entry.operation = None  # not a delete
    entry.revision = revision
    return entry


class MockKeyWatcher:
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
        _mock_entry("sensorkit.kv.device.key_a", b"value_a", revision=1),
        _mock_entry("sensorkit.kv.device.key_b", b"value_b", revision=2),
    ]
    live = [
        _mock_entry("sensorkit.kv.device.key_c", b"value_c", revision=3),
    ]

    watcher = MockKeyWatcher(initial, live)

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
        _mock_entry("sensorkit.kv.device.key_a", b"first", revision=1),
    ]

    watcher = MockKeyWatcher(initial=[], live=live)

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
