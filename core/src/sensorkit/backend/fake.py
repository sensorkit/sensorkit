# SPDX-License-Identifier: Apache-2.0
import asyncio
import bisect
from collections.abc import AsyncIterator, Iterable, MutableMapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import override

import pygtrie
from loguru import logger

from sensorkit.backend.base import (
    BackendImpl,
    Entity,
    KeyNotFound,
    KVEntry,
    RequestCallback,
    RevisionError,
    SpecialProperty,
    StreamMessage,
    Subject,
    UnregisteredResponder,
)

type Trie[K, V] = pygtrie.Trie | MutableMapping[K, V]


@dataclass
class FakeBrokerElement[K, V]:
    """Storage node for a single key in the FakeBroker trie, holding value history and subscriber queues."""

    key: K
    history: list[V] = field(default_factory=list)
    direct: set[asyncio.Queue[tuple[K, V]]] = field(default_factory=set)
    shallow_wildcard: set[asyncio.Queue[tuple[K, V]]] = field(default_factory=set)
    deep_wildcard: set[asyncio.Queue[tuple[K, V]]] = field(default_factory=set)

    @classmethod
    async def _observe(cls, queue: asyncio.Queue[tuple[K, V]], queue_set: set[asyncio.Queue]):
        try:
            while True:
                yield await queue.get()
        finally:
            queue_set.discard(queue)

    def subscribe(self):
        """Return an async generator that yields (key, value) tuples published directly to this key."""
        queue = asyncio.Queue()
        self.direct.add(queue)
        return self._observe(queue, self.direct)

    def subscribe_children(self):
        """Return an async generator that yields (key, value) tuples for immediate children."""
        queue = asyncio.Queue()
        self.shallow_wildcard.add(queue)
        return self._observe(queue, self.shallow_wildcard)

    def subscribe_descendants(self):
        """Return an async generator that yields (key, value) tuples for all descendants."""
        queue = asyncio.Queue()
        self.deep_wildcard.add(queue)
        return self._observe(queue, self.deep_wildcard)

    def direct_set(self, value: V):
        """Append value to history and notify direct subscribers."""
        self.history.append(value)
        data = (self.key, value)

        for queue in self.direct:
            queue.put_nowait(data)

    def descendant_set(self, value: V, immediate=False):
        """Notify wildcard subscribers of a descendant value update."""
        data = (self.key, value)

        if immediate:
            for queue in self.shallow_wildcard:
                queue.put_nowait(data)

        for queue in self.deep_wildcard:
            queue.put_nowait(data)


class FakeBroker[K, V]:
    """In-memory pub/sub broker with hierarchical subscription support.

    Manages a trie-based storage system for publish-subscribe messaging with support
    for direct subscriptions, shallow wildcard subscriptions (immediate children),
    and deep wildcard subscriptions (all descendants).

    Type Parameters:
        K: Key type
        V: Value type for messages published to the broker
    """

    def __init__(self):
        self._trie: Trie[K, FakeBrokerElement[K, V]] = pygtrie.Trie()

    def element(self, key: K) -> FakeBrokerElement[K, V]:
        """Return the broker element for the given key, creating it if it does not yet exist."""
        if key not in self._trie:
            self._trie[key] = FakeBrokerElement(key=key)

        return self._trie[key]

    def children_of(self, key: K):
        """Return the immediate child keys of the given key in the trie."""
        def node_factory(path_conv, path, children, *value):
            current_key = path_conv(path)
            if current_key == key:
                return [c for c in children if c is not None]
            return current_key if value else None

        try:
            return self._trie.traverse(node_factory, prefix=key)
        except KeyError:
            return []

    def descendants_of(self, key: K):
        """Return all descendant keys (at any depth) of the given key in the trie."""
        try:
            return (k for k in self._trie.keys(prefix=key, shallow=False) if k != key)
        except KeyError:
            return []

    def publish(self, key: K, value: V):
        """Publish a value to a key, notifying all direct and ancestor wildcard subscribers."""
        # Get the element corresponding to the given key and set its new value.
        elem = self.element(key)
        elem.direct_set(value)

        # Use our trie to find all prefixes of the key.
        ancestors: list[FakeBrokerElement[K, V]] = list(
            step.value for step in self._trie.prefixes(key)
        )

        # The last prefix should always correspond to our full key.
        _elem = ancestors.pop()
        assert elem is _elem

        # If there is a parent, propagate to the parent to handle shallow wildcards.
        parent = ancestors.pop() if ancestors else None

        if parent:
            parent.descendant_set(value, immediate=True)

        # If there are more ancestors, propagate similarly to handle deep wildcards.
        for ancestor in reversed(ancestors):
            ancestor.descendant_set(value)

    def defined(self, key: K):
        """Return True if the key exists in the trie and has at least one history entry."""
        return key in self._trie and len(self._trie[key].history) > 0


class FakeBackendImpl(BackendImpl):
    """In-memory BackendImpl used for testing, backed by FakeBroker instances."""

    @override
    @classmethod
    async def create(cls, *args, **kwargs) -> BackendImpl:
        return cls()

    def __init__(self):
        self._req: dict[Subject, RequestCallback] = {}
        self._kv: FakeBroker[tuple[str, ...], KVEntry] = FakeBroker()
        self._kv_ttls: dict[Subject, float] = {}
        self._kv_expirers: dict[Subject, asyncio.Task] = {}
        self._streams: FakeBroker[tuple[str, ...], StreamMessage] = FakeBroker()
        self._sequence = 0

    @override
    async def request_invoke(self, target: Subject, payload: bytes) -> bytes:
        if target not in self._req:
            raise UnregisteredResponder(target)

        return await asyncio.create_task(self._req[target](payload))

    @override
    async def request_listen(self, target: Subject, func: RequestCallback):
        self._req[target] = func

    @override
    async def request_purge_listeners(self):
        self._req.clear()

    @override
    async def stream_list(self, entity: Entity) -> list[Subject]:
        return [
            Subject(path=p[:-1], prop=p[-1]) for p in self._streams.descendants_of(entity.path)
        ]

    @override
    async def stream_publish(self, target: Subject, payload: bytes):
        stream = self._streams.element(target.full_path())
        was_empty = len(stream.history) == 0
        now = datetime.now(UTC)
        self._sequence += 1

        assert was_empty or stream.history[-1].timestamp < now

        msg = StreamMessage(
            subject=target,
            sequence=self._sequence,
            timestamp=now,
            data=payload,
        )

        self._streams.publish(target.full_path(), msg)

    def _stream_filtered_history(
        self,
        keys: Iterable[tuple[str, ...]],
        include_latest: bool,
        start_at: int | datetime | None,
    ):
        histories = [self._streams.element(key).history for key in keys]

        for history in histories:
            if not history:
                continue

            last = len(history) - 1

            if include_latest:
                assert start_at is None

                yield history[last]
            else:
                match start_at:
                    case int():
                        start_idx = bisect.bisect_left(history, start_at, key=lambda x: x.sequence)
                    case datetime():
                        start_idx = bisect.bisect_left(
                            history, start_at, key=lambda x: x.timestamp
                        )
                    case None:
                        start_idx = len(history) - 1

                for i in range(start_idx, last + 1):
                    yield history[i]

    @override
    async def stream_consume(
        self,
        target: Subject,
        *,
        durable_name: str | None = None,
        start_at: int | datetime | None = None,
        include_latest: bool = False,
    ) -> AsyncIterator[StreamMessage]:
        match target.prop:
            case SpecialProperty.ALL_PROPERTIES:
                elem = self._streams.element(target.path)
                subscription = elem.subscribe_children()
                history = self._stream_filtered_history(
                    self._streams.children_of(target.path),
                    include_latest,
                    start_at,
                )
            case SpecialProperty.ALL_DESCENDANTS:
                elem = self._streams.element(target.path)
                subscription = elem.subscribe_descendants()
                history = self._stream_filtered_history(
                    self._streams.descendants_of(target.path),
                    include_latest,
                    start_at,
                )
            case _:
                elem = self._streams.element(target.full_path())
                subscription = elem.subscribe()
                history = self._stream_filtered_history(
                    (target.full_path(),), include_latest, start_at
                )

        async def _stream_consume():
            for msg in history:
                yield msg

            async for _, msg in subscription:
                yield msg

        return _stream_consume()

    @override
    async def kv_monitor(self, target: Subject) -> AsyncIterator[list[KVEntry]]:
        match target.prop:
            case SpecialProperty.ALL_PROPERTIES:
                elem = self._kv.element(target.path)
                subscription = elem.subscribe_children()
                history = [
                    self._kv.element(key).history[-1] for key in self._kv.children_of(target.path)
                ]
            case SpecialProperty.ALL_DESCENDANTS:
                elem = self._kv.element(target.path)
                subscription = elem.subscribe_descendants()
                history = [
                    self._kv.element(key).history[-1]
                    for key in self._kv.descendants_of(target.path)
                ]
            case _:
                elem = self._kv.element(target.full_path())
                subscription = elem.subscribe()
                history = [elem.history[-1]] if elem.history else []

        async def _kv_monitor():
            yield history

            async for _, entry in subscription:
                yield [entry]

        return _kv_monitor()

    def _kv_defined_not_deleted(self, key: tuple[str, ...]):
        return self._kv.defined(key) and not self._kv.element(key).history[-1].deleted()

    @override
    async def kv_get(self, target: Subject) -> KVEntry:
        key = target.full_path()

        if not self._kv_defined_not_deleted(key):
            raise KeyNotFound(subject=target, deleted=self._kv.defined(key))

        return self._kv.element(key).history[-1]

    NOT_SET = object()

    def _kv_do_update(
        self,
        target: Subject,
        payload: bytes,
        revision: int | None,
        ttl: float | None = NOT_SET,
    ):
        key = target.full_path()

        if revision is None:
            revision = 0
        elif self._kv.defined(key):
            entry = self._kv.element(key).history[-1]

            if revision != entry.revision:
                raise RevisionError("revision does not match")

        revision += 1

        # Determine whether there is an applicable TTL, and if so, enforce it by creating tasks
        # to send a `kv_delete` at expiry.

        if ttl is self.NOT_SET:
            ttl = self._kv_ttls.get(key, None)
        else:
            self._kv_ttls[key] = ttl

        if old_expirer := self._kv_expirers.get(target):
            old_expirer.cancel()

        if ttl:
            async def _expire_ttl():
                await asyncio.sleep(ttl)
                await self.kv_delete(target, revision)

            def _expire_cleanup(t):
                del self._kv_expirers[target]

                if not t.cancelled():
                    t.exception()

            if payload is not KVEntry.DELETE_MARKER:
                task = asyncio.create_task(_expire_ttl())
                self._kv_expirers[target] = task
                task.add_done_callback(_expire_cleanup)

        entry = KVEntry(key=target, value=payload, revision=revision)
        self._kv.publish(key, entry)
        logger.debug(f"KV: {entry}")
        return entry

    @override
    async def kv_create(
        self,
        target: Subject,
        payload: bytes,
        ttl: float | None = None,
    ) -> KVEntry:
        try:
            return self._kv_do_update(target, payload, 0, ttl)
        except RevisionError as err:
            entry = self._kv.element(target.full_path()).history[-1]

            if entry.deleted():
                return self._kv_do_update(target, payload, entry.revision, ttl)

            raise err

    @override
    async def kv_update(
        self,
        target: Subject,
        payload: bytes,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> KVEntry:
        return self._kv_do_update(target, payload, revision)

    @override
    async def kv_delete(self, target: Subject, revision: int | None = None):
        if self._kv_defined_not_deleted(target.full_path()):
            self._kv_do_update(target, KVEntry.DELETE_MARKER, revision)
