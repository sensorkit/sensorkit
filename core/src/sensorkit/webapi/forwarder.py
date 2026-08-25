# SPDX-License-Identifier: Apache-2.0
import asyncio
import collections
import contextlib
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Literal, NamedTuple, override

from loguru import logger
from pydantic import TypeAdapter
from pydantic_core import from_json

from sensorkit import api as sk
from sensorkit.webapi.serve import ServeHandler


class SKRecord(NamedTuple):
    kind: Literal["stream", "event", "state", "product"]
    time: datetime
    subject: sk.Subject
    payload: dict[str, Any] | None

    def serialize(self) -> str:
        """Serialize to a JSON string."""
        return _record_adapter.dump_json(self).decode()


_record_adapter = TypeAdapter(SKRecord)
type RecordQueueSet = set[asyncio.Queue[SKRecord | None]]


class Forwarder(ABC):
    """Base class that caches backend updates and broadcasts them to target queues.

    Subclasses implement `_monitor` to yield `SKRecord` updates from a particular
    backend source; the base loop keeps a per-entity cache (treating a `None` payload
    as a deletion) and fans each update out to every registered target queue.
    """

    def __init__(self, *, targets: RecordQueueSet):
        self.targets = targets
        self.cache: dict[str, dict[str, SKRecord]] = collections.defaultdict(dict)
        self.task: asyncio.Task | None = None

    async def start(self, *, task_group: asyncio.TaskGroup):
        """Start background monitoring task for this forwarder."""
        self.task = task_group.create_task(self._run())
        return self.task

    async def stop(self):
        """Stop the background monitoring task, if one is running."""
        if self.task is None:
            return

        self.task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await self.task

    async def _run(self):
        async for update in self._monitor():
            entity = str(update.subject.entity())
            attr = update.subject.prop

            # Update the cache. Note the update payload being None is used as a deletion marker.
            # This assumes that all incoming data are JSON objects (non-primitives) and therefore
            # can never legitimately be `null`.
            attr_cache = self.cache[entity]

            if update.payload is None:
                attr_cache.pop(attr, None)
            else:
                attr_cache[attr] = update

            # Broadcast to all targets.
            for queue in tuple(self.targets):
                try:
                    queue.put_nowait(update)
                except asyncio.QueueFull:
                    # A subscriber that has stopped draining is cut loose.
                    logger.warning("dropping a subscriber that is not keeping up")
                    self.targets.discard(queue)

                    while not queue.empty():
                        queue.get_nowait()

                    queue.put_nowait(None)

    @abstractmethod
    def _monitor(self) -> AsyncIterator[SKRecord]:
        """Yield updates as they are received from the backend."""

    def snapshot(self, entity_id: str | None = None) -> list[SKRecord]:
        """Return the cached records, optionally limited to a single entity."""
        caches = self.cache.values() if entity_id is None else (self.cache.get(entity_id, {}),)
        return [record for attrs in caches for record in attrs.values()]


class KeyValueForwarder(Forwarder):
    """Forwarder specialization that monitors Key-Value changes."""

    def __init__(self, kit: sk.SensorKit, *, targets: RecordQueueSet):
        super().__init__(targets=targets)
        self.kit = kit

    @override
    async def _monitor(self) -> AsyncIterator[SKRecord]:
        monitor = await self.kit.entity()._kv.monitor_all(deep=True)

        async for entry in monitor:
            ts = datetime.now(UTC)  # FIXME: Pending backend exposure of KV update timestamp.
            payload = from_json(entry.value) if not entry.deleted() else None

            if not isinstance(payload, dict) and payload is not None:
                logger.warning(f"Invalid KV payload for entity {entry.key.entity()}: {payload}")
                continue

            yield SKRecord(
                kind="state",
                subject=entry.key,
                time=ts,
                payload=payload,
            )


class StreamForwarder(Forwarder):
    """Forwarder specialization that monitors stream updates (state/events)."""

    def __init__(self, kit: sk.SensorKit, *, targets: RecordQueueSet):
        super().__init__(targets=targets)
        self.kit = kit

    @override
    async def _monitor(self) -> AsyncIterator[SKRecord]:
        consumer = await self.kit.entity()._stream.consume(
            key=sk.SpecialProperty.ALL_DESCENDANTS,
            include_latest=True,
        )

        async for msg in consumer:
            payload = from_json(msg.data)

            if not isinstance(payload, dict):
                logger.warning(f"Invalid stream payload for entity {msg.subject}: {payload}")
                continue

            yield SKRecord(
                kind="event" if msg.subject.prop == sk.SpecialProperty.EVENTS else "stream",
                subject=msg.subject,
                time=msg.timestamp,
                payload=payload,
            )


class ProductForwarder(Forwarder):
    """Forwarder specialization that monitors data products from a ServeHandler."""

    def __init__(self, serve_handler: ServeHandler, *, targets: RecordQueueSet):
        super().__init__(targets=targets)
        self.serve_handler = serve_handler

    @override
    async def _monitor(self) -> AsyncIterator[SKRecord]:
        async for info in self.serve_handler.watch_listing():
            yield SKRecord(
                kind="product",
                subject=sk.Subject(path=(info.controller_id,), prop=info.product_id),
                time=datetime.now(UTC),
                payload=info.model_dump(),
            )
