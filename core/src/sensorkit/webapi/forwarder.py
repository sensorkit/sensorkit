import asyncio
import collections
import contextlib
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Literal, NamedTuple, override

from loguru import logger
from pydantic_core import from_json

from sensorkit import api as sk


class SKRecord(NamedTuple):
    kind: Literal["stream", "event", "state"]
    time: datetime
    subject: sk.Subject
    payload: dict[str, Any] | None


class Forwarder(ABC):

    def __init__(self, kit: sk.SensorKit, *, targets: Collection[asyncio.Queue[SKRecord]]):
        self.kit = kit
        self.targets = targets
        self.cache: dict[str, dict[str, SKRecord]] = collections.defaultdict(dict)

    async def start(self, *, task_group=asyncio):
        """Start background monitoring task for this forwarder."""
        self.task = task_group.create_task(self._run())
        return self.task

    async def stop(self):
        """Stop the background monitoring task.

        Raises:
            AttributeError: if start() was not called before stop()
        """
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
            for queue in self.targets:
                queue.put_nowait(update)

    @abstractmethod
    def _monitor(self) -> AsyncIterator[SKRecord]:
        """Yield updates as they are received from the backend."""

    def snapshot(self) -> list[SKRecord]:
        """Return a list of Update objects representing the current state."""
        return [update for attrs in self.cache.values() for update in attrs.values()]


class KeyValueForwarder(Forwarder):
    """Forwarder specialization that monitors Key-Value changes."""

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
