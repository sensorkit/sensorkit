# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
import contextlib
import itertools
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, ClassVar, Unpack, override

import uuid_utils.compat as uuid
from loguru import logger
from pydantic import Field

from sensorkit.backend.base import SpecialProperty
from sensorkit.common.aio import cleanup_future
from sensorkit.common.model import ModelRegistry, RegistryBaseModel

if TYPE_CHECKING:
    from sensorkit.backend.base import StreamContext


class Event(RegistryBaseModel):
    """Base SensorKit event model."""

    event_model: str | None = None
    event_id: uuid.UUID = Field(default_factory=uuid.uuid7)

    model_config: ClassVar[dict] = {"frozen": True}
    registry: ClassVar[ModelRegistry[Event]] = ModelRegistry(
        discriminator="event_model",
        default_tag="UnknownEvent",
    )

    @classmethod
    def model_registry(cls):
        return cls.registry

    def timestamp(self):
        """Return the millisecond UTC timestamp encoded in the UUIDv7 event_id."""
        return self.event_id.int >> 80


class UnknownEvent(Event, extra="allow"):
    """Represents an event for which no corresponding model is known."""


class EventSubscription:
    """Own one ordered event queue and its registrations.

    Closing preserves buffered events for iteration or draining. Canceling an iterator leaves
    the subscription open. Multiple consumers share the queue.
    """

    def __init__(self, *subscriber_sets: set[asyncio.Queue[Event]]):
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self._subscriber_sets = subscriber_sets

        for subscribers in subscriber_sets:
            subscribers.add(self.queue)

    def close(self):
        """Unsubscribe and wake waiters, preserving queued events."""
        for subscribers in self._subscriber_sets:
            subscribers.discard(self.queue)

        self.queue.shutdown()

    async def __aiter__(self) -> AsyncIterator[Event]:
        """Yield events in arrival order until the subscription is closed and drained."""
        while True:
            try:
                event = await self.queue.get()
            except asyncio.QueueShutDown:
                return
            yield event

    def drain(self) -> list[Event]:
        """Remove and return all buffered events in arrival order."""
        events = []

        while True:
            try:
                events.append(self.queue.get_nowait())
            except (asyncio.QueueEmpty, asyncio.QueueShutDown):
                return events

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class EventMultiplexer:
    """Distributes parsed events to per-type and wildcard subscriber queues."""

    def __init__(self):
        self._all_queues: set[asyncio.Queue[Event]] = set()
        self._single_queues: dict[str, set[asyncio.Queue[Event]]] = collections.defaultdict(set)

    def ready(self) -> bool:
        """Return whether events are being received. A plain multiplexer has no source to wait for."""
        return True

    async def wait_ready(self):
        """Wait until events are being received. A plain multiplexer has no source to wait for."""

    async def parse_event(self, json: bytes):
        """Deserialise a JSON event payload and enqueue it to all matching subscriber queues."""
        # Parse the event and narrow its type.
        try:
            model = Event.model_validate_json(json)
        except Exception:
            logger.exception("Failed to parse event")
            raise

        # Notify queues registered for this event.
        single = self._single_queues.get(model.event_model, ())

        for queue in itertools.chain(self._all_queues, single):
            queue.put_nowait(model)

    def subscribe_all(self) -> EventSubscription:
        """Subscribe to all events until the subscription is closed."""
        return EventSubscription(self._all_queues)

    def subscribe(
        self,
        *event_types: Unpack[tuple[type[Event], *tuple[type[Event], ...]]],
    ) -> EventSubscription:
        """Subscribe to one or more event types through one ordered queue."""
        return EventSubscription(
            *(self._single_queues[event_type.model_tag()] for event_type in event_types)
        )

    @contextlib.contextmanager
    def event_queue(self, event: type[Event]):
        """Context manager yielding a Queue that receives events of the specified type."""
        queue: asyncio.Queue[Event] = asyncio.Queue()
        name = event.model_tag()

        try:
            self._single_queues[name].add(queue)
            yield queue
        finally:
            self._single_queues[name].discard(queue)

    @contextlib.contextmanager
    def all_events(self):
        """Context manager yielding a Queue that receives all events regardless of type."""
        queue: asyncio.Queue[Event] = asyncio.Queue()

        try:
            self._all_queues.add(queue)
            yield queue
        finally:
            self._all_queues.discard(queue)


class EventStreamConsumer(EventMultiplexer):
    """An EventMultiplexer that consumes events from a given stream."""

    def __init__(self, context: StreamContext):
        super().__init__()
        self._context = context
        self._startup: asyncio.Future[None] | None = None
        self._task: asyncio.Task[None] | None = None

    @override
    def ready(self):
        """Return whether the stream subscription is live.

        Raises:
            Exception: If the subscription failed to start.
        """
        if self._startup is None or not self._startup.done():
            return False

        # Raise the startup error, if any.
        self._startup.result()
        return True

    @override
    async def wait_ready(self):
        """Wait until the stream subscription is live, starting it on first use.

        Returns without suspending once the subscription is live. Cancelling the wait leaves
        startup running for other waiters.

        Raises:
            Exception: If the subscription failed to start.
        """
        if self._startup is None:
            self._startup = asyncio.get_running_loop().create_future()
            self._task = asyncio.create_task(self._event_consumer_task(self._startup))
            self._task.add_done_callback(cleanup_future)

        await asyncio.shield(self._startup)

    async def _event_consumer_task(self, startup: asyncio.Future[None]):
        try:
            stream = await self._context.consume(SpecialProperty.EVENTS)
        except BaseException as e:
            startup.set_exception(e)
            raise
        else:
            startup.set_result(None)

        async for msg in stream:
            # A bad event must not stop delivery of the events behind it. Parsing logs the error.
            with contextlib.suppress(Exception):
                await self.parse_event(msg.data)
