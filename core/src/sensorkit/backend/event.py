# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
import contextlib
import itertools
from typing import TYPE_CHECKING, ClassVar, override

import uuid_utils.compat as uuid
from loguru import logger
from pydantic import Field, ValidationError

from sensorkit.backend.base import SpecialProperty
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


class EventMultiplexer:
    """Distributes parsed events to per-type and wildcard subscriber queues."""

    def __init__(self):
        self._all_queues: set[asyncio.Queue[Event]] = set()
        self._single_queues: dict[str, set[asyncio.Queue[Event]]] = collections.defaultdict(set)

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
        self._startup = asyncio.get_running_loop().create_future()

    async def start(self):
        """Start the background event consumer task and wait until the stream is ready."""
        self._task = asyncio.create_task(self._event_consumer_task())
        await self._startup

    @override
    async def wait_ready(self):
        """Wait until the stream subscription is live, raising if it failed to start.

        Blocks indefinitely until `start` is called, so callers must hold a multiplexer that is
        already starting.
        """
        await self._startup

    async def _event_consumer_task(self):
        try:
            stream = await self._context.consume(SpecialProperty.EVENTS)
        except BaseException as e:
            self._startup.set_exception(e)
            raise
        else:
            self._startup.set_result(None)

        async for msg in stream:
            try:
                await self.parse_event(msg.data)
            except ValidationError:
                logger.exception("Event validation failed")
