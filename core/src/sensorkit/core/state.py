from __future__ import annotations

import asyncio
import inspect
import threading
from typing import ClassVar, Self, get_type_hints

from pydantic import BaseModel

from sensorkit.backend.base import KVError
from sensorkit.backend.event import Event
from sensorkit.core.entity import EntityBase, EntityClient
from sensorkit.core.impl.entity import EntityImpl


class EventSourcedState(BaseModel):
    """State model that can recover values of Event-typed fields from an event stream."""

    _event_fields: ClassVar[dict[type[Event], str]] = {}
    _event_fields_lock: ClassVar[threading.Lock] = threading.Lock()

    def model_post_init(self, __context):
        """Introspect fields at init time to identify Event-typed fields."""
        super().model_post_init(__context)
        self._set_event_fields()
        self._update_lock = asyncio.Lock()

    @classmethod
    def _set_event_fields(cls):
        if cls._event_fields:
            return

        with cls._event_fields_lock:
            # Double-check pattern to avoid redundant work if another thread already initialized
            if cls._event_fields:
                return

            # Store at class level.
            cls._event_fields = cls._introspect_event_fields()

    @classmethod
    def _introspect_event_fields(cls):
        type_hints = get_type_hints(cls)
        event_fields = {}

        for field, field_type in type_hints.items():
            # Check if field type is a subclass of Event
            if inspect.isclass(field_type) and issubclass(field_type, Event):
                if field_type in event_fields:
                    raise RuntimeError(
                        f"Duplicate event type {field_type.model_tag()}"
                        f" found at {cls.__name__}.{field}"
                    )

                event_fields[field_type] = field

        return event_fields

    @property
    def update_lock(self):
        """Async lock that serialises concurrent state updates."""
        return self._update_lock

    async def update(
        self,
        entity: EntityImpl,
        *events: Event,
        publish_state=True,
        return_snapshot=False,
    ):
        """Apply events to the state, emit them on the entity's stream, and optionally publish the state to KV.

        Returns a deep copy of the updated state if return_snapshot is True, else None.
        """
        async with self._update_lock:
            for event in events:
                field = self._event_fields.get(type(event))

                if field is None:
                    raise KeyError(f"No {event.event_model} event field exists")

                setattr(self, field, event)
                await entity.emit_event(event)

            # Ordering is crucial here: events must be emitted first since they are the source of
            # truth.
            if publish_state:
                await entity.kv_put_model(self)

            # Make a snapshot while we hold the lock,
            snapshot = self.model_copy(deep=True) if return_snapshot else None

        return snapshot

    @classmethod
    async def event_stream[T: Event](cls, entity: EntityClient, event_type: type[T]):
        """Yield a continuous stream of events of the given type, starting from the current stored state."""
        field = cls._event_fields[event_type]
        stream = await entity.monitor_event(event_type)
        state = await entity.kv_get_model(cls)
        original: T | None = getattr(state, field)
        original_id = original.event_id.int if original is not None else 0

        yield original

        # FIXME: For this to work properly, we need the backend to be enhanced to expose a method
        #        of starting the stream at a given timestamp. NATS supports this so this is just a
        #        much needed backend iteration. Below is a poor-man's substitute that kind of works
        #        only because we currently always publish a state update subsequent to every event.
        #
        # Skip any stream events already reflected in `original` (or that produced it), then yield
        # everything after. We order by the full uuid7 event_id rather than timestamp(): the latter
        # is only millisecond-resolution, so two events emitted in the same millisecond compare
        # equal and a genuinely newer event would be silently dropped, hanging the consumer.
        async for event in stream:
            if event.event_id.int > original_id:
                yield event
                break

        async for event in stream:
            yield event

        raise RuntimeError("unexpected end of stream")

    @classmethod
    async def recover(cls, entity: EntityBase) -> Self:
        """Recover state from the KV store and validate Event fields are up to date."""
        # Retrieve the stored data from KV.
        obj = await entity.kv_get_model(cls)

        # Verify that each cached event is the latest event of that type. If it isn't, retrieve
        # the latest event.
        for field in cls._event_fields.values():
            event: Event = getattr(obj, field)
            newer = await cls._get_latest_event_if_newer(entity, event)

            if newer is not None:
                setattr(obj, field, newer)

        return obj

    @classmethod
    async def recover_or_init(cls, entity: EntityBase, **kwargs):
        """Recover state from KV, or create and publish a new instance using kwargs if none exists."""
        try:
            return await cls.recover(entity)
        except KVError:
            new = cls(**kwargs)
            await entity.kv_put_model(new)
            return new

    @staticmethod
    async def _get_latest_event_if_newer(entity: EntityBase, event: Event):
        # TODO: Implement.
        return event or None
