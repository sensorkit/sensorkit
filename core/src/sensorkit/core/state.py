# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import threading
from collections.abc import AsyncIterator, Iterable
from typing import ClassVar, Protocol, Self, get_type_hints

from pydantic import BaseModel

from sensorkit.backend.base import KVError
from sensorkit.backend.event import Event
from sensorkit.core.entity import EntityBase, EntityClient
from sensorkit.core.impl.entity import EntityImpl


class StateUpdate[S](Protocol):
    """Publish state events within an update scope."""

    async def __call__(
        self,
        *events: Event,
        publish_state: bool = True,
        return_snapshot: bool = False,
    ) -> S | None:
        """Apply events and optionally publish or copy the updated state."""


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
        async with self.updating(entity) as update:
            return await update(
                *events, publish_state=publish_state, return_snapshot=return_snapshot
            )

    @contextlib.asynccontextmanager
    async def updating(self, entity: EntityImpl) -> AsyncIterator[StateUpdate[Self]]:
        """Hold the update lock while reading state and publishing derived events.

        Use the yielded updater for writes in this scope.
        It accepts `update`'s event and publication arguments and raises `RuntimeError`
        after the scope exits.
        Within the scope, calling `update` or entering another update scope on this state
        would deadlock.
        """
        async with self._update_lock:
            active = True

            async def update(*events: Event, publish_state=True, return_snapshot=False):
                if not active:
                    raise RuntimeError("State update scope has exited")

                return await self._apply(entity, events, publish_state, return_snapshot)

            try:
                yield update
            finally:
                active = False

    async def _apply(
        self,
        entity: EntityImpl,
        events: Iterable[Event],
        publish_state: bool,
        return_snapshot: bool,
    ):
        for event in events:
            field = self._event_fields.get(type(event))

            if field is None:
                raise KeyError(f"No {event.event_model} event field exists")

            setattr(self, field, event)
            await entity.emit_event(event)

        # Events are the source of truth and precede the KV snapshot.
        if publish_state:
            await entity.kv_put_model(self)

        if not return_snapshot:
            return None

        # Give the snapshot a fresh lock so runtime waiters are excluded from the copy.
        return copy.deepcopy(self, {id(self._update_lock): asyncio.Lock()})

    @classmethod
    async def event_stream[T: Event](cls, entity: EntityClient, event_type: type[T]):
        """Yield a continuous stream of events of the given type, starting from the current stored state."""
        field = cls._event_fields[event_type]
        stream = await entity.monitor_event(event_type)
        state = await entity.kv_get_model(cls)
        original: T | None = getattr(state, field)
        original_id = original.event_id.int if original is not None else 0

        yield original

        # TODO: Add backend support for resuming an event stream from a snapshot.
        # Subscribe before reading the snapshot, then filter events already reflected in it.
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
