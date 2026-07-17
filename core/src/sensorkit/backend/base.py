# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import traceback
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Self

from loguru import logger
from pydantic import BaseModel


class Backend:
    """Exposes a system backend implementation."""

    def __init__(self, impl: BackendImpl):
        self.impl = impl

    async def register_service(self, info: ServiceInfo):
        """Announce a service session to the backend."""
        return await self.impl.register_service(info)

    async def shutdown_request_listeners(self):
        """Remove all registered request listeners from the backend."""
        return await self.impl.request_purge_listeners()

    def request(self, entity: Entity | None = None) -> RequestResponseContext:
        """Return a request-response context scoped to the given entity."""
        return RequestResponseContext(self.impl, entity or Entity())

    def stream(self, entity: Entity | None = None) -> StreamContext:
        """Return a stream context scoped to the given entity."""
        return StreamContext(self.impl, entity or Entity())

    def key_value(self, entity: Entity | None = None) -> KeyValueContext:
        """Return a key-value context scoped to the given entity."""
        return KeyValueContext(self.impl, entity or Entity())


class SpecialProperty(StrEnum):
    """Well-known subject property tokens used to address groups of keys or event streams."""

    ALL_PROPERTIES = "$ALL$"
    ALL_DESCENDANTS = "$WILDCARD$"
    EVENTS = "$EVENT$"
    NONE = "$NONE$"


@dataclass(slots=True, frozen=True, eq=True)
class Entity:
    """An addressable entity identified by a tuple of path segments."""

    path: tuple[str, ...] = ()

    @classmethod
    def at(cls, *path: str) -> Self:
        """Construct an Entity from individual path segments."""
        return cls(path=path)

    def subject(self, name: str) -> Subject:
        """Return a Subject for the named property on this entity."""
        return Subject(self.path, name)

    def __str__(self):
        return ".".join(self.path)

    def __repr__(self):
        return f"Entity({repr(self.path)})"


@dataclass(slots=True, frozen=True, eq=True)
class Subject(Entity):
    """An entity path combined with a named property, forming a fully-qualified address."""

    prop: str | SpecialProperty = SpecialProperty.NONE

    def full_path(self):
        """Return the complete path tuple including the property segment."""
        return self.path + (self.prop,)

    def entity(self):
        """Return the parent Entity, stripping the property segment."""
        return Entity(self.path)

    def __str__(self):
        return f"{".".join(self.path)}.{self.prop}"

    def __repr__(self):
        return f"Subject({repr(self.path)}, {repr(self.prop)})"


# FIXME: This *Context class hierarchy should be collapsed so that only one need exist per entity.
#        Differentiation between stream, kv, etc., operations can be achieved via Protocols.
@dataclass(slots=True, frozen=True)
class BaseContext(ABC):
    impl: BackendImpl
    entity: Entity


type RequestCallback = Callable[[bytes], Coroutine[Any, Any, bytes]]


class BackendError(Exception):
    """Indicates a backend communication error."""


class RemoteRequestError(BackendError):
    """Indicates an error raised in the remote request handler."""
    MAGIC: ClassVar[bytes] = b"ERR\0"

    def __init__(self, *args, details: str | None = None):
        super().__init__(*args)
        self.details = details

    @classmethod
    def parse(cls, name: str, data: bytes) -> Self | None:
        """Parse a response payload and return a RemoteRequestError if it contains the error magic prefix."""
        if data.startswith(cls.MAGIC):
            _, message, *details = data.split(b"\0", maxsplit=2)
            decoded = message.decode("utf-8", errors="replace")
            return cls(
                f"Remote '{name}' request error: {decoded}",
                details=details[0].decode("utf-8", errors="replace") if details else None,
            )

        return None

    @classmethod
    def from_exception(cls, err: BaseException) -> Self:
        """Construct a RemoteRequestError from an exception, capturing its traceback as details."""
        return cls(
            f"{type(err).__name__}: {err}",
            details="".join(traceback.format_exception(type(err), err, err.__traceback__)),
        )

    def encode(self) -> bytes:
        """Encode this error into a wire-format bytes payload prefixed with the error magic."""
        return (
            self.MAGIC
            + str(self).replace("\0", "\ufffd").encode("utf-8", errors="replace")
            + (b"\0" + self.details.encode("utf-8", errors="replace") if self.details else b"")
        )


class UnregisteredResponder(BackendError):
    """Raised when a request is made to a subject with no registered handler."""

    def __init__(self, subject: Subject):
        super().__init__(f"Remote `{subject.prop}` request error: responder for {subject} not registered")


@dataclass(slots=True, frozen=True)
class RequestResponseContext(BaseContext):
    """Exposes request-response methods of a backend implementation."""

    async def invoke(self, name: str, payload: bytes):
        """Send a request to the named subject and return the response bytes.

        Raises RemoteRequestError if the remote handler returned an encoded error.
        """
        res = await self.impl.request_invoke(self.entity.subject(name), payload)

        if err := RemoteRequestError.parse(name, res):
            logger.opt(lazy=True).debug(
                f"{err}{{details}}",
                details=lambda: "\n\t".join(["", *err.details.split("\n")] if err.details else []),
            )
            raise err

        return res

    async def handle_request(self, name: str, callback: RequestCallback):
        """Register a callback to handle incoming requests for the named subject.

        Errors raised by the callback are caught and returned to the caller as encoded
        RemoteRequestError payloads.
        """
        async def _request_wrapper(payload: bytes):
            try:
                res = await callback(payload)
            except (asyncio.CancelledError, Exception) as e:
                logger.opt(exception=e).debug(f"error in {self.entity}.{name} request callback")
                res = RemoteRequestError.from_exception(e).encode()

            return res

        return await self.impl.request_listen(self.entity.subject(name), _request_wrapper)


@dataclass(slots=True, frozen=True)
class StreamContext(BaseContext):
    """Exposes stream methods of a backend implementation."""

    async def list_keys(self):
        """Return a coroutine that resolves to the list of stream subject keys for this entity."""
        return await self.impl.stream_list(self.entity)

    async def consume(
        self,
        key: str | None = None,
        *,
        durable_name: str | None = None,
        from_sequence: int | None = None,
        from_time: datetime | None = None,
        include_latest: bool = False,
    ):
        """Return a coroutine that resolves to an async iterator of StreamMessages.

        If key is None, all properties for the entity are consumed.
        """
        return await self.impl.stream_consume(
            self.entity.subject(key if key else SpecialProperty.ALL_PROPERTIES),
            durable_name=durable_name,
            start_at=from_time or from_sequence,
            include_latest=include_latest,
        )

    async def publish(self, key: str, payload: bytes):
        """Return a coroutine that publishes payload to the named stream subject."""
        return await self.impl.stream_publish(self.entity.subject(key), payload)

    async def publish_event(self, payload: bytes):
        """Return a coroutine that publishes payload to the entity's event stream subject."""
        return await self.impl.stream_publish(self.entity.subject(SpecialProperty.EVENTS), payload)


@dataclass(slots=True, frozen=True)
class KeyValueContext(BaseContext):
    """Exposes key-value store methods of a backend implementation."""

    async def monitor_all(self, *, deep=False):
        """Return an async generator that yields KVEntry objects as keys are created or updated.

        With deep=True, monitors all descendants rather than direct properties only.
        """
        if deep:
            # FIXME: This option is expeditious at the moment, but can lead to a lot of duplicative
            #        messages. In general, this backend code needs a major iteration to use multi-
            #        plexing. Use of ALL_DESCENDANTS is generally problematic. Perhaps a client-
            #        scoped option: set up a single "firehose" subscription and use a global multi-
            #        plexer, or, disallow use of ALL_DESCENDANTS and use shared subscriptions per
            #        entity, possibly via AsyncObserver. In the latter case, calling this method
            #        with deep=True would raise.
            logger.debug("warning: monitor_all(deep=True) may cause excess traffic")

        prop = SpecialProperty.ALL_DESCENDANTS if deep else SpecialProperty.ALL_PROPERTIES
        monitor = await self.impl.kv_monitor(self.entity.subject(prop))

        async def key_value_monitor():
            async for batch in monitor:
                for entry in batch:
                    yield entry

        return key_value_monitor()

    async def monitor(self, key: str):
        """Return an async generator that yields KVEntry objects for changes to the named key."""
        monitor = await self.impl.kv_monitor(self.entity.subject(key))

        async def key_value_monitor():
            async for batch in monitor:
                for entry in batch:
                    yield entry

        return key_value_monitor()

    async def get_all(self, *, deep=False) -> list[KVEntry]:
        """Return all current (non-deleted) KV entries for this entity.

        With deep=True, includes entries from descendant entities as well.
        """
        prop = SpecialProperty.ALL_DESCENDANTS if deep else SpecialProperty.ALL_PROPERTIES
        subject = self.entity.subject(prop)
        out = []

        # Monitor for a single batch, which are the current values.
        async for batch in await self.impl.kv_monitor(subject):
            for entry in batch:
                if not entry.deleted():
                    out.append(entry)

            break

        return out

    async def get(self, key: str):
        """Fetch the current KVEntry for the named key."""
        return await self.impl.kv_get(self.entity.subject(key))

    async def create(
        self,
        key: str,
        value: bytes,
        *,
        ttl: float | None = None,
    ):
        """Create a new KV entry. Raises if the key already exists."""
        return await self.impl.kv_create(self.entity.subject(key), value, ttl)

    async def update(
        self,
        key: str,
        value: bytes,
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ):
        """Update an existing KV entry, optionally requiring a specific revision."""
        return await self.impl.kv_update(self.entity.subject(key), value, revision, ttl)

    async def delete(
        self,
        key: str,
        *,
        revision: int | None = None,
    ):
        """Delete the KV entry for the named key, optionally requiring a specific revision."""
        await self.impl.kv_delete(self.entity.subject(key), revision)


@dataclass(slots=True, frozen=True, eq=True)
class KVEntry:
    """A single key-value store entry."""
    key: Subject
    value: bytes
    revision: int

    DELETE_MARKER: ClassVar[bytes] = b"DELETED"
    """Sentinel value denoting a deleted KV entry."""

    def deleted(self):
        """Return True if this entry has been marked as deleted."""
        return self.value is KVEntry.DELETE_MARKER


class KVError(BackendError):
    """Base class for key-value store errors."""


class RevisionError(KVError):
    """Raised when a KV update or delete is rejected due to a revision mismatch."""


class KeyNotFound(KVError):
    """Raised when a requested KV key does not exist."""

    def __init__(self, subject: Subject, *, deleted: bool):
        super().__init__(f"key for `{subject}` not found")
        self.deleted = deleted


class ServiceInfo(BaseModel):
    """Information about a service."""
    name: str
    version: str


@dataclass(slots=True, frozen=True, eq=True)
class StreamMessage:
    """A message read from a stream, including its subject, sequence number, timestamp, and payload."""

    subject: Subject
    sequence: int
    timestamp: datetime
    data: bytes


class BackendImpl(ABC):
    """Interface providing access to a backend."""

    @classmethod
    @abstractmethod
    async def create(cls, *args, **kwargs) -> BackendImpl:
        """Factory to create a BackendImpl instance."""

    async def register_service(self, info: ServiceInfo):
        """Notify the backend of a new service session. Optional method."""

    @abstractmethod
    async def request_invoke(self, target: Subject, payload: bytes) -> bytes:
        """Invoke a request."""

    @abstractmethod
    async def request_listen(self, target: Subject, coro: RequestCallback):
        """Listen for requests."""

    @abstractmethod
    async def request_purge_listeners(self):
        """Remove all request listeners."""

    @abstractmethod
    async def stream_list(self, entity: Entity) -> list[Subject]:
        """List defined stream subjects for an entity."""

    @abstractmethod
    async def stream_publish(self, target: Subject, payload: bytes):
        """Publish to a stream subject."""

    @abstractmethod
    async def stream_consume(
        self,
        target: Subject,
        *,
        durable_name: str | None = None,
        start_at: int | datetime | None = None,
        include_latest: bool = False,
    ) -> AsyncIterator[StreamMessage]:
        """Consume a stream subject."""

    @abstractmethod
    async def kv_monitor(self, target: Subject) -> AsyncIterator[list[KVEntry]]:
        """Subscribe to KV modifications for the given entity."""

    @abstractmethod
    async def kv_get(self, target: Subject) -> KVEntry:
        """Return the value associated with a key."""

    @abstractmethod
    async def kv_create(
        self,
        target: Subject,
        payload: bytes,
        ttl: float | None = None,
    ) -> KVEntry:
        """Create a KV record."""

    @abstractmethod
    async def kv_update(
        self,
        target: Subject,
        payload: bytes,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> KVEntry:
        """Update a KV record."""

    @abstractmethod
    async def kv_delete(self, target: Subject, revision: int | None = None):
        """Delete a KV record."""
