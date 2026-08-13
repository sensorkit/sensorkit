# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import functools
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Annotated, Any, Callable, ClassVar, Coroutine, Literal

from loguru import logger
from pydantic import BaseModel, PlainSerializer, PlainValidator, ValidationError

from sensorkit.backend.base import Entity
from sensorkit.backend.event import Event, EventMultiplexer, EventStreamConsumer
from sensorkit.backend.request import (
    Call,
    ExtendedCall,
    ExtendedHandlerFunc,
    ExtendedResponse,
    HandlerFunc,
    Request,
)
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.keyword import (
    Keyword,
    declare_keyword,
    get_keyword_info,
    validate_keyword_json,
)
from sensorkit.common.model import ModelRegistry, RegistryBaseModel
from sensorkit.core.trait import match_archetype, match_traits
from sensorkit.data.graph import DataGraph

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit

type EntityType = Literal["generic", "device", "controller", "program"]


class DeviceDetails(BaseModel):
    """Device info."""

    supported_commands: frozenset[str]
    """Identifiers of each command supported by this device."""

    published_keywords: frozenset[str]
    """Identifiers of each keyword this device declares it publishes."""

    @functools.cached_property
    def archetype(self):
        return match_archetype(self)

    @functools.cached_property
    def traits(self):
        return match_traits(self, exclude_archetypes=True)


class ControllerDetails(BaseModel):
    """Controller info."""

    supported_tasks: list[str]
    """Identifiers of each task supported by this controller."""
    controlled_devices: list[str]
    """Names of each device commanded by this controller."""


@declare_keyword
class EntityInfo(BaseModel):
    """Keyword describing an entity's type and capability details, stored in the KV backend."""

    entity_type: EntityType
    details: DeviceDetails | ControllerDetails | None


class EntityBase:
    """Base object that represents an entity exposed via a backend."""

    def __init__(self, sensorkit: SensorKit, entity: Entity):
        self._sensorkit = sensorkit
        self.backend = sensorkit.backend
        self.entity = entity

    def sensorkit(self) -> SensorKit:
        """Return the SensorKit instance backing this entity."""
        return self._sensorkit

    @functools.cached_property
    def _request(self):
        return self.backend.request(self.entity)

    @functools.cached_property
    def _stream(self):
        return self.backend.stream(self.entity)

    @functools.cached_property
    def _kv(self):
        return self.backend.key_value(self.entity)

    async def kv_put_model(self, model: BaseModel):
        """Serialise model to JSON and write it to the KV store under its class name."""
        await self._kv.update(
            key=model.__class__.__name__,
            value=model.model_dump_json().encode(),
        )

    async def kv_get_model[M: BaseModel](self, model_type: type[M]) -> M:
        """Fetch and deserialise a model from the KV store by its class name."""
        entry = await self._kv.get(model_type.__name__)
        return model_type.model_validate_json(entry.value)

    async def kv_monitor_model[M: BaseModel](self, model_type: type[M]):
        """Monitor changes to a model in the KV store by its class name."""
        stream = await self._kv.monitor(model_type.__name__)

        async for entry in stream:
            if not entry.deleted():
                with contextlib.suppress(ValidationError):
                    yield model_type.model_validate_json(entry.value)


# --- Commands -------------------------------------------------------------------------------
# Command vocabulary lives here, on the entity layer, rather than in core.device: every entity
# can now receive commands, and both the client (EntityClient.command, below) and the impl
# (EntityImpl) reference these. core.device re-exports them for back-compat.


class DeviceCommand(RegistryBaseModel):
    """Base Command model. (Historically device-only; named for back-compat.)"""

    command_id: str = None

    registry: ClassVar[ModelRegistry[DeviceCommand]] = ModelRegistry(discriminator="command_id")

    @classmethod
    def model_registry(cls):
        return cls.registry


type CommandHandlerCallback = Callable[[DeviceCommand], Coroutine[Any, Any, BaseModel | None]]


class Abort(DeviceCommand):
    """Built-in abort command."""


class CommandStarted(Event):
    """Event indicating a Command has been accepted and execution has begun."""

    command_id: str
    call_id: uuid.UUID


class CommandDone(Event):
    """Event indicating a Command has finished execution."""

    command_id: str
    call_id: uuid.UUID
    success: bool


class CommandRequestMessage(BaseModel):
    """Message model representing a Command execution request."""

    command: DeviceCommand


class CommandResult(BaseModel):
    """Message model representing a response to a Command execution request."""

    data: Any


run_command_request = Request.define(
    "command",
    payload=CommandRequestMessage,
    result=CommandResult,
)


class EntityClient(EntityBase):
    """Object that exposes client-side functionality of an entity."""

    def __init__(self, sensorkit: SensorKit, entity: Entity):
        super().__init__(sensorkit, entity)
        self._online_monitor: asyncio.Task | None = None
        self._online_observer = AsyncObserver(False)

    def command(self, command: DeviceCommand) -> Call[ExtendedResponse, CommandResult]:
        """Send a command to this entity and return a Call tracking the result."""
        return self.call(run_command_request, CommandRequestMessage(command=command))

    async def _monitor_online_state(self):
        stream = await self._kv.monitor("EntityLease")

        async for entry in stream:
            online = not entry.deleted()

            if self._online_observer.value != online:
                logger.debug(f"{self.entity} is now {'offline' if entry.deleted() else 'online'}")
                self._online_observer.notify(online)

    def observe_online_state(self):
        """Return an async generator that yields True/False as the entity comes online or goes offline."""
        if not self._online_monitor:
            self._online_monitor = asyncio.create_task(self._monitor_online_state())

        return self._online_observer.consume(initial_value=True)

    @functools.cache
    def get_event_mux(self) -> EventMultiplexer:
        """Return the shared event consumer object for this entity."""
        ec = EventStreamConsumer(self._stream)
        self._ec_startup = asyncio.create_task(ec.start())
        return ec

    @staticmethod
    async def _monitor_receive[M: Event](queue: asyncio.Queue[M], context: contextlib.ExitStack):
        with context:
            while True:
                yield await queue.get()
                queue.task_done()

    async def _await_event_consumer(self, context: contextlib.ExitStack):
        """Wait until the entity's event subscription is live, releasing the queue on failure."""
        try:
            await self._ec_startup
        except BaseException:
            context.close()
            raise

    async def monitor_event[M: Event](self, event_type: type[M]):
        """Return an async generator yielding events of the specified type from the entity's stream."""
        consumer = self.get_event_mux()
        context = contextlib.ExitStack()
        queue: asyncio.Queue[M] = context.enter_context(consumer.event_queue(event_type))

        await self._await_event_consumer(context)

        return self._monitor_receive(queue, context)

    async def monitor_all_events(self):
        """Return an async generator yielding all events from the entity's stream."""
        consumer = self.get_event_mux()
        context = contextlib.ExitStack()
        queue = context.enter_context(consumer.all_events())

        await self._await_event_consumer(context)

        return self._monitor_receive(queue, context)

    async def monitor[M: Keyword | BaseModel](self, model_type: type[M]):
        """Return an async generator yielding (subject, model) tuples for the given keyword or model type."""
        key = info.key if (info := get_keyword_info(model_type)) else None

        if key is not None:
            validate_func = functools.partial(validate_keyword_json, key)
        else:
            validate_func = model_type.model_validate_json

        consumer = await self._stream.consume(key, include_latest=True)

        async def receive_data():
            async for msg in consumer:
                try:
                    model: M = validate_func(msg.data)
                    yield msg.subject, model
                except ValidationError:
                    logger.exception("Data validation failed")
                except Exception:
                    logger.exception("Error while monitoring")

        return receive_data()

    async def monitor_all(self):
        """Return an async generator yielding (subject, model) tuples for all keyword updates on this entity."""
        consumer = await self._stream.consume(include_latest=True)

        async def receive_data():
            async for msg in consumer:
                try:
                    model = validate_keyword_json(msg.subject.prop, msg.data)
                except ValidationError:
                    logger.exception("Data validation failed")
                    continue

                try:
                    yield msg.subject, model
                except Exception:
                    logger.exception("Error while monitoring")

        return receive_data()

    async def request[M: BaseModel](self, name: str, data: BaseModel, response_type: type[M]) -> M:
        """Send a named raw request and return the deserialised response."""
        received = await self._request.invoke(
            name=name,
            payload=data.model_dump_json().encode(),
        )
        return response_type.model_validate_json(received)

    def call[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None](
        self,
        request: Request[P, R, V],
        data: P,
    ) -> Call[R, V]:
        """Invoke a request."""
        payload = b"" if data is None else data.model_dump_json().encode()
        request_coro = self._request.invoke(
            name=request.name,
            payload=payload,
        )

        if request.is_extended():
            return ExtendedCall(
                request_coro,
                request.response,
                request.result,
                self.get_event_mux(),
            )
        else:
            return Call(request_coro, request.response)


class EntityRef[T: EntityClient = EntityClient]:
    """A serializable reference to an entity client."""

    name: str | None

    def __init__(self, name: str | None = None):
        """Create a reference from an entity name.

        Args:
            name: the serialized entity name, or `None` for an unset reference.
        """
        self.name = name
        self._client: T | None = None

    def _get_client(self, kit: SensorKit) -> T:
        """Return the client of the appropriate type for this reference."""
        return kit.entity(self.name)

    def resolve(self, kit: SensorKit) -> None:
        """Resolve this reference against a SensorKit instance.

        If `name` is set, caches the corresponding entity client for later access via
        `get`, `require`, or `__call__`.
        """
        if self.name is not None:
            self._client = self._get_client(kit)

    def get(self) -> T | None:
        """Return the resolved client, or `None` if the reference is unset.

        Raises:
            RuntimeError: if the reference has a name but has not yet been resolved.
        """
        if self.name is not None and self._client is None:
            raise RuntimeError("Entity reference not resolved")
        return self._client

    def require(self) -> T:
        """Return the resolved client, requiring that the reference be usable.

        Raises:
            RuntimeError: if the reference is unset or has not been resolved.
        """
        obj = self.get()
        if obj is None:
            raise RuntimeError("Required entity reference was not defined")
        return obj

    def __call__(self) -> T | None:
        """Convenience accessor for `get`."""
        return self.get()

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, handler):
        # Treat this reference as a string-like value.
        return handler(
            Annotated[
                str,
                PlainValidator(lambda obj: cls(name=obj) if isinstance(obj, str) else obj),
                PlainSerializer(lambda obj: obj.name),
            ]
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):
        schema = handler(core_schema)
        schema["type"] = "string"
        return schema


class EntityInterface(ABC):
    """Interface describing an implementation of an entity."""

    @abstractmethod
    def sensorkit(self) -> SensorKit:
        """Return the SensorKit instance backing this entity."""
        ...

    @property
    @abstractmethod
    def task_group(self) -> asyncio.TaskGroup:
        """Return the TaskGroup used for background tasks on this entity."""
        ...

    @abstractmethod
    async def kv_put_model(self, model: BaseModel):
        """Serialise model and write it to the KV store."""
        ...

    @abstractmethod
    async def kv_get_model[M: BaseModel](self, model_type: type[M]) -> M:
        """Fetch and deserialise a model from the KV store."""
        ...

    @abstractmethod
    async def emit_event(self, event: Event):
        """Publish an event to the entity's event stream."""
        ...

    @abstractmethod
    async def publish(self, model: Keyword):
        """Publish a keyword model to the entity's data stream."""
        ...

    @abstractmethod
    async def handle_request[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None](
        self,
        request: Request[P, R, V],
        func: HandlerFunc[P, R] | ExtendedHandlerFunc[P, R, V],
    ):
        """Register a handler for the given Request definition."""
        ...

    @abstractmethod
    async def data_graph(self) -> DataGraph:
        """Return the DataGraph for this entity, creating it if necessary."""
        ...

    @abstractmethod
    async def publish_entity_info(self) -> EntityInfo:
        """Build and publish EntityInfo to the KV store."""
        ...
