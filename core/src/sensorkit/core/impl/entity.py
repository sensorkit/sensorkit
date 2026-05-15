from __future__ import annotations

import asyncio
import contextlib
from _contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Callable, Iterable, Self, override

from loguru import logger
from pydantic import BaseModel, TypeAdapter

from sensorkit.backend.base import Entity, KVError, SpecialProperty
from sensorkit.backend.event import Event
from sensorkit.backend.request import ExtendedHandlerFunc, HandlerFunc, Request
from sensorkit.common.keyword import Keyword, dump_keyword_json, get_keyword_info
from sensorkit.core.entity import EntityBase, EntityInfo, EntityInterface
from sensorkit.data.graph import DataGraph

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit, ServiceContext


class EntityImpl(EntityBase, EntityInterface):
    """Object that exposes implementation-side functionality of an entity."""

    current: ClassVar[ContextVar[EntityImpl | None]] = ContextVar("current_entity", default=None)
    """The instance in the current execution context, if any."""

    @classmethod
    def for_service_context(cls, context: ServiceContext, entity: str) -> Self:
        """Return the entity in the current execution context."""
        return cls(
            sensorkit=context.sensorkit(),
            entity=Entity.at(entity),
            task_group=context.task_group,
            perpetual_group=context.perpetual_group,
        )

    def __init__(
        self,
        sensorkit: SensorKit,
        entity: Entity,
        *,
        task_group: asyncio.TaskGroup,
        perpetual_group,
    ):
        super().__init__(sensorkit, entity)
        self._data_graph: DataGraph | None = None
        self._task_group = task_group
        self._perpetual_group = perpetual_group

    async def init_impl(self):
        """Perform one-time initialization of this service binding."""
        pass

    @contextlib.contextmanager
    def enter_context(self):
        """Store a reference to this entity in the current execution context."""
        var = type(self).current
        token = var.set(self)

        try:
            yield
        finally:
            var.reset(token)

    @override
    @property
    def task_group(self):
        return self._task_group

    @override
    @property
    def perpetual_group(self):
        return self._perpetual_group

    @override
    async def emit_event(self, event: Event):
        """Serialise and publish the event to the entity's event stream subject."""
        await self._stream.publish(
            SpecialProperty.EVENTS,
            event.model_dump_json().encode(),
        )

    @override
    async def publish(self, model: Keyword):
        """Serialise and publish a keyword model to the entity's data stream."""
        if info := get_keyword_info(model):
            await self._stream.publish(
                info.key,
                dump_keyword_json(model),
            )
        else:
            await self._stream.publish(
                model.__class__.__name__,
                model.model_dump_json()
                if isinstance(model, BaseModel)
                else TypeAdapter(model).dump_json(model),
            )

    @override
    async def handle_request[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None](
        self,
        request: Request[P, R, V],
        func: HandlerFunc[P, R] | ExtendedHandlerFunc[P, R, V],
    ):
        """Register a handler for the given Request definition on the backend."""
        await self._request.handle_request(
            request.name,
            request.create_handler(func, self._stream),
        )

    @override
    async def data_graph(self):
        """Return an AppSource if there is a DataGraph is associated with this entity."""
        if not self._data_graph:
            with contextlib.suppress(KVError):
                dg = await self.kv_get_model(DataGraph)

                # Make sure a concurrent call does not result in two graphs.
                if not self._data_graph:
                    self._data_graph = dg
                    self._data_graph.start(task_group=self.task_group)
                    logger.info(f"Started DataGraph with {len(self._data_graph.nodes)} ops")

        return self._data_graph

    @override
    async def publish_entity_info(self) -> EntityInfo:
        """Publish a generic EntityInfo entry to the KV store."""
        info = EntityInfo(entity_type="generic", details=None)
        await self.kv_put_model(info)
        return info

    async def _call_with_context(
        self,
        funcs: Iterable[Callable],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] = None,
    ):
        if kwargs is None:
            kwargs = {}

        with self.enter_context():
            excs = await asyncio.gather(
                *(func(*args, **kwargs) for func in funcs),
                return_exceptions=True,
            )

        for e in excs:
            if e is not None:
                logger.opt(exception=e).debug("error in callback")

        return excs
