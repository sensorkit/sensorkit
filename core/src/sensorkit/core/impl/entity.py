# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
from _contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterable, Literal, Self, final, override

from loguru import logger
from pydantic import BaseModel, TypeAdapter, ValidationError

from sensorkit.backend.base import Entity, KVError, SpecialProperty
from sensorkit.backend.event import Event
from sensorkit.backend.request import (
    CallContext,
    ExtendedHandlerFunc,
    HandlerFunc,
    Request,
)
from sensorkit.common.keyword import Keyword, dump_keyword_json, get_keyword_info
from sensorkit.core.entity import (
    CommandDone,
    CommandHandlerCallback,
    CommandRequestMessage,
    CommandResult,
    CommandStarted,
    DeviceCommand,
    EntityBase,
    EntityInfo,
    EntityInterface,
    run_command_request,
)
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
        )

    def __init__(self, sensorkit: SensorKit, entity: Entity, *, task_group: asyncio.TaskGroup):
        super().__init__(sensorkit, entity)
        self._data_graph: DataGraph | None = None
        self._task_group = task_group
        self._attach_hooks: list[Callable] = []
        self._detach_hooks: list[Callable] = []
        self._handlers: dict[str, CommandHandlerCallback] = {}

    async def init_impl(self):
        """Implements initialization of the entity.

        This method is called after the backend is up and available, but before `attach` is run.
        Implementations will typically retrieve configuration, restore persisted state, and
        perform any other initialization to prepare resources required by user `on_attach` hooks.
        """
        pass

    def on_attach(self, func: Callable):
        """Register a callback to run when this entity is being attached."""
        self._attach_hooks.append(func)
        return func

    @final
    async def attach(self):
        """Attach the entity implementation to the running service context.

        Runs all attach hooks and publishes entity info. An exception raised in any attach hook
        is propagated as fatal.
        """
        await self._call_with_context(self._attach_hooks, error_policy="raise")

        with self.enter_context():
            await self.attach_impl()

        await self.publish_entity_info()

    async def attach_impl(self):
        """Implements "attach". Runs after the user attach hooks."""
        await self.handle_request(run_command_request, self._command_request)

    def command_handler(self, command_type: type[DeviceCommand]):
        """Register a handler for the given command type and return the wrapped func."""
        key = command_type.model_tag()

        def decorator(func: CommandHandlerCallback):
            self._handlers[key] = func
            return func

        return decorator

    async def _command_request(
        self,
        message: CommandRequestMessage,
        call: CallContext[None, CommandResult],
    ):
        command_id = message.command.command_id

        if command_id not in self._handlers:
            logger.warning(f"Rejecting unhandled command: {command_id}")
            call.reject(response=None)
            return

        call.accept(response=None)
        handler_func = self._handlers[command_id]
        success = False

        logger.debug(f"Incoming {command_id} Command: {call.call_id}")
        await self.emit_event(CommandStarted(command_id=command_id, call_id=call.call_id))

        try:
            with self.enter_context():
                task = asyncio.create_task(handler_func(message.command))
            await call.progress_from_task(task, cadence=6.0, ttl=10.0)
            success = True
        except ValidationError:
            logger.exception(f"Incoming {command_id} Command failed validation")
            raise
        except asyncio.CancelledError:
            logger.exception(f"Execution of {command_id} Command cancelled")
            raise
        except Exception:
            logger.exception(f"Error invoking {command_id} Command handler")
            raise
        else:
            await call.succeed(result=CommandResult(data=task.result()))
        finally:
            try:
                await self.emit_event(
                    CommandDone(command_id=command_id, call_id=call.call_id, success=success)
                )
            except Exception as e:
                logger.warning(f"Error logging command done event ({type(e).__name__})")

    def on_detach(self, func: Callable):
        """Register a callback to run when this entity is being detached."""
        self._detach_hooks.append(func)
        return func

    @final
    async def detach(self):
        """Detach the entity implementation from the running service context.

        Runs all detach hooks. An exception raised in the internal detach hook (`detach_impl`) is
        propagated as fatal, while exceptions from user detach hooks are logged.
        """
        with self.enter_context():
            await self.detach_impl()

        for result in await self._call_with_context(self._detach_hooks, error_policy="ignore"):
            match result:
                case asyncio.CancelledError():
                    logger.warning(f"Cancelled during {self.entity} detach")
                case Exception():
                    logger.warning(f"Error during {self.entity} detach ({type(result).__name__})")
                    logger.opt(exception=result).debug("detach hook raised")
                case BaseException():
                    raise result

    async def detach_impl(self):
        """Implements "detach". Runs before the user detach hooks."""
        pass

    @contextlib.contextmanager
    def enter_context(self):
        """Store a reference to this entity in the current execution context."""
        var = type(self).current
        token = var.set(self)

        # Also set the base EntityImpl.current so sk.entity() resolves for all entity types.
        base_var = EntityImpl.current
        base_token = base_var.set(self) if var is not base_var else None

        try:
            yield
        finally:
            var.reset(token)

            if base_token is not None:
                base_var.reset(base_token)

    @override
    @property
    def task_group(self):
        return self._task_group

    @override
    async def emit_event(self, event: Event):
        """Serialize and publish the event to the entity's event stream subject."""
        await self._stream.publish(
            SpecialProperty.EVENTS,
            event.model_dump_json().encode(),
        )

    @override
    async def publish(self, model: Keyword):
        """Serialize and publish a keyword model to the entity's data stream."""
        if info := get_keyword_info(model):
            await self._stream.publish(
                info.key,
                dump_keyword_json(model),
            )
        else:
            await self._stream.publish(
                model.__class__.__name__,
                model.model_dump_json().encode()
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

    def entity_info(self) -> EntityInfo:
        """Build this entity's info record from its current state."""
        return EntityInfo(entity_type="generic", details=None)

    @override
    async def publish_entity_info(self) -> EntityInfo:
        """Publish this entity's EntityInfo entry to the KV store."""
        info = self.entity_info()
        await self.kv_put_model(info)
        return info

    async def _call_with_context(
        self,
        funcs: Iterable[Callable],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        error_policy: Literal["raise", "log", "ignore"] = "log",
    ) -> list[Any]:
        """Invoke hooks concurrently within this entity's execution context."""
        call_kwargs = kwargs if kwargs is not None else {}

        async def invoke(func: Callable):
            aw = func(*args, **call_kwargs)
            return await aw if asyncio.iscoroutine(aw) else aw

        with self.enter_context():
            tasks = [asyncio.ensure_future(invoke(func)) for func in funcs]

            if error_policy == "raise":
                # Fail fast: propagate the first hook exception immediately, cancelling any
                # still-running siblings so none are left orphaned on the loop.
                try:
                    return await asyncio.gather(*tasks)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

            results = await asyncio.gather(*tasks, return_exceptions=True)

        if error_policy == "log":
            for result in results:
                match result:
                    case Exception():
                        logger.opt(exception=result).debug("error in callback")
                    case BaseException():
                        logger.debug(f"error in callback ({type(result).__name__})")

        return results
