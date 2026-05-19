from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Callable, override

from pydantic import BaseModel

from sensorkit.core.controller import (
    ControllerDevice,
    ControllerInterface,
    InternalControllerState,
)
from sensorkit.core.device import DeviceCommand, DeviceInterface
from sensorkit.core.entity import EntityInterface
from sensorkit.core.executor import TaskFactoryFunc
from sensorkit.core.program import ProgramInterface
from sensorkit.core.task import ControllerTask

if TYPE_CHECKING:
    from collections.abc import Collection, Coroutine
    from datetime import datetime

    from sensorkit.backend.event import Event
    from sensorkit.backend.request import ExtendedHandlerFunc, HandlerFunc, Request
    from sensorkit.common.keyword import Keyword, KeywordDict
    from sensorkit.core.client import SensorKit
    from sensorkit.data.context import Context
    from sensorkit.data.graph import DataGraph


class EntityDelegate(EntityInterface):
    """Delegates EntityInterface calls to a target implementation."""

    @property
    @abstractmethod
    def delegate_target(self) -> EntityInterface:
        """Return the EntityInterface implementation to which calls are delegated."""
        ...

    @override
    async def kv_put_model(self, model: BaseModel):
        return await self.delegate_target.kv_put_model(model)

    @override
    async def kv_get_model[M: BaseModel](self, model_type: type[M]) -> M:
        return await self.delegate_target.kv_get_model(model_type)

    @override
    @property
    def task_group(self):
        return self.delegate_target.task_group

    @override
    async def emit_event(self, event: Event):
        return await self.delegate_target.emit_event(event)

    @override
    async def publish(self, model: Keyword):
        return await self.delegate_target.publish(model)

    @override
    async def handle_request[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None](
        self,
        request: Request[P, R, V],
        func: HandlerFunc[P, R] | ExtendedHandlerFunc[P, R, V],
    ):
        return await self.delegate_target.handle_request(request, func)

    @override
    async def data_graph(self) -> DataGraph:
        return await self.delegate_target.data_graph()

    @override
    async def publish_entity_info(self):
        return await self.delegate_target.publish_entity_info()

    @override
    def sensorkit(self) -> SensorKit:
        return self.delegate_target.sensorkit()


class ControllerDelegate(EntityDelegate, ControllerInterface):
    """Delegates ControllerInterface calls to a target implementation."""

    @property
    @abstractmethod
    def delegate_target(self) -> ControllerInterface:
        """Return the ControllerInterface implementation to which calls are delegated."""
        ...

    @override
    def on_enable(self, func: Callable[[], None]):
        return self.delegate_target.on_enable(func)

    @override
    def on_disable(self, func: Callable[[], None]):
        return self.delegate_target.on_disable(func)

    @override
    def use_device(self, name: str, *, subscribe: list[type] | None = None):
        return self.delegate_target.use_device(name, subscribe=subscribe)

    @override
    def get_device(self, name: str) -> ControllerDevice:
        return self.delegate_target.get_device(name)

    @override
    def all_devices(self) -> Collection[ControllerDevice]:
        return self.delegate_target.all_devices()

    @override
    async def start_device_subscriptions(self):
        return await self.delegate_target.start_device_subscriptions()

    @override
    async def stop_device_subscriptions(self):
        return await self.delegate_target.stop_device_subscriptions()

    @override
    async def update_context(self, base: KeywordDict | None = None, **kwargs) -> Context:
        return await self.delegate_target.update_context(base=base, **kwargs)

    @override
    def task_handler(
        self, task_type: type[ControllerTask]
    ) -> Callable[[Callable[..., Coroutine[Any, Any, None]]], Callable[..., Coroutine[Any, Any, None]]]:
        return self.delegate_target.task_handler(task_type)

    @override
    def task_running(self) -> bool:
        return self.delegate_target.task_running()

    @override
    async def set_internal_state(self, state: InternalControllerState):
        return await self.delegate_target.set_internal_state(state)


class DeviceDelegate(EntityDelegate, DeviceInterface):
    """Delegates DeviceInterface calls to a target implementation."""

    @property
    @abstractmethod
    def delegate_target(self) -> DeviceInterface:
        """Return the DeviceInterface implementation to which calls are delegated."""
        ...

    @override
    def on_enable(self, func: Callable[[], None]):
        return self.delegate_target.on_enable(func)

    @override
    def on_disable(self, func: Callable[[], None]):
        return self.delegate_target.on_disable(func)

    @override
    def command_handler(
        self, command_type: type[DeviceCommand]
    ) -> Callable[..., Callable[[DeviceCommand], Coroutine[Any, Any, BaseModel | None]]]:
        return self.delegate_target.command_handler(command_type)


class ProgramDelegate(EntityDelegate, ProgramInterface):
    """Delegates ProgramInterface calls to a target implementation."""

    @property
    @abstractmethod
    def delegate_target(self) -> ProgramInterface:
        """Return the ProgramInterface implementation to which calls are delegated."""
        ...

    @override
    def on_enable(self, func: Callable[[], None]):
        return self.delegate_target.on_enable(func)

    @override
    def on_disable(self, func: Callable[[], None]):
        return self.delegate_target.on_disable(func)

    @override
    def task_factory(self, func: TaskFactoryFunc) -> TaskFactoryFunc:
        return self.delegate_target.task_factory(func)

    @override
    def get_offers(self) -> list[Any]:
        return self.delegate_target.get_offers()

    @override
    async def publish_offers(self):
        return await self.delegate_target.publish_offers()

    @override
    def add_offer(self, start: datetime, end: datetime, obj: Any = None):
        return self.delegate_target.add_offer(start, end, obj)

    @override
    def remove_offer(self, start: datetime, end: datetime, obj: Any = None):
        return self.delegate_target.remove_offer(start, end, obj)

    @override
    def clear_offers(self):
        return self.delegate_target.clear_offers()
