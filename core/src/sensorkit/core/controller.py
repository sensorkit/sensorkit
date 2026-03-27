from __future__ import annotations

from abc import abstractmethod
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import Any, Callable, ClassVar, Collection, Mapping, Self

import uuid_utils.compat as uuid
from pydantic import BaseModel, Field, model_validator

from sensorkit.backend.event import Event
from sensorkit.backend.request import Call, ExtendedResponse, Request
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.device import DeviceClient
from sensorkit.core.entity import EntityClient, EntityInterface, EntityRef
from sensorkit.core.state import EventSourcedState
from sensorkit.core.task import ControllerTask
from sensorkit.data.context import Context, ContextSubscription


class InternalControllerState(StrEnum):
    """High-level Controller states."""
    OPERATE = auto()
    STANDBY = auto()
    SHUTDOWN = auto()
    ERROR = auto()
    UNKNOWN = auto()


class ControllerEnableState(Event):
    """Event indicating the Controller enable state has changed."""
    enabled: bool


class TaskFinishInfo(BaseModel):
    """Metadata recorded when a task completes, including whether it was aborted or raised an error."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    aborted: bool = False
    error: bool | str = False


class TaskExecutionState(Event):
    """Event representing the current task execution status of a Controller."""

    executing: bool = False
    aborting: bool = False
    finished: TaskFinishInfo | None = None
    task: ControllerTask | None
    context: dict | None = None

    @model_validator(mode="after")
    def _validate(self):
        if self.executing and self.task is None:
            raise ValueError("inconsistent state fields: executing with no task")

        if self.aborting and not self.executing:
            raise ValueError("inconsistent state fields: aborting but not executing")

        if self.finished is not None and (self.executing or self.aborting):
            raise ValueError("inconsistent state fields: finished but executing or aborting")

        return self


class ControllerOperatingState(Event):
    """Event indicating the Controller operating state has changed."""
    current: InternalControllerState
    previous: InternalControllerState | None = None
    target: InternalControllerState | None = None

    NO_CHANGE: ClassVar[object] = object()

    def derive(
        self,
        *,
        current: InternalControllerState = NO_CHANGE,
        target: InternalControllerState | None = NO_CHANGE,
    ) -> Self:
        """Return a new ControllerOperatingState based on this one, updating only the specified fields."""
        if current == self.current:
            # This ensures `previous` isn't unnecessarily rotated out.
            current = self.NO_CHANGE

        return ControllerOperatingState(
            current=self.current if current == self.NO_CHANGE else current,
            previous=self.previous if current == self.NO_CHANGE else self.current,
            target=self.target if target == self.NO_CHANGE else target,
        )


class ControllerState(EventSourcedState):
    """Controller state."""
    enable_state: ControllerEnableState
    operating_state: ControllerOperatingState
    execution_state: TaskExecutionState


class ControllerEnableStateRequest(BaseModel):
    """Request that a Controller enable or disable its task handlers."""
    enable: bool


set_enable_state_request = Request.define(
    "set_enable_state",
    payload=ControllerEnableStateRequest,
)
"""Set the enable state of a Controller."""


class ExecuteRequestMessage(BaseModel):
    """Message model representing a Task execution request."""
    task: ControllerTask
    interrupt: bool = False


class ExecuteResponseMessage(ExtendedResponse):
    """Message model representing a response to a Task execution request."""
    task_id: uuid.UUID
    start_time: datetime | None = None


class AbortRequestMessage(BaseModel):
    """A Task abort request."""
    task_id: uuid.UUID | None
    """If given, verifies the running task ID matches before aborting."""


class AbortResponseMessage(ExtendedResponse):
    """Response to a Task abort request."""
    aborting: bool
    """True if the abort is underway, False if the abort was rejected."""

    task_id: uuid.UUID | None
    """If the abort was accepted, the task ID of the task being aborted."""


class TaskExecutionResult(BaseModel):
    """Result of a successful Task execution."""
    task_id: uuid.UUID
    start_time: datetime
    end_time: datetime


execute_task_request = Request.define(
    name="execute_task",
    payload=ExecuteRequestMessage,
    response=ExecuteResponseMessage,
    result=TaskExecutionResult,
)

abort_task_request = Request.define(
    name="abort_task",
    payload=AbortRequestMessage,
    response=AbortResponseMessage,
)


class ControllerClient(EntityClient):
    """Object that exposes client-side functionality of a Controller."""

    async def enable(self):
        """Request that the Program enable task sourcing for the target Controller."""
        return await self.call(
            set_enable_state_request,
            ControllerEnableStateRequest(enable=True)
        )

    async def disable(self):
        """Request that the Program disable task sourcing."""
        return await self.call(
            set_enable_state_request,
            ControllerEnableStateRequest(enable=False)
        )

    def execute_task(
        self,
        task: ControllerTask,
        interrupt=False,
    ) -> Call[ExecuteResponseMessage, TaskExecutionResult]:
        """Send a task execution request to the controller and return a Call tracking completion."""
        return self.call(
            execute_task_request,
            ExecuteRequestMessage(task=task, interrupt=interrupt),
        )

    def abort_task(self, task_id: uuid.UUID | None = None) -> Call[AbortResponseMessage, None]:
        """Send an abort request to the controller for the optionally specified task ID."""
        return self.call(abort_task_request, AbortRequestMessage(task_id=task_id))

    async def wait_for_task(self, task_id: uuid.UUID | None = None):
        """Wait until the controller reports that no task (or the specified task) is executing."""
        # FIXME: This is unreliable at present due to backend limitations.
        async for event in ControllerState.event_stream(self, TaskExecutionState):
            if task_id is not None:
                if event.task is None or event.task.task_id != task_id:
                    return None

            if not event.executing:
                return event

        raise RuntimeError("stream ended unexpectedly")


class ControllerRef(EntityRef[ControllerClient]):
    """A serializable reference to a controller client."""


@dataclass
class ControllerDevice(Mapping[Any, Any]):
    """A device attached to a controller, providing cached keyword access via its ContextSubscription."""

    client: DeviceClient
    subscription: ContextSubscription

    def __getitem__(self, key, /):
        return self.subscription.cache[key]

    def __len__(self):
        return len(self.subscription.cache)

    def __iter__(self):
        return iter(self.subscription.cache)


type TaskHandlerCallback[T: ControllerTask] = Callable[[T], Coroutine[Any, Any, None]]


class ControllerInterface(EntityInterface):
    """Interface describing an implementation of a controller."""

    @abstractmethod
    def on_enable(self, func: Callable[[], None]):
        """Register a callback to invoke when the controller is enabled."""
        ...

    @abstractmethod
    def on_disable(self, func: Callable[[], None]):
        """Register a callback to invoke when the controller is disabled."""
        ...

    @abstractmethod
    def use_device(self, name: str, *, subscribe: list[type] | None = None):
        """Declare a device dependency, optionally subscribing to the listed keyword types."""
        ...

    @abstractmethod
    def get_device(self, name: str) -> ControllerDevice:
        """Return the attached ControllerDevice for the given name."""
        ...

    @abstractmethod
    def all_devices(self) -> Collection[ControllerDevice]:
        """Return all attached ControllerDevice objects."""
        ...

    @abstractmethod
    async def start_device_subscriptions(self):
        """Start keyword subscriptions for all declared devices."""
        ...

    @abstractmethod
    async def stop_device_subscriptions(self):
        """Stop keyword subscriptions for all declared devices."""
        ...

    @abstractmethod
    def build_context(
        self,
        base: KeywordDict | None = None,
        **kwargs,
    ) -> Context:
        """Build a Context from the current device keyword state, merging any provided base values."""
        ...

    @abstractmethod
    def task_handler(self, task_type: type[ControllerTask]) -> Callable[..., TaskHandlerCallback]:
        """Register a handler for the given task type and return a decorator."""
        ...

    @abstractmethod
    def task_running(self) -> bool:
        """Return True if a task is currently executing on this controller."""
        ...

    @abstractmethod
    async def set_internal_state(self, state: InternalControllerState):
        """Transition the controller to the given internal operating state."""
        ...
