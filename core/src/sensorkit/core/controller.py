# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import abstractmethod
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Collection, Mapping, Self

import uuid_utils.compat as uuid
from pydantic import BaseModel, Field, model_validator

from sensorkit.backend.event import Event
from sensorkit.backend.request import Call, ExtendedResponse, Request
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.device import DeviceClient
from sensorkit.core.entity import EntityClient, EntityInterface, EntityRef
from sensorkit.core.state import EventSourcedState
from sensorkit.core.task import Task, TaskExecution
from sensorkit.data.context import Context, ContextSubscription

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit


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
    execution: TaskExecution | None
    context: dict | None = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_task(cls, data: Any) -> Any:
        """Upgrade events persisted by versions predating the Task/TaskExecution split.

        Two legacy shapes are upgraded here:

        * Versions before the split stored the executing task on `task` as a flat
          `ControllerTask` — identity (`task_id`, `controller_id`), `context` and the
          `end_time` deadline embedded alongside the discriminated task fields. This version
          expects a `TaskExecution` envelope wrapping the semantic `task`.
        * Versions after the split but before `task` was renamed to `execution` stored the
          envelope under the `task` key.

        Both are carried onto `execution`. Rewriting the legacy shape here covers both reads:
        the KV `ControllerState` snapshot (whose `execution_state` is validated as this model)
        and event-stream replay (where each event is validated through the registry). Without it,
        state carried by a NATS broker from an older version would raise a `ValidationError`.
        """
        if not isinstance(data, dict):
            return data

        # The pre-rename wire format carried the executing task (flat or enveloped) under `task`.
        # Lift it onto `execution` so the rest of the migration — and the model — see one key.
        if "execution" not in data and "task" in data:
            data = dict(data)
            data["execution"] = data.pop("task")

        legacy = data.get("execution")

        # A legacy task is a flat `ControllerTask`: it carries the discriminator at the top level
        # and has no nested `task` envelope. A current `TaskExecution` always nests `task`.
        if not isinstance(legacy, dict) or "task_type" not in legacy or "task" in legacy:
            return data

        # Lift the envelope fields off the task; the remaining keys (`task_type` plus domain
        # fields) become the wrapped semantic task. The legacy `end_time` was the execution
        # deadline, so it maps to the envelope's `expiry_time`; task types that still declare
        # `end_time` as a domain field (e.g. `standard_collect`) keep their own copy because it
        # is left in the wrapped task.
        envelope = {
            "task": {
                k: v for k, v in legacy.items() if k not in ("task_id", "controller_id", "context")
            },
            "task_id": legacy.get("task_id"),
            "controller_id": legacy.get("controller_id"),
            "context": legacy.get("context"),
            "expiry_time": legacy.get("end_time"),
        }

        return {**data, "execution": envelope}

    @model_validator(mode="after")
    def _validate(self):
        if self.executing and self.execution is None:
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
    """A task execution request.

    The client supplies the semantic `task` plus optional execution parameters. The controller
    mints the identity (`task_id`, `controller_id`) and returns the resulting `TaskExecution` in
    the response.
    """
    task: Task
    context: KeywordDict | None = None
    expiry_time: datetime | None = None
    interrupt: bool = False


class ExecuteResponseMessage(ExtendedResponse):
    """A response to a task execution request.

    Carries the minted `TaskExecution` envelope so the client learns the assigned `task_id` (e.g.
    for a subsequent abort) without having to supply it.
    """
    execution: TaskExecution | None = None


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
        task: Task,
        *,
        context: KeywordDict | None = None,
        expiry_time: datetime | None = None,
        interrupt=False,
    ) -> Call[ExecuteResponseMessage, TaskExecutionResult]:
        """Send a task execution request to the controller and return a `Call` tracking completion.

        The controller assigns the `task_id` and `controller_id`. The optional execution
        parameters are recorded on the resulting `TaskExecution`.

        Args:
            task: The semantic task to execute.
            context: Optional keyword context to attach to the execution.
            expiry_time: Optional time after which the execution should be considered expired.
            interrupt: Whether to interrupt any task currently in progress.

        Returns:
            A `Call` that yields the final `TaskExecutionResult` when awaited.
        """
        return self.call(
            execute_task_request,
            ExecuteRequestMessage(
                task=task,
                context=context,
                expiry_time=expiry_time,
                interrupt=interrupt,
            ),
        )

    async def start_task(
        self,
        task: Task,
        *,
        context: KeywordDict | None = None,
        expiry_time: datetime | None = None,
        interrupt=False,
    ) -> TaskExecution:
        """Submit a task and return its execution envelope once the controller acknowledges it.

        Unlike `execute_task`, this awaits only the controller's initial response, then returns the
        minted `TaskExecution`. The controller assigns the identity (`task_id`, `controller_id`);
        the returned execution is bound to the in-flight call, so the caller can read the assigned
        identity immediately and `await` the execution itself for the final `TaskExecutionResult`.

        Args:
            task: The semantic task to execute.
            context: Optional keyword context to attach to the execution.
            expiry_time: Optional time after which the execution should be considered expired.
            interrupt: Whether to interrupt any task currently in progress.

        Returns:
            The minted execution envelope, awaitable for the final result.
        """
        call = self.execute_task(
            task, context=context, expiry_time=expiry_time, interrupt=interrupt
        )
        response = await call.invoke()
        execution = response.execution
        assert execution is not None, "controller acknowledged task without an execution envelope"

        # The in-flight call's future resolves to the result in this (client) context.
        execution.bind_result(call.get_future())

        return execution

    def abort_task(self, task_id: uuid.UUID | None = None) -> Call[AbortResponseMessage, None]:
        """Send an abort request to the controller for the optionally specified task ID."""
        return self.call(abort_task_request, AbortRequestMessage(task_id=task_id))

    async def wait_for_task(self, task_id: uuid.UUID | None = None):
        """Wait until the controller reports that no task (or the specified task) is executing."""
        # FIXME: This is unreliable at present due to backend limitations.
        async for event in ControllerState.event_stream(self, TaskExecutionState):
            if task_id is not None:
                if event.execution is None or event.execution.task_id != task_id:
                    return None

            if not event.executing:
                return event

        raise RuntimeError("stream ended unexpectedly")


class ControllerRef(EntityRef[ControllerClient]):
    """A serializable reference to a controller client."""

    def _get_client(self, kit: SensorKit) -> ControllerClient:
        return kit.controller(self.name)


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


type TaskHandlerCallback[T: Task] = Callable[[T], Coroutine[Any, Any, None]]


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
    async def update_context(
        self,
        *args,
        **kwargs,
    ) -> Context:
        """Update the context of the currently executing task."""
        ...

    @abstractmethod
    def task_handler(self, task_type: type[Task]) -> Callable[..., TaskHandlerCallback]:
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
