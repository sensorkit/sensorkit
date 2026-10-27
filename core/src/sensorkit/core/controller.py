# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import math
from abc import abstractmethod
from collections.abc import AsyncIterator, Coroutine, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Collection, Mapping, NoReturn, Self

import uuid_utils.compat as uuid
from pydantic import BaseModel, Field, model_validator

from sensorkit.backend.event import Event
from sensorkit.backend.request import Call, declare_long_request, declare_request
from sensorkit.common.aio import AsyncObserver, OverflowPolicy, defer_cancellation
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
    """A known failure prevents asserting an achieved lifecycle state."""
    UNKNOWN = auto()
    """No achieved lifecycle contract is asserted."""
    TRANSITION = auto()
    """A lifecycle task is changing the controller's operating state."""


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
    """Reported operating state and lifecycle input.

    Achieved states require explicit context, including an empty mapping.
    Missing or null context changes an achieved state to UNKNOWN.
    """
    current: InternalControllerState
    previous: InternalControllerState | None = None
    target: InternalControllerState | None = None
    context: KeywordDict = Field(default_factory=KeywordDict)
    """Lifecycle input for the current or most recent attempt.

    Achieved states assert this contract. Other states retain it for diagnosis.
    """

    NO_CHANGE: ClassVar[object] = object()

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_context(cls, data: Any) -> Any:
        """Normalize lifecycle records with missing or null context."""
        if not isinstance(data, dict) or data.get("context") is not None:
            return data

        # Keep missing context detectable during startup normalization.
        data = {key: value for key, value in data.items() if key != "context"}

        match data.get("current"):
            case (
                InternalControllerState.OPERATE
                | InternalControllerState.STANDBY
                | InternalControllerState.SHUTDOWN
            ):
                return {
                    **data,
                    "current": InternalControllerState.UNKNOWN,
                    "previous": data["current"],
                }

        return data

    def matches(
        self,
        state: InternalControllerState,
        context: KeywordDict,
    ) -> bool:
        """Return whether the reported state and context match with no transition pending."""
        return self.current == state and self.target is None and self.context == context

    def derive(
        self,
        *,
        current: InternalControllerState = NO_CHANGE,
        target: InternalControllerState | None = NO_CHANGE,
        context: KeywordDict = NO_CHANGE,
    ) -> Self:
        """Return a new ControllerOperatingState based on this one, updating only the specified fields."""
        if current == self.current:
            # This ensures `previous` isn't unnecessarily rotated out.
            current = self.NO_CHANGE

        return ControllerOperatingState(
            current=self.current if current == self.NO_CHANGE else current,
            previous=self.previous if current == self.NO_CHANGE else self.current,
            target=self.target if target == self.NO_CHANGE else target,
            context=self.context if context is self.NO_CHANGE else context,
        )


class ControllerState(EventSourcedState):
    """Controller state."""
    enable_state: ControllerEnableState
    operating_state: ControllerOperatingState
    execution_state: TaskExecutionState


class ControllerEnableStateRequest(BaseModel):
    """Request that a Controller enable or disable its task handlers."""
    enable: bool


set_enable_state_request = declare_request(
    "set_enable_state",
    message=ControllerEnableStateRequest,
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


class ExecuteResponseMessage(BaseModel):
    """A response to a task execution request.

    Carries the minted `TaskExecution` envelope so the client learns the assigned `task_id` (e.g.
    for a subsequent abort) without having to supply it.
    """
    execution: TaskExecution | None = None


class AbortRequestMessage(BaseModel):
    """A Task abort request."""
    task_id: uuid.UUID | None
    """If given, verifies the running task ID matches before aborting."""


class AbortResponseMessage(BaseModel):
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


execute_task_request = declare_long_request(
    name="execute_task",
    message=ExecuteRequestMessage,
    response=ExecuteResponseMessage,
    result=TaskExecutionResult,
)

abort_task_request = declare_long_request(
    name="abort_task",
    message=AbortRequestMessage,
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


class ExecutionStateStream:
    """Stream state events for one execution, including events received before it is known.

    The stream buffers a controller's state events from creation. Once bound to an execution,
    iteration yields that execution's events in arrival order. After closing, iteration ends once
    the buffered events are consumed. Multiple iterators share the buffer.
    """

    def __init__(self, controller: ControllerClient):
        self.execution: TaskExecution | None = None
        """The execution whose state events are yielded."""
        self._subscription = contextlib.ExitStack()
        self._states = self._subscription.enter_context(
            controller.get_event_mux().event_queue(TaskExecutionState)
        )

    def bind(self, execution: TaskExecution):
        """Select the execution whose state events are yielded."""
        self.execution = execution

    def close(self):
        """Stop receiving state events, leaving buffered events readable."""
        self._subscription.close()
        self._states.shutdown()

    async def __aiter__(self) -> AsyncIterator[TaskExecutionState]:
        """Yield state events for the bound execution.

        Raises:
            RuntimeError: If no execution is bound.
        """
        if self.execution is None:
            raise RuntimeError("Execution state stream is not bound")

        task_id = self.execution.task_id

        while True:
            try:
                state = await self._states.get()
            except asyncio.QueueShutDown:
                return

            if state.execution is not None and state.execution.task_id == task_id:
                yield state


class ControllerRef(EntityRef[ControllerClient]):
    """A serializable reference to a controller client."""

    def _get_client(self, kit: SensorKit) -> ControllerClient:
        return kit.controller(self.name)


class ControllerStateUnknown(Exception):
    """Controller state is unavailable because monitoring is not ready or has ended."""


class ControllerMonitor:
    """Continuously monitor controller state.

    Create another monitor to resume after failure or stop.
    A healthy monitor does not prove the controller is online.
    """

    def __init__(self, controller: ControllerClient):
        self.controller = controller
        self._observer: AsyncObserver[ControllerOperatingState] = AsyncObserver()
        self._state: ControllerState | None = None
        self._lifecycle: (
            tuple[InternalControllerState, InternalControllerState | None, KeywordDict] | None
        ) = None
        self._error: Exception | None = None
        self._reader: asyncio.Task[None] | None = None
        self._teardown: asyncio.Task[None] | None = None

        # Set by the first state, a failure, or a stop.
        self._startup_complete = asyncio.Event()

    async def start(self, *, timeout: float = 10.0) -> None:
        """Start monitoring controller state and wait for the first valid state.

        A state that does not exist yet is awaited until created.

        Args:
            timeout: Seconds to wait for the first valid state.

        Raises:
            ValueError: If `timeout` is not positive and finite.
            RuntimeError: If the monitor was already started.
            TimeoutError: If no valid state arrives in time.
            ControllerStateUnknown: If state monitoring fails or stops first.
        """
        if not (math.isfinite(timeout) and timeout > 0):
            raise ValueError(f"timeout must be positive and finite, got {timeout}")

        if self._reader is not None:
            raise RuntimeError("A controller monitor can only be started once")

        self._reader = asyncio.create_task(self._read())

        try:
            async with asyncio.timeout(timeout):
                await self._startup_complete.wait()
        except TimeoutError:
            error = TimeoutError(f"{self.controller.entity} state not received in {timeout} s")
            self._fail(error)
            await self._abandon(error)
        except asyncio.CancelledError as e:
            await self._abandon(e)

        try:
            self._healthy_state()
        except ControllerStateUnknown as e:
            await self._abandon(e)

    async def stop(self) -> None:
        """Stop monitoring controller state and wait until its subscription is released.

        Repeated calls join the same cleanup. Before the first start, this does nothing.
        Subscription cleanup failures propagate to the caller.
        """
        if self._reader is not None:
            await self._shut_down()

    @property
    def state(self) -> ControllerState:
        """Copy of the latest controller state.

        Raises:
            ControllerStateUnknown: If state monitoring is not ready, failed, or stopped.
        """
        return self._healthy_state().model_copy(deep=True)

    def healthy(self) -> bool:
        """Whether valid controller state is available and monitoring has not ended.

        This does not check whether the controller is online.
        """
        return self._state is not None and not self._observer.closed

    @property
    def operating_state(self) -> ControllerOperatingState:
        """Copy of the latest operating state.

        Raises:
            ControllerStateUnknown: If state monitoring is not ready, failed, or stopped.
        """
        return self._healthy_state().operating_state.model_copy(deep=True)

    @property
    def last_state(self) -> ControllerState | None:
        """Copy of the last state received, including after monitoring ends."""
        return None if self._state is None else self._state.model_copy(deep=True)

    @property
    def error(self) -> Exception | None:
        """The first state monitoring failure, retained after shutdown."""
        return self._error

    @contextlib.contextmanager
    def state_changes(
        self,
        *,
        initial_value: bool = True,
        maxsize: int = 64,
        overflow: OverflowPolicy = "close",
    ) -> Iterator[asyncio.Queue[ControllerOperatingState]]:
        """Subscribe to changes of operating state, target, or lifecycle context.

        Subscribers share each state update and must not modify it. The queue shuts down
        when monitoring fails or stops, or on overflow with the `close` policy.

        Args:
            initial_value: Whether to queue the current state first.
            maxsize: Bound on queued state updates.
            overflow: Handling of a full queue. `close` shuts down this queue only.
                `drop_oldest` discards its oldest state update.

        Yields:
            The subscriber queue, unsubscribed on exit.

        Raises:
            ControllerStateUnknown: If state monitoring is not ready, failed, or stopped.
            ValueError: If `maxsize` or `overflow` is invalid.
        """
        self._healthy_state()

        with self._observer.subscription(
            initial_value=initial_value, maxsize=maxsize, overflow=overflow
        ) as queue:
            yield queue

    def _healthy_state(self) -> ControllerState:
        """Return the cached state, or raise if the monitor is unhealthy."""
        if self._state is not None and not self._observer.closed:
            return self._state

        match self._error, self._observer.closed:
            case None, False:
                reason = "is not ready"
            case None, True:
                reason = "stopped"
            case _:
                reason = "failed"

        raise ControllerStateUnknown(
            f"{self.controller.entity} state is unknown because monitoring {reason}"
        ) from self._error

    async def _read(self):
        states = self.controller.kv_monitor_model(ControllerState, strict=True)

        try:
            async with contextlib.aclosing(states):
                async for state in states:
                    self._receive(state)

            raise RuntimeError(f"{self.controller.entity} state stream ended")
        except asyncio.CancelledError as e:
            if self._teardown is None:
                failure = RuntimeError(f"{self.controller.entity} state monitoring canceled")
                failure.__cause__ = e
                self._fail(failure)

            raise
        except Exception as e:
            # Cleanup failures after a stop belong to the stopping caller.
            if self._teardown is not None:
                raise

            self._fail(e)

    def _receive(self, state: ControllerState):
        operating = state.operating_state
        lifecycle = (operating.current, operating.target, operating.context)
        self._state = state

        if lifecycle != self._lifecycle:
            self._lifecycle = lifecycle

            # Subscribers share this copy, apart from the cache.
            self._observer.notify(operating.model_copy(deep=True))

        self._startup_complete.set()

    def _fail(self, error: Exception):
        """Record the first failure and stop monitoring controller state."""
        if self._error is None:
            self._error = error

        self._observer.close()
        self._startup_complete.set()

    async def _abandon(self, failure: BaseException) -> NoReturn:
        """Join teardown after a failed start, then raise the startup failure."""
        # A later stop can report cleanup failure without replacing the startup failure.
        with contextlib.suppress(Exception):
            await self._shut_down()

        raise failure

    async def _shut_down(self):
        await defer_cancellation(self._request_teardown())

    def _request_teardown(self) -> asyncio.Task[None]:
        if self._teardown is None:
            # No await before cancellation, so the reader cannot deliver after closure.
            self._observer.close()
            self._startup_complete.set()
            self._reader.cancel()
            self._teardown = asyncio.create_task(self._join_reader(self._reader))

        return self._teardown

    @staticmethod
    async def _join_reader(reader: asyncio.Task[None]):
        # The reader's own cancellation is expected and does not propagate.
        await asyncio.wait((reader,))

        if not reader.cancelled():
            reader.result()


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
"""A task handler receiving its task."""

type ExecutionHandlerCallback[T: Task] = Callable[[TaskExecution[T]], Coroutine[Any, Any, None]]
"""A task handler receiving its task's execution envelope."""


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
    def add_task_handler[T: Task](
        self,
        task_model: type[T],
        *,
        task_callback: TaskHandlerCallback[T] | None = None,
        execution_callback: ExecutionHandlerCallback[T] | None = None,
    ) -> None:
        """Register the handler for a task type.

        Supply exactly one callback.
        A task callback receives the task.
        An execution callback receives the task's execution envelope.

        Raises:
            TypeError: If neither or both callbacks are supplied.
            RuntimeError: If the task type already has a handler.
        """
        ...

    @abstractmethod
    def task_handler(self, task_type: type[Task]) -> Callable[..., TaskHandlerCallback]:
        """Return a decorator for a handler that receives the task."""
        ...

    @abstractmethod
    def task_running(self) -> bool:
        """Return True if a task is currently executing on this controller."""
        ...

    @abstractmethod
    async def set_internal_state(
        self,
        state: InternalControllerState,
        *,
        context: KeywordDict | None = None,
    ):
        """Report controller state and its lifecycle parameters.

        Args:
            state: The settled state.
            context: Lifecycle input. Omission asserts empty input for an achieved state.
                For UNKNOWN or ERROR, omission preserves the recorded input.

        Raises:
            ValueError: If TRANSITION is requested or a lifecycle task is running.
        """
        ...
