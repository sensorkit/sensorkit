# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import copy
from _contextvars import ContextVar
from collections.abc import Coroutine, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import (
    Any,
    Callable,
    ClassVar,
    Collection,
    Mapping,
    NamedTuple,
    overload,
    override,
)

import uuid_utils.compat as uuid
from loguru import logger

from sensorkit.backend.base import Entity
from sensorkit.backend.event import Event
from sensorkit.backend.request import CallContext
from sensorkit.common.aio import defer_cancellation
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    AbortRequestMessage,
    AbortResponseMessage,
    ControllerDevice,
    ControllerEnableState,
    ControllerEnableStateRequest,
    ControllerInterface,
    ControllerOperatingState,
    ControllerState,
    ExecuteRequestMessage,
    ExecuteResponseMessage,
    ExecutionHandlerCallback,
    InternalControllerState,
    KnownControllerState,
    LatchedControllerState,
    TaskExecutionResult,
    TaskExecutionState,
    TaskFinishInfo,
    TaskHandlerCallback,
    abort_task_request,
    execute_task_request,
    set_enable_state_request,
)
from sensorkit.core.device import DeviceClient
from sensorkit.core.entity import ControllerDetails, EntityInfo
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.state import StateUpdate
from sensorkit.core.task import RecoverTask, Task, TaskExecution, TaskInfo
from sensorkit.data.context import Context, ContextSubscription


class ControllerDeviceMap(Mapping[str, ControllerDevice]):
    """Ordered mapping of device names to their ControllerDevice instances, created on demand."""

    def __init__(self):
        self._dict: dict[str, ControllerDevice] = {}

    def get_or_add(self, name: str, *, get_client_func: Callable[[str], DeviceClient]):
        """Return the ControllerDevice for the given name, creating it if it does not yet exist."""
        if name not in self._dict:
            client = get_client_func(name)
            self._dict[name] = ControllerDevice(client, ContextSubscription(client))

        return self._dict[name]

    def __getitem__(self, key, /):
        return self._dict[key]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)


class StateWithInput(NamedTuple):
    """An operating state paired with its lifecycle input."""

    state: InternalControllerState
    context: KeywordDict


class ControllerStateResolver:
    """Select a controller's settled state from task results, observed states, and a latch.

    Until a known state is reported, a successful lifecycle task establishes its target
    and input. Afterward, observed states and their input take precedence over task
    results. An UNKNOWN or ERROR latch takes precedence over both until a lifecycle task
    succeeds.
    """

    def __init__(self, context: KeywordDict | None = None, *, latched: bool = False):
        context = context or KeywordDict()

        self._observed: StateWithInput | None = None
        self._observed_changed = asyncio.Event()
        """Set when a report changes the observed state or latches."""

        self.inferred = InternalControllerState.UNKNOWN
        """State established by lifecycle task results, used until a known state is reported."""
        self.commanded = StateWithInput(InternalControllerState.UNKNOWN, context)
        """Target and input of the most recent successful lifecycle task.

        Starts as UNKNOWN with the recorded lifecycle input.
        """
        self.latch = StateWithInput(InternalControllerState.UNKNOWN, context) if latched else None
        """UNKNOWN or ERROR held until a lifecycle task succeeds."""

    @property
    def observed(self) -> StateWithInput | None:
        """The latest observed known state and input, or None if no known state was reported."""
        return self._observed

    @property
    def settled(self) -> StateWithInput:
        """The state to publish while no lifecycle task is pending or running."""
        if self.latch is not None:
            return self.latch

        if self._observed is not None:
            return self._observed

        return StateWithInput(self.inferred, self.commanded.context)

    def record_observed(self, state: InternalControllerState, context: KeywordDict | None):
        """Apply a state reported by the controller.

        UNKNOWN and ERROR latch with the settled input. Other states replace the observed
        state and input.
        """
        match state, context:
            case InternalControllerState.UNKNOWN | InternalControllerState.ERROR, _:
                self.latch = StateWithInput(state, self.settled.context)
            case _, KeywordDict() as reported if (state, reported) != self._observed:
                self._observed = StateWithInput(state, reported)
            case _:
                return

        # Wake current waiters. Later waiters wait for the next change.
        self._observed_changed.set()
        self._observed_changed = asyncio.Event()

    def record_success(self, target: InternalControllerState | None, context: KeywordDict):
        """Apply a successful lifecycle task, with no target for recovery."""
        self.latch = None

        if target is None:
            self.inferred = InternalControllerState.UNKNOWN
        else:
            self.inferred = target
            self.commanded = StateWithInput(target, context)

    def record_finish(self, run: TaskRun):
        """Apply a finished lifecycle task.

        Success clears the latch. An abort after the task began latches UNKNOWN, and a
        failure latches ERROR.
        Recovery retains the settled lifecycle input.
        """
        target = run.execution.task.target_state()
        context = run.submitted_context if target is not None else self.settled.context

        match run.finish_info:
            # A task that never began leaves the hardware and the latch as they were.
            case TaskFinishInfo(error=False, aborted=True) if run.start_time is None:
                pass
            case TaskFinishInfo(error=False, aborted=False):
                self.record_success(target, context)
            case TaskFinishInfo(error=False):
                self.latch = StateWithInput(InternalControllerState.UNKNOWN, context)
            case _:
                self.latch = StateWithInput(InternalControllerState.ERROR, context)

    def confirms(self, target: StateWithInput) -> bool:
        """Return whether no known state was reported or the latest one is the target."""
        return self._observed is None or self._observed == target

    async def wait_until_observed(self, target: StateWithInput):
        """Wait until the observed state and input are the target.

        Raises:
            RuntimeError: If UNKNOWN or ERROR is reported while waiting.
        """
        latch = self.latch

        while self._observed != target:
            await self._observed_changed.wait()

            # Each latching report records a new latch.
            if self.latch is not None and self.latch is not latch:
                raise RuntimeError(f"Controller reported {self.latch.state} before {target.state}")


class TaskRunPhase(StrEnum):
    """Progress of a task run.

    A run is received until its runner first steps. It is interrupting while stopping its
    predecessor, executing through handler completion and state confirmation, and finished
    during settlement publication.
    """

    RECEIVED = auto()
    INTERRUPTING = auto()
    EXECUTING = auto()
    FINISHED = auto()


@dataclass(eq=False)
class TaskRun:
    """Own a task's runner and execution outcome."""

    execution: TaskExecution
    submitted_context: KeywordDict
    """Framework copy of submitted context, independent of the handler-visible copies."""

    phase: TaskRunPhase = field(default=TaskRunPhase.RECEIVED, init=False)
    cancel_requested: bool = field(default=False, init=False)
    handler_latched_state: StateWithInput | None = field(default=None, init=False)
    """Controller's latched state when the handler returns successfully.

    Reporting UNKNOWN or ERROR after the handler returns and before settlement fails the task.
    """

    finish_info: TaskFinishInfo = field(
        default_factory=lambda: TaskFinishInfo(aborted=True),
        init=False,
    )
    start_time: datetime | None = field(default=None, init=False)

    _handler: ExecutionHandlerCallback[Any] | None = field(default=None, init=False)
    _runner: asyncio.Task[TaskExecutionResult] | None = field(default=None, init=False)

    @classmethod
    def create(
        cls,
        controller_id: str,
        task: Task,
        *,
        context: KeywordDict | None,
        expiry_time: datetime | None,
    ) -> TaskRun:
        """Mint execution metadata without scheduling a runner."""
        execution = TaskExecution(
            task=task,
            task_id=uuid.uuid7(),
            controller_id=controller_id,
            context=context,
            expiry_time=expiry_time,
        )
        submitted = copy.deepcopy(execution.context)

        # Add task identity to the working context.
        execution.context.set(
            TaskInfo(task=task, task_id=execution.task_id, controller_id=controller_id)
        )

        return cls(execution, submitted)

    @property
    def lifecycle(self) -> bool:
        """Whether the task changes the controller's operating state."""
        task = self.execution.task
        return task.target_state() is not None or isinstance(task, RecoverTask)

    @property
    def transition(self) -> bool:
        """Whether lifecycle work is pending, executing, or awaiting confirmation."""
        return self.lifecycle and self.phase != TaskRunPhase.FINISHED

    @property
    def handler(self) -> ExecutionHandlerCallback[Any]:
        assert self._handler is not None, "execution is not attached"
        return self._handler

    @property
    def runner(self) -> asyncio.Task[TaskExecutionResult]:
        assert self._runner is not None, "execution is not attached"
        return self._runner

    def attach(
        self,
        handler: ExecutionHandlerCallback[Any],
        run: Coroutine[Any, Any, TaskExecutionResult],
    ):
        """Bind the handler and schedule the runner of an admitted execution.

        Raises:
            RuntimeError: If the execution is already attached.
        """
        if self._runner is not None:
            raise RuntimeError("execution is already attached")

        self._handler = handler
        self._runner = asyncio.create_task(run)

    def done(self) -> bool:
        return self.runner.done()

    def cancel(self, msg: str | None = None):
        """Request cancellation once."""
        if self.cancel_requested:
            return

        self.cancel_requested = True

        # Canceling before the first step would skip the runner's cleanup.
        if self.phase != TaskRunPhase.RECEIVED and not self.runner.done():
            self.runner.cancel(msg)

    async def join(self):
        """Join the runner, propagating errors other than execution cancellation."""
        await asyncio.wait((self.runner,))

        if not self.runner.cancelled() and (error := self.runner.exception()):
            raise error

    async def wait(self):
        """Join the runner without propagating its outcome.

        Caller cancellation waits for completion, then propagates.
        """
        try:
            await defer_cancellation(asyncio.wait((self.runner,)))
        finally:
            # Observe failures before propagating caller cancellation.
            if self.runner.done() and not self.runner.cancelled():
                self.runner.exception()

    @contextlib.contextmanager
    def execute(self) -> Iterator[TaskExecution]:
        """Time execution and record success, failure, or cancellation.

        Raises:
            asyncio.CancelledError: If cancellation was requested before execution.
        """
        if self.cancel_requested:
            raise asyncio.CancelledError()

        self.phase = TaskRunPhase.EXECUTING
        self.start_time = datetime.now(UTC)

        try:
            yield self.execution
        except Exception as e:
            self.fail(e)
            raise
        else:
            self.finish_info = TaskFinishInfo()

    def fail(self, error: Exception):
        """Record a failure as the execution's finish."""
        self.finish_info = TaskFinishInfo(error=str(error) or type(error).__name__)

    def result(self) -> TaskExecutionResult:
        """Build a successful result with the current time as its end."""
        assert self.start_time is not None, "execution has not started"

        return TaskExecutionResult(
            task_id=self.execution.task_id,
            start_time=self.start_time,
            end_time=datetime.now(UTC),
        )


class ControllerImpl(EntityImpl, ControllerInterface):
    """Helper for implementing server-side functionality of a Controller.

    A controller reports UNKNOWN when its service starts. Attach hooks that can determine the
    hardware state report it with `report_state`.

    Until a known state is reported, a successful lifecycle task establishes its target state
    and input. Afterward, a lifecycle task succeeds once the controller reports its target
    and input.
    """

    current: ClassVar[ContextVar[ControllerImpl | None]] = ContextVar("current_controller", default=None)
    confirmation_timeout: ClassVar[float] = 5.0
    """Seconds a lifecycle task waits after its handler returns to observe its target and input."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._enable_hooks: list[Callable[[], None]] = []
        self._disable_hooks: list[Callable[[], None]] = []
        self._task_handlers: dict[type[Task], ExecutionHandlerCallback[Any]] = {}
        self._execution: TaskRun | None = None
        self._states = ControllerStateResolver()
        self._devices = ControllerDeviceMap()

    @override
    def task_running(self):
        return self._execution is not None and not self._execution.done()

    @override
    def on_enable(self, func: Callable[[], None]):
        self._enable_hooks.append(func)
        return func

    @override
    def on_disable(self, func: Callable[[], None]):
        self._disable_hooks.append(func)
        return func

    @override
    async def init_impl(self):
        self._state = await ControllerState.recover_or_init(
            self,
            enable_state=ControllerEnableState(enabled=True),
            operating_state=ControllerOperatingState(
                current=InternalControllerState.UNKNOWN,
                context=KeywordDict(),
            ),
            execution_state=TaskExecutionState(execution=None),
        )

        operating = self._state.operating_state

        # A restart is not a successful command, so an unsettled state stays latched.
        match operating.current, operating.previous, operating.target:
            # UNKNOWN since the first start has nothing to latch.
            case InternalControllerState.UNKNOWN, None, None:
                latched = False
            case (
                 InternalControllerState.UNKNOWN
                 | InternalControllerState.ERROR
                 | InternalControllerState.TRANSITION
                 ), _, _:
                latched = True
            case _, _, target:
                latched = target is not None

        await self._normalize_startup_state()

        self._states = ControllerStateResolver(operating.context, latched=latched)

    async def _normalize_startup_state(self):
        async with self._state.updating(self) as update:
            operating = self._state.operating_state
            current = self._state.execution_state

            # Persist legacy normalization before accepting work.
            legacy = "context" not in operating.model_fields_set

            if (
                operating.current == InternalControllerState.UNKNOWN
                and operating.target is None
                and not legacy
                and not current.executing
            ):
                return

            events = []

            # A pending target also identifies an interrupted lifecycle execution.
            if (
                operating.current == InternalControllerState.TRANSITION
                or operating.target is not None
            ):
                logger.warning(f"Recovering from interrupted transition to {operating.target}")

            if current.executing:
                events.append(
                    TaskExecutionState(
                        finished=TaskFinishInfo(aborted=True),
                        execution=current.execution,
                        context=current.context,
                    )
                )

            # The persisted state describes hardware before the restart.
            events.append(operating.derive(current=InternalControllerState.UNKNOWN, target=None))
            await update(*events)

    @override
    async def attach_impl(self):
        if self._state.enable_state.enabled:
            await self._call_with_context(self._enable_hooks)

        await self.handle_request(set_enable_state_request, self._set_enable_state)
        await self.handle_request(abort_task_request, self._abort_request)
        await self.handle_request(execute_task_request, self._execute_request)

        await self.start_device_subscriptions()

    @override
    async def detach_impl(self):
        await self.stop_device_subscriptions()

    @override
    def use_device(self, name: str, *, subscribe: list[type] | None = None) -> DeviceClient:
        """Register a controlled device and return a client for it.

        Registers a device that this controller interacts with. Optionally subscribes to
        specific keyword types published by the device. Subscribed keywords are automatically
        cached and made available in contexts via `update_context()`.

        Note:
            Subscriptions remain inactive until `start_device_subscriptions()` is called.
            When using the declarative API, this occurs automatically after initialization.

        Args:
            name: Entity identifier for the device (e.g., "mount", "camera").
            subscribe: Optional list of keyword model types (e.g., Pydantic models) to
                subscribe to. Each subscribed keyword will be monitored, and its latest
                value cached for inclusion in contexts.

        Returns:
            A client instance for interacting with the registered device.
                Access the subscription object via `get_device(name).subscription` to
                retrieve cached values directly.

        Raises:
            KeyError: If attempting to access a device via `get_device(name)` that
                hasn't been registered with `use_device()`.

        Examples:
            Basic device registration without subscriptions:

            >>> camera = self.use_device("camera")

            Register device and subscribe to specific keyword types:

            >>> mount = self.use_device("mount", subscribe=[AltAzPointing, RADecPointing])

            Access cached subscription values from the ControlledDevice instance:

            >>> pointing = self.get_device("mount").get(AltAzPointing)
        """
        device = self._devices.get_or_add(
            name,
            get_client_func=lambda _: self.sensorkit().device(Entity((name,))),
        )

        if subscribe:
            for kw_type in subscribe:
                device.subscription.add(kw_type)

        return device.client

    @override
    def get_device(self, name: str) -> ControllerDevice:
        return self._devices[name]

    @override
    def all_devices(self) -> Collection[ControllerDevice]:
        return self._devices.values()

    @override
    async def start_device_subscriptions(self):
        """Start all keyword subscriptions registered via `use_device`.

        This is called automatically by the declarative API after init
        callbacks have run.  It may also be called manually if the
        controller is not using the declarative API.
        """
        for device in self._devices.values():
            await device.subscription.start()

    @override
    async def stop_device_subscriptions(self):
        """Stop all keyword subscriptions registered via `use_device`.

        Called automatically by the declarative API before deinit
        callbacks run.
        """
        for device in self._devices.values():
            await device.subscription.stop()

    @override
    async def update_context(
        self,
        *args,
        **kwargs,
    ) -> Context:
        """Update the context of the currently executing task.

        Merges the base context with fresh snapshots from all device keyword
        subscriptions and additional provided values. The updated context is
        persisted to the execution state and returned.

        This method allows task handlers to refresh their context mid-execution,
        incorporating the latest device state.

        Args:
            **kwargs: Additional literal key-value pairs to include in the context.

        Returns:
            The newly updated `Context` containing (in precedence order,
            highest-last): current task context, fresh device snapshots,
            and *kwargs*.

        Raises:
            RuntimeError: If called when no task is currently executing.

        Examples:
            Update context with latest device data during task execution:

            >>> async def my_task_handler(self, task):
            ...     # ... some work ...
            ...     ctx = await self.update_context()
            ...     pointing = ctx.get(AltAzPointing)

            Add custom values while updating:

            >>> ctx = await self.update_context(iteration=5, timestamp=time.time())
        """
        async with self._state.updating(self) as update:
            current = self._state.execution_state

            if not current.executing or current.execution is None:
                raise RuntimeError("no task executing")

            ctx = Context(current.context)

            for device in self._devices.values():
                device.subscription.snapshot(into=ctx)

            ctx.set(*args)

            for key, value in kwargs.items():
                ctx.set_value(key, value)

            await update(
                TaskExecutionState(
                    executing=current.executing,
                    aborting=current.aborting,
                    execution=current.execution,
                    context=ctx,
                ),
            )

        return ctx

    async def _set_enable_state(self, request: ControllerEnableStateRequest):
        if request.enable == self._state.enable_state.enabled:
            return

        await self._state.update(self, ControllerEnableState(enabled=request.enable))

        if request.enable:
            await self._call_with_context(self._enable_hooks)
        else:
            #TODO: Abort. Need a nice way of calling a local "loopback" request.
            # await self.call_loopback(abort_task_request, AbortRequestMessage(task_id=None))

            await self._call_with_context(self._disable_hooks)

    async def _abort_request(
        self,
        requested: AbortRequestMessage,
        call: CallContext[None, AbortResponseMessage],
    ):
        async with self._state.updating(self) as update:
            target = self._execution if self.task_running() else None
            current = self._state.execution_state

            # A finished runner may still be completing its request.
            if target is None:
                call.reject(
                    response=AbortResponseMessage(aborting=False, task_id=None),
                    reason="No task is running",
                )
                return

            task_id = target.execution.task_id

            # If given, ensure the task_id matches the latest accepted execution.
            if requested.task_id and task_id != requested.task_id:
                call.reject(
                    response=AbortResponseMessage(aborting=False, task_id=None),
                    reason=f"Running task {task_id} does not match",
                )
                return

            logger.debug(f"aborting running task {task_id}")
            call.accept(response=AbortResponseMessage(aborting=True, task_id=task_id))

            # Pending work leaves its predecessor's published execution alone.
            if current.executing and current.execution.task_id == task_id:
                await update(
                    TaskExecutionState(
                        executing=True,
                        aborting=True,
                        execution=current.execution,
                        context=current.context,
                    ),
                )

            target.cancel("Aborted by remote request")

        # Wait for the execution and its predecessors to end.
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await call.progress_from_task(target.runner, cadence=5, ttl=10)

        if target.done():
            # Success.
            await call.succeed(result=None)
        else:
            await call.fail()

    async def _admit(
        self,
        msg: ExecuteRequestMessage,
        call: CallContext[TaskExecutionResult, ExecuteResponseMessage],
    ) -> TaskRun | None:
        task = msg.task

        async with self._state.updating(self) as update:
            predecessor = self._execution if self.task_running() else None
            running = TaskRun.create(
                str(self.entity),
                task,
                context=msg.context,
                expiry_time=msg.expiry_time,
            )

            response = ExecuteResponseMessage(execution=running.execution)
            handler = self._task_handlers.get(type(task))

            enabled = self._state.enable_state.enabled

            match enabled, predecessor, msg.interrupt, running.lifecycle, handler:
                case False, _, _, _, _:
                    reason = "Controller is disabled"
                # Only a lifecycle task may run while the controller is between states.
                case _, TaskRun(transition=True), _, False, _:
                    reason = "Controller is between states"
                case _, TaskRun(), False, _, _:
                    reason = "Task already in progress"
                case _, _, _, _, None:
                    reason = f"No handler registered for {task.task_type}"
                case _, _, _, _, handler:
                    try:
                        # Publish the transition before an interrupted task starts stopping.
                        if running.lifecycle:
                            await self._enter_transition(running, update)
                    except asyncio.CancelledError:
                        call.reject(response=response, reason="Admission was canceled")
                        raise
                    except Exception as e:
                        reason = f"Could not publish transition ({e})"
                    else:
                        if predecessor:
                            logger.info(
                                f"Interrupting {predecessor.execution.task.task_type} task"
                            )

                        # Ownership is installed before acceptance becomes visible.
                        running.attach(handler, self._run_execution(running, predecessor))
                        self._execution = running
                        call.accept(response=response)

                        return running

        logger.warning(f"Rejecting {task.task_type} task: {reason}")
        call.reject(response=response, reason=reason)

        return None

    async def _enter_transition(self, running: TaskRun, update: StateUpdate):
        """Publish the transition of an admitted lifecycle task.

        On failure or cancellation, attempt to restore the prior state before propagating.
        """
        prior = self._state.operating_state

        try:
            await self._publish_operating_state(update, running)
        except BaseException:
            # The predecessor keeps its transition and lifecycle input.
            with contextlib.suppress(Exception):
                await defer_cancellation(update(prior.derive()))

            raise

    async def _execute_request(
        self,
        msg: ExecuteRequestMessage,
        call: CallContext[TaskExecutionResult, ExecuteResponseMessage],
    ):
        task = msg.task
        running = await self._admit(msg, call)

        if running is None:
            return

        execution = running.execution

        try:
            await call.progress_from_task(running.runner, cadence=6.0, ttl=10.0)
        except asyncio.CancelledError:
            with self.enter_context():
                logger.warning(f"Execution of {task.task_type} task cancelled")

            raise
        except Exception as e:
            with self.enter_context():
                logger.error(f"Error executing {task.task_type} task")
                logger.opt(exception=e).debug(f"{task.task_type} ({execution.task_id}) failed")
        else:
            with self.enter_context():
                logger.info(f"Finished {task.task_type} task")

            await call.succeed(result=running.runner.result())
        finally:
            # Join this request's execution even after a successor replaced it.
            if not running.done():
                running.cancel()

            try:
                await running.wait()
            finally:
                if self._execution is running:
                    self._execution = None

                # Clients treat the final event as proof the run ended, so it follows the join.
                if not call.finalized:
                    await defer_cancellation(self._fail_call(call, task))

    @staticmethod
    async def _fail_call(call: CallContext, task: Task):
        try:
            await call.fail()
        except Exception as e:
            logger.warning(f"Error reporting failed {task.task_type} task ({type(e).__name__})")

    async def _run_execution(
        self,
        running: TaskRun,
        predecessor: TaskRun | None,
    ) -> TaskExecutionResult:
        with self.enter_context():
            try:
                # Cancellation targets the runner from here. Without a predecessor, nothing
                # awaits before execution.
                if predecessor is not None:
                    running.phase = TaskRunPhase.INTERRUPTING

                    try:
                        predecessor.cancel("Interrupted")
                        await defer_cancellation(predecessor.join())
                    except Exception as e:
                        running.fail(e)
                        raise
                    finally:
                        # Release the predecessor so repeated interruption cannot retain a chain.
                        predecessor = None

                with running.execute() as execution:
                    logger.info(f"Executing {execution.task.task_type} task")
                    logger.debug(f"execution begun {execution=}")

                    await self._publish_start(running)
                    await running.handler(execution)
                    running.handler_latched_state = self._states.latch
                    await self._confirm_observed_state(running)
            finally:
                # Joining the runner includes settlement.
                await defer_cancellation(self._publish_finish(running))

            return running.result()

    async def _confirm_observed_state(self, running: TaskRun):
        """Wait for the controller to report the target and input once it reports known states.

        Raises:
            RuntimeError: If the controller reports UNKNOWN or ERROR, or does not report the
                target and input in time.
        """
        target = running.execution.task.target_state()

        if target is None or self._states.observed is None:
            return

        expected = StateWithInput(target, running.submitted_context)

        try:
            async with asyncio.timeout(self.confirmation_timeout):
                await self._states.wait_until_observed(expected)
        except TimeoutError as e:
            raise RuntimeError(f"Controller did not report {target} with the task input") from e

    async def _publish_start(self, running: TaskRun):
        execution = running.execution

        async with self._state.updating(self) as update:
            # Execution context is the base for later device snapshots.
            await defer_cancellation(
                update(
                    TaskExecutionState(
                        executing=True, execution=execution, context=execution.context
                    )
                )
            )

    async def _publish_finish(self, running: TaskRun):
        """Publish a finished execution and its effect on the settled state.

        Raises:
            RuntimeError: If a new fault or mismatched observation prevents lifecycle completion.
        """
        events = []
        mismatch = None

        async with self._state.updating(self) as update:
            if running.lifecycle:
                mismatch = self._settle(running)

            running.phase = TaskRunPhase.FINISHED

            # Record a finish once execution has begun.
            if running.start_time is not None:
                events.append(
                    TaskExecutionState(finished=running.finish_info, execution=running.execution)
                )

            await self._publish_operating_state(update, self._execution, *events)

        if mismatch is not None:
            raise mismatch

    def _settle(self, running: TaskRun) -> RuntimeError | None:
        """Apply a finished lifecycle task to the settled state.

        Returns:
            A lifecycle confirmation error, or None.
        """
        finish = running.finish_info
        target = running.execution.task.target_state()
        states = self._states

        # Reports may have arrived while waiting for the update lock.
        match finish:
            case TaskFinishInfo(error=False, aborted=False) if (
                (latch := states.latch) is not None and latch is not running.handler_latched_state
            ):
                mismatch = RuntimeError(f"Controller reported {latch.state} before settling")
            case TaskFinishInfo(error=False, aborted=False) if target is not None and not (
                states.confirms(StateWithInput(target, running.submitted_context))
            ):
                mismatch = RuntimeError(f"Controller no longer reports {target} with the task input")
            case _:
                mismatch = None

        if mismatch is not None:
            running.fail(mismatch)

        states.record_finish(running)

        return mismatch

    def _derive_operating_state(self, newest: TaskRun | None) -> ControllerOperatingState:
        """Derive the operating state from the newest task run and the settled state."""
        settled = self._states.settled

        match newest:
            case TaskRun(transition=True) as run:
                current = InternalControllerState.TRANSITION
                target = run.execution.task.target_state()
                context = run.submitted_context if target is not None else settled.context
            case _:
                current, target, context = settled.state, None, settled.context

        return self._state.operating_state.derive(current=current, target=target, context=context)

    async def _publish_operating_state(
        self, update: StateUpdate, newest: TaskRun | None, *events: Event
    ):
        """Publish execution events and any operating-state change."""
        published = self._state.operating_state
        derived = self._derive_operating_state(newest)

        if (derived.current, derived.target, derived.context) != (
            published.current,
            published.target,
            published.context,
        ):
            events = (*events, derived)

        if events:
            await defer_cancellation(update(*events))

    @override
    def add_task_handler[T: Task](
        self,
        task_model: type[T],
        *,
        task_callback: TaskHandlerCallback[T] | None = None,
        execution_callback: ExecutionHandlerCallback[T] | None = None,
    ) -> None:
        match task_callback, execution_callback:
            case None, None:
                raise TypeError("A task handler requires a task or execution callback")
            case _, None:
                handler = lambda execution: task_callback(execution.task)
            case None, _:
                handler = execution_callback
            case _:
                raise TypeError("A task handler takes one callback")

        if task_model in self._task_handlers:
            raise RuntimeError("multiple handlers for task type")

        self._task_handlers[task_model] = handler

    @override
    def task_handler[T: Task](self, task_model: type[T]):
        def decorator(func: TaskHandlerCallback[T]):
            self.add_task_handler(task_model, task_callback=func)
            return func

        return decorator

    @override
    def entity_info(self) -> EntityInfo:
        return EntityInfo(
            entity_type="controller",
            details=ControllerDetails(
                published_keywords=frozenset(self._published_keywords),
                supported_tasks=[t.__name__ for t in self._task_handlers.keys()],
                controlled_devices=list(self._devices.keys()),
            ),
        )

    @overload
    async def report_state(self, state: KnownControllerState, context: KeywordDict): ...

    @overload
    async def report_state(self, state: LatchedControllerState): ...

    @override
    async def report_state(
        self,
        state: InternalControllerState,
        context: KeywordDict | None = None,
    ):
        match state, context:
            case InternalControllerState.TRANSITION, _:
                raise ValueError("A transition requires a lifecycle task")
            case InternalControllerState.UNKNOWN | InternalControllerState.ERROR, None:
                pass
            case InternalControllerState.UNKNOWN | InternalControllerState.ERROR, _:
                raise ValueError(f"{state} keeps the recorded lifecycle input")
            case _, None:
                raise ValueError(f"{state} requires its lifecycle input")
            case _:
                context = copy.deepcopy(context)

        async with self._state.updating(self) as update:
            self._states.record_observed(state, context)
            await self._publish_operating_state(update, self._execution)
