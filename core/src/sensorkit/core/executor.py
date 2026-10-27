# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

import uuid_utils.compat as uuid
from loguru import logger

from sensorkit.backend.request import Call, CallError, CallRejected
from sensorkit.common.aio import cleanup_future, defer_cancellation, interruptible_wait
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerClient,
    ControllerMonitor,
    ExecuteResponseMessage,
    InternalControllerState,
    TaskExecutionResult,
    TaskExecutionState,
)
from sensorkit.core.entity import EventsNotSubscribed
from sensorkit.core.program import TaskingFault, TaskingLoopState, TaskingStage
from sensorkit.core.task import Task, TaskContextMap, TaskExecution, TaskSubmission

type TaskFactoryResult = Task | TaskSubmission | None
type TaskFactoryFunc = Callable[
    [],
    Coroutine[Any, Any, TaskFactoryResult]
    | AsyncGenerator[TaskFactoryResult, TaskExecution]
    | TaskFactoryResult,
]
type TaskingChangeCallback = Callable[
    [TaskingLoopState, TaskExecutionState | None], Coroutine[Any, Any, None]
]
"""Receive session progress and the accepted execution's latest state event."""


class NoTaskAvailable(Exception):
    """There is no task available for execution."""


async def _invoke_factory(
    func: TaskFactoryFunc,
) -> tuple[AsyncGenerator[TaskFactoryResult, TaskExecution] | None, TaskSubmission]:
    """Invoke a task factory and normalize its output.

    The factory may be a plain function or coroutine returning a `Task`/`TaskSubmission`/`None`, or
    an async generator that yields one of those and is later resumed with the minted
    `TaskExecution` (see `_resume_factory`).

    Returns:
        A tuple of the generator to resume after dispatch (or `None` for non-generator factories)
        and the normalized `TaskSubmission`.

    Raises:
        NoTaskAvailable: If the factory offers no task this cycle.
    """
    obj = func()

    match obj:
        case AsyncGenerator():
            agen = obj
            try:
                result = await agen.asend(None)
            except StopAsyncIteration as e:
                # A generator that never yields is a no-task scenario rather than an error.
                raise NoTaskAvailable from e
        case Coroutine():
            agen = None
            result = await obj
        case Task() | TaskSubmission() | None:
            agen = None
            result = obj
        case _:
            raise RuntimeError("task factory returned invalid type")

    if result is None:
        # The factory had no task for us. Close any generator so its finally-blocks run.
        if agen is not None:
            await agen.aclose()

        raise NoTaskAvailable

    # Normalize a bare task into a submission with no execution parameters.
    submission = result if isinstance(result, TaskSubmission) else TaskSubmission(task=result)

    return agen, submission


async def _resume_factory(
    agen: AsyncGenerator[TaskFactoryResult, TaskExecution], execution: TaskExecution
) -> None:
    """Resume a factory generator once, handing it the minted execution.

    The generator may simply end (ignoring the result), or `await` the execution itself to consume
    the result before ending. Either way it is expected to stop after a single resumption.
    """
    try:
        await agen.asend(execution)
    except StopAsyncIteration:
        # Expected: the factory ran its post-dispatch logic (if any) and ended.
        return
    else:
        logger.warning(
            "Task factory generator yielded more than once! This indicates a programming error."
        )
        await agen.aclose()


class TaskOutcomeUnknown(Exception):
    """Task acceptance or execution completion could not be confirmed."""


class TaskFailed(CallError):
    """A task execution ended without success."""

    def __init__(self, *args, aborted: bool = False):
        super().__init__(*args)
        self.aborted = aborted
        """Whether this call requested an abort before receiving the failed result."""


class TaskCall:
    """Send one task to a controller and follow the execution it starts.

    Rejection confirms that the task did not run. After acceptance, the execution's end event
    confirms success or failure. Missing acceptance or completion leaves the call unresolved.

    The call buffers controller state events for its accepted execution.
    Observation ends when the call completes or `close()` is called.
    """

    def __init__(
        self,
        controller: ControllerClient,
        task: Task,
        *,
        context: KeywordDict | None = None,
        expiry_time: datetime | None = None,
        interrupt: bool = False,
    ):
        """Create a call without sending it.

        Args:
            controller: The controller to execute the task.
            task: The task to execute.
            context: Optional keyword context to attach to the execution.
            expiry_time: Optional time after which the execution should be considered expired.
            interrupt: Whether to interrupt any task currently in progress.
        """
        self.controller = controller
        self.task = task
        self.context = context
        self.expiry_time = expiry_time
        self.interrupt = interrupt
        self.execution: TaskExecution | None = None
        """The execution the controller identified, if any."""
        self.execution_state: TaskExecutionState | None = None
        """Latest `TaskExecutionState` event for the accepted execution."""
        loop = asyncio.get_running_loop()
        self._outcome: asyncio.Future[TaskExecutionResult] = loop.create_future()
        self._outcome.add_done_callback(cleanup_future)
        self._abort = asyncio.Event()
        self._sent = False
        self._follower: asyncio.Task[TaskExecutionResult] | None = None
        self._subscription = contextlib.ExitStack()
        self._received_states: asyncio.Queue[TaskExecutionState] | None = None
        self._accepted_states: asyncio.Queue[TaskExecutionState] = asyncio.Queue()
        self._observing = True
        self._observer: asyncio.Task[None] | None = None

    async def send(self) -> TaskExecution:
        """Send the request and wait until the controller accepts the task.

        The request goes out before the caller first suspends. Canceling the caller while it
        waits for the reply leaves the call unresolved. The returned execution can be awaited
        for the call's result.

        Raises:
            RuntimeError: If the call was already sent.
            EventsNotSubscribed: If the controller's events are not subscribed.
            BackendError: If the controller's event subscription failed to start.
            CallRejected: If the controller rejected the task without running it.
            TaskOutcomeUnknown: If acceptance could not be established.
        """
        if self._sent:
            raise RuntimeError("Task call already sent")

        mux = self.controller.get_event_mux()

        if not mux.ready():
            raise EventsNotSubscribed(f"{self.controller.entity} events are not subscribed")

        self._sent = True

        # Buffer state events until acceptance identifies the execution.
        self._received_states = self._subscription.enter_context(mux.event_queue(TaskExecutionState))
        call = self.controller.execute_task(
            self.task, context=self.context, expiry_time=self.expiry_time, interrupt=self.interrupt
        )

        try:
            await call.invoke()
        except CallRejected as e:
            # A rejection reply can still identify the execution it declined.
            if call.response is not None:
                self.execution = call.response.execution

            self._settle_outcome(error=e)
            raise
        except asyncio.CancelledError as e:
            # Losing the reply leaves acceptance unknown even if the controller started the task.
            self._settle_outcome(error=self._outcome_unknown("acceptance is unknown", e))
            raise
        except Exception as e:
            error = self._outcome_unknown("acceptance is unknown", e)
            self._settle_outcome(error=error)
            raise error from e

        if call.response is None or call.response.execution is None:
            error = self._outcome_unknown("was accepted without an execution")
            self._settle_outcome(error=error)
            raise error

        execution = call.response.execution
        self.execution = execution

        # Holders of the execution await the outcome without canceling it.
        shared = asyncio.shield(self._outcome)
        shared.add_done_callback(cleanup_future)
        execution.bind_result(shared)

        # Queue buffered events for the accepted execution before reading new events.
        self._select_received()
        self._observer = asyncio.create_task(self._observe_states())
        self._observer.add_done_callback(cleanup_future)

        self._follower = asyncio.create_task(self._follow(call, execution))
        self._follower.add_done_callback(self._settle)

        return execution

    async def execution_states(self) -> AsyncIterator[TaskExecutionState]:
        """Yield state events for the accepted execution in arrival order.

        When the call completes or observation closes, iteration ends after yielding the
        buffered events. Multiple iterators share the buffer. Each event is yielded only once.
        """
        while True:
            try:
                yield await self._accepted_states.get()
            except asyncio.QueueShutDown:
                return

    def close(self):
        """Release the subscription to execution state events without aborting the task.

        Result tracking continues. Buffered state events remain available for iteration.
        """
        self._select_received()
        self._observing = False
        self._subscription.close()
        self._accepted_states.shutdown()

        if self._observer is not None:
            self._observer.cancel()

    def _select_received(self):
        """Queue buffered events whose task ID matches the accepted execution."""
        if self.execution is None or self._received_states is None:
            return

        while self._observing and not self._received_states.empty():
            self._select(self._received_states.get_nowait())

    def _select(self, state: TaskExecutionState):
        if not self._observing or state.execution is None:
            return

        if state.execution.task_id == self.execution.task_id:
            self.execution_state = state
            self._accepted_states.put_nowait(state)

    async def _observe_states(self):
        while True:
            self._select(await self._received_states.get())

    def _settle_outcome(
        self, result: TaskExecutionResult | None = None, *, error: BaseException | None = None
    ):
        """Release state observation and complete the result future."""
        # Keep buffered state events available after completing the result future.
        self.close()

        if self._outcome.done():
            return

        if error is not None:
            self._outcome.set_exception(error)
        else:
            self._outcome.set_result(result)

    def abort(self):
        """Request an abort of the execution once the controller accepts it.

        Repeated requests retry a failed abort until the controller accepts one.
        The execution's end event determines the result.
        """
        self._abort.set()

    def future(self) -> asyncio.Future[TaskExecutionResult]:
        """Return the shared future for task rejection or the execution result.

        The future raises `CallRejected` if the task never ran, `TaskFailed` if the execution
        ended without success, and `TaskOutcomeUnknown` if acceptance or completion is unknown.
        Use `asyncio.wait` or `asyncio.shield` to keep waiter cancellation from canceling it.
        """
        return self._outcome

    def error(self) -> BaseException | None:
        """Return why the call ended without success, or None if running or successful."""
        if not self._outcome.done():
            return None

        if self._outcome.cancelled():
            return self._outcome_unknown("outcome was cancelled")

        return self._outcome.exception()

    def _outcome_unknown(self, reason: str, cause: BaseException | None = None) -> TaskOutcomeUnknown:
        error = TaskOutcomeUnknown(f"{self.task.task_type} {reason}")
        error.__cause__ = cause
        return error

    def _settle(self, follower: asyncio.Task[TaskExecutionResult]):
        if follower.cancelled():
            self._settle_outcome(error=self._outcome_unknown("call was cancelled"))
        elif (error := follower.exception()) is not None:
            self._settle_outcome(error=error)
        else:
            self._settle_outcome(follower.result())

    async def _follow(
        self,
        call: Call[ExecuteResponseMessage, TaskExecutionResult],
        execution: TaskExecution,
    ) -> TaskExecutionResult:
        result = call.get_future()
        aborted = False
        delivered = False

        while not result.done():
            if not await interruptible_wait((result,), interrupt=self._abort):
                break

            # Requests received during this send trigger another attempt if the send fails.
            self._abort.clear()
            aborted = True

            if not delivered:
                delivered = await self._send_abort(execution)

        error = asyncio.CancelledError() if result.cancelled() else result.exception()

        match error:
            case None:
                return result.result()
            case CallError():
                outcome = "aborted" if aborted else "failed"
                raise TaskFailed(f"{self.task.task_type} {outcome}", aborted=aborted) from error
            case _:
                # The end event was lost, so the execution may still be running.
                raise self._outcome_unknown("end is unknown", error)

    async def _send_abort(self, execution: TaskExecution) -> bool:
        """Send a targeted abort and return whether the controller accepted it."""
        try:
            await self.controller.abort_task(execution.task_id).invoke()
        except Exception as e:
            logger.warning(f"Could not abort {self.task.task_type} execution ({e!r})")
            return False

        return True


class TaskingLoop:
    """Produce tasks while a controller reports OPERATE.

    Each loop owns one session and its final task call. Faults end task production and
    retain their origin and failed stage through cleanup.
    """

    def __init__(
        self,
        controller: ControllerClient,
        factory_func: TaskFactoryFunc,
        contexts: TaskContextMap,
        task_group: asyncio.TaskGroup,
        *,
        on_tasking_change: TaskingChangeCallback | None = None,
    ):
        self.controller = controller
        self.task_factory: TaskFactoryFunc = factory_func
        self.contexts = contexts
        self._task_group = task_group
        self._on_tasking_change = on_tasking_change
        self.loop_state = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
        """Current operation and any terminal fault."""
        self.execution_state: TaskExecutionState | None = None
        """Latest controller report for this session's execution."""
        self._published: tuple[TaskingLoopState, TaskExecutionState | None] | None = None
        self._publication_lock = asyncio.Lock()
        self._monitor = ControllerMonitor(controller)
        self._aio_task: asyncio.Task | None = None
        self._stop_requested = asyncio.Event()
        self.finalized = False
        """Whether task production ended and the last call returned a result or error."""
        self.unresolved = False
        """Whether the last call could not confirm task acceptance or completion."""
        self._call: TaskCall | None = None
        self._publisher: asyncio.Task[None] | None = None
        self._finalization: asyncio.Task | None = None

    def start(self):
        """Activate the tasking loop."""
        if self._aio_task is not None or self.finalized:
            raise RuntimeError("Tasking loop was already started")

        self._aio_task = self._task_group.create_task(self._run())
        return self._aio_task

    async def finalize(self):
        """Wait for the producer and last task call, preserving them if the caller is canceled."""
        await asyncio.shield(self._finalizing())

    def _finalizing(self) -> asyncio.Task[None]:
        """Return the shared finalization task, starting it if needed."""
        if self._finalization is None:
            self._finalization = asyncio.create_task(self._finalize())
            self._finalization.add_done_callback(cleanup_future)

        return self._finalization

    @property
    def running(self) -> bool:
        """Whether the task producer is running and no stop has been requested."""
        return not self._producer_done() and not self._stop_requested.is_set()

    @property
    def stop_requested(self):
        """Return True if a stop has been requested for this tasking loop."""
        return self._stop_requested.is_set()

    def request_stop(self, *, abort: bool = False):
        """Stop producing tasks, optionally canceling the factory and aborting the current task."""
        # Record cleanup intent before canceling work.
        self._stop_requested.set()

        if abort:
            self._end_work()

    def _end_work(self):
        """Cancel task production and request an execution abort."""
        self._cancel_producer()

        if self._call is not None:
            self._call.abort()

    def _producer_done(self) -> bool:
        return self._aio_task is None or self._aio_task.done()

    def _cancel_producer(self):
        if self._aio_task is not None and not self._aio_task.cancelling():
            self._aio_task.cancel()

    def _advance(self, stage: TaskingStage, task_type: str | None = None):
        """Change the current operation while the session is healthy."""
        if self.loop_state.fault is None:
            self.loop_state = TaskingLoopState(
                session_id=self.loop_state.session_id, stage=stage, task_type=task_type
            )

    def _fail(
        self,
        fault: TaskingFault,
        error: BaseException | str,
        *,
        stage: TaskingStage | None = None,
    ):
        """Retain the first fault ending this session."""
        state = self.loop_state

        if isinstance(error, BaseException):
            error = f"{type(error).__name__}: {error}"

        if state.fault is not None:
            logger.warning(f"Tasking {self.controller.entity} also failed ({error})")
            return

        stage = state.stage if stage is None else stage
        logger.error(f"Tasking {self.controller.entity} failed during {stage} ({error})")

        self.loop_state = TaskingLoopState(
            session_id=state.session_id,
            stage=stage,
            task_type=state.task_type,
            fault=fault,
            error=error,
        )

    def _classify(self, call: TaskCall):
        """Classify a failed controller call.

        Requested aborts retain the session's existing outcome.
        """
        if self.loop_state.fault is not None:
            return

        match error := call.error():
            case None | TaskFailed(aborted=True):
                return
            case CallRejected():
                self._fail("controller", error, stage="dispatch")
            case TaskOutcomeUnknown() if call.execution is None:
                self._fail("controller", error, stage="dispatch")
            case _:
                self._fail("controller", error, stage="execution")

    async def _publish_tasking_state(self):
        """Publish changes to session progress and execution state."""
        if self._on_tasking_change is None:
            return

        async with self._publication_lock:
            snapshot = (self.loop_state, self.execution_state)

            if snapshot == self._published:
                return

            try:
                await self._on_tasking_change(*snapshot)
            except Exception as e:
                logger.opt(exception=e).warning(
                    f"Could not report {self.controller.entity} tasking"
                )
            else:
                self._published = snapshot

    def _resolve_context(self, submission: TaskSubmission) -> KeywordDict | None:
        """Merge the factory-supplied context with the per-task-type context bundle."""
        context = KeywordDict(submission.context or {})

        if type_context := self.contexts.get(submission.task.task_type):
            context.update(type_context)

        return context or None

    def _resolve_expiry(self, submission: TaskSubmission) -> datetime | None:
        """Resolve the absolute expiry time for a submission.

        Uses the factory-supplied `expiry_time` when present, otherwise the task's
        `default_expiry`. A `timedelta` default is anchored to the current time.
        """
        if submission.expiry_time is not None:
            return submission.expiry_time

        default = submission.task.default_expiry()

        if isinstance(default, timedelta):
            return datetime.now(UTC) + default

        return default

    async def _run(self):
        # Publish session identity before task production.
        await self._publish_tasking_state()

        try:
            await self._supervised()
        except Exception as e:
            self._fail("program", e)
        finally:
            try:
                await self._monitor.stop()
            except Exception as e:
                logger.opt(exception=e).warning(
                    f"{self.controller.entity} observation release failed"
                )

    async def _supervised(self):
        """Run task production under controller observation."""
        try:
            # Subscribe before starting calls that depend on execution events.
            await self.controller.subscribe_events()
            await self._monitor.start()
            current = self._monitor.operating_state.current
        except Exception as e:
            self._fail("controller", e)
            return

        if current != InternalControllerState.OPERATE:
            self._fail("controller", f"{self.controller.entity} reported {current}")
            return

        with self._monitor.state_changes(initial_value=False) as changes:
            supervisor = asyncio.create_task(self._supervise(changes))

            try:
                await self._produce()
            finally:
                supervisor.cancel()
                await asyncio.wait((supervisor,))

    async def _supervise(self, changes: asyncio.Queue):
        """End task production if the controller leaves OPERATE."""
        while True:
            try:
                current = (await changes.get()).current
            except asyncio.QueueShutDown:
                error = f"{self.controller.entity} observation ended ({self._monitor.error!r})"
                break

            if current != InternalControllerState.OPERATE:
                error = f"{self.controller.entity} reported {current}"
                break

        # Ignore departures caused by requested cleanup.
        if self._stop_requested.is_set():
            return

        self._fail("controller", error)
        self._end_work()
        await asyncio.shield(self._publish_tasking_state())

    async def _produce(self):
        while not self._stop_requested.is_set():
            # Clear the previous execution before starting another factory attempt.
            self.execution_state = None
            self._advance("factory")
            await self._publish_tasking_state()

            try:
                agen, submission = await _invoke_factory(self.task_factory)
            except NoTaskAvailable:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_requested.wait(), timeout=5.0)

                continue
            except Exception as e:
                self._fail("program", e)
                return

            if self._stop_requested.is_set():
                await self._throw(agen, asyncio.CancelledError())
                return

            # Continue task production only after successful dispatch and handling.
            if not await self._dispatch(agen, submission):
                return

    async def _dispatch(
        self,
        agen: AsyncGenerator[TaskFactoryResult, TaskExecution] | None,
        submission: TaskSubmission,
    ) -> bool:
        """Execute one factory submission and its program handling.

        Returns:
            Whether execution and program handling completed successfully.
        """
        task = submission.task
        context = self._resolve_context(submission)
        expiry_time = self._resolve_expiry(submission)
        ttl = (expiry_time - datetime.now(UTC)).total_seconds() if expiry_time else None

        self._advance("dispatch", task.task_type)
        await self._publish_tasking_state()

        logger.info(
            f"Dispatching {task.task_type} ({type(task).__name__}) to {self.controller.entity}"
        )

        call = TaskCall(self.controller, task, context=context, expiry_time=expiry_time)
        self._call = call

        # Deliver dispatch errors to the suspended factory so its error handling and cleanup run.
        try:
            # Preserve acceptance so cancellation can abort the specific execution that started.
            execution = await defer_cancellation(call.send())
        except asyncio.CancelledError as e:
            # Interrupted tasking aborts the execution once the controller accepts it.
            call.abort()
            self._publish_execution(call)
            await self._throw(agen, e)
            return False
        except Exception as e:
            self._fail("controller", e)
            self._publish_execution(call)
            await self._publish_tasking_state()
            await self._throw(agen, e)
            return False

        publisher = self._publish_execution(call)

        try:
            async with asyncio.timeout(ttl) as deadline:
                # Resumed generator code can run before execution ends.
                if agen is not None:
                    self._advance("post_task", task.task_type)
                    await self._publish_tasking_state()
                    resumed, agen = agen, None
                    await _resume_factory(resumed, execution)

                if not call.future().done():
                    self._advance("execution", task.task_type)
                    await self._publish_tasking_state()
                    await asyncio.shield(call.future())
        except (Exception, asyncio.CancelledError) as e:
            logger.opt(exception=e).debug(f"Tasking ended during {execution.task_id}")
            self._end_task(call, e, expired=deadline.expired())
            await self._publish_tasking_state()
            await self._throw(agen, e)
            return False

        # Check the execution outcome independently of generator handling.
        await asyncio.wait((publisher,))

        if self.loop_state.fault is not None:
            return False

        self._call = None
        self._publisher = None
        logger.info(f"Finished {task.task_type} ({type(task).__name__})")

        return True

    def _end_task(self, call: TaskCall, error: BaseException, *, expired: bool):
        """Record a terminal fault before aborting the execution."""
        # Classify established execution failure before program handling errors.
        self._classify(call)

        match error:
            case _ if expired and not call.future().done():
                self._fail("controller", f"{call.task.task_type} expired", stage="execution")
            case asyncio.CancelledError():
                # Cleanup preserves any fault already recorded.
                pass
            case _ if error is call.error():
                pass
            case _:
                self._fail("program", error)

        call.abort()

    def _publish_execution(self, call: TaskCall) -> asyncio.Task[None] | None:
        """Start publishing state events for the accepted execution."""
        if call.execution is None:
            return None

        self._publisher = asyncio.create_task(self._publish_execution_states(call))
        self._publisher.add_done_callback(cleanup_future)

        return self._publisher

    async def _publish_execution_states(self, call: TaskCall):
        """Publish execution state changes and any terminal controller fault."""
        async for state in call.execution_states():
            self.execution_state = state
            await self._publish_tasking_state()

        await asyncio.wait((call.future(),))
        self._classify(call)
        await self._publish_tasking_state()

    async def _throw(
        self, agen: AsyncGenerator[TaskFactoryResult, TaskExecution] | None, e: BaseException
    ):
        """Resume a suspended factory with an exception."""
        if agen is None:
            return

        try:
            await agen.athrow(e)
        except Exception as error:
            # Keep factory cleanup errors as secondary diagnostics.
            if error is not e and not isinstance(error, StopAsyncIteration):
                logger.warning(
                    f"Task factory cleanup failed for {self.controller.entity} ({error!r})"
                )
        except BaseException:
            pass

    async def _finalize(self):
        if self._aio_task is not None:
            await asyncio.wait((self._aio_task,))

        # The producer can exit while its last controller task is still running.
        if (call := self._call) is not None:
            await asyncio.wait((call.future(),))
            self.unresolved = isinstance(call.error(), TaskOutcomeUnknown)

        if self._publisher is not None:
            await asyncio.wait((self._publisher,))

        # Retry fault publication interrupted by cancellation.
        await self._publish_tasking_state()

        self._call = None
        self._publisher = None
        self.finalized = True
