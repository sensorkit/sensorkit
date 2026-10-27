# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from loguru import logger

from sensorkit.backend.request import Call, CallError, CallRejected
from sensorkit.common.aio import cleanup_future, defer_cancellation, interruptible_wait
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerClient,
    ExecuteResponseMessage,
    TaskExecutionResult,
    TaskExecutionState,
)
from sensorkit.core.entity import EventsNotSubscribed
from sensorkit.core.task import Task, TaskContextMap, TaskExecution, TaskSubmission

type TaskFactoryResult = Task | TaskSubmission | None
type TaskFactoryFunc = Callable[
    [],
    Coroutine[Any, Any, TaskFactoryResult]
    | AsyncGenerator[TaskFactoryResult, TaskExecution]
    | TaskFactoryResult,
]
type TaskChangeCallback = Callable[[TaskExecution | None], Coroutine[Any, Any, None]]


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
    """Produce tasks for one controller and track the last call through finalization."""

    def __init__(
        self,
        controller: ControllerClient,
        factory_func: TaskFactoryFunc,
        contexts: TaskContextMap,
        task_group: asyncio.TaskGroup,
        *,
        on_task_change: TaskChangeCallback | None = None,
    ):
        self.controller = controller
        self.task_factory: TaskFactoryFunc = factory_func
        self.contexts = contexts
        self._task_group = task_group
        # Notify the program when a task starts or completes normally.
        # On failure or abort, the program clears tasking state after loop finalization.
        self._on_task_change = on_task_change
        self._aio_task: asyncio.Task | None = None
        self._stop_requested = asyncio.Event()
        self.finalized = False
        """Whether task production ended and the last call returned a result or error."""
        self.unresolved = False
        """Whether the last call could not confirm task acceptance or completion."""
        self._call: TaskCall | None = None
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
        self._stop_requested.set()

        if abort:
            self._cancel_producer()

            if self._call is not None:
                self._call.abort()

    def _producer_done(self) -> bool:
        return self._aio_task is None or self._aio_task.done()

    def _cancel_producer(self):
        if self._aio_task is not None and not self._aio_task.cancelling():
            self._aio_task.cancel()

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

    async def _tasking_loop(self):
        # Task calls follow their executions through the controller's events.
        await self.controller.subscribe_events()

        while not self._stop_requested.is_set():
            # Source the next task from the factory.
            try:
                agen, submission = await _invoke_factory(self.task_factory)
            except NoTaskAvailable:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_requested.wait(), timeout=5.0)

                continue

            if self._stop_requested.is_set():
                await self._throw(agen, asyncio.CancelledError())
                return

            # Dispatch and run it. A fatal error ends the loop.
            if not await self._dispatch(agen, submission):
                return

    async def _run(self):
        try:
            await self._tasking_loop()
        except Exception as e:
            logger.opt(exception=e).error(f"Task factory failed for {self.controller.entity}")

    async def _dispatch(
        self,
        agen: AsyncGenerator[TaskFactoryResult, TaskExecution] | None,
        submission: TaskSubmission,
    ) -> bool:
        """Dispatch a single task and run it to completion.

        Returns:
            True if the task completed (success, or a failure the factory handled without
            re-raising); False if a fatal error occurred and the loop should stop.
        """
        task = submission.task
        context = self._resolve_context(submission)
        expiry_time = self._resolve_expiry(submission)
        ttl = (expiry_time - datetime.now(UTC)).total_seconds() if expiry_time else None

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
            await self._throw(agen, e)
            return False
        except Exception as e:
            logger.error(f"Failed to dispatch {task.task_type} to {self.controller.entity}: {e!r}")
            await self._throw(agen, e)
            return False

        try:
            await self._notify_task_change(execution)

            async with asyncio.timeout(ttl):
                # Resume the factory with the running execution (task_id now known). It may await
                # the execution to consume the result and run post-task logic.
                if agen is not None:
                    resumed, agen = agen, None
                    await _resume_factory(resumed, execution)

                # Wait for completion if the factory did not await the execution.
                # A factory that handled a completed call's error allows tasking to continue.
                if not call.future().done():
                    await asyncio.shield(call.future())
        except (Exception, asyncio.CancelledError) as e:
            # The program clears tasking state after loop finalization.
            logger.opt(exception=e).debug(f"Tasking ended during {execution.task_id}")
            call.abort()
            await self._throw(agen, e)
            return False

        self._call = None
        await self._notify_task_change(None)
        logger.info(f"Finished {task.task_type} ({type(task).__name__})")

        return True

    async def _notify_task_change(self, execution: TaskExecution | None):
        """Signal the owner that the currently running task changed, if a callback is registered."""
        if self._on_task_change is not None:
            await self._on_task_change(execution)

    @staticmethod
    async def _throw(
        agen: AsyncGenerator[TaskFactoryResult, TaskExecution] | None, e: BaseException
    ):
        """Send an error to a factory suspended at its first task."""
        if agen is not None:
            with contextlib.suppress(BaseException):
                await agen.athrow(e)

    async def _finalize(self):
        if self._aio_task is not None:
            await asyncio.wait((self._aio_task,))

        # The producer can exit while its last controller task is still running.
        if (call := self._call) is not None:
            await asyncio.wait((call.future(),))
            self.unresolved = isinstance(call.error(), TaskOutcomeUnknown)

        self._call = None
        self.finalized = True
