from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from loguru import logger

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import ControllerClient
from sensorkit.core.task import Task, TaskContexts, TaskExecution, TaskSubmission

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


class TaskingLoop:
    """Executes a loop that sources tasks from a factory and runs them on a target Controller."""

    def __init__(
        self,
        controller: ControllerClient,
        factory_func: TaskFactoryFunc,
        contexts: TaskContexts,
        task_group: asyncio.TaskGroup,
        *,
        on_task_change: TaskChangeCallback | None = None,
    ):
        self.controller = controller
        self.task_factory: TaskFactoryFunc = factory_func
        self.contexts = contexts
        self._task_group = task_group
        # Invoked with the live `TaskExecution` when a task begins running and with `None` when it
        # completes, so an owner (the program) can publish its current tasking state. Error/abort
        # exits are left to the owner's loop-teardown path rather than signalled here.
        self._on_task_change = on_task_change
        self._aio_task: asyncio.Task | None = None
        self._stop_requested = asyncio.Event()

    def start(self):
        """Activate the tasking loop."""
        if self._aio_task:
            raise RuntimeError("Tasking loop was already started")

        self._aio_task = self._task_group.create_task(self._tasking_loop())
        return self._aio_task

    async def stop(self, *, timeout=None):
        """Deactivate the tasking loop, invalidating the object."""
        try:
            # Wait for a graceful stop.
            self._stop_requested.set()

            async with asyncio.timeout(timeout):
                await self._aio_task
        except TimeoutError:
            # The loop task was cancelled and will call abort on the Controller.
            with contextlib.suppress(asyncio.CancelledError):
                await self._aio_task

    @property
    def stop_requested(self):
        """Return True if a stop has been requested for this tasking loop."""
        return self._stop_requested.is_set()

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
        while not self._stop_requested.is_set():
            # Source the next task from the factory.
            try:
                agen, submission = await _invoke_factory(self.task_factory)
            except NoTaskAvailable:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_requested.wait(), timeout=5.0)

                continue

            # Dispatch and run it. A fatal error ends the loop.
            if not await self._dispatch(agen, submission):
                return

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

        # Instruct the controller to begin execution. The controller mints the identity and binds
        # the in-flight call as the execution's result future.
        try:
            execution = await self.controller.start_task(
                task, context=context, expiry_time=expiry_time
            )
        except (Exception, asyncio.CancelledError) as e:
            # Dispatch failed before the factory was resumed. Deliver the error into the still-
            # suspended generator so its except/finally blocks run, then abandon this task.
            logger.error(f"Failed to dispatch {task.task_type} to {self.controller.entity}: {e!r}")

            if agen is not None:
                with contextlib.suppress(BaseException):
                    await agen.athrow(e)

            return False

        # Client-side association: the program can reach the execution (and its result future) via
        # `task.execution`.
        task.associate_execution(execution)
        await self._notify_task_change(execution)

        try:
            async with asyncio.timeout(ttl):
                # Resume the factory with the running execution (task_id now known). It may await
                # the execution to consume the result and run post-task logic.
                if agen is not None:
                    await _resume_factory(agen, execution)

                # Guarantee completion before the next iteration, regardless of whether the factory
                # awaited the execution itself. If it did, the call is already settled and this
                # returns (or re-raises) immediately; if it consumed an error without re-raising,
                # the execution is done and we leave that decision to the factory.
                if not execution.done():
                    await execution
        except (Exception, asyncio.CancelledError) as e:
            # On error/abort the loop terminates; clearing the tasking state is left to the owner's
            # teardown so we don't emit while unwinding (possibly under cancellation).
            await self._abort(execution, e)
            return False

        await self._notify_task_change(None)
        logger.info(f"Finished {task.task_type} ({type(task).__name__})")

        return True

    async def _notify_task_change(self, execution: TaskExecution | None):
        """Signal the owner that the currently running task changed, if a callback is registered."""
        if self._on_task_change is not None:
            await self._on_task_change(execution)

    async def _abort(self, execution: TaskExecution, e: BaseException):
        """Log and abort an in-flight task following a timeout, interrupt, or unhandled error."""
        msg = f"Aborting {execution.task.task_type} on {self.controller.entity}"

        match e:
            case asyncio.CancelledError():
                logger.error(f"{msg} due to interrupt")
            case _:
                logger.error(f"{msg} due to error")
                logger.opt(exception=e).debug(
                    f"{type(e).__name__} during execution of {execution.task_id}"
                )

        with contextlib.suppress(BaseException):
            # Kick off an abort here, but don't wait for it to complete.
            await self.controller.abort_task(execution.task_id).invoke()
