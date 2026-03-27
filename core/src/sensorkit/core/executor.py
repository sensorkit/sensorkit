from __future__ import annotations

import asyncio
import contextlib
import functools
from collections.abc import AsyncGenerator, Coroutine
from datetime import UTC, datetime
from typing import Any, Callable

from loguru import logger

from sensorkit.core.controller import ControllerClient, TaskExecutionResult
from sensorkit.core.task import ControllerTask, TaskContexts

type TaskExecutorCallback = Callable[
    [TaskExecutionResult | BaseException], Coroutine[Any, Any, None]
]
type TaskFactoryFunc = Callable[
    [],
    Coroutine[Any, Any, ControllerTask]
    | AsyncGenerator[ControllerTask, TaskExecutionResult | None]
    | ControllerTask
    | None,
]


class NoTaskAvailable(Exception):
    """There is no task available for execution."""


class TaskExecutor:
    """Manages the state of generating and executing a single Task on a target Controller."""

    @classmethod
    async def from_factory(cls, func: TaskFactoryFunc):
        """Create an instance by generating a Task using the given factory function."""
        # Call the user-supplied task factory function to create a Task.
        obj = func()

        # Also support the func returning a coroutine or an async generator.
        match obj:
            case AsyncGenerator():
                try:
                    task = await obj.asend(None)
                except StopAsyncIteration as e:
                    # If the func is an async generator and it doesn't yield, we consider it a no-
                    # task scenario rather than an error.
                    raise NoTaskAvailable from e
            case Coroutine():
                task = await obj
            case ControllerTask() | None:
                task = obj
            case _:
                raise RuntimeError("task factory returned invalid type")

        if task is None:
            # The factory func didn't have a Task for us, so we can't do anything.
            raise NoTaskAvailable

        # If the factory func is an async generator, we need to iterate it once more after the task
        # completes execution to allow the caller to perform post-task actions. This class supports
        # an async "done" callback to do exactly this.
        done_cb = (
            functools.partial(cls._notify_async_generator, obj)
            if isinstance(obj, AsyncGenerator)
            else None
        )

        return cls(task, done_callback=done_cb)

    @staticmethod
    async def _notify_async_generator(
        agen: AsyncGenerator[ControllerTask, TaskExecutionResult | None],
        result: TaskExecutionResult | BaseException,
    ):
        try:
            match result:
                case TaskExecutionResult():
                    await agen.asend(result)
                case BaseException():
                    await agen.athrow(type(result), result)
                case _:
                    raise RuntimeError("task executor result is invalid type")
        except StopAsyncIteration:
            # Factory funcs that are async generators should only yield once, so we expect this
            # exception to be raised here.
            pass
        else:
            logger.warning(
                "Task factory generator yields more than once! This indicates a programming error."
            )

    def __init__(self, task: ControllerTask, *, done_callback: TaskExecutorCallback | None = None):
        self.task = task
        self._done_callback = done_callback

    async def run_on_controller(self, controller: ControllerClient):
        """Execute the Task on the target controller."""
        try:
            # Instruct the target controller to begin Task execution.
            call = controller.execute_task(self.task)
            response = await call.invoke()

            # Await task completion event.
            logger.debug(f"program waiting for task {response.task_id} to complete")
            result = await call
        except (asyncio.CancelledError, Exception) as e:
            if self._done_callback:
                await self._done_callback(e)

            raise
        else:
            if self._done_callback:
                await self._done_callback(result)

        return result


class TaskingLoop:
    """Executes a loop that creates and invokes TaskExecutor instances."""

    def __init__(
        self,
        controller: ControllerClient,
        factory_func: TaskFactoryFunc,
        contexts: TaskContexts,
        task_group: asyncio.TaskGroup,
    ):
        self.controller = controller
        self.task_factory: TaskFactoryFunc = factory_func
        self.contexts = contexts
        self._task_group = task_group
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

    async def _tasking_loop(self):
        while not self._stop_requested.is_set():
            # Generate a task and executor object.
            try:
                executor = await TaskExecutor.from_factory(self.task_factory)
                task = executor.task
            except NoTaskAvailable:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_requested.wait(), timeout=5.0)

                continue

            # Propagate context.
            if context := self.contexts.get(task.task_type):
                task.set_context(context)

            # Execute the task on the target controller.
            logger.info(
                f"Dispatching {task.task_type} ({type(task).__name__} {task.task_id})"
                f" to {task.controller_id}"
            )

            # Determine the time until we should abort the task.
            ttl = (
                (task.end_time - datetime.now(UTC)).seconds if task.end_time is not None else None
            )

            try:
                async with asyncio.timeout(ttl):
                    # Run the task.
                    await executor.run_on_controller(self.controller)
            except (Exception, asyncio.CancelledError) as e:
                msg = f"Aborting {task.task_type} on {task.controller_id}"

                match e:
                    case asyncio.CancelledError():
                        logger.error(f"{msg} due to interrupt")
                    case Exception():
                        logger.error(f"{msg} due to error")
                        logger.opt(exception=e).debug(
                            f"{type(e).__name__} during execution of {task.task_id}"
                        )

                with contextlib.suppress(BaseException):
                    # Kick off an abort here, but don't wait for it to complete.
                    await self.controller.abort_task(task.task_id).invoke()

                return

            logger.info(f"Finished {task.task_type} ({type(task).__name__} {task.task_id})")
