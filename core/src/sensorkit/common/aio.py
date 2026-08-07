# SPDX-License-Identifier: Apache-2.0
"""Async utilities: scoped futures, periodic loops, value latches, and observer queues."""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import AsyncContextManager, ClassVar, Self, overload


def scoped_waiter[T](aw: Awaitable[T]) -> AsyncContextManager[asyncio.Future[T]]:
    """Create an async context manager wrapping an awaitable in a scoped `asyncio.Future`.

    The future is cancelled if it has not completed before the `async with` block
    exits or if an exception propagates out of the block.

    Important:
        Any exceptions raised by the awaitable are silently discarded. This makes
        `scoped_waiter` suitable only for "waiter" type tasks where exceptions either
        won't occur or are not meaningful to handle (e.g. background monitoring tasks,
        optional notifications, or advisory operations).

    Args:
        aw: The awaitable object to be wrapped in an `asyncio.Future`.

    Returns:
        An async context manager that yields the `asyncio.Future` for the awaitable.
    """
    return _scoped_waiter(aw)


@overload
def cleanup_future(fut: asyncio.Task): ...

@overload
def cleanup_future(fut: asyncio.Future): ...

def cleanup_future(fut: asyncio.Future | asyncio.Task):
    """Safely clean up a Future or an already-completed Task.

    Args:
        fut: The asyncio.Future to clean up.
    """
    if not fut.done():
        fut.cancel()
    elif not fut.cancelled():
        fut.exception()


@contextlib.asynccontextmanager
async def _scoped_waiter(aw: Awaitable) -> AsyncGenerator[asyncio.Task]:
    fut = asyncio.ensure_future(aw)

    try:
        yield fut
    finally:
        if not fut.done():
            fut.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await fut
        else:
            with contextlib.suppress(asyncio.CancelledError):
                fut.exception()


class AsyncValueLatch[T]:
    """Stores a value and stages pending changes."""

    def __init__(self, initial_value: T):
        self.value: T = initial_value
        self.pending_value: T = initial_value
        self._pending = asyncio.Event()

    def update(self, value: T, only_if_different=True):
        """Flag a pending value change and set that pending value."""
        if only_if_different and value == self.pending_value:
            return False

        self.pending_value = value
        self._pending.set()
        return True

    def pending_change(self):
        """Return True if there is a pending value change."""
        return self._pending.is_set()

    async def wait_until_pending(self):
        """Wait until there is a pending value change."""
        await self._pending.wait()

    def apply(self):
        """Apply the pending value if there is one and return the current value."""
        if self._pending.is_set():
            self._pending.clear()
            self.value = self.pending_value

        return self.value


class AsyncObserver[T]:
    """Observer pattern with concurrent value updates.

    For simple notifications, call notify() directly. For atomic check-and-notify patterns, use
    the async context manager:

        async with observer:
            if observer.value != new_value:
                await observer.notify(new_value)
    """

    NOT_SET: ClassVar[object] = object()

    def __init__(self, initial_value: T = NOT_SET):
        self._observers: weakref.WeakSet[asyncio.Queue[T]] = weakref.WeakSet()
        self._value = initial_value
        self._lock = asyncio.Lock()

    def subscribe(self, *, initial_value: bool = False):
        """Create and return a new observer queue."""
        queue: asyncio.Queue[T] = asyncio.Queue()
        self._observers.add(queue)

        if initial_value and self._value is not self.NOT_SET:
            queue.put_nowait(self._value)

        return queue

    def unsubscribe(self, queue: asyncio.Queue[T]):
        """Remove an observer queue."""
        self._observers.discard(queue)
        queue.shutdown()

    async def notify(self, value: T):
        """Update the current value and notify all observers."""
        self._value = value
        await asyncio.gather(*(queue.put(value) for queue in self._observers))

    @property
    def value(self) -> T:
        """Get the current value."""
        if self._value is self.NOT_SET:
            raise RuntimeError("Observed value must be set prior to access")

        return self._value

    async def consume(self, *, initial_value: bool = False):
        """Yield successive values as an async generator, unsubscribing automatically on exit."""
        queue = self.subscribe(initial_value=initial_value)

        try:
            while True:
                yield await queue.get()
                queue.task_done()
        except GeneratorExit:
            queue.task_done()
        except asyncio.QueueShutDown as e:
            raise StopAsyncIteration() from e
        finally:
            self.unsubscribe(queue)

    async def __aenter__(self):
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()


class AsyncLoop:
    """Runs a function repeatedly on a fixed interval.

    The interval is a plain attribute, so assigning to it retunes a running loop from
    its next sleep onward.
    """

    def __init__(
        self,
        func: Callable[[], Awaitable[None] | None],
        *,
        interval: float,
        log: bool = False,
        label: str | None = None,
    ):
        """Initialize the loop without starting it.

        Args:
            func: Called once per iteration. Takes no arguments.
            interval: Seconds to sleep between iterations.
            log: Whether to log exceptions raised by `func`.
            label: Optional label for logging.
        """
        self.func = func
        self.interval = interval
        self._log = log
        self._label = label or getattr(func, "__qualname__", str(func))
        self._task: asyncio.Task | None = None

    @property
    def active(self) -> bool:
        """Report whether the loop is currently running."""
        return self._task is not None and not self._task.done()

    def start(self) -> Self:
        """Start the loop, returning self.

        Does nothing if the loop is already running, so callers that cannot easily
        tell may start it unconditionally.
        """
        if not self.active:
            self._task = asyncio.create_task(self._run())

        return self

    async def stop(self):
        """Cancel the loop and wait for it to unwind.

        Safe to call on a loop that was never started, and on one already stopped.
        """
        if self._task is None:
            return

        task = self._task
        task.cancel()
        self._task = None

        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self):
        while True:
            try:
                if coro := self.func():
                    await coro
            except Exception as e:
                if self._log:
                    from loguru import logger

                    msg = f": {e}" if str(e) else ""
                    logger.exception(f"{type(e).__name__} in {self._label} loop{msg}")

            await asyncio.sleep(self.interval)
