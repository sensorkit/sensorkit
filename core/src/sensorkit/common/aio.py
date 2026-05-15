"""Async utilities: scoped futures, value latches, and observer queues."""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncGenerator, Awaitable
from typing import AsyncContextManager, ClassVar


def scoped_waiter[T](aw: Awaitable[T]) -> AsyncContextManager[asyncio.Future[T]]:
    """
    Creates an asynchronous context manager for an awaitable object, producing an
    `asyncio.Future` that can be managed within the `async with` block.

    This utility ensures proper cancellation of the future if it is not completed
    before exiting the context or if an exception occurs in the `async with` block.

    **Important:** Any exceptions raised by the awaitable are silently discarded.
    This makes `scoped_waiter` suitable only for "waiter" type tasks where exceptions
    either won't occur or are not meaningful to handle (e.g., background monitoring
    tasks, optional notifications, or advisory operations).

    :param aw: The awaitable object to be wrapped in an `asyncio.Future`.
    :return: An asynchronous context manager that provides an `asyncio.Future` corresponding
             to the given awaitable.
    """
    return _scoped_waiter(aw)


def cleanup_future(fut: asyncio.Future | asyncio.Task):
    """Safely clean up a completed future by checking for cancellation or exception.

    This function is intended to be called on futures that have already completed.
    If the future was cancelled, this function returns immediately without further
    action. Any exceptions raised during cleanup are silently suppressed.

    Args:
        fut: The asyncio.Future to clean up.
    """
    if not fut.cancelled():
        try:
            fut.exception()
        except BaseException:
            pass


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


class PerpetualGroup:
    """A TaskGroup wrapper for tasks that should be force-cancelled at shutdown.

    Use this for perpetual background work — infinite-loop monitors, lifecycle
    drivers, polling tasks — where exiting the ``async with`` block normally
    must *not* wait for children to complete on their own (because they never
    will). ``cancel_all()`` triggers cancellation; the underlying ``TaskGroup``
    then drains the cancelled tasks via its normal ``__aexit__``.

    Use ``asyncio.TaskGroup`` directly for tasks that represent bounded work
    that should be politely awaited on shutdown.
    """

    def __init__(self):
        self._tasks: set[asyncio.Task] = set()
        self._tg = asyncio.TaskGroup()

    async def __aenter__(self):
        await self._tg.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._tg.__aexit__(exc_type, exc_val, exc_tb)

    def create_task(self, coro, *, name: str | None = None):
        """Schedule *coro* in the group, tracking the task for cancel_all()."""
        t = self._tg.create_task(coro, name=name)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
        return t

    def cancel_all(self):
        """Cancel every tracked task.

        TaskGroup does not treat CancelledError as a failure, so after this
        call the group's ``__aexit__`` will drain the cancelled tasks and
        exit cleanly with no exception.
        """
        for t in list(self._tasks):
            t.cancel()


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
