# SPDX-License-Identifier: Apache-2.0
"""Async-friendly filesystem watching over watchdog.

Wraps watchdog's threaded, callback-based API in an asyncio-native interface. `watch_dir`
watches a directory for the duration of a block; `wait_for_file` suspends until a single
file appears. Every directory watch in the process shares one Observer.

Caveats:
- The watched directory (and, for `wait_for_file`, the file's parent directory) must already
  exist; scheduling a watch on a missing path raises.
- A directory gets one physical watch, whose recursion is fixed by its first subscriber. A
  non-recursive consumer can share a recursive watch, but requesting `recursive=True` for a
  directory already being watched non-recursively raises ValueError.
- A temp-then-rename write surfaces as `MOVED` (path = destination), not `CREATED`;
  include `MOVED` in `kinds` when watching for "a file became ready".
- `existing=True` reports already-present entries with kind `EXISTING`, scanned after
  subscribing so no live event is missed. A present entry may therefore appear both via
  the scan and a concurrent live event; consumers should be idempotent.
- Queues are unbounded by default (lossless); pass `max_queue` to bound them, in which
  case the newest event is dropped and logged on overflow.
- `wait_for_file` polls rather than using a native watcher, so it also sees remote writes
  on network mounts, at a detection latency of up to `poll_interval`.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import functools
import os
import pathlib
import sys
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Collection
from dataclasses import dataclass
from queue import Queue

from loguru import logger
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver, ObservedWatch
from watchdog.observers.polling import PollingObserver

__all__ = [
    "FileEvent",
    "FileEventKind",
    "watch_dir",
    "wait_for_file",
]


class FileEventKind(enum.Enum):
    """The kind of filesystem change a `FileEvent` represents.

    The live-event values mirror watchdog's `EVENT_TYPE_*` strings, so a watchdog event
    maps straight back via `FileEventKind(event.event_type)`. `EXISTING` has no watchdog
    counterpart: it marks an entry surfaced by an initial existing-file scan, rather than
    a change observed live.
    """

    CREATED = "created"
    MODIFIED = "modified"
    MOVED = "moved"
    DELETED = "deleted"
    EXISTING = "existing"


@dataclass(frozen=True, slots=True)
class FileEvent:
    """An observed filesystem change.

    Attributes:
        kind: What happened to the path.
        path: The affected path. For `MOVED` events this is the *destination*.
        src_path: The original path for `MOVED` events; `None` otherwise.
        is_directory: Whether the affected path is a directory.
    """

    kind: FileEventKind
    path: pathlib.Path
    src_path: pathlib.Path | None
    is_directory: bool


def to_file_event(event: FileSystemEvent) -> FileEvent | None:
    """Translate a watchdog event into a `FileEvent`, or `None` if we don't model it.

    Returns `None` for event types without a `FileEventKind` (e.g. opened/closed).
    """
    try:
        kind = FileEventKind(event.event_type)
    except ValueError:
        return None

    if kind is FileEventKind.MOVED:
        return FileEvent(
            kind=kind,
            path=pathlib.Path(os.fsdecode(event.dest_path)),
            src_path=pathlib.Path(os.fsdecode(event.src_path)),
            is_directory=event.is_directory,
        )

    return FileEvent(
        kind=kind,
        path=pathlib.Path(os.fsdecode(event.src_path)),
        src_path=None,
        is_directory=event.is_directory,
    )


class Subscriber:
    """A single async consumer of events from one directory watch.

    The watchdog observer thread calls `deliver` (off the event loop); it filters via
    *predicate* and marshals matching events onto *queue* via the subscriber's loop.
    """

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue[FileEvent],
        predicate: Callable[[FileEvent], bool],
        real_dir: str,
    ):
        self._loop = loop
        self._queue = queue
        self._predicate = predicate
        self._real_dir = real_dir

    def deliver(self, event: FileEvent) -> None:
        """Called from the observer thread; hand a matching event to the consumer loop."""
        if not self._predicate(event):
            return

        try:
            self._loop.call_soon_threadsafe(self._put, event)
        except RuntimeError:
            # The consumer's event loop is closed; nothing to deliver to.
            pass

    def _put(self, event: FileEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(
                f"filewatch queue full for {self._real_dir!r}, dropping event for {event.path}"
            )
        except asyncio.QueueShutDown:
            pass


class DispatchHandler(FileSystemEventHandler):
    """The single watchdog handler per directory; filters and bridges events to subscribers.

    watchdog already shares one emitter per directory across this handler -- we are not
    reimplementing that. This handler's job is the async bridge (onto each subscriber's
    event loop) and per-subscriber filtering, which watchdog does not provide.
    """

    def __init__(self, watch: DirWatch):
        self._watch = watch

    def on_any_event(self, event: FileSystemEvent) -> None:
        file_event = to_file_event(event)
        if file_event is None:
            return

        # Convert once, then snapshot under the lock and deliver outside it. Delivery is a
        # non-blocking call_soon_threadsafe, but we avoid iterating the live set.
        with self._watch.lock:
            subscribers = tuple(self._watch.subscribers)

        for subscriber in subscribers:
            subscriber.deliver(file_event)


class DirWatch:
    """One physical watch on a directory, shared by its subscribers.

    `recursive` is set by the first subscriber. A non-recursive consumer may share a
    recursive watch (filtered); a recursive consumer cannot share a non-recursive watch and
    is rejected (re-creating the watch would drop events on existing subscribers).
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.subscribers: set[Subscriber] = set()
        self.handler = DispatchHandler(self)
        self.observed_watch: ObservedWatch | None = None
        self.recursive = False


class WatchManager:
    """Process-wide registry funneling every watch through one shared Observer.

    Thread-safety: `subscribe`/`unsubscribe` may be called from different event-loop
    threads (e.g. tests).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._observer: BaseObserver | None = None
        self._watches: dict[str, DirWatch] = {}
        self._pending: Queue[tuple[str, ObservedWatch]] = Queue()
        self._reaper: threading.Thread | None = None

    def subscribe(self, real_dir: str, subscriber: Subscriber, *, recursive: bool) -> None:
        with self._lock:
            watch = self._watches.get(real_dir)

            if watch is None:
                watch = DirWatch()
                observer = self._ensure_observer_locked()
                watch.recursive = recursive
                watch.observed_watch = observer.schedule(
                    watch.handler, real_dir, recursive=recursive
                )
                self._watches[real_dir] = watch
            elif recursive and not watch.recursive:
                # The directory is already watched non-recursively. A recursive consumer
                # cannot be served from that watch, and re-creating it as recursive would drop
                # events on the existing subscribers during the gap -- so reject it. (The
                # reverse, a non-recursive consumer of a recursive watch, is fine: it filters.)
                raise ValueError(
                    f"{real_dir!r} is already watched non-recursively; it cannot also be "
                    f"watched recursively (one shared emitter per directory)"
                )

            with watch.lock:
                watch.subscribers.add(subscriber)

    def unsubscribe(self, real_dir: str, subscriber: Subscriber) -> None:
        """Drop *subscriber*, queueing the physical unschedule if it was the last one.

        Non-blocking, and safe to call from an event loop thread.
        """
        with self._lock:
            watch = self._watches.get(real_dir)
            if watch is None:
                return

            with watch.lock:
                watch.subscribers.discard(subscriber)
                if watch.subscribers:
                    return

            del self._watches[real_dir]

            if self._observer is None or watch.observed_watch is None:
                return

            self._pending.put((real_dir, watch.observed_watch))
            self._ensure_reaper_locked()

    def _reap(self) -> None:
        """Unschedule watches queued by `unsubscribe`, one at a time. Runs on its own thread."""
        # This exists to keep the unschedule off the caller's thread: it joins the watch's
        # emitter with no timeout, and a PollingEmitter only notices the stop between
        # passes, so the join can wait out a full walk of the tree. It holds the observer
        # lock throughout, stalling dispatch for every other watch as well.
        while True:
            real_dir, observed_watch = self._pending.get()

            try:
                with self._lock:
                    self._unschedule_locked(real_dir, observed_watch)
            except Exception:
                # The reaper is process-wide: letting it die would leak every later watch.
                logger.exception(f"filewatch failed to unschedule watch on {real_dir!r}")
            finally:
                self._pending.task_done()

    def _unschedule_locked(self, real_dir: str, observed_watch: ObservedWatch) -> None:
        observer = self._observer

        if observer is None:
            return

        # Make sure a watch hasn't been re-added. An ObservedWatch compares by path and
        # recursion and scheduling an equal one reuses the emitter, so unscheduling here
        # would tear down the new subscriber's watch. One re-added with the opposite
        # recursion is a distinct emitter and must still be reaped.
        live = self._watches.get(real_dir)

        if live is not None and live.observed_watch == observed_watch:
            return

        try:
            observer.unschedule(observed_watch)
        except KeyError:
            # Already gone, e.g. via `unschedule_all`.
            pass

    def _ensure_observer_locked(self) -> BaseObserver:
        observer = self._observer

        if observer is None:
            # One Observer process-wide: on macOS, two watches on the same directory in
            # separate Observer instances collide, whereas one Observer keeps a single
            # emitter per directory and dispatches it to every handler.
            observer = Observer()
            observer.start()
            self._observer = observer

        return observer

    def _ensure_reaper_locked(self) -> None:
        if self._reaper is None:
            self._reaper = threading.Thread(
                target=self._reap, name="filewatch-reaper", daemon=True
            )
            self._reaper.start()

    def _reset(self) -> None:
        """Drop all watches, leaving the shared Observer running. Intended for tests."""
        # Let queued teardowns finish first, so they cannot fire against a later test's
        # watches. Blocking here is fine: `_reset` runs off the event loop.
        self._pending.join()

        with self._lock:
            observer = self._observer
            self._watches.clear()

        if observer is not None:
            observer.unschedule_all()


def patch_windows_emitter_handle_close() -> None:
    """Make watchdog's Windows emitter close its directory handle at most once.

    Through watchdog 6.0.0 the Windows emitter closes its directory handle from whichever
    thread stops it and never clears the attribute, and `on_thread_stop` runs more than
    once per emitter during ordinary teardown. The handle is therefore closed twice, and
    between the two closes Windows is free to hand that handle value to something else --
    so the second close destroys an unrelated object. When the value has been reused for
    one of CPython's parking-lot semaphores, the interpreter dies with "Fatal Python
    error: _PySemaphore_Wakeup: parking_lot: ReleaseSemaphore failed". Under pytest the
    message is swallowed by output capture, leaving only a silent non-zero exit.

    Clearing the attribute before closing makes the second call a no-op. Upstream fixes
    this by rewriting the emitter around DirectoryChangeReader, which keeps the handle on
    the thread that owns it; this patch detects that version and does nothing, and the
    whole function can be dropped once it ships.

    See https://github.com/gorakhargosh/watchdog/issues/1132.
    """
    from watchdog.observers import winapi
    from watchdog.observers.read_directory_changes import WindowsApiEmitter

    if hasattr(winapi, "DirectoryChangeReader"):
        return

    def on_thread_stop(self: WindowsApiEmitter) -> None:
        whandle = self._whandle
        if whandle:
            self._whandle = None
            winapi.close_directory_handle(whandle)

    WindowsApiEmitter.on_thread_stop = on_thread_stop


if sys.platform == "win32":
    patch_windows_emitter_handle_close()

manager = WatchManager()


def event_matches(
    event: FileEvent,
    *,
    kinds: frozenset[FileEventKind] | None,
    real_dir: pathlib.Path,
    recursive: bool,
) -> bool:
    """Whether *event* should be reported to a subscriber watching *real_dir*.

    Filters by kind, drops events on the watched directory itself, and -- for a
    non-recursive consumer -- drops anything below its immediate children, which is what
    lets such a consumer share a recursive physical watch.
    """
    if kinds is not None and event.kind not in kinds:
        return False
    if event.path == real_dir:
        return False
    return recursive or event.path.parent == real_dir


async def event_stream(
    *,
    queue: asyncio.Queue[FileEvent],
    existing_events: list[FileEvent],
    existing_done: asyncio.Event | None,
) -> AsyncGenerator[FileEvent, None]:
    """Yield the buffered initial scan, then live events."""
    # Deliberately owns no cleanup: an async generator closed or dropped before its first
    # `__anext__` never runs its body, so unsubscribing here would be skipped exactly when
    # nothing was consumed. The enclosing `watch_dir` block does it instead.
    for event in existing_events:
        yield event

    # The buffered scan is drained; a sequential consumer has processed it all by now.
    if existing_done is not None:
        existing_done.set()

    while True:
        yield await queue.get()
        queue.task_done()


def scan_existing(real_dir: str, recursive: bool) -> list[FileEvent]:
    """Synthesize `EXISTING` events for entries already present under *real_dir*."""
    base = pathlib.Path(real_dir)
    entries = base.rglob("*") if recursive else base.iterdir()

    return [
        FileEvent(
            kind=FileEventKind.EXISTING,
            path=path,
            src_path=None,
            is_directory=path.is_dir(),
        )
        for path in entries
    ]


@contextlib.asynccontextmanager
async def watch_dir(
    directory: str | os.PathLike[str],
    *,
    recursive: bool = True,
    kinds: Collection[FileEventKind] | None = None,
    existing: bool = False,
    existing_done: asyncio.Event | None = None,
    max_queue: int = 0,
) -> AsyncIterator[AsyncGenerator[FileEvent, None]]:
    """Watch *directory* for the duration of the block, yielding a stream of its events.

    Entering the block subscribes -- the watch is armed and, with `existing=True`, the
    initial scan is complete by the time the body starts, so a caller may scan the
    directory itself without racing live writes. Leaving it unsubscribes, however the
    block exits.

    Use it directly, or compose several watches with `contextlib.AsyncExitStack`:

        async with watch_dir(root, kinds=(FileEventKind.CREATED,)) as events:
            async for event in events:
                ...

    Args:
        directory: Directory to watch. Must already exist.
        recursive: When `False`, only events whose parent is *directory* itself are
            reported. Raises ValueError if `True` and the directory is already watched
            non-recursively.
        kinds: If given, only report events of these kinds; otherwise report all. To
            include the initial scan when `existing=True`, include `EXISTING`.
        existing: When `True`, the stream first yields an `EXISTING` event for each entry
            already present (scanned after subscribing, so no live event is missed). A present
            entry may therefore also be reported by a concurrent live event.
        existing_done: If given, set once the stream has yielded the last buffered
            `EXISTING` event and is about to await live events. For a consumer that processes
            each event before requesting the next (an `async for` that awaits in its body),
            this fires right after the initial scan has been fully *processed* -- the signal a
            caller needs to mark an initial listing complete. Fires on first iteration when
            `existing=False` (the initial scan is empty). Intended for use with `existing=True`.
        max_queue: Maximum number of buffered events (0 means unbounded). On overflow the
            newest event is dropped and logged.

    Yields:
        An async generator of matching `FileEvent`s, live until the block exits.
    """
    real_dir = os.path.realpath(directory)
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[FileEvent] = asyncio.Queue(maxsize=max_queue)

    predicate = functools.partial(
        event_matches,
        kinds=frozenset(kinds) if kinds is not None else None,
        real_dir=pathlib.Path(real_dir),
        recursive=recursive,
    )
    subscriber = Subscriber(loop=loop, queue=queue, predicate=predicate, real_dir=real_dir)

    def subscribe_and_scan() -> list[FileEvent]:
        # Subscribing blocks off the loop: it opens a directory handle and starts watchdog's
        # threads, and a recursive inotify watch walks the tree adding one watch per
        # directory -- slow enough on a deep tree or a stalled network mount to matter.
        manager.subscribe(real_dir, subscriber, recursive=recursive)

        # The watch is now live: matching events are delivered to `queue` and cannot be
        # missed. Only now scan for pre-existing entries, and undo the subscription if that
        # fails, since the block whose exit would otherwise clean up is never entered.
        try:
            return scan_existing(real_dir, recursive) if existing else []
        except BaseException:
            manager.unsubscribe(real_dir, subscriber)
            raise

    def drop_orphaned_subscription(task: asyncio.Task[list[FileEvent]]) -> None:
        if not task.cancelled() and task.exception() is None:
            manager.unsubscribe(real_dir, subscriber)

    # Shield the worker rather than abandoning it on cancellation: a thread cannot be
    # interrupted, so a subscription that lands after we stop waiting would never be taken
    # back out. The initial listing is complete by the time this coroutine returns.
    subscribing = asyncio.create_task(asyncio.to_thread(subscribe_and_scan))

    try:
        existing_events = [e for e in await asyncio.shield(subscribing) if predicate(e)]
    except BaseException:
        subscribing.add_done_callback(drop_orphaned_subscription)
        raise

    stream = event_stream(
        queue=queue,
        existing_events=existing_events,
        existing_done=existing_done,
    )

    try:
        yield stream
    finally:
        # Synchronous by design: this also runs when the block is left by cancellation or
        # during interpreter shutdown, where awaiting can raise or never resume and the
        # watch would leak for the life of the process. `unsubscribe` is non-blocking, so
        # the emitter join it defers costs the caller nothing here.
        manager.unsubscribe(real_dir, subscriber)
        await stream.aclose()


class WaitFileHandler(FileSystemEventHandler):
    """Sets an asyncio.Event when a file of the target name appears in the watched directory.

    Used by `wait_for_file`. Runs on the PollingObserver thread, so it marshals the signal
    back to the waiting loop via `call_soon_threadsafe`.
    """

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, appeared: asyncio.Event):
        self._name = name
        self._loop = loop
        self._appeared = appeared

    def _signal(self, raw_path: bytes | str) -> None:
        if os.path.basename(os.fsdecode(raw_path)) != self._name:
            return
        try:
            self._loop.call_soon_threadsafe(self._appeared.set)
        except RuntimeError:
            # The waiting loop is gone; nothing to signal.
            pass

    def on_created(self, event: FileSystemEvent) -> None:
        self._signal(event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        self._signal(event.dest_path)


async def wait_for_file(path: str | os.PathLike[str], *, poll_interval: float = 1.0) -> None:
    """Suspend until *path* exists, polling its parent directory.

    Returns immediately if the file already exists. The file's parent directory must exist.
    Uses a dedicated, short-lived PollingObserver rather than the shared Observer: polling
    works on any filesystem (including network mounts, where native watchers miss remote
    writes) and cannot collide with the shared Observer on macOS. Detection latency is up to
    `poll_interval` seconds.
    """
    target = pathlib.Path(path)
    if await asyncio.to_thread(target.exists):
        return

    loop = asyncio.get_running_loop()
    appeared = asyncio.Event()
    observer = PollingObserver(timeout=poll_interval)
    observer.schedule(WaitFileHandler(target.name, loop, appeared), str(target.parent))
    observer.start()

    try:
        # Re-check: the file may have appeared between the check above and the observer start.
        if not await asyncio.to_thread(target.exists):
            await appeared.wait()
    finally:
        observer.stop()
        await asyncio.to_thread(observer.join)
