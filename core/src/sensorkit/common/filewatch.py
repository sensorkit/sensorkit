"""Async-friendly filesystem watching over watchdog.

Wraps watchdog's threaded, callback-based API in an asyncio-native, async-generator
interface, and funnels every watch in the process through one shared Observer.

Sharing a single Observer also sidesteps a macOS FSEvents limitation: two watches on the
same directory in *separate* Observer instances collide. With one Observer, watchdog keeps
a single emitter per directory and dispatches it to every handler. To preserve that
one-emitter-per-directory guarantee, each directory has exactly one physical watch whose
recursion level is set by its first subscriber. A recursive watch is a superset of a
non-recursive one, so a non-recursive consumer can always be served from a recursive watch
by filtering on `event.path.parent == directory`. The reverse cannot be done without
re-creating the watch -- which would drop events on existing subscribers during the gap --
so a recursive consumer of a directory already watched non-recursively is rejected.

The shared Observer serves directory watches (`watch_dir`). A single file can be awaited
with `wait_for_file`, which runs its own short-lived PollingObserver instead of joining the
shared Observer: polling works on any filesystem (including network mounts, where native
watchers miss remote writes) and cannot collide with the shared Observer on macOS. (watchdog
cannot portably watch a file directly, nor one that does not exist yet, so the file's parent
directory is watched.)

Caveats:
- The watched directory (and, for `wait_for_file`, the file's parent directory) must already
  exist; scheduling a watch on a missing path raises.
- A temp-then-rename write surfaces as `MOVED` (path = destination), not `CREATED`;
  include `MOVED` in `kinds` when watching for "a file became ready".
- `existing=True` reports already-present entries with kind `EXISTING`, scanned after
  subscribing so no live event is missed. A present entry may therefore appear both via
  the scan and a concurrent live event; consumers should be idempotent.
- Queues are unbounded by default (lossless); pass `max_queue` to bound them, in which
  case the newest event is dropped and logged on overflow.
"""

from __future__ import annotations

import asyncio
import enum
import os
import pathlib
import threading
from collections.abc import AsyncGenerator, Callable, Collection
from dataclasses import dataclass

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

    def __init__(self, real_dir: str):
        self.real_dir = real_dir
        self.lock = threading.Lock()
        self.subscribers: set[Subscriber] = set()
        self.handler = DispatchHandler(self)
        self.observed_watch: ObservedWatch | None = None
        self.recursive = False


class WatchManager:
    """Process-wide registry funneling every watch through one shared Observer.

    Thread-safety: `subscribe`/`unsubscribe` may be called from different event-loop
    threads (e.g. tests). A single lock guards the observer and the watch registry. We
    never join watchdog threads while holding the lock, since an observer thread mid
    dispatch may be waiting on a `DirWatch` lock -- joining under the manager lock could
    otherwise deadlock.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._observer: BaseObserver | None = None
        self._watches: dict[str, DirWatch] = {}

    def subscribe(self, real_dir: str, subscriber: Subscriber, *, recursive: bool) -> None:
        with self._lock:
            watch = self._watches.get(real_dir)

            if watch is None:
                watch = DirWatch(real_dir)
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
        observer_to_stop: BaseObserver | None = None
        watch_to_unschedule: tuple[BaseObserver, ObservedWatch] | None = None

        with self._lock:
            watch = self._watches.get(real_dir)
            if watch is None:
                return

            with watch.lock:
                watch.subscribers.discard(subscriber)
                empty = not watch.subscribers

            if empty:
                del self._watches[real_dir]

                if self._observer is not None and watch.observed_watch is not None:
                    if self._watches:
                        # Other watches remain; just drop this one.
                        watch_to_unschedule = (self._observer, watch.observed_watch)
                    else:
                        # Last watch went away; tear the whole observer down.
                        observer_to_stop = self._observer
                        self._observer = None

        # Perform thread joins outside the lock to avoid deadlocking against an observer
        # thread that is blocked acquiring a DirWatch lock during dispatch.
        if watch_to_unschedule is not None:
            observer, observed_watch = watch_to_unschedule
            observer.unschedule(observed_watch)

        if observer_to_stop is not None:
            observer_to_stop.stop()
            observer_to_stop.join()

    def _ensure_observer_locked(self) -> BaseObserver:
        observer = self._observer
        if observer is None:
            observer = Observer()
            observer.start()
            self._observer = observer

        return observer

    def _reset(self) -> None:
        """Tear down all watches and the observer. Intended for tests."""
        with self._lock:
            observer = self._observer
            self._observer = None
            self._watches.clear()

        if observer is not None:
            observer.stop()
            observer.join()


manager = WatchManager()


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


async def watch_dir(
    directory: str | os.PathLike[str],
    *,
    recursive: bool = True,
    kinds: Collection[FileEventKind] | None = None,
    existing: bool = False,
    max_queue: int = 0,
) -> AsyncGenerator[FileEvent, None]:
    """Yield filesystem events occurring under *directory*.

    Args:
        directory: Directory to watch. Must already exist.
        recursive: When `False`, only events whose parent is *directory* itself are
            reported, with a non-recursive physical watch -- unless the directory is already
            watched recursively, in which case that watch is shared and filtered. Requesting
            `recursive=True` for a directory already watched non-recursively raises ValueError.
        kinds: If given, only report events of these kinds; otherwise report all. To
            include the initial scan when `existing=True`, include `EXISTING`.
        existing: When `True`, first emit an `EXISTING` event for each entry already
            present (scanned after subscribing, so no live event is missed). A present
            entry may therefore also be reported by a concurrent live event.
        max_queue: Maximum number of buffered events (0 means unbounded). On overflow the
            newest event is dropped and logged.

    Yields:
        FileEvent: Matching events, until the generator is closed.
    """
    real_dir = os.path.realpath(directory)
    real_dir_path = pathlib.Path(real_dir)
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[FileEvent] = asyncio.Queue(maxsize=max_queue)
    kind_set = frozenset(kinds) if kinds is not None else None

    def predicate(event: FileEvent) -> bool:
        if kind_set is not None and event.kind not in kind_set:
            return False
        if not recursive and event.path.parent != real_dir_path:
            return False
        return True

    subscriber = Subscriber(loop=loop, queue=queue, predicate=predicate, real_dir=real_dir)
    manager.subscribe(real_dir, subscriber, recursive=recursive)

    try:
        if existing:
            for event in await asyncio.to_thread(scan_existing, real_dir, recursive):
                if predicate(event):
                    yield event

        while True:
            yield await queue.get()
            queue.task_done()
    finally:
        manager.unsubscribe(real_dir, subscriber)


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
