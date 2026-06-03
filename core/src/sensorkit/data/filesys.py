import asyncio
import contextlib
import os
import pathlib
from collections import deque
from collections.abc import AsyncGenerator
from fnmatch import fnmatch
from typing import Literal

import aiofile
from loguru import logger
from watchdog.events import (
    FileCreatedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer

from sensorkit.common.aio import cleanup_future
from sensorkit.data.graph import Context, DataFlow, DataOp, SourceOp
from sensorkit.data.streams import StreamReader, StreamWriter


def _resolve_template(template: str, context: Context) -> str:
    """Resolve a template string against a Context.

    Supports f-string syntax (e.g. ``f"{capture_time:%Y%m%dT%H%M%S}"``) which is
    evaluated as a Python expression, or plain ``format_map`` syntax
    (e.g. ``"{program_name}"``).
    """
    if template.startswith(('f"', "f'")):
        return str(context.eval(template))
    return template.format_map(context)


class WatchDirectory(SourceOp):
    """DataGraph source that watches a directory and triggers a run for each new file."""
    op: Literal["watch_directory"] = "watch_directory"
    directory: str
    match: str = "*"
    recursive: bool = False

    async def graph_source(self) -> AsyncGenerator[None, DataFlow]:
        logger.debug(f"starting WatchDirectory source at {self.directory}")
        await asyncio.to_thread(pathlib.Path(self.directory).mkdir, parents=True, exist_ok=True)
        queue: asyncio.Queue[pathlib.Path] = asyncio.Queue()
        handler = FileAppearedHandler(queue)
        observer = Observer()
        observer.schedule(handler, self.directory, recursive=self.recursive)
        observer.start()

        try:
            while True:
                # Wait for the graph to be ready.
                edge = yield

                # Wait for a matching file to appear.
                while True:
                    path = await queue.get()

                    # Make sure the filename matches the pattern.
                    if fnmatch(path.name, self.match):
                        break

                logger.debug(f"file {path} appeared")

                try:
                    await edge.send(
                        Context(file_path=path),
                        b"",
                    )
                except Exception:
                    logger.exception("error sending file path to graph")

                queue.task_done()
        finally:
            observer.stop()
            queue.shutdown(True)


class WriteFile(DataOp):
    """DataGraph node that writes incoming stream data to a file on disk."""
    op: Literal["write_file"] = "write_file"
    directory: str | None = None
    max_chunk_size: int = 2**16

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        assert len(outgoing) <= 1

        # Read incoming data as a stream.
        context, reader = await incoming[0].receive("stream")

        # Resolve the output directory against context values (e.g. "{program_name}").
        directory = pathlib.Path(_resolve_template(self.directory, context))
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        file_name = _resolve_template(context["file_name"], context)
        context["file_path"] = directory / file_name

        # Get the file writer.
        writers: list[StreamWriter] = [await get_file_writer(context["file_path"])]

        if outgoing:
            # If there is a single outgoing edge, write to that too.
            writers.append(await outgoing[0].send(context))

        while not reader.at_eof():
            chunk = await reader.read(self.max_chunk_size)

            # FIXME: aiofile requires bytes, which forces us into an unnecessary copy
            if not isinstance(chunk, bytes):
                chunk = bytes(chunk)

            for writer in writers:
                writer.write(chunk)

        await asyncio.gather(*(writer.drain() for writer in writers))
        for writer in writers:
            writer.close()

        await asyncio.gather(*(writer.wait_closed() for writer in writers))


class ReadFile(DataOp):
    """DataGraph node that reads a file from disk and passes it as a stream to the next node."""
    op: Literal["read_file"] = "read_file"
    max_chunk_size: int = 2**16
    wait_for_file: bool = False
    wait_for_file_timeout: float = 5.0

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        assert len(outgoing) == 1

        # We expect input data to be empty.
        context, buffer = await incoming[0].receive("buffer")
        assert len(buffer) == 0

        # The path to the file must be present in context.
        # FIXME Formalization of context keywords is pending.
        path = pathlib.Path(context["file_path"])

        # Get the file reader.
        reader = await get_file_reader(
            path,
            self.max_chunk_size,
            wait_until_exists=self.wait_for_file_timeout if self.wait_for_file else None,
        )

        # Send to the graph.
        writer = await outgoing[0].send(context)
        read_count = 0

        while not reader.at_eof():
            chunk = await reader.read(self.max_chunk_size)
            read_count += len(chunk)
            writer.write(chunk)

        logger.debug(f"ReadFile read {read_count//1024} KB from {path}")
        writer.close()
        await writer.wait_closed()


class FileAppearedHandler(FileSystemEventHandler):
    """Watchdog event handler that enqueues new file paths as they appear in a directory."""

    def __init__(
        self,
        queue: asyncio.Queue[pathlib.Path],
        /,
        *,
        match_suffix: str | None = None,
        seen_capacity: int = 10,
    ):
        self._match_suffix = match_suffix
        self._queue = queue
        self._seen: deque[str] = deque(maxlen=seen_capacity)

    def _enqueue_path(self, path_str: str):
        if path_str in self._seen:
            return
        self._seen.append(path_str)

        try:
            path = pathlib.Path(path_str)
            self._queue.put_nowait(path)
        except Exception:
            logger.exception("error putting file path in queue")

    def on_created(self, event: FileCreatedEvent):
        if self._match_suffix and not event.src_path.endswith(self._match_suffix):
            return
        self._enqueue_path(event.src_path)

    def on_moved(self, event: FileMovedEvent):
        if self._match_suffix and not event.dest_path.endswith(self._match_suffix):
            return
        self._enqueue_path(event.dest_path)



async def wait_file_exists(path: pathlib.Path):
    """Suspend until the given path exists, using a filesystem watcher to avoid polling."""
    if not await asyncio.to_thread(os.path.exists, path):
        logger.debug(f"waiting for {path} to exist...")
        queue = asyncio.Queue()
        handler = FileAppearedHandler(queue, match_suffix=path.name)
        observer = Observer()
        observer.schedule(handler, path.parent)
        observer.start()

        try:
            if not await asyncio.to_thread(os.path.exists, path):
                await queue.get()
                queue.task_done()
        finally:
            observer.stop()


async def get_file_reader(
    path: pathlib.Path,
    read_size: int,
    wait_until_exists: float | None = None,
) -> StreamReader:
    """Return a StreamReader that asynchronously feeds file content in the background.

    If *wait_until_exists* is set, waits up to that many seconds for the file to appear.
    """
    if wait_until_exists is not None:
        async with asyncio.timeout(wait_until_exists):
            # Wait until the file exists before opening.
            await wait_file_exists(path)

    ctx = contextlib.AsyncExitStack()
    f = await ctx.enter_async_context(aiofile.async_open(path, "rb"))
    reader = asyncio.StreamReader()

    async def feed_data():
        async with ctx:
            while True:
                chunk = await f.read(read_size)

                if not chunk:
                    break

                reader.feed_data(chunk)

            reader.feed_eof()

    # Start feeding data in the background.
    task = asyncio.create_task(feed_data())
    task.add_done_callback(cleanup_future)
    ctx.callback(lambda: task)  # Prevent task GC until done

    return reader


async def get_file_writer(path: pathlib.Path) -> StreamWriter:
    """Return a StreamWriter that asynchronously writes to a file, renaming from a .tmp path on close."""
    return await FileAsyncWriter.open(path)


class FileAsyncWriter:
    """Write a file asynchronously using the StreamWriter protocol."""

    @classmethod
    async def open(cls, path: pathlib.Path):
        """Open a new FileAsyncWriter that writes to *path* via a temporary .tmp file."""
        temp_path = path.with_suffix(".tmp")
        file = await aiofile.async_open(temp_path, mode="wb")
        writer = FileAsyncWriter(temp_path, file, path)
        writer.start()
        return writer

    def __init__(
        self,
        path: pathlib.Path,
        file: aiofile.FileIOWrapperBase,
        target_path: pathlib.Path | None = None,
    ):
        self._path = path
        self._target_path = target_path
        self._file = file
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._drained = asyncio.Event()
        self._close_requested = False

    def start(self):
        """Start the background write task."""
        self._task = asyncio.create_task(self._write_task())

    async def _write_task(self):
        try:
            while True:
                # Get the next data chunk to write and write it.
                data = await self._queue.get()
                await self._file.write(data)
                self._queue.task_done()

                # Set the drained signal as appropriate.
                if self._queue.empty():
                    self._drained.set()
        except Exception:
            logger.exception("error writing to file")
        finally:
            # When we reach here, either:
            #
            # 1. The task was cancelled due a call to close() -- this is the happy path.
            # 2. The task was cancelled externally, e.g. interpreter shutdown.
            # 3. An error occurred while writing data.
            #
            # In all cases we want to try to close the file (which may be partially written in the
            # error case) and move it to its target path.
            self._close_requested = True

            with contextlib.suppress(asyncio.QueueEmpty):
                while data := self._queue.get_nowait():
                    await self._file.write(data)
                    self._queue.task_done()

            self._queue.shutdown()
            self._drained.set()

            try:
                async with asyncio.timeout(5.0):
                    logger.debug(f"closing file {self._target_path or self._path}")
                    await self._file.close()

                    if self._target_path:
                        await asyncio.to_thread(
                            os.rename,
                            self._path,
                            self._target_path,
                        )
            except BaseException as e:
                final_path = self._target_path or self._path
                logger.warning(f"error finalizing {final_path}: {str(e)}")

    def write(self, data: bytes):
        if self._close_requested:
            raise RuntimeError("writer is closing or closed")

        self._queue.put_nowait(data)
        self._drained.clear()

    async def drain(self):
        await self._drained.wait()

    def close(self):
        self._close_requested = True

        # We leverage task cancellation to trigger closing and finalization of the output file.
        self._task.cancel()

    def is_closing(self):
        return self._close_requested and not self._task.done()

    async def wait_closed(self):
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
