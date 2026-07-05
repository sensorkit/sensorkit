# SPDX-License-Identifier: Apache-2.0
import asyncio
import contextlib
import os
import pathlib
from collections.abc import AsyncGenerator
from fnmatch import fnmatch
from typing import Literal

import aiofile
from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.common.aio import cleanup_future
from sensorkit.common.filewatch import FileEventKind, watch_dir, wait_for_file
from sensorkit.data.graph import Context, DataFlow, DataOp, SourceOp
from sensorkit.data.streams import StreamReader, StreamWriter


@sk.declare_keyword
class FileNameTemplate(BaseModel):
    """Template for file naming."""
    template: str


@sk.declare_keyword
class FileInfo(BaseModel):
    """Information about a file on disk."""
    path: pathlib.Path
    size: int | None = None
    create_time: float | None = None
    access_time: float | None = None
    change_time: float | None = None

    @classmethod
    async def from_path(cls, path: pathlib.Path, **other):
        """Create a FileInfo from a local path.

        Populates: path, size, create_time, access_time, change_time.
        Additional fields can be provided via keyword arguments.
        """
        stat = await asyncio.to_thread(path.stat)

        return cls.model_construct(
            path=path,
            size=stat.st_size,
            create_time=stat.st_ctime,
            access_time=stat.st_atime,
            change_time=stat.st_mtime,
            **other,
        )


class WatchDirectory(SourceOp):
    """DataGraph source that triggers a run for each new file in a directory.

    Watches a directory with a filesystem observer and, for every newly appeared file
    whose name matches, starts one graph run seeded with a fresh Context describing that
    file.

    Attributes:
        directory: Path of the directory to watch. Created if it does not exist.
        match: Glob pattern a filename must match to trigger a run.
        recursive: Whether to watch subdirectories as well.

    Supplies context:
        FileInfo: Describes the appeared file, with path and stat metadata populated
            from disk.
    """
    op: Literal["watch_directory"] = "watch_directory"
    directory: str
    match: str = "*"
    recursive: bool = False

    async def graph_source(self) -> AsyncGenerator[None, DataFlow]:
        logger.debug(f"starting WatchDirectory source at {self.directory}")
        await asyncio.to_thread(pathlib.Path(self.directory).mkdir, parents=True, exist_ok=True)

        events = await watch_dir(
            self.directory,
            recursive=self.recursive,
            kinds=(FileEventKind.CREATED, FileEventKind.MOVED),
        )

        try:
            while True:
                # Wait for the graph to be ready.
                edge = yield

                # Wait for a matching file to appear.
                async for event in events:
                    if fnmatch(event.path.name, self.match):
                        path = event.path
                        break

                logger.debug(f"file {path} appeared")

                try:
                    await edge.send(
                        Context(await FileInfo.from_path(path)),
                        b"",
                    )
                except Exception:
                    logger.exception("error sending file path to graph")
        finally:
            await events.aclose()


class WriteFile(DataOp):
    """DataGraph node that writes incoming stream data to a file on disk.

    If an outgoing edge is connected, the data is forwarded to it as well.

    Attributes:
        directory: Base output directory, resolved against context (e.g.
            "{program_name}"). When None, relative paths and templates resolve under the
            current working directory.
        max_chunk_size: Maximum number of bytes read from the stream per chunk.

    Expects context:
        FileInfo (optional): An explicit or pre-resolved output path. A relative path is
            taken under `directory`; an absolute path is used as-is, and is an error if
            `directory` is also set.
        FileNameTemplate (optional): A naming template resolved into a name under
            `directory`, used only when no FileInfo is present.

        One of FileInfo or FileNameTemplate must be present.

    Supplies context:
        FileInfo: Created from FileNameTemplate when no FileInfo was present.

    Mutates context:
        FileInfo: Path rewritten to the resolved output location.

    Raises:
        ValueError: If neither FileInfo nor FileNameTemplate is present, or if
            the resolved path is absolute while `directory` is also set.
    """
    op: Literal["write_file"] = "write_file"
    directory: str | None = None
    max_chunk_size: int = 2**16

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        assert len(outgoing) <= 1

        # Read incoming data as a stream.
        context, reader = await incoming[0].receive("stream")

        # Resolve the configured base directory against context values (e.g. {program_name}).
        base = (
            pathlib.Path(context.resolve(self.directory, as_type=str))
            if self.directory is not None
            else pathlib.Path.cwd()
        )
        info = context.get(FileInfo)

        if info is None:
            # No FileInfo, so look for a FileNameTemplate to resolve the name.
            template = context.get(FileNameTemplate)

            if template is None:
                raise ValueError("WriteFile requires a FileInfo or FileNameTemplate in context")

            info = FileInfo(path=pathlib.Path(context.resolve(template.template, as_type=str)))
            context.set(info)

        # Make sure we don't have two absolute paths.
        if info.path.is_absolute():
            if self.directory is not None:
                raise ValueError(
                    f"ambiguous write location: FileInfo.path is absolute ({info.path}) "
                    f"but WriteFile.directory is also set ({self.directory!r})"
                )
        else:
            # Update the path, applying our base directory.
            info.path = base / info.path

        # Ensure the output directory exists before opening the writer.
        await asyncio.to_thread(info.path.parent.mkdir, parents=True, exist_ok=True)

        # Get the file writer.
        writers: list[StreamWriter] = [await get_file_writer(info.path)]

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
    """DataGraph node that reads a file from disk into a downstream stream.

    Opens the file named by FileInfo.path and streams its contents to the single
    outgoing edge. The incoming data is expected to be empty.

    Attributes:
        max_chunk_size: Maximum number of bytes read from the file per chunk.
        wait_for_file: Whether to wait for the file to appear before reading.
        wait_for_file_timeout: Seconds to wait for the file when `wait_for_file` is set.

    Expects context:
        FileInfo: Provides path, the location of the file to read.

    Mutates context:
        FileInfo: Replaced with stat metadata (size and timestamps) read from disk when
            FileInfo.size was not already populated.
    """
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
        info = context[FileInfo]

        # Get the file reader.
        reader = await get_file_reader(
            info.path,
            self.max_chunk_size,
            wait_until_exists=self.wait_for_file_timeout if self.wait_for_file else None,
        )

        # Populate stat metadata if it has not been determined yet. The file is known to
        # exist at this point, since get_file_reader has opened (or waited for) it.
        if info.size is None:
            context.set(await FileInfo.from_path(info.path))

        # Send to the graph.
        writer = await outgoing[0].send(context)
        read_count = 0

        while not reader.at_eof():
            chunk = await reader.read(self.max_chunk_size)
            read_count += len(chunk)
            writer.write(chunk)

        logger.debug(f"ReadFile read {read_count//1024} KB from {info.path}")
        writer.close()
        await writer.wait_closed()


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
            await wait_for_file(path)

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
