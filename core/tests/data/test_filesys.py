# SPDX-License-Identifier: Apache-2.0
import asyncio
import pathlib
import tempfile

import aiofile
import pytest
from loguru import logger

from sensorkit.data.context import Context
from sensorkit.data.filesys import (
    FileAsyncWriter,
    FileInfo,
    FileNameTemplate,
    ReadFile,
    WatchDirectory,
    WriteFile,
    get_file_writer,
)
from sensorkit.data.graph import DataFlow, DataGraph
from sensorkit.data.local import AppSink


@pytest.mark.asyncio
async def test_write_file():
    # Create a temporary directory for the test
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a WriteFile node
        write_file = WriteFile(directory=temp_dir)

        # Create DataEdge objects for incoming and outgoing connections
        incoming_edge = DataFlow()
        outgoing_edge = DataFlow()

        # Start the process task
        process_task = asyncio.create_task(write_file.process([incoming_edge], [outgoing_edge]))

        # Create test data and context
        test_data = b"Hello, WriteFile test!"
        context = Context()
        context.set(FileNameTemplate(template="test_file.txt"))

        # Start a task to receive from the outgoing edge
        async def receiver():
            out_context, buffer = await outgoing_edge.receive("buffer")
            logger.debug(f"receiver function received {buffer=}")
            # Verify context is passed through
            assert out_context[FileInfo].path is not None
            assert out_context[FileInfo].path.name == "test_file.txt"
            logger.debug(f"{out_context=}")
            # Verify data is passed through
            assert buffer == test_data

        receiver_task = asyncio.create_task(receiver())

        # Send data through the incoming edge
        writer = await incoming_edge.send(context)
        writer.write(test_data)
        writer.close()

        # Wait for the receiver to finish
        await receiver_task

        # Wait for the process task to finish
        await process_task

        # # Verify the file was created with the correct content
        # file_path = pathlib.Path(temp_dir) / "test_file.txt"
        # assert os.path.exists(file_path)
        # with open(file_path, "rb") as f:
        #     content = f.read()
        #     assert content == test_data


@pytest.mark.asyncio
async def test_write_file_no_outgoing():
    # Test WriteFile without an outgoing edge
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a WriteFile node
        write_file = WriteFile(directory=temp_dir)

        # Create DataEdge for incoming connection only
        incoming_edge = DataFlow()

        # Start the process task with empty outgoing list
        process_task = asyncio.create_task(write_file.process([incoming_edge], []))

        # Create test data and context
        test_data = b"Hello, WriteFile without outgoing edge!"
        context = Context()
        context.set(FileNameTemplate(template="test_file_no_outgoing.txt"))

        # Send data through the incoming edge
        writer = await incoming_edge.send(context)
        writer.write(test_data)
        writer.close()

        # Wait for the process task to finish
        await process_task

        # Verify the file was created with the correct content
        file_path = pathlib.Path(temp_dir) / "test_file_no_outgoing.txt"
        assert await asyncio.to_thread(file_path.exists)

        async with aiofile.async_open(file_path, "rb") as f:
            content = await f.read()
            assert content == test_data


@pytest.mark.asyncio
async def test_write_file_dynamic_directory():
    with tempfile.TemporaryDirectory() as temp_dir:
        write_file = WriteFile(directory=f"{temp_dir}/{{program_name}}")

        incoming_edge = DataFlow()
        process_task = asyncio.create_task(write_file.process([incoming_edge], []))

        test_data = b"Dynamic directory test data"
        context = Context()
        context.set(FileNameTemplate(template="image.fits"))
        context.set_value("program_name", "survey_north")

        writer = await incoming_edge.send(context)
        writer.write(test_data)
        writer.close()

        await process_task

        # Verify the subdirectory was created and file written there.
        expected = pathlib.Path(temp_dir) / "survey_north" / "image.fits"
        assert await asyncio.to_thread(expected.exists)

        async with aiofile.async_open(expected, "rb") as f:
            assert await f.read() == test_data

        assert context[FileInfo].path == expected


@pytest.mark.asyncio
async def test_read_file():
    # Create a temporary directory and file with test content
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test data and file
        test_data = b"Hello, ReadFile test!"
        file_name = "test_input.txt"
        file_path = pathlib.Path(temp_dir) / file_name

        # Write test data to the file
        async with aiofile.async_open(file_path, "wb") as f:
            await f.write(test_data)

        # Create a ReadFile node
        read_file = ReadFile(wait_for_file=False)

        # Create DataEdge objects for incoming and outgoing connections
        incoming_edge = DataFlow()
        outgoing_edge = DataFlow()

        # Start the process task
        process_task = asyncio.create_task(read_file.process([incoming_edge], [outgoing_edge]))

        # Create context with the file path
        context = Context()
        context.set(FileInfo(path=file_path))

        # Start a task to receive from the outgoing edge
        async def receiver():
            logger.debug("started receiver task")
            out_context, reader = await outgoing_edge.receive("stream")
            # Verify context is passed through
            assert out_context[FileInfo].path == file_path

            # Read all data from the stream
            data = bytearray()
            while not reader.at_eof():
                chunk = await reader.read()
                data.extend(chunk)

            # Verify the data matches the original test data
            assert bytes(data) == test_data

        receiver_task = asyncio.create_task(receiver())
        print(f"sending context: {context}")

        async with asyncio.timeout(1):
            await asyncio.gather(
                incoming_edge.send(context, b""),
                receiver_task,
                process_task,
            )


@pytest.mark.asyncio
async def test_read_file_wait_exists():
    # Test ReadFile with a file that doesn't exist yet
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test data
        test_data = b"Hello, ReadFile wait_file_exists test!"
        file_name = "test_delayed.txt"
        file_path = pathlib.Path(temp_dir) / file_name

        # wait_for_file polls on a 1s cadence, so the timeout must leave room for
        # at least one poll after the file appears.
        read_file = ReadFile(wait_for_file=True, wait_for_file_timeout=5.0)

        # Create DataEdge objects for incoming and outgoing connections
        incoming_edge = DataFlow()
        outgoing_edge = DataFlow()

        # Start the process task
        process_task = asyncio.create_task(read_file.process([incoming_edge], [outgoing_edge]))

        # Create context with the file path that doesn't exist yet
        context = Context()
        context.set(FileInfo(path=file_path))

        # Start a task to receive from the outgoing edge
        async def receiver():
            out_context, reader = await outgoing_edge.receive("stream")
            # Verify context is passed through
            assert out_context[FileInfo].path == file_path

            # Read all data from the stream
            data = bytearray()
            while not reader.at_eof():
                chunk = await reader.read()
                data.extend(chunk)

            # Verify the data matches the original test data
            assert bytes(data) == test_data

        receiver_task = asyncio.create_task(receiver())

        # Send empty buffer with context through the incoming edge
        await incoming_edge.send(context, b"")

        # Create the file after a short delay
        async def create_delayed_file():
            await asyncio.sleep(0.5)  # Short delay
            async with aiofile.async_open(file_path, "wb") as f:
                await f.write(test_data)

        create_file_task = asyncio.create_task(create_delayed_file())

        # Wait for all tasks to finish
        await asyncio.gather(create_file_task, receiver_task, process_task)


@pytest.mark.asyncio
async def test_watch_directory():
    """A file appearing starts one run naming it.

    Detection is all this source does, so the file needs no contents: it names the file
    and leaves stat metadata to whoever reads it.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        graph = DataGraph()
        graph.add("source", WatchDirectory(directory=temp_dir, output=["sink"]))

        sink = AppSink()
        graph.add("sink", sink)
        graph.start()

        test_file = pathlib.Path(temp_dir) / "test_watch.txt"

        await asyncio.sleep(0.5)
        await asyncio.to_thread(test_file.touch)

        async with asyncio.timeout(5.0):
            async for context, _ in sink.consume():
                info = context[FileInfo]
                assert info.path.name == "test_watch.txt"
                assert info.size is None
                break

            await graph.stop()


@pytest.mark.asyncio
async def test_read_file_waits_for_slow_writer():
    """A file still being written when it is detected is read whole, not truncated.

    The directory watch reports the file at creation, when it is still empty. ReadFile
    holds it until the writer stops extending it, and only then stats it, so the
    FileInfo it publishes describes the finished file.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        graph = DataGraph()
        graph.add("source", WatchDirectory(directory=temp_dir, output=["read"]))
        graph.add("read", ReadFile(settle_seconds=0.3, output=["sink"]))

        sink = AppSink()
        graph.add("sink", sink)
        graph.start()

        chunks = [b"chunk-%02d;" % i for i in range(10)]
        test_file = pathlib.Path(temp_dir) / "slow_write.txt"

        await asyncio.sleep(0.5)

        async def write_slowly():
            async with aiofile.async_open(test_file, "wb") as f:
                for chunk in chunks:
                    await f.write(chunk)
                    await f.file.fsync()
                    await asyncio.sleep(0.05)

        writer = asyncio.create_task(write_slowly())

        try:
            async with asyncio.timeout(10.0):
                async for context, data in sink.consume():
                    assert context[FileInfo].path.name == "slow_write.txt"
                    assert bytes(data) == b"".join(chunks)
                    assert context[FileInfo].size == len(b"".join(chunks))
                    break

                await graph.stop()
        finally:
            await writer


async def published_contents(path: pathlib.Path) -> bytes:
    """Return the contents of a published file, checking its temporary file is gone."""
    assert not await asyncio.to_thread(path.with_suffix(".tmp").exists)
    return await asyncio.to_thread(path.read_bytes)


@pytest.mark.asyncio
async def test_file_writer_immediate_close(tmp_path):
    """Closing a writer that never received data publishes an empty file."""
    path = tmp_path / "empty.bin"
    writer = await get_file_writer(path)
    writer.close()

    async with asyncio.timeout(5.0):
        await writer.wait_closed()

    assert writer.is_closing()
    assert await published_contents(path) == b""


@pytest.mark.asyncio
async def test_file_writer_close_with_queued_data(tmp_path):
    """Data queued and closed before the write task first runs is written and published."""
    path = tmp_path / "queued.bin"
    chunks = [b"chunk-%02d;" % i for i in range(10)]

    writer = await get_file_writer(path)

    for chunk in chunks:
        writer.write(chunk)

    writer.close()

    with pytest.raises(RuntimeError):
        writer.write(b"late")

    async with asyncio.timeout(5.0):
        await writer.wait_closed()

    assert await published_contents(path) == b"".join(chunks)


@pytest.mark.asyncio
async def test_file_writer_empty_drain(tmp_path):
    """Draining a writer with nothing queued returns without waiting for data."""
    path = tmp_path / "drain.bin"
    writer = await get_file_writer(path)

    async with asyncio.timeout(1.0):
        await writer.drain()

    writer.write(b"after drain")
    writer.close()

    async with asyncio.timeout(5.0):
        await writer.wait_closed()

    assert await published_contents(path) == b"after drain"


@pytest.mark.asyncio
async def test_file_writer_drain_between_writes(tmp_path):
    """Writes separated by drains land in order, and the file is published on close."""
    path = tmp_path / "ordinary.bin"
    writer = await get_file_writer(path)

    async with asyncio.timeout(5.0):
        writer.write(b"first;")
        await writer.drain()
        assert not writer.is_closing()

        writer.write(b"second;")
        writer.write(b"third;")
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    assert await published_contents(path) == b"first;second;third;"


@pytest.mark.asyncio
async def test_file_writer_cancelled_wait_closed(tmp_path):
    """Canceling wait_closed leaves queued writes and file publication running."""
    path = tmp_path / "cancelled.bin"
    chunks = [b"chunk-%02d;" % i for i in range(10)]

    writer = await get_file_writer(path)

    for chunk in chunks:
        writer.write(chunk)

    writer.close()

    waiter = asyncio.create_task(writer.wait_closed())
    await asyncio.sleep(0)
    waiter.cancel()

    with pytest.raises(asyncio.CancelledError):
        await waiter

    async with asyncio.timeout(5.0):
        await writer.wait_closed()

    assert await published_contents(path) == b"".join(chunks)


@pytest.mark.asyncio
async def test_file_writer_rename_failure(tmp_path):
    """A rename that cannot publish the file is raised from wait_closed."""
    path = tmp_path / "taken.bin"
    await asyncio.to_thread(path.mkdir)

    writer = await get_file_writer(path)
    writer.write(b"data")
    writer.close()

    async with asyncio.timeout(5.0):
        with pytest.raises(OSError):
            await writer.wait_closed()

    assert await asyncio.to_thread(path.is_dir)
    assert await asyncio.to_thread(path.with_suffix(".tmp").read_bytes) == b"data"


class FailingFile:
    """An aiofile handle whose writes fail."""

    def __init__(self):
        self.closed = False

    async def write(self, data: bytes):
        raise OSError("disk full")

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_file_writer_write_failure(tmp_path):
    """Write errors reach wait_closed after releasing the writer."""
    file = FailingFile()
    writer = FileAsyncWriter(tmp_path / "failing.bin", file)
    writer.start()

    writer.write(b"first")
    writer.write(b"second")

    async with asyncio.timeout(5.0):
        await writer.drain()

        with pytest.raises(OSError, match="disk full"):
            await writer.wait_closed()

    assert file.closed
    assert writer.is_closing()

    with pytest.raises(RuntimeError):
        writer.write(b"late")
