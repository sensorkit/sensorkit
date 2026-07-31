# SPDX-License-Identifier: Apache-2.0
import asyncio
import pathlib
import tempfile

import aiofile
import pytest
from loguru import logger

from sensorkit.data.context import Context
from sensorkit.data.filesys import (
    FileInfo,
    FileNameTemplate,
    ReadFile,
    WatchDirectory,
    WriteFile,
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
        graph.add("read", ReadFile(settle_seconds=0.1, output=["sink"]))

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
