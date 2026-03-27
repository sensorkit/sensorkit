import pytest

from sensorkit.data.streams import BufferReader, BufferWriter, create_connected_streams


@pytest.mark.asyncio
async def test_buffer_reader():
    # Create test data
    test_data = bytearray(b"Line 1\nLine 2\nLine 3")

    # Create a BufferReader
    reader = BufferReader(test_data)

    # Test read method
    data = await reader.read(5)
    assert data == b"Line "

    # Test readline method
    line = await reader.readline()
    assert line == b"1\n"

    # Test readuntil method
    until = await reader.readuntil(b" ")
    assert until == b"Line "

    # Test readexactly method
    exact = await reader.readexactly(1)
    assert exact == b"2"

    # Test read to end
    rest = await reader.read()
    assert rest == b"\nLine 3"

    # Test at_eof
    assert reader.at_eof()

    # Test read after EOF
    empty = await reader.read()
    assert empty == b""


@pytest.mark.asyncio
async def test_buffer_writer():
    # Create a BufferStreamWriter
    writer = BufferWriter()

    # Write some data to it
    writer.write(b"Hello, ")
    writer.write(b"World!")

    # Write EOF and close
    writer.write_eof()
    writer.close()

    # Wait for the writer to be closed
    await writer.wait_closed()

    # Get the buffer contents
    fut = writer.get_future()
    await fut
    buffer = fut.result()

    # Print the buffer contents
    print(f"Buffer contents: {buffer}")

    # Verify the contents
    assert buffer == b"Hello, World!"
    print("Test passed!")


@pytest.mark.asyncio
async def test_connected_streams():
    # Create connected reader and writer
    reader, writer = create_connected_streams()

    # Test data
    test_data = b"Hello, connected streams!"

    # Write data to the writer
    writer.write(test_data)

    # Read data from the reader
    data = await reader.read()
    assert data is test_data

    # Test writing multiple chunks
    writer.write(b"Chunk 1 ")
    writer.write(b"Chunk 2 ")
    writer.write(b"Chunk 3")
    writer.close()

    # Read all chunks
    chunk1 = await reader.read(8)  # Read "Chunk 1 "
    assert chunk1 == b"Chunk 1 "

    chunk2 = await reader.readuntil(b"2")  # Read "Chunk 2"
    assert chunk2 == b"Chunk 2"

    chunk3 = await reader.readexactly(8)  # Read the rest
    assert chunk3 == b" Chunk 3"

    # After closing, reader should eventually reach EOF
    await reader.read()
    assert reader.at_eof()

    # Verify writer is closed
    assert writer.is_closing()
    await writer.wait_closed()


