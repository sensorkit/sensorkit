# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from collections.abc import Buffer
from typing import Protocol, runtime_checkable

type Buf = bytes | bytearray | memoryview


@runtime_checkable
class StreamReader(Protocol):
    """A variant of the asyncio.StreamReader interface."""

    def exception(self) -> Exception: ...
    def at_eof(self) -> bool: ...
    async def readline(self) -> Buf: ...
    async def readuntil(self, separator=b"\n") -> Buf: ...
    async def read(self, n=-1) -> Buf: ...
    async def readexactly(self, n: int) -> Buf: ...

    async def __aiter__(self):
        while not self.at_eof():
            yield await self.read()


@runtime_checkable
class StreamWriter(Protocol):
    """A variant of the asyncio.StreamWriter interface."""

    def write(self, data: bytes): ...
    def close(self): ...
    def is_closing(self) -> bool: ...
    async def wait_closed(self): ...
    async def drain(self): ...


class BufferReader(StreamReader):
    """A StreamReader that reads from an immutable input buffer."""

    def __init__(self, data: Buffer):
        self.data: Buf = memoryview(data)
        self.ptr = 0
        self._exception = None

    def exception(self) -> Exception:
        return self._exception

    def at_eof(self) -> bool:
        return self.ptr >= len(self.data)

    async def readline(self) -> Buf:
        return await self.readuntil()

    async def readuntil(self, separator=b"\n") -> Buf:
        try:
            start = self.ptr
            sep_len = len(separator)

            # Find the separator in the buffer
            for i in range(start, len(self.data) - sep_len + 1):
                if self.data[i:i+sep_len] == separator:
                    self.ptr = i + sep_len
                    return self.data[start:self.ptr]

            # Separator not found, return the rest of the buffer
            self.ptr = len(self.data)
            return self.data[start:]
        except Exception as e:
            self._exception = e
            raise

    async def read(self, n=-1) -> Buf:
        try:
            start = self.ptr
            end = min(start + n, len(self.data)) if n >= 0 else len(self.data)
            self.ptr = end
            return self.data[start:end]
        except Exception as e:
            self._exception = e
            raise

    async def readexactly(self, n: int) -> Buf:
        buf = await self.read(n)

        if len(buf) != n:
            raise asyncio.IncompleteReadError(bytes(buf), n)

        return buf


class BufferWriter(StreamWriter):
    """A StreamWriter that writes to a buffer."""

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None):
        loop = loop or asyncio.get_event_loop()
        self._future: asyncio.Future[Buffer] = loop.create_future()
        self._bytes = bytearray()

    def write(self, data: bytes):
        self._bytes.extend(data)

    def write_eof(self):
        # No eof.
        pass

    def close(self):
        self._future.set_result(self._bytes)

    def is_closing(self) -> bool:
        return self._future.done()

    async def wait_closed(self):
        await self._future

    async def drain(self):
        # Writes are immediate.
        pass

    def get_future(self):
        """Return a Future that resolves to the accumulated buffer once the writer is closed."""
        return self._future


class QueueReader(StreamReader):
    """A StreamReader that reads from a queue with minimal copying."""

    def __init__(self, queue: asyncio.Queue[bytes | None]):
        self._queue = queue
        self._buffer = bytearray()  # Only used for partial chunks
        self._eof = False
        self._exception: Exception | None = None

    def exception(self) -> Exception:
        return self._exception

    def at_eof(self) -> bool:
        return self._eof and len(self._buffer) == 0

    async def _get_next_chunk(self):
        """Get next chunk from queue or return None on EOF."""
        if self._eof:
            return None

        try:
            chunk = await self._queue.get()
            self._queue.task_done()

            # Check for end of stream marker.
            if chunk is None:
                self._eof = True
                return None

            return chunk
        except Exception as e:
            self._exception = e
            raise

    async def read(self, n=-1) -> Buf:
        if n == 0:
            return b""

        # If we have data in the buffer, handle that first
        if self._buffer:
            if n == -1:
                n = len(self._buffer)

            if len(self._buffer) <= n:
                remainder = bytearray()
            else:
                remainder = self._buffer[n:]
                del self._buffer[n:]

            result = self._buffer
            self._buffer = remainder
            return result

        # No buffer data, read directly from queue
        chunk = await self._get_next_chunk()

        if chunk is None:
            return b""

        if n == -1:
            n = len(chunk)

        if len(chunk) <= n:
            return chunk

        # Store excess in buffer
        view = memoryview(chunk)
        self._buffer = bytearray(view[n:])
        return view[:n]

    async def readline(self) -> Buf:
        return await self.readuntil()

    async def readuntil(self, separator=b"\n") -> Buf:
        idx = 0

        while True:
            # Check whether the newest part of the current buffer has the needle.
            try:
                idx = self._buffer.index(separator, idx) + len(separator)
                remainder = self._buffer[idx:]
                del self._buffer[idx:]
                result = self._buffer
                self._buffer = remainder
                return result
            except ValueError:
                idx = len(self._buffer) - len(separator) + 1

            # Read the next chunk from the queue.
            chunk = await self._get_next_chunk()

            if chunk is None:
                result = self._buffer
                self._buffer = bytearray()
                return result

            self._buffer.extend(chunk)

    async def readexactly(self, n: int) -> Buf:
        while len(self._buffer) < n:
            chunk = await self._get_next_chunk()

            if chunk is None:
                partial = bytes(self._buffer)
                self._buffer = bytearray()
                raise asyncio.IncompleteReadError(partial, n)

            self._buffer.extend(chunk)

        remainder = self._buffer[n:]
        del self._buffer[n:]
        result = self._buffer
        self._buffer = remainder
        return result


class QueueWriter(StreamWriter):
    """A StreamWriter that writes to a queue."""

    def __init__(self, queue: asyncio.Queue[bytes | None]):
        self._queue = queue
        self._closed = False

    def write(self, data: bytes):
        if self._closed:
            raise RuntimeError("Writer is closed")
        self._queue.put_nowait(data)

    def close(self):
        if not self._closed:
            # Signal end of stream.
            self._queue.put_nowait(None)
            self._closed = True

    def is_closing(self) -> bool:
        return self._closed

    async def wait_closed(self):
        if self._closed:
            await self._queue.join()

    async def drain(self):
        # Writes are immediate.
        pass


def create_connected_streams() -> tuple[StreamReader, StreamWriter]:
    """Return a pair of streams that are connected by a queue."""
    queue = asyncio.Queue()
    return QueueReader(queue), QueueWriter(queue)
