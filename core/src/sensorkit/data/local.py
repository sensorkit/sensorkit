# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from collections.abc import Buffer
from typing import Any, AsyncGenerator, Literal

from loguru import logger

from sensorkit.data.context import Context
from sensorkit.data.graph import DataFlow, SinkOp, SourceOp
from sensorkit.data.streams import StreamReader, StreamWriter, create_connected_streams


class AppSource(SourceOp):
    """Source node for a DataGraph."""
    op: Literal["app_source"] = "app_source"

    def __init__(self, /, **data: Any):
        super().__init__(**data)
        self._queue: asyncio.Queue[tuple[Context, Buffer | StreamReader]] = asyncio.Queue()

    def produce(
        self,
        context: Context | None = None,
        obj: Buffer | StreamReader | None = None,
    ) -> StreamWriter | None:
        """Enqueue a graph run.

        If *obj* is omitted, a connected StreamWriter is returned for the caller to write into.
        """
        result = None

        if obj is None:
            obj, result = create_connected_streams()

        self._queue.put_nowait((context or Context(), obj))
        return result

    async def graph_source(self) -> AsyncGenerator[None, DataFlow]:
        while True:
            edge = yield
            context, obj = await self._queue.get()

            try:
                logger.debug(f"AppSource executing DataGraph with {context=}")
                await edge.send(context, obj)
            finally:
                self._queue.task_done()


class AppSink(SinkOp):
    """Sink node for a DataGraph."""
    op: Literal["app_sink"] = "app_sink"

    def __init__(self, /, **data: Any):
        super().__init__(**data)
        self._queue: asyncio.Queue[tuple[Context, Buffer]] = asyncio.Queue()

    async def consume(self):
        """Async generator that yields `(context, buffer)` tuples as graph runs complete."""
        while True:
            record = await self._queue.get()
            self._queue.task_done()
            yield record

    async def graph_sink(self, incoming: DataFlow):
        record = await incoming.receive("buffer")
        await self._queue.put(record)
        logger.debug(f"AppSink consumed DataGraph with context={record[0]}")
