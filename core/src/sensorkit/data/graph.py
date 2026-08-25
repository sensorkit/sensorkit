# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
import contextlib
import io
import weakref
from abc import ABC, abstractmethod
from collections.abc import Buffer
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    ClassVar,
    Literal,
    Self,
    Unpack,
    cast,
    final,
    overload,
)

from loguru import logger
from pydantic import BaseModel, Field, model_validator

from sensorkit.common.model import ModelRegistry, RegistryBaseModel
from sensorkit.data.context import Context
from sensorkit.data.streams import (
    BufferReader,
    BufferWriter,
    StreamReader,
    StreamWriter,
    create_connected_streams,
)


class DataGraphError(Exception): ...
class DataGraphCycleError(DataGraphError): ...
class DataGraphSourceError(DataGraphError): ...


type ReceiveKind = Literal["stream", "buffer"] | None


class DataFlow:
    """An edge in a DataGraph."""

    def __init__(self):
        self.send_called = False
        self.send_ready = asyncio.Event()
        self.receive_ready = asyncio.Event()
        self.receive_kind: ReceiveKind = None
        self.receive_result: tuple[Context, Buffer | StreamReader] | None = None

    @overload
    async def send(
        self,
        context: Context,
        arg: StreamReader,
    ): ...

    @overload
    async def send(
        self,
        context: Context,
        arg: Buffer,
    ): ...

    @overload
    async def send(
        self,
        context: Context,
    ) -> StreamWriter: ...

    async def send(
        self,
        context: Context,
        arg: Buffer | StreamReader | None = None,
    ) -> StreamWriter | None:
        """Send data through this edge to the consumer.

        Pass a Buffer or StreamReader directly, or omit *arg* to receive a StreamWriter that
        the caller can write into.  May only be called once per edge instance.
        """
        if self.send_called:
            raise RuntimeError("send may only be called once")

        self.send_called = True

        # Wait for the consumer to call the `receive` method.
        await self.receive_ready.wait()

        # Implement each of the data passing combinations based on the type of the input argument
        # and the requested receive kind. Note that a `None` receive kind means that the argument
        # type dictates the effective receive kind, enabling "passthrough" semantics.
        match (arg, self.receive_kind):
            # When a buffer is given, we either pass it straight through to the consumer by
            # reference or we return a StreamReader backed by it. In the former case, ownership
            # of the buffer is transferred to the consumer. In the latter case, the buffer must
            # be immutable.
            case (Buffer(), "buffer" | None):
                # Buffer to buffer. We do this by reference to avoid a copy.
                self._send_ready(context, arg)
            case (Buffer(), "stream"):
                # Buffer to stream. Return a StreamReader backed by the input buffer.
                self._send_ready(context, BufferReader(arg))

            # When a StreamReader is given, we either drain it immediately (buffer case) or pass
            # it straight through to the consumer.
            case (StreamReader(), "buffer"):
                bio = io.BytesIO()

                async for chunk in arg:
                    # FIXME: event loop starvation possible here
                    bio.write(chunk)

                self._send_ready(context, bio.getvalue())
            case (StreamReader(), "stream" | None):
                self._send_ready(context, arg)

            # When no argument is supplied, we return a StreamWriter that writes data to the
            # requested destination.
            case (None, "buffer"):
                # Stream to buffer. Return a StreamWriter that will fill a buffer.
                writer = BufferWriter()
                fut = writer.get_future()
                fut.add_done_callback(lambda _: self._send_ready(context, fut.result()))
                return writer
            case (None, "stream" | None):
                # Stream to stream.
                reader, writer = create_connected_streams()
                self._send_ready(context, reader)
                return writer
            case _:
                raise TypeError()

        return None

    def _send_ready(self, *result: Unpack[tuple[Context, Buffer | StreamReader]]):
        # Signal the consumer that the receive result is ready.
        self.receive_result = result
        self.send_ready.set()

    @overload
    async def receive(
        self,
        kind: Literal["stream"],
    ) -> tuple[Context, StreamReader]: ...

    @overload
    async def receive(
        self,
        kind: Literal["buffer"],
    ) -> tuple[Context, Buffer]: ...

    @overload
    async def receive(
        self,
    ) -> tuple[Context, Buffer | StreamReader]: ...

    async def receive(
        self,
        kind: ReceiveKind | None = None,
    ) -> tuple[Context, StreamReader | Buffer]:
        """Wait for the producer to send data and return `(context, data)`.

        *kind* controls whether data is delivered as a buffer or stream; `None` defers to
        the producer's choice.  May only be called once per edge instance.
        """
        if self.receive_ready.is_set():
            raise RuntimeError("receive may only be called once")

        # Signal that we are ready to receive so the producer can proceed.
        self.receive_kind = kind
        self.receive_ready.set()

        # Wait for the producer to provide a result for us and return it.
        await self.send_ready.wait()
        return self.receive_result


class DataOp(RegistryBaseModel, ABC):
    """A node in a DataGraph."""
    op: Literal[None] | str = None
    output: list[str] = Field(default_factory=list)
    registry: ClassVar[ModelRegistry[DataOp]] = ModelRegistry(discriminator="op")

    @classmethod
    def model_registry(cls):
        return cls.registry

    @abstractmethod
    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        """Execute this node, consuming *incoming* edges and producing *outgoing* edges."""
        ...


class SourceOp(DataOp, ABC):
    """A DataGraph node that triggers graph runs."""

    @abstractmethod
    def graph_source(self) -> AsyncGenerator[None, DataFlow]:
        """Async generator that triggers each graph run by sending a DataFlow edge."""
        ...

    @final
    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        assert len(outgoing) == 1
        context, data = await incoming[0].receive()
        await outgoing[0].send(context, data)


class SinkOp(DataOp, ABC):
    """A DataGraph node that consumes graph runs."""

    @abstractmethod
    async def graph_sink(self, incoming: DataFlow):
        """Consume the final DataFlow edge to complete a graph run."""
        ...

    @final
    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        assert len(incoming) == 1 and not outgoing
        await self.graph_sink(incoming[0])


def _parse_simple(simple: list[dict[str, Any]]):
    """Parse a simple representation of a linear DataGraph."""
    counts = collections.Counter()
    nodes = {}
    prev_id = None

    for node in simple:
        if "output" in node:
            raise ValueError("Simple DataGraph representations cannot contain 'output' fields")

        op = node["op"]
        counts[op] += 1
        node_id = f"{op}_{counts[op]}"

        if prev_id:
            nodes[prev_id]["output"] = [node_id]

        nodes[node_id] = dict(node)
        prev_id = node_id

    return nodes


def _describe_simple_form(schema: dict[str, Any]):
    """Declare the shorthand accepted in place of a node mapping.

    Args:
        schema: The generated JSON Schema for the model.
    """
    schema["properties"]["simple"] = {
        "type": "array",
        "items": {"type": "object"},
        "description": "Operations to run in order, in place of 'nodes'.",
    }


class DataGraph(BaseModel, json_schema_extra=_describe_simple_form):
    """A graph of operations on data."""

    nodes: dict[str, DataOp] = Field(default_factory=dict)

    @model_validator(mode="before")
    def _validate(cls, values):
        if simple := values.get("simple"):
            if "nodes" in values:
                raise ValueError("Only one of 'nodes' or 'simple' can be defined")

            del values["simple"]
            values["nodes"] = dict(_parse_simple(simple))

        return values

    def __init__(self, /, **data: Any):
        super().__init__(**data)
        self._inputs: dict[str, list[DataOp]] = collections.defaultdict(list)
        self._sources: set[str] = set()
        self._sinks: set[str] = set()
        self._runners: set[DataGraphRunner] = set()

        for name, node in self.nodes.items():
            self._index_node(name, node)

    def add(self, name: str, node: DataOp):
        """Add a named node to the graph.  Raises DataGraphError if already started or duplicate."""
        if self._runners:
            raise DataGraphError("cannot modify graph while executing")

        if name in self.nodes:
            raise DataGraphError(f"node '{name}' already defined")

        self.nodes[name] = node
        self._index_node(name, node)

    def _index_node(self, name: str, node: DataOp):
        if isinstance(node, SourceOp):
            self._sources.add(name)

        if isinstance(node, SinkOp):
            self._sinks.add(name)

        for output in node.output:
            self._inputs[output].append(node)

    def find_source[M: SourceOp](self, kind: type[M] | None = None) -> M:
        """Return the source node of the given type, or the sole source if *kind* is omitted."""
        if kind:
            for source in self._sources:
                source_node = self.nodes[source]

                if isinstance(source_node, kind):
                    return source_node

            raise DataGraphSourceError(f"no source of type '{kind.__name__}' defined")

        if len(self._sources) != 1:
            raise DataGraphError(f"expected exactly one source, got {len(self._sources)}")

        return cast(SourceOp, self.nodes[next(iter(self._sources))])

    def app_source(self):
        """Return the AppSource node in this graph."""
        from sensorkit.data.local import AppSource

        return self.find_source(AppSource)

    def find_sink[M: SinkOp](self, kind: type[M] | None = None) -> M:
        """Return the sink node of the given type, or the sole sink if *kind* is omitted."""
        if kind:
            for sink in self._sinks:
                sink_node = self.nodes[sink]

                if isinstance(sink_node, kind):
                    return sink_node

            raise DataGraphSourceError(f"no sink of type '{kind.__name__}' defined")

        if len(self._sinks) != 1:
            raise DataGraphError(f"expected exactly one sink, got {len(self._sinks)}")

        return cast(SinkOp, self.nodes[next(iter(self._sinks))])

    def app_sink(self):
        """Return the AppSink node in this graph."""
        from sensorkit.data.local import AppSink

        return self.find_sink(AppSink)

    def _validate_component(self, source: str):
        if source not in self._sources:
            raise DataGraphSourceError(f"source '{source}' is undefined")

        queue = collections.deque([source])
        visited = set()

        while queue:
            node_name = queue.popleft()

            if node_name in visited:
                raise DataGraphCycleError(f"cycle detected at node '{node_name}'")

            visited.add(node_name)

            if node_name not in self.nodes:
                raise DataGraphError(f"node '{node_name}' is undefined")

            node = self.nodes[node_name]

            if node_name != source and isinstance(node, SourceOp):
                # FIXME: This may actually not be an error condition. There may be legitimate use
                #        cases for having multiple sources in a traversal (only one actually acting
                #        as a source at a time), and the way SourceOp is implemented would
                #        naturally support no-op passthrough of intermediate sources.
                raise DataGraphSourceError(f"multiple sources in traversal at '{source}'")

            for next_name in node.output:
                queue.append(next_name)

        return visited

    def start(self, *, task_group: asyncio.TaskGroup = asyncio):
        """Validate and start all source components of the graph in the given task group."""
        if not self._sources:
            raise DataGraphSourceError("no sources defined")

        for source in self._sources:
            self._validate_component(source)
            runner = DataGraphRunner(self, source, task_group)
            self._runners.add(runner)
            runner.start(done_callback=self._runners.discard)

    async def stop(self):
        """Cancel all running DataGraphRunner instances and wait for them to finish."""
        await asyncio.gather(
            *(runner.stop() for runner in self._runners.copy()),
            return_exceptions=True
        )


class DataGraphRunner:
    """Invokes runs of one component of a DataGraph."""

    def __init__(self, graph: DataGraph, source: str, task_group: asyncio.TaskGroup):
        self._graph = weakref.ref(graph)
        self._source = source
        self._task_group = task_group
        self._exec_task: asyncio.Task | None = None

    def start(self, done_callback: Callable[[Self], Any] | None = None):
        """Start executing the graph component, optionally invoking *done_callback* on exit."""
        if self._exec_task:
            raise RuntimeError("DataGraph runner already started")

        # Get a hard reference to the DataGraph.
        graph = self._graph()

        if not graph:
            raise RuntimeError("DataGraph was disposed")

        # Ensure that exceptions are not propagated to the main task group, as runs should
        # keep the service alive until they complete but errors should not kill the service.
        self._exec_task = self._task_group.create_task(self._run_graph(graph))
        logger.info("DataGraph runner started")

        def _finalize_graph_run(t: asyncio.Task):
            if t.cancelled():
                logger.info("DataGraph runner shut down")
            elif e := t.exception():
                logger.error("DataGraph runner exited due to error")
                logger.opt(exception=e).debug("DataGraph runner failed")
            else:
                logger.warning("DataGraph runner exited")

            if done_callback:
                done_callback(self)

        self._exec_task.add_done_callback(_finalize_graph_run)

    async def stop(self):
        """Cancel the graph runner task and wait for it to finish."""
        self._exec_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await self._exec_task

    async def _run_graph(self, graph: DataGraph):
        # Invoke the source and prime the generator it returns.
        source = cast(SourceOp, graph.nodes[self._source])
        gen = source.graph_source()
        running = True

        await gen.asend(None)

        # Execute the graph for each iteration of the source op.
        while running:
            shutdown = False

            # Create a new edge to be injected as the incoming edge to the source node.
            edge = DataFlow()

            try:
                # Run the graph in a TaskGroup so that if any node fails, all nodes for this
                # run are cancelled.
                async with asyncio.TaskGroup() as tg:
                    self._run_tasks(graph, source, edge, task_group=tg)

                    try:
                        # Wait for the SourceOp to trigger and feed the incoming edge.
                        await gen.asend(edge)
                    except asyncio.CancelledError:
                        # FIXME: This check is likely not sufficient to guarantee no deadlock.
                        if not edge.send_called:
                            raise

                        # Wait for tasks to complete before shutting down. We do this by
                        # suppressing the cancellation and allowing the task group to complete.
                        # Then we fall through to re-raise via the `shutdown` flag.
                        asyncio.current_task().uncancel()
                        shutdown = True
            except* StopAsyncIteration:
                logger.debug("DataGraph source stopped producing")
                running = False
            except* Exception:
                # We log this as an exception so that the traceback is preserved.
                logger.exception("DataGraph run encountered an error")

            if shutdown:
                raise asyncio.CancelledError()

    def _run_tasks(
        self,
        graph: DataGraph,
        source: DataOp,
        source_incoming: DataFlow,
        *,
        task_group: asyncio.TaskGroup,
    ):
        tasks: list[asyncio.Task] = []
        incoming: dict[str, list[DataFlow]] = collections.defaultdict(list)
        outgoing: dict[str, list[DataFlow]] = collections.defaultdict(list)

        for name, node in graph.nodes.items():
            for output in node.output:
                edge = DataFlow()
                incoming[output].append(edge)
                outgoing[name].append(edge)

            if node is source:
                incoming[name].append(source_incoming)

        for name, node in graph.nodes.items():
            tasks.append(
                task_group.create_task(node.process(incoming[name], outgoing[name]))
            )

        return tasks
