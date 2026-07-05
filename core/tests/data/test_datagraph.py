# SPDX-License-Identifier: Apache-2.0
import asyncio
from typing import ClassVar, Literal

import pytest
from loguru import logger

from sensorkit.data.context import Context
from sensorkit.data.graph import (
    DataFlow,
    DataGraph,
    DataGraphCycleError,
    DataGraphSourceError,
    DataOp,
)
from sensorkit.data.local import AppSink, AppSource


@pytest.mark.asyncio
async def test_data_flow():
    edge = DataFlow()

    async def receiver():
        context, buffer = await edge.receive("buffer")
        assert buffer == b"Hello from stream!"

    receiver_task = asyncio.create_task(receiver())

    # Send as stream
    context = Context()
    context["source"] = "test"
    writer = await edge.send(context)

    # Write to the stream
    writer.write(b"Hello from stream!")
    writer.close()

    # Wait for the receiver to finish
    await receiver_task


class TestOp(DataOp):
    __test__: ClassVar[bool] = False
    op: Literal["test_op"] = "test_op"
    in_kind: str = "buffer"
    out_kind: str = "buffer"

    async def process(self, incoming: list[DataFlow], outgoing: list[DataFlow]):
        context, data = await incoming[0].receive(self.in_kind)
        context["count"] += 1

        if self.out_kind == "buffer":
            if self.in_kind == "buffer":
                await outgoing[0].send(context, data)
            else:
                combined = bytearray()

                async for chunk in data:
                    combined.extend(chunk)

                await outgoing[0].send(context, combined)
        else:
            writer = await outgoing[0].send(context)

            if self.in_kind == "buffer":
                writer.write(data)
            else:
                async for chunk in data:
                    writer.write(chunk)

            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "in_kind,out_kind,n,size",
    [
        ("buffer", "buffer", 500, 10**6),
        ("buffer", "stream", 500, 10**6),
        ("stream", "buffer", 500, 10**6),
        ("stream", "stream", 500, 10**6),
    ],
)
async def test_data_graph(n: int, size: int, in_kind: str, out_kind: str):
    logger.info(f"Creating {n}-node DataGraph")

    # Create a source to trigger graph execution.
    source = AppSource(output=["process0"])

    # Create a sink node to collect the graph result.
    sink = AppSink()

    # Create the graph.
    graph = DataGraph()
    graph.add("source", source)

    for i in range(n - 2):
        graph.add(
            f"process{i}",
            TestOp(
                output=[f"process{i + 1}"],
                in_kind=in_kind,
                out_kind=out_kind,
            ),
        )

    graph.add(f"process{i + 1}", sink)

    # Run the graph.
    graph.start()

    source.produce(
        Context(count=0),
        b"x" * size,
    )

    async for context, buf in sink.consume():
        assert context["count"] == n - 2
        assert len(buf) == size
        break

    await graph.stop()


@pytest.mark.asyncio
async def test_data_graph_cycle():
    graph = DataGraph(
        nodes={
            "source": AppSource(output=["node1"]),
            "node1": TestOp(output=["node2"]),
            "node2": TestOp(output=["node1"]),
        }
    )

    with pytest.raises(DataGraphCycleError):
        graph.start()


@pytest.mark.asyncio
async def test_data_graph_sources():
    # Missing source.
    graph = DataGraph(nodes={})

    with pytest.raises(DataGraphSourceError):
        graph.start()

    # Multiple sources.
    graph = DataGraph(
        nodes={
            "source1": AppSource(output=["source2"]),
            "source2": AppSource(),
        }
    )

    with pytest.raises(DataGraphSourceError):
        graph.start()


@pytest.mark.asyncio
async def test_data_graph_from_json():
    # Load graph from JSON definition
    graph_config = {
        "nodes": {"source": {"op": "app_source", "output": ["sink"]}, "sink": {"op": "app_sink"}}
    }

    graph = DataGraph.model_validate(graph_config)
    graph.start()

    source = graph.nodes["source"]
    sink = graph.nodes["sink"]

    # Send test data through the graph
    source.produce(Context(count=0), b"test data")

    async for _context, buf in sink.consume():
        assert buf == b"test data"
        break

    await graph.stop()


def test_data_graph_from_simple_repr():
    graph_config = {
        "nodes": {
            "app_source_1": {"op": "app_source", "output": ["app_sink_1"]},
            "app_sink_1": {"op": "app_sink"}
        }
    }
    simple_config = {
        "simple": [
            {"op": "app_source"},
            {"op": "app_sink"},
        ]
    }

    assert DataGraph.model_validate(graph_config) == DataGraph.model_validate(simple_config)

    simple_config["nodes"] = []

    with pytest.raises(ValueError):
        DataGraph.model_validate(simple_config)
