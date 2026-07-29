# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the UDL suite."""

from __future__ import annotations

import pytest
import pytest_asyncio

from sensorkit.data.graph import DataGraph
from sensorkit.data.local import AppSink, AppSource


@pytest.fixture(autouse=True)
def _program_context(program_impl):
    """Run every UDL test inside a live program context.

    A live service enters the program context around every task factory and lifecycle hook, so the
    `sk.program()` calls in the driver resolve without any special-casing here.
    """


@pytest_asyncio.fixture
async def program_data_graph(program_impl):
    """Install a source-to-sink DataGraph on the program entity.

    The UDL publisher reads collected frames from `sk.program().data_graph()`, so a test feeds it
    by producing into the returned graph's source.
    """
    await program_impl.kv_put_model(
        DataGraph(nodes={"source": AppSource(output=["sink"]), "sink": AppSink()})
    )

    graph = await program_impl.data_graph()

    yield graph

    await graph.stop()
