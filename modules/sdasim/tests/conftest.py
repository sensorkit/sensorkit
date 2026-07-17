# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for sdasim module unit tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_sk_device():
    """Mock sk.device() so device commands can publish/kv_put without a real service."""
    mock_device = MagicMock()
    mock_device.publish = AsyncMock()
    mock_device.kv_put_model = AsyncMock()
    mock_device.kv_get_model = AsyncMock(side_effect=Exception("no saved state"))
    mock_device.entity = "SimulatedCamera"
    mock_device.data_graph = AsyncMock(return_value=None)

    with patch("sensorkit.api.device", return_value=mock_device):
        yield mock_device


def make_data_graph_writer():
    """Build a (graph, writer) pair of mocks for the DataGraph write path."""
    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()

    source = MagicMock()
    source.produce = MagicMock(return_value=writer)

    graph = MagicMock()
    graph.app_source = MagicMock(return_value=source)

    return graph, writer
