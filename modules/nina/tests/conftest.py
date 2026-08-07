# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the NINA suite."""

from __future__ import annotations

import pytest
import pytest_asyncio

from sensorkit.common.aio import AsyncLoop
from sensorkit.nina.mount import NinaMountConfig, NinaMountState


@pytest.fixture(autouse=True)
def _autouse_device_context(device_impl):
    """All tests in this suite may access an active `DeviceImpl` via `sk.device()`."""


@pytest_asyncio.fixture
async def mount(client):
    """A connected mount, in the state it reaches once attach has run."""
    config = NinaMountConfig(device_type="mount")
    m = config.create_device()
    m._client = client
    m.state = NinaMountState()
    m.device_connected = True
    m._site_lat = 32.0
    m._site_lon = -110.0
    m._site_elev = 700.0
    m._tracking = None
    m._slewing = None
    m.status_loop = AsyncLoop(m.status_publish, interval=config.status_frequency_slow)
    m.fast_loop = AsyncLoop(m._publish_mount_status, interval=config.status_frequency_fast)

    yield m

    # Following a target starts the fast loop, which publishes pointing several times a
    # second. Awaiting its cancellation keeps it from running on into later tests.
    await m.status_loop.stop()
    await m.fast_loop.stop()
