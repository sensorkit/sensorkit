# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the PWI4 suite."""

from __future__ import annotations

import pytest
import pytest_asyncio

from sensorkit.common.aio import AsyncLoop
from sensorkit.pwi4.mount import PWI4Mount, PWI4MountConfig, PWI4MountState

from .fakes import FakePWI4Client


@pytest.fixture(autouse=True)
def _autouse_device_context(device_impl):
    """All tests in this suite may access an active `DeviceImpl` via `sk.device()`."""


@pytest.fixture
def client():
    return FakePWI4Client()


@pytest_asyncio.fixture
async def mount(client):
    """A connected mount, in the state it reaches once attach has run."""
    config = PWI4MountConfig(device_type="mount")
    mount = PWI4Mount(config=config, client=client)
    mount.state = PWI4MountState()
    mount.device_connected = True
    mount.status_loop = AsyncLoop(mount.status_publish, interval=config.status_frequency_slow)
    mount.fast_loop = AsyncLoop(mount.status_publish_fast, interval=config.status_frequency_fast)

    yield mount

    await mount.status_loop.stop()
    await mount.fast_loop.stop()
