# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the PWI4 suite."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
import pytest_asyncio

from .fakes import FakePWI4Client
from sensorkit.pwi4.mount import PWI4Mount, PWI4MountConfig, PWI4MountState


@pytest.fixture(autouse=True)
def _autouse_device_context(device_impl):
    """All tests in this suite may access an active `DeviceImpl` via `sk.device()`."""


@pytest.fixture
def client():
    return FakePWI4Client()


@pytest_asyncio.fixture
async def mount(client):
    """A connected mount, in the state it reaches once attach has run."""
    mount = PWI4Mount(config=PWI4MountConfig(device_type="mount"), client=client)
    mount.state = PWI4MountState()
    mount.device_connected = True

    yield mount

    # Following a target starts a status loop that publishes converted coordinates ten times a
    # second. Awaiting its cancellation keeps it from running on into later tests.
    task = mount._fast_status_task
    mount._stop_fast_status()

    if task is not None:
        with contextlib.suppress(asyncio.CancelledError):
            await task
