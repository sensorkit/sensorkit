# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for the NINA suite."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _autouse_device_context(device_impl):
    """All tests in this suite may access an active `DeviceImpl` via `sk.device()`."""
