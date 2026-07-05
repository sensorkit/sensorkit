# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures and mocks for Alpaca device tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class MockAlpacaSDKDevice:
    """Mock for any alpyca SDK device (Camera, Dome, Telescope, etc.).

    Simulates the synchronous property/method interface that alpyca exposes.
    Properties are stored in a dict and accessed via __getattr__/__setattr__.
    Unknown attribute reads return a no-op callable (for method calls).
    """

    def __init__(self, **properties):
        super().__setattr__("_properties", {
            "Connected": False,
            "Connecting": False,
            **properties,
        })

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        props = super().__getattribute__("_properties")
        if name in props:
            return props[name]
        # Return a no-op callable for methods
        return lambda *args, **kwargs: None

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self._properties[name] = value

    def Connect(self):
        self._properties["Connected"] = True
        self._properties["Connecting"] = False

    def Disconnect(self):
        self._properties["Connected"] = False
        self._properties["Connecting"] = False


@pytest.fixture(autouse=True)
def _mock_sk_device():
    """Mock sk.device() so device commands can publish/kv_put without a real service."""
    mock_device = MagicMock()
    mock_device.publish = AsyncMock()
    mock_device.kv_put_model = AsyncMock()
    mock_device.kv_get_model = AsyncMock(side_effect=Exception("no saved state"))
    mock_device.entity = "test_entity"
    mock_device.data_graph = AsyncMock(return_value=None)

    with patch("sensorkit.api.device", return_value=mock_device):
        yield mock_device
