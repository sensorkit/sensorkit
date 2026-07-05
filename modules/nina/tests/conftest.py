# SPDX-License-Identifier: Apache-2.0
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class MockNinaClient:
    """Mock for NinaClient that records requests and returns configurable responses."""

    def __init__(self, info_response: dict | None = None):
        self._requests: list[tuple[str, dict]] = []
        self._username = None
        self._password = None
        self._info_response = info_response or {
            "Connected": True,
            "Slewing": False,
            "Tracking": False,
            "AtHome": True,
            "AtPark": False,
        }

    async def get(self, path: str, **params):
        self._requests.append((path, params))
        return self._info_response

    async def get_raw(self, path: str, **params):
        self._requests.append((path, params))
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Success": True, "Response": self._info_response}
        return mock_resp

    async def login(self):
        pass

    async def close(self):
        pass

    def set_info(self, **overrides):
        self._info_response.update(overrides)

    def find_requests(self, path: str) -> list[tuple[str, dict]]:
        return [(p, params) for p, params in self._requests if p == path]

    def last_request(self) -> tuple[str, dict]:
        return self._requests[-1] if self._requests else ("", {})


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
