"""Shared fixtures for PWI4 unit tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from sensorkit.pwi4.device import PWI4Client


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


def make_status(**overrides) -> dict[str, str]:
    """Build a default PWI4 status dict with optional overrides."""
    defaults = {
        "mount.is_connected": "true",
        "mount.is_slewing": "false",
        "mount.is_tracking": "true",
        "mount.ra_j2000_hours": "12.0",
        "mount.dec_j2000_degs": "45.0",
        "mount.altitude_degs": "60.0",
        "mount.azimuth_degs": "180.0",
        "mount.axis0.is_enabled": "true",
        "mount.axis1.is_enabled": "true",
        "mount.axis0.is_position_initialized": "true",
        "mount.axis1.is_position_initialized": "true",
        "mount.axis0.position_degs": "180.0",
        "mount.axis1.position_degs": "60.0",
        "mount.axis0.measured_velocity_degs_per_sec": "0.004",
        "mount.axis1.measured_velocity_degs_per_sec": "0.001",
        "mount.axis0.max_velocity_degs_per_sec": "20.0",
        "mount.axis1.max_velocity_degs_per_sec": "20.0",
        "mount.axis0.acceleration_degs_per_sec_sqr": "5.0",
        "mount.axis1.acceleration_degs_per_sec_sqr": "5.0",
        "mount.axis0.min_mech_position_degs": "-180.0",
        "mount.axis1.min_mech_position_degs": "-5.0",
        "mount.axis0.max_mech_position_degs": "540.0",
        "mount.axis1.max_mech_position_degs": "95.0",
        "mount.axis0.dist_to_target_arcsec": "0.5",
        "mount.axis1.dist_to_target_arcsec": "0.3",
        "mount.axis0.rms_error_arcsec": "1.2",
        "mount.axis1.rms_error_arcsec": "0.8",
        "mount.axis0_wrap_range_min_degs": "0.0",
        "site.latitude_degs": "41.9168",
        "site.longitude_degs": "-84.029",
        "site.height_meters": "300.0",
        "focuser.is_connected": "true",
        "focuser.is_enabled": "true",
        "focuser.is_moving": "false",
        "focuser.position": "15000.0",
        "rotator.is_connected": "true",
        "rotator.is_enabled": "true",
        "rotator.is_moving": "false",
        "rotator.mech_position_degs": "90.0",
        "mirrorcover.is_connected": "true",
        "mirrorcover.overall_state_name": "Closed",
    }
    defaults.update(overrides)
    return defaults


class MockPWI4Client(PWI4Client):
    """PWI4Client with mocked HTTP — returns canned status responses."""

    def __init__(self, status_overrides: dict[str, str] | None = None):
        super().__init__(host="localhost", port=8220)
        self._status_data = make_status(**(status_overrides or {}))
        self._requests: list[tuple[str, dict | None]] = []

    async def request(self, endpoint, params=None, postdata=None):
        self._requests.append((endpoint, params))
        return dict(self._status_data)

    async def request_raw(self, endpoint, params=None, postdata=None):
        self._requests.append((endpoint, params))
        return b""

    async def status(self):
        return dict(self._status_data)

    async def poll(self, condition, interval=0.01, start_delay=0.0):
        return dict(self._status_data)

    async def close(self):
        pass

    def set_status(self, **overrides):
        self._status_data.update(overrides)

    def last_request(self) -> tuple[str, dict | None]:
        return self._requests[-1] if self._requests else ("", None)

    def find_requests(self, endpoint: str) -> list[tuple[str, dict | None]]:
        return [(e, p) for e, p in self._requests if e == endpoint]
