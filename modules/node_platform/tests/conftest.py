"""Shared fixtures for Node Platform unit tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


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


class MockNodePlatformAPI:
    """Mock NodePlatformAPI that records calls and returns canned responses."""

    def __init__(self, responses: dict[str, object] | None = None):
        self._responses = responses or {}
        self._calls: list[tuple[str, tuple, dict]] = []
        self._configuration = MagicMock()
        self._configuration.host = "http://localhost:9080"
        self._configuration.access_token = "test-token"
        self._client = MagicMock()
        self.lineage_id = ""

    def set_response(self, method: str, response: object):
        self._responses[method] = response

    async def call(self, method_name: str, *args, **kwargs):
        self._calls.append((method_name, args, kwargs))
        resp = self._responses.get(method_name)
        if callable(resp):
            return resp(*args, **kwargs)
        return resp

    async def close(self):
        pass

    def find_calls(self, method_name: str) -> list[tuple[str, tuple, dict]]:
        return [(m, a, k) for m, a, k in self._calls if m == method_name]

    def last_call(self) -> tuple[str, tuple, dict] | None:
        return self._calls[-1] if self._calls else None


def make_mount_status(**overrides):
    """Build a mock V2MountStatus."""
    status = MagicMock()
    status.connected = overrides.get("connected", True)
    status.is_slewing = overrides.get("is_slewing", False)
    status.is_tracking = overrides.get("is_tracking", False)
    status.ra_j2000_degrees = overrides.get("ra_j2000_degrees", 180.0)
    status.dec_j2000_degrees = overrides.get("dec_j2000_degrees", 45.0)
    status.altitude_degrees = overrides.get("altitude_degrees", 60.0)
    status.azimuth_degrees = overrides.get("azimuth_degrees", 200.0)
    status.motor_a = MagicMock()
    status.motor_a.measured_velocity_degrees_per_second = overrides.get("az_rate", 0.004)
    status.motor_b = MagicMock()
    status.motor_b.measured_velocity_degrees_per_second = overrides.get("alt_rate", 0.001)
    return status


def make_enclosure_status(**overrides):
    """Build a mock V2EnclosureStatus."""
    import ourskyai_node_platform_api as osapi

    shutter = MagicMock()
    shutter.connected = overrides.get("connected", True)
    shutter.state = overrides.get("state", osapi.EnclosureShutterState.CLOSED)
    shutter.position_percent = overrides.get("position_percent", 0.0)

    status = MagicMock()
    status.shutters = MagicMock()
    status.shutters.statuses = [shutter]
    return status


def make_cover_status(**overrides):
    """Build a mock V1OpticalTubeCoverStatus."""
    return SimpleNamespace(
        connected=overrides.get("connected", True),
        is_open=overrides.get("is_open", False),
    )


def make_rotator_status(**overrides):
    """Build a mock V1RotatorStatus."""
    status = MagicMock()
    status.connected = overrides.get("connected", True)
    status.moving = overrides.get("moving", False)
    status.position = MagicMock()
    status.position.mechanical_angle_degrees = overrides.get("position", 90.0)
    return status


def make_focuser_status(**overrides):
    """Build a mock V1FocuserStatus."""
    status = MagicMock()
    status.connected = overrides.get("connected", True)
    status.moving = overrides.get("moving", False)
    status.position = MagicMock()
    status.position.zaxis_microns = overrides.get("position", 15000.0)
    return status


class SimpleNamespace:
    """Simple attribute holder that doesn't auto-create attributes like MagicMock."""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_weather_station_status(**overrides):
    """Build a mock V1WeatherStationStatus."""
    return SimpleNamespace(connected=overrides.get("connected", True))


def make_safety_status(**overrides):
    """Build a mock V1SafetyStatus."""
    status = MagicMock()
    status.is_safe = overrides.get("is_safe", True)
    status.is_weather_safe = overrides.get("is_weather_safe", True)
    status.is_all_sky_safe = overrides.get("is_all_sky_safe", True)
    status.is_night = overrides.get("is_night", True)
    return status


def make_operation_status(mode="ASSISTED"):
    """Build a mock V1SystemOperationStatus."""
    import ourskyai_node_platform_api as osapi

    status = MagicMock()
    status.system_operation_mode = osapi.V1SystemOperationMode(mode)
    return status
