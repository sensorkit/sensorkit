# SPDX-License-Identifier: Apache-2.0
"""OurSky Node Platform SDK fake.

The Platform is an external service reached through the `ourskyai_node_platform_api` SDK, so the
SDK and the status models it returns are stubbed rather than run against a real stack. The status
builders below carry only the fields the drivers actually read; anything missing surfaces as an
`AttributeError` rather than a silently-truthy value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class SDKConfiguration:
    """osapi.Configuration — the base URL and token the raw-request path reads."""

    host: str = "http://localhost:9080"
    access_token: str = "test-token"


class RecordingSDK:
    """osapi.DefaultApi — every method is a recording no-op returning `None`.

    `NodePlatformAPI.call` looks the method up by name and dispatches it through a thread, so the
    fake only has to answer `getattr` with something callable.
    """

    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        def method(*args, **kwargs):
            self.calls.append((name, args, kwargs))

        return method

    def find_calls(self, method_name: str) -> list[tuple[tuple, dict]]:
        """Every recorded call to the named SDK method, oldest first."""
        return [(args, kwargs) for name, args, kwargs in self.calls if name == method_name]


class SDKClient:
    """osapi.ApiClient — only the raw REST escape hatch is reachable from the drivers."""

    def __init__(self):
        self.rest_client = RecordingSDK()


class FakeNodePlatformAPI:
    """NodePlatformAPI with the SDK stubbed — records calls and returns canned responses.

    A response registered with `set_response` may be a value or a callable; a callable is invoked
    with the call's arguments, which is how a test simulates Platform state that its own commands
    mutate (see `install_shutter` in the enclosure tests).
    """

    def __init__(self, responses: dict[str, object] | None = None):
        self._responses = responses or {}
        self._calls: list[tuple[str, tuple, dict]] = []
        self._configuration = SDKConfiguration()
        self._client = SDKClient()
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


@dataclass
class MountMotor:
    """V2MountStatus.motor_a / motor_b."""

    is_enabled: bool = True
    measured_velocity_degrees_per_second: float = 0.0


@dataclass
class MountStatus:
    """V2MountStatus."""

    connected: bool = True
    is_slewing: bool = False
    is_tracking: bool = False
    ra_j2000_degrees: float = 180.0
    dec_j2000_degrees: float = 45.0
    altitude_degrees: float = 60.0
    azimuth_degrees: float = 200.0
    motor_a: MountMotor = field(default_factory=MountMotor)
    motor_b: MountMotor = field(default_factory=MountMotor)


def make_mount_status(**overrides) -> MountStatus:
    """Build a V2MountStatus for an idle, connected mount with its motors enabled."""
    az_rate = overrides.pop("az_rate", 0.004)
    alt_rate = overrides.pop("alt_rate", 0.001)

    return MountStatus(
        motor_a=MountMotor(measured_velocity_degrees_per_second=az_rate),
        motor_b=MountMotor(measured_velocity_degrees_per_second=alt_rate),
        **overrides,
    )


@dataclass
class ShutterStatus:
    """V2EnclosureStatus.shutters.statuses[n]."""

    connected: bool = True
    state: Any = None
    position_percent: float = 0.0


@dataclass
class ShutterStatuses:
    """V2EnclosureStatus.shutters."""

    statuses: list[ShutterStatus] = field(default_factory=list)


@dataclass
class EnclosureStatus:
    """V2EnclosureStatus."""

    shutters: ShutterStatuses = field(default_factory=ShutterStatuses)


def make_enclosure_status(**overrides) -> EnclosureStatus:
    """Build a V2EnclosureStatus wrapping a single shutter."""
    import ourskyai_node_platform_api as osapi

    shutter = ShutterStatus(
        connected=overrides.get("connected", True),
        state=overrides.get("state", osapi.EnclosureShutterState.CLOSED),
        position_percent=overrides.get("position_percent", 0.0),
    )

    return EnclosureStatus(shutters=ShutterStatuses(statuses=[shutter]))


@dataclass
class CoverStatus:
    """V1OpticalTubeCoverStatus."""

    connected: bool = True
    is_open: bool = False


def make_cover_status(**overrides) -> CoverStatus:
    return CoverStatus(**overrides)


@dataclass
class RotatorPosition:
    """V1RotatorStatus.position."""

    mechanical_angle_degrees: float = 90.0


@dataclass
class RotatorStatus:
    """V1RotatorStatus."""

    connected: bool = True
    moving: bool = False
    position: RotatorPosition = field(default_factory=RotatorPosition)


def make_rotator_status(**overrides) -> RotatorStatus:
    return RotatorStatus(
        connected=overrides.get("connected", True),
        moving=overrides.get("moving", False),
        position=RotatorPosition(mechanical_angle_degrees=overrides.get("position", 90.0)),
    )


@dataclass
class FocuserPosition:
    """V1FocuserStatus.position."""

    zaxis_microns: float = 15000.0


@dataclass
class FocuserStatus:
    """V1FocuserStatus."""

    connected: bool = True
    moving: bool = False
    position: FocuserPosition = field(default_factory=FocuserPosition)


def make_focuser_status(**overrides) -> FocuserStatus:
    return FocuserStatus(
        connected=overrides.get("connected", True),
        moving=overrides.get("moving", False),
        position=FocuserPosition(zaxis_microns=overrides.get("position", 15000.0)),
    )


@dataclass
class WeatherStationStatus:
    """V1WeatherStationStatus."""

    connected: bool = True


def make_weather_station_status(**overrides) -> WeatherStationStatus:
    return WeatherStationStatus(**overrides)


@dataclass
class SafetyStatus:
    """V1SafetyStatus."""

    is_safe: bool = True
    is_weather_safe: bool = True
    is_all_sky_safe: bool = True
    is_night: bool = True


def make_safety_status(**overrides) -> SafetyStatus:
    return SafetyStatus(**overrides)


@dataclass
class OperationStatus:
    """V1SystemOperationStatus."""

    system_operation_mode: Any = None


def make_operation_status(mode: str = "ASSISTED") -> OperationStatus:
    import ourskyai_node_platform_api as osapi

    return OperationStatus(system_operation_mode=osapi.V1SystemOperationMode(mode))


@dataclass
class SystemMetric:
    """V1SystemMetrics.metrics[n]."""

    name: str
    value: float
    measured_at: datetime


@dataclass
class SystemMetrics:
    """V1SystemMetrics."""

    metrics: list[SystemMetric] = field(default_factory=list)


@dataclass
class SystemMetricNames:
    """V1SystemMetricNames."""

    metric_names: list[str] = field(default_factory=list)
