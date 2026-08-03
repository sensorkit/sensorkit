# SPDX-License-Identifier: Apache-2.0
"""The `sensors:` configuration section, shared by both implementations.

Held apart from the code that consumes it because everything in this package
reads it: the derivation of a workflow plan, the client, the controller wrapper
and the legacy implementation alike. One direction of import, and no cycle.
"""

from __future__ import annotations

from enum import IntEnum

from pydantic import BaseModel, Field

from sensorkit.astro.common import SitePosition


class Implementation(IntEnum):
    """Which orchestration a sensor entry selects."""

    LEGACY = 1
    """Hand-written orchestration: what every site runs today."""

    WORKFLOW = 2
    """Compiled from a workflow plan derived from this same section."""


class SensorDevices(BaseModel):
    """Device entity references for sensor control."""

    mount: str | None = None
    camera: str | None = None
    focuser: str | None = None
    rotator: str | None = None
    filter_wheel: str | None = None
    mirror_cover: str | None = None
    dome: str | None = None

    def refs(self) -> list[str]:
        """Every configured device reference, in field order and without duplicates."""
        seen: dict[str, None] = {}

        for name in type(self).model_fields:
            if ref := getattr(self, name):
                seen.setdefault(ref)

        return list(seen)


class SensorPolicies(BaseModel):
    """Policies for sensor control."""

    concurrent_dome_and_mount_init: bool = False
    """Whether the dome and mount can be initialized concurrently."""

    concurrent_dome_and_mount_deinit: bool = False
    """Whether the dome and mount can be deinitialized concurrently."""

    concurrent_dome_init_open: bool = False
    """Whether the dome can be initialized and opened concurrently."""

    concurrent_dome_deinit_close: bool = False
    """Whether the dome can be closed and deinitialized concurrently."""

    always_deinit_dome: bool = False
    """Whether the dome should be deinitialized even if other parts of deinitialization fail."""

    dome_open_close_timeout: float = 120.0
    """Timeout for fully opening or closing the dome."""

    dome_init_timeout: float = 300.0
    """Timeout for initializing the dome, including homing if required."""

    dome_deinit_timeout: float = 300.0
    """Timeout for deinitializing the dome, including parking if required."""

    concurrent_mount_and_mirror_cover_init: bool = False
    """Whether the mount and mirror cover can be initialized concurrently."""

    mirror_cover_open_close_timeout: float = 60.0
    """Timeout for fully opening or closing the mirror cover."""

    mount_init_timeout: float = 30.0
    """Timeout for powering the mount and enabling axis control."""

    mount_home_timeout: float = 300.0
    """Timeout for homing the mount."""

    minimum_target_altitude_degrees: float | None = None
    """Minimum altitude for a target to be tracked during collection."""

    sun_separation_degrees: float | None = None
    """Distance a target must be away from the sun during collection."""

    moon_separation_degrees: float | None = None
    """Distance a target must be away from the moon during collection"""


class SensorConfig(BaseModel):
    """Configuration for standard sensor control."""

    controller_name: str
    devices: SensorDevices
    site_position: SitePosition
    policies: SensorPolicies = Field(default_factory=SensorPolicies)

    implementation: Implementation = Implementation.LEGACY
    """Which sensor implementation this entry selects.

    Distinct from the unified configuration's own top-level version key, which
    describes the document rather than what reads it. An unrecognized value is a
    load-time error rather than a silent fallback, so a typo cannot quietly run
    the wrong orchestration against real hardware.
    """
