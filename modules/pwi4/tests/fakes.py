# SPDX-License-Identifier: Apache-2.0
"""PWI4 HTTP client fake.

PWI4 is an external service reached over HTTP, so its client is stubbed rather than run against
a real stack.
"""

from __future__ import annotations

from sensorkit.pwi4.device import PWI4Client


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


MOTION_ENDPOINTS = frozenset(
    {
        "/mount/find_home",
        "/mount/follow_tle",
        "/mount/goto_alt_az",
        "/mount/goto_ra_dec_j2000",
        "/mount/park",
        "/mount/radecpath/apply",
    }
)
"""Endpoints that put the mount in motion, so the next status poll reports a slew."""


class FakePWI4Client(PWI4Client):
    """PWI4Client with its HTTP layer replaced — returns canned status responses."""

    def __init__(self, status_overrides: dict[str, str] | None = None):
        # Deliberately skips PWI4Client.__init__: every method that would touch the network is
        # overridden here, and building the real httpx transport costs ~170ms per instance.
        self.host = "localhost"
        self.port = 8220
        self.base_url = f"http://{self.host}:{self.port}"

        self._status_data = make_status(**(status_overrides or {}))
        self._requests: list[tuple[str, dict | None]] = []
        self._slewing = False

    async def request(self, endpoint, params=None, postdata=None):
        self._requests.append((endpoint, params))

        if endpoint in MOTION_ENDPOINTS:
            self._slewing = True

        return dict(self._status_data)

    async def request_raw(self, endpoint, params=None, postdata=None):
        self._requests.append((endpoint, params))
        return b""

    async def status(self):
        # A commanded slew shows up as motion on the next poll and has settled by the one after,
        # which is what the driver's onset-then-settle wait expects to see.
        if self._slewing:
            self._slewing = False
            return dict(self._status_data) | {"mount.is_slewing": "true"}

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
