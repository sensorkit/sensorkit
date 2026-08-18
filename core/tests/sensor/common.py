# SPDX-License-Identifier: Apache-2.0
"""What the sensor tests share besides fixtures.

`sensor.yaml` describes one sensor exercising every structural feature the
package has, so the tests read one document rather than inventing one each.

`REPORTED` is what each device says about itself, and `snapshot_of` turns it
into a capability snapshot, so a test that needs a device to report something
else passes a variant of the mapping rather than building details by hand.

`BENCH` is one mount shared by two instrument branches. The east branch has a
focuser and a camera, and the west branch has a filter wheel and a camera.

A `Rig` serves devices on the fake backend, so commands cross the real request
machinery. Each device records what it received and what else was in flight
when it arrived, and can hold, delay or refuse a command. Ordering is read from
those records and from arrival and ending events, never from sleeping.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path


import sensorkit.std.traits  # noqa: F401  registers the command vocabulary

SENSOR_YAML = Path(__file__).resolve().parent / "sensor.yaml"

type Reported = Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]]
"""What devices report, as command ids and published keyword keys."""

REPORTED: Reported = {
    "mount": (("Connect", "Disconnect", "Home", "Stop"), ()),
    "dome": (("Connect", "Disconnect", "Enable", "Disable", "Init"),
             ("Enabled",)),
    "cover": (("Connect", "Disconnect"), ()),
    "pickoff": (("Connect", "Disconnect", "Home"), ()),
    "foc-sci": (("Connect", "Disconnect", "Home"), ()),
    "wheel": (("Connect", "Disconnect", "Home"), ()),
    "cam-sci": (("Connect", "Disconnect", "Init", "Deinit"), ()),
    "cam-guide": (("Connect", "Disconnect", "Init"), ()),
    "cam-acq": (("Connect",), ()),
}
"""What each device in `sensor.yaml` reports.

`cam-acq` reports least, which is what makes an unsupported command and a
device satisfying no trait reachable in a test.
"""

TAKEN = datetime(2026, 1, 1, tzinfo=UTC)
"""Provenance the tests never read, fixed so nothing varies by clock."""

"""The east and west instrument branches, to follow a list of root devices."""

"""One mount shared by the east and west branches."""

"""Followed by rate, so a sidereal frame is a change of pointing."""
