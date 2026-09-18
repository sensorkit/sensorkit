# SPDX-License-Identifier: Apache-2.0
"""Generated tables and deadline rules, composed and run.

Generated tables are ordinary definitions, so each case composes them against a
bound sensor, compiles them with the real compiler and, where the behavior is a
runtime one, runs them through the executor against devices on the fake
backend.

A node is found by what it runs and where, as `device.Command`, never by an id.
"""
from __future__ import annotations

from collections.abc import Callable

import pytest

from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.lifecycle import OpSpec
from sensorkit.sensor.policies import SensorPolicies
from sensorkit.sensor.topology import Structure

from .common import sensor_of


DEPLOYED_FIELDS = {
    "concurrent_dome_and_mount_init": (bool, False),
    "concurrent_dome_and_mount_deinit": (bool, False),
    "concurrent_dome_init_open": (bool, False),
    "concurrent_dome_deinit_close": (bool, False),
    "always_deinit_dome": (bool, False),
    "dome_open_close_timeout": (float, 120.0),
    "dome_init_timeout": (float, 300.0),
    "dome_deinit_timeout": (float, 300.0),
    "concurrent_mount_and_mirror_cover_init": (bool, False),
    "mirror_cover_open_close_timeout": (float, 60.0),
    "mount_init_timeout": (float, 30.0),
    "mount_home_timeout": (float, 300.0),
}
"""Each field of a deployed policy block, in order, with its type and default."""

ADDED_FIELDS = {
    "mount_deinit_timeout": (float, 60.0),
    "stop_timeout": (float, 30.0),
    "follow_target_timeout": (float, 300.0),
    "filter_change_timeout": (float, 30.0),
    "camera_configure_timeout": (float, 30.0),
    "focus_change_timeout": (float, 30.0),
    "default_timeout": (float, 300.0),
}
"""Each field added since, with its type and default, so a deployed block
without it still loads."""


def structure(*devices: str) -> Structure:
    return Structure.model_validate(
        {"name": "bench", "components": [{"device": d} for d in devices]})






















@pytest.fixture(scope="module")
def handles():
    """What each device handles, which is also what it reports.

    The mount, dome and cover satisfy the mount, enclosure and mirror cover
    traits. The camera satisfies none of them and still has a Stop, so it shows
    a halt addressing a device whatever its traits.
    """
    return {
        "mount": ("Connect", "Init", "Deinit", "MoveToPark", "SetParkPosition",
                  "Stop", "Home", "FollowTarget", "Abort"),
        "dome": ("Connect", "Init", "Deinit", "OpenEnclosure",
                 "CloseEnclosure", "Stop", "Abort"),
        "cover": ("Connect", "OpenMirrorCover", "CloseMirrorCover", "Stop"),
        "cam": ("Connect", "Stop"),
    }


@pytest.fixture(scope="module")
def devices(handles) -> tuple[str, ...]:
    return tuple(handles)


@pytest.fixture(scope="module")
def bound(handles) -> Callable[..., BoundSensor]:
    """Binds a sensor holding the named devices, each reporting what it handles
    unless a case passes what it reports instead."""
    def bind(*devices: str, reported=handles) -> BoundSensor:
        return sensor_of(structure(*devices),
                         {device: (reported[device], ()) for device in devices})

    return bind


@pytest.fixture(scope="module")
def sensor(bound, devices) -> BoundSensor:
    return bound(*devices)


# Policy fields


def test_every_policy_field_keeps_its_name_type_and_default():
    fields = SensorPolicies.model_fields

    assert set(fields) == set(DEPLOYED_FIELDS) | set(ADDED_FIELDS)

    for name, expected in (DEPLOYED_FIELDS | ADDED_FIELDS).items():
        assert (fields[name].annotation, fields[name].default) == expected, name


def test_a_deployed_policy_block_loads_unchanged():
    block = {"always_deinit_dome": True, "dome_init_timeout": 12.5}

    defaults = {name: default for name, (_, default)
                in (DEPLOYED_FIELDS | ADDED_FIELDS).items()}

    assert SensorPolicies.model_validate(block).model_dump() == defaults | block


@pytest.mark.parametrize("name", ["minimum_target_altitude_degrees",
                                 "sun_separation_degrees", "moon_separation_degrees",
                                 "dome_init_timout"])
def test_unused_and_misspelled_policies_are_rejected(name):
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SensorPolicies.model_validate({name: 20.0})


# Generation


def test_generation_names_four_tables_and_standby_is_init():
    tables = {table.name: table for table in SensorPolicies().tables()}

    assert list(tables) == ["init", "standby", "shutdown", "recover"]
    assert tables["standby"] == tables["init"].model_copy(
        update={"name": "standby"})


@pytest.mark.parametrize("policies", [
    SensorPolicies(),
    SensorPolicies(**{name: True for name, field in
                      SensorPolicies.model_fields.items()
                      if field.annotation is bool}),
])
def test_every_generated_operation_omits_what_its_device_lacks(policies):
    specs: list[OpSpec] = [
        op for table in policies.tables()
        for group in (*table.phases, *table.cleanup)
        for entry in group.entries for op in entry.ops]

    assert specs
    assert {op.unsupported for op in specs} == {"omit"}








# Concurrency flags










# Absent equipment
















def test_the_selection_helper_decides_nothing(sensor, bound):
    no_dome = bound("mount", "cover")
    init = next(t for t in SensorPolicies().tables() if t.name == "init")
    enclosure = init.phases[0].entries[0]

    assert enclosure.targets(no_dome) == ()
    assert [p.device for p in enclosure.targets(sensor)] == ["dome"]




# Generated and authored equivalence








# always_deinit_dome, run
#
# A site guaranteeing its dome closes configures `always_deinit_dome`. Whatever
# goes wrong before the close, the close is attempted. A returned report marked
# completed means the dome acknowledged a successful close, and only a domain
# abort or hard cancellation leaves the close unattempted.


"""The guarantee, with deadlines short enough that a hung operation ends
within the case."""


















# Omission and failure






# Bring-up cleanup


















# Deadlines








"""A dome reporting no Init or Deinit, so it fails the enclosure trait."""

"""A mount reporting no park or home, so it fails the mount trait."""
