# SPDX-License-Identifier: Apache-2.0
"""What binding establishes, what it refuses, and where a command goes.

The sensor is `sensor.yaml`. `Home` is supported at four depths on the science
chain, which is what makes a depth rule, a tie and a narrowing visible without
inventing a second structure.
"""
from __future__ import annotations

import pytest

from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.topology import Device, Structure, Topology

from .common import REPORTED, snapshot_of


def without(device: str) -> dict:
    """What every device but this one reports."""
    return {k: v for k, v in REPORTED.items() if k != device}


def reporting(device: str, commands: tuple[str, ...]) -> dict:
    """What every device reports, with this one's commands replaced."""
    return {**REPORTED, device: (commands, REPORTED[device][1])}


# Establishing a surface


def test_traits_are_established_from_what_a_device_reports(facts, at):
    # `cover` declares nothing and `mount` declares MustConnect. Both report
    # Connect and Disconnect, so both satisfy it.
    assert "MustConnect" in facts.traits(at("cover"))
    assert "MustConnect" in facts.traits(at("mount"))


def test_a_device_satisfying_nothing_establishes_nothing(facts, at):
    assert "MustConnect" not in facts.traits(at("cam-acq"))




def test_kind_and_instrument_come_from_the_record(facts, at):
    assert facts.kind(at("pickoff")) == "selector"
    assert facts.kind(at("wheel")) == "device"
    assert not facts.instrument(at("wheel"))
    assert facts.instrument(at("cam-sci"))


def test_the_report_lists_what_each_placement_established(topology, snapshot):
    _, report = BoundSensor.bind(topology, snapshot)

    assert len(report.established) == len(topology.placements())
    assert report.established[0].startswith("'mount' at '<root>'")
    assert "MustConnect" in report.established[0]
    assert report.established[-1] == "'cam-acq' at 'ota/guide/cam-acq' " \
                                     "satisfies no trait"


# Refusing to bind


@pytest.mark.parametrize("device,where", [("cover", "ota"),
                                          ("mount", "<root>")])
def test_a_device_that_reported_nothing_raises(topology, device, where):
    # `cover` declares no trait and `mount` declares one. Silence is a
    # deployment error either way.
    with pytest.raises(ValueError, match=f"'{device}' at '{where}'"):
        BoundSensor.bind(topology, snapshot_of(without(device)))


def test_a_declared_trait_the_device_does_not_satisfy_raises(topology):
    # Without Disable the dome no longer satisfies MustEnable, which its
    # record declares.
    lost = reporting("dome", ("Connect", "Disconnect", "Enable", "Init"))

    with pytest.raises(ValueError,
                       match="'dome' at '<root>' declares trait 'MustEnable'"):
        BoundSensor.bind(topology, snapshot_of(lost))


def test_an_unregistered_declared_trait_says_so():
    structure = Structure(name="tiny", components=(
        Device(device="cam", traits=("Imaginary",), instrument=True),))
    snapshot = snapshot_of({"cam": (("Connect",), ())})

    with pytest.raises(ValueError, match="'Imaginary', which is not registered"):
        BoundSensor.bind(Topology(structure), snapshot)


# Routing by depth














# Sensor scope covers every participant






# Narrowing before the depth rule










# Naming a device outright








def test_binding_answers_a_selection_authored_in_a_table(definition, facts):
    # The bring-up table selects on a capability, which loads with no devices
    # present and is answered here.
    table = next(t for t in definition.tables if t.name == "bring-up")
    phase = next(p for p in table.phases if p.name == "home")
    entry = phase.entries[0]
    admitted = {p.device for p in facts.topology.placements()
                if entry.select.matches(p, facts)}

    assert admitted == {"mount", "pickoff", "foc-sci", "wheel"}


def test_a_bound_sensor_is_what_a_selection_reads(facts):
    assert isinstance(facts, BoundSensor)
