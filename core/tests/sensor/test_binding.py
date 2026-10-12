# SPDX-License-Identifier: Apache-2.0
"""What binding establishes and what it refuses.

The sensor is `sensor.yaml`.
"""
from __future__ import annotations

import pytest

from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.topology import Device, Structure, Topology

from .common import REPORTED, details_of


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


# Refusing to bind


@pytest.mark.parametrize("device,where", [("cover", "ota"),
                                          ("mount", "<root>")])
def test_a_device_that_reported_nothing_raises(topology, device, where):
    # `cover` declares no trait and `mount` declares one. Silence is a
    # deployment error either way.
    with pytest.raises(ValueError, match=f"'{device}' at '{where}'"):
        BoundSensor.bind(topology, details_of(without(device)))


def test_a_declared_trait_the_device_does_not_satisfy_raises(topology):
    # Without Disable the dome no longer satisfies MustEnable, which its
    # record declares.
    lost = reporting("dome", ("Connect", "Disconnect", "Enable", "Init"))

    with pytest.raises(ValueError,
                       match="'dome' at '<root>' declares trait 'MustEnable'"):
        BoundSensor.bind(topology, details_of(lost))


def test_an_unregistered_declared_trait_says_so():
    structure = Structure(components=(
        Device(device="cam", traits=("Imaginary",), instrument=True),))
    details = details_of({"cam": (("Connect",), ())})

    with pytest.raises(ValueError, match="'Imaginary', which is not registered"):
        BoundSensor.bind(Topology(structure), details)


# Answering selections


def test_binding_answers_a_selection_authored_in_a_table(definition, facts):
    # The bring-up table selects on a capability, which loads with no devices
    # present and is answered here.
    table = next(t for t in definition.tables if t.name == "bring-up")
    phase = next(p for p in table.phases if p.name == "home")
    entry = phase.entries[0]
    admitted = {p.device for p in facts.topology.placements()
                if entry.select.matches(p, facts)}

    assert admitted == {"mount", "pickoff", "foc-sci", "wheel"}
