# SPDX-License-Identifier: Apache-2.0
"""What binding establishes, what it refuses, and where a command goes.

The sensor is `sensor.yaml`. `Home` is supported at four depths on the science
chain, which is what makes a depth rule, a tie and a narrowing visible without
inventing a second structure.
"""
from __future__ import annotations

import pytest

from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.selection import IsKind
from sensorkit.sensor.topology import Device, Structure, Topology
from sensorkit.std.traits import (
    Connect,
    Deinit,
    Disconnect,
    Home,
    MoveToPark,
    Stop,
)

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


def test_a_declared_trait_restricts_nothing(facts, at):
    # `mount` is declared MustConnect, which says nothing about Stop.
    (routed,) = facts.route(Stop(), "sensor", (at("cam-sci"),))

    assert routed.target == at("mount")


def test_kind_and_instrument_come_from_the_record(facts, at):
    assert facts.kind(at("pickoff")) == "selector"
    assert facts.kind(at("wheel")) == "device"
    assert not facts.instrument(at("wheel"))
    assert facts.instrument(at("cam-sci"))


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


# Routing by depth


def test_deepest_wins_at_instrument_scope(facts, at):
    # mount, pickoff and cam-guide's chain stops there, so the selector is the
    # deepest thing that homes.
    (routed,) = facts.route(Home(), "instrument", (at("cam-guide"),))

    assert routed.target == at("pickoff")


def test_shallowest_wins_at_sensor_scope(facts, at):
    (routed,) = facts.route(Home(), "sensor", (at("cam-guide"),))

    assert routed.target == at("mount")


def test_subject_is_independent_of_participant_count(facts, at):
    # One participant does not make a command instrument scope.
    (routed,) = facts.route(Home(), "sensor", (at("cam-sci"),))

    assert routed.target == at("mount")


def test_two_devices_at_one_depth_raise(facts, at):
    # foc-sci and wheel both home, both at ota/science.
    with pytest.raises(ValueError, match="at the same position"):
        facts.route(Home(), "instrument", (at("cam-sci"),))


def test_a_device_on_two_chains_is_one_candidate(facts, at):
    # pickoff is reached from both, and is the deepest on either.
    (routed,) = facts.route(Home(), "instrument",
                         (at("cam-guide"), at("cam-acq")))

    assert routed.target == at("pickoff")


def test_a_command_nothing_supports_raises(facts, at):
    with pytest.raises(ValueError, match="no device on .* supports 'MoveToPark'"):
        facts.route(MoveToPark(), "instrument", (at("cam-sci"),))


# Sensor scope covers every participant


def test_a_sensor_scope_winner_must_be_on_every_chain(facts, at):
    # Only cam-sci deinits, and cam-guide never looks through it.
    with pytest.raises(ValueError, match="looks through"):
        facts.route(Deinit(), "sensor", (at("cam-sci"), at("cam-guide")))


def test_an_instrument_scope_winner_must_be_on_every_chain(facts, at):
    # cam-sci deinits, and nothing on cam-guide's chain does.
    with pytest.raises(ValueError, match="supports 'Deinit'"):
        facts.route(Deinit(), "instrument", (at("cam-sci"), at("cam-guide")))


# Narrowing before the depth rule


def test_scope_filters_which_side_of_a_chain_is_eligible(facts, at):
    # foc-sci and wheel are private to cam-sci, so shared leaves the selector.
    (routed,) = facts.route(Home(), "instrument", (at("cam-sci"),),
                         scope="shared")

    assert routed.target == at("pickoff")


def test_a_private_device_is_eligible_on_its_own_account(facts, at):
    (routed,) = facts.route(Connect(), "instrument", (at("cam-guide"),),
                         scope="private")

    assert routed.target == at("cam-guide")


def test_a_selection_narrows_before_depth_decides(facts, at):
    (routed,) = facts.route(Home(), "instrument", (at("cam-sci"),),
                         select=IsKind(kind="selector"))

    assert routed.target == at("pickoff")


def test_a_selection_admitting_nothing_raises(facts, at):
    with pytest.raises(ValueError, match="satisfies the selection"):
        facts.route(Home(), "instrument", (at("cam-sci"),),
                    select=IsKind(kind="instrument"))


# Naming a device outright


def test_a_named_device_settles_what_would_tie(facts, at):
    (routed,) = facts.route(Home(), "instrument", (at("cam-sci"),),
                         device="wheel")

    assert routed.target == at("wheel")


def test_a_named_device_off_the_participating_chains_raises(facts, at):
    with pytest.raises(ValueError, match="'cam-guide' is not on"):
        facts.route(Connect(), "instrument", (at("cam-sci"),),
                    device="cam-guide")


def test_naming_a_device_does_not_skip_the_support_check(facts, at):
    with pytest.raises(ValueError,
                       match="'cam-acq' does not support 'Disconnect'"):
        facts.route(Disconnect(), "instrument", (at("cam-acq"),),
                    device="cam-acq")


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
