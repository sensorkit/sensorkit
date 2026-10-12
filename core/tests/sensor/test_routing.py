# SPDX-License-Identifier: Apache-2.0
"""Where a command goes.

The sensor is `sensor.yaml`. `Home` is supported at four depths on the science
chain, which is what makes a depth rule, a tie and a narrowing visible without
inventing a second structure.
"""
from __future__ import annotations

import pytest

from sensorkit.sensor.routing import CommandRouter
from sensorkit.sensor.selection import IsKind
from sensorkit.std.traits import (
    Connect,
    Deinit,
    Disconnect,
    Home,
    MoveToPark,
    Stop,
)


@pytest.fixture(scope="module")
def router(facts) -> CommandRouter:
    return CommandRouter(facts)


def test_a_declared_trait_restricts_nothing(router, at):
    # `mount` is declared MustConnect, which says nothing about Stop.
    (routed,) = router.route(Stop(), "sensor", (at("cam-sci"),))

    assert routed.target == at("mount")


# Routing by depth


def test_deepest_wins_at_instrument_scope(router, at):
    # mount, pickoff and cam-guide's chain stops there, so the selector is the
    # deepest thing that homes.
    (routed,) = router.route(Home(), "instrument", (at("cam-guide"),))

    assert routed.target == at("pickoff")


def test_shallowest_wins_at_sensor_scope(router, at):
    (routed,) = router.route(Home(), "sensor", (at("cam-guide"),))

    assert routed.target == at("mount")


def test_subject_is_independent_of_participant_count(router, at):
    # One participant does not make a command instrument scope.
    (routed,) = router.route(Home(), "sensor", (at("cam-sci"),))

    assert routed.target == at("mount")


def test_two_devices_at_one_depth_raise(router, at):
    # foc-sci and wheel both home, both at ota/science.
    with pytest.raises(ValueError, match="at the same position"):
        router.route(Home(), "instrument", (at("cam-sci"),))


def test_a_device_on_two_chains_is_one_candidate(router, at):
    # pickoff is reached from both, and is the deepest on either.
    (routed,) = router.route(Home(), "instrument",
                             (at("cam-guide"), at("cam-acq")))

    assert routed.target == at("pickoff")


def test_a_command_nothing_supports_raises(router, at):
    with pytest.raises(ValueError, match="no device on .* supports 'MoveToPark'"):
        router.route(MoveToPark(), "instrument", (at("cam-sci"),))


# Sensor scope covers every participant


def test_a_sensor_scope_winner_must_be_on_every_chain(router, at):
    # Only cam-sci deinits, and cam-guide never looks through it.
    with pytest.raises(ValueError, match="looks through"):
        router.route(Deinit(), "sensor", (at("cam-sci"), at("cam-guide")))


def test_an_instrument_scope_winner_must_be_on_every_chain(router, at):
    # cam-sci deinits, and nothing on cam-guide's chain does.
    with pytest.raises(ValueError, match="supports 'Deinit'"):
        router.route(Deinit(), "instrument", (at("cam-sci"), at("cam-guide")))


# Narrowing before the depth rule


def test_scope_filters_which_side_of_a_chain_is_eligible(router, at):
    # foc-sci and wheel are private to cam-sci, so shared leaves the selector.
    (routed,) = router.route(Home(), "instrument", (at("cam-sci"),),
                             scope="shared")

    assert routed.target == at("pickoff")


def test_a_private_device_is_eligible_on_its_own_account(router, at):
    (routed,) = router.route(Connect(), "instrument", (at("cam-guide"),),
                             scope="private")

    assert routed.target == at("cam-guide")


def test_a_private_sensor_command_passes_over_shared_devices(router, at):
    # Unscoped, the mount would win as the shallowest device that connects.
    (routed,) = router.route(Connect(), "sensor", (at("cam-guide"),),
                             scope="private")

    assert routed.target == at("cam-guide")


def test_a_device_private_to_one_participant_covers_no_other(router, at):
    # Each camera is private to itself, and neither looks through the other.
    with pytest.raises(ValueError, match="looks through"):
        router.route(Connect(), "sensor", (at("cam-guide"), at("cam-acq")),
                     scope="private")


def test_a_selection_narrows_before_depth_decides(router, at):
    (routed,) = router.route(Home(), "instrument", (at("cam-sci"),),
                             select=IsKind(kind="selector"))

    assert routed.target == at("pickoff")


def test_a_selection_admitting_nothing_raises(router, at):
    with pytest.raises(ValueError, match="satisfies the selection"):
        router.route(Home(), "instrument", (at("cam-sci"),),
                     select=IsKind(kind="instrument"))


# Naming a device outright


def test_a_named_device_settles_what_would_tie(router, at):
    (routed,) = router.route(Home(), "instrument", (at("cam-sci"),),
                             device="wheel")

    assert routed.target == at("wheel")


def test_a_named_device_off_the_participating_chains_raises(router, at):
    with pytest.raises(ValueError, match="'cam-guide' is not on"):
        router.route(Connect(), "instrument", (at("cam-sci"),),
                     device="cam-guide")


def test_naming_a_device_does_not_skip_the_support_check(router, at):
    with pytest.raises(ValueError,
                       match="'cam-acq' does not support 'Disconnect'"):
        router.route(Disconnect(), "instrument", (at("cam-acq"),),
                     device="cam-acq")


def test_naming_a_device_does_not_skip_the_scope(router, at):
    # The wheel is private to cam-sci.
    with pytest.raises(ValueError, match="supports 'Home' as a shared device"):
        router.route(Home(), "instrument", (at("cam-sci"),), device="wheel",
                     scope="shared")


def test_naming_a_device_does_not_skip_the_selection(router, at):
    with pytest.raises(ValueError, match="supports 'Home' and satisfies the selection"):
        router.route(Home(), "instrument", (at("cam-sci"),), device="wheel",
                     select=IsKind(kind="selector"))


# Routing without participants


def test_nothing_is_supported_without_participants(router):
    assert not router.supported_on("Home", ())


def test_an_instrument_command_without_participants_routes_nowhere(router):
    assert router.route(Home(), "instrument", ()) == ()
    assert router.route(Home(), "instrument", (), device="mount") == ()


def test_a_sensor_command_without_participants_raises(router):
    with pytest.raises(ValueError, match="no device on <no instrument> supports 'Home'"):
        router.route(Home(), "sensor", ())

    with pytest.raises(ValueError, match="'mount' is not on <no instrument>"):
        router.route(Home(), "sensor", (), device="mount")
