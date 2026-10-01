# SPDX-License-Identifier: Apache-2.0
"""Expanding a request into units, routing a setting, and packing an intent.

Intents are authored by hand, which is what a caller building a collect
directly does and what the standard task adapter will produce. Segmented
requests are authored here too, so the assignment contract is exercised before
anything generates one.

The sensor is `sensor.yaml` except where a case needs hardware it does not
have, and those cases build their own structure.
"""
from __future__ import annotations

import pytest

from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    BoundCollect,
    CollectIntent,
    CommandRequest,
    InstrumentRequest,
    PackingConflict,
    RequestEpoch,
    SettingUnsatisfiable,
    pack,
)
from sensorkit.sensor.selection import (
    AnyOf,
    HasTag,
    IsInstrument,
    IsRef,
    Supports,
)
from sensorkit.std.collect import CameraParameterSet
from sensorkit.std.collect import Collect as CollectMetadata
from sensorkit.std.instrument import Binning, ConfigureCameraSensor
from sensorkit.std.optics import SetFilter
from sensorkit.std.traits import Home

from .common import REPORTED, TARGET, details_of, sensor_of


def asking(id: str, count: int = 1, seconds: float = 1.0, **kw
           ) -> InstrumentRequest:
    """One instrument request, with the acquisition fields inlined."""
    acquisition = AcquisitionRequest(
        integration_time_s=seconds, count=count,
        distribute=kw.pop("distribute", "one"),
        timeout_s=kw.pop("timeout_s", None))

    return InstrumentRequest(id=id, acquisition=acquisition, **kw)


def intent_of(*epochs: RequestEpoch, **kw) -> CollectIntent:
    """One collect intent over these authored epochs."""
    return CollectIntent(name="demo", epochs=epochs, **kw)


def taken(collect: BoundCollect) -> list[list[str]]:
    """Which instrument takes each unit, one list per epoch."""
    return [[unit.target.device for unit in epoch.units]
            for epoch in collect.epochs]


def numbered(collect: BoundCollect, request: str,
             device: str) -> list[tuple[int, int]]:
    """Each unit of one request on one instrument, as ordinal and frame number,
    in the order they are taken."""
    return [(u.acquisition.index, u.acquisition.frame_number)
            for epoch in collect.epochs for u in epoch.units
            if u.acquisition.request == request and u.target.device == device]


def settings_of(collect: BoundCollect) -> list[list[str]]:
    """Which device each epoch's settings land on."""
    return [[routed.target.device for routed in epoch.settings]
            for epoch in collect.epochs]


BINNED = {**REPORTED, "cover": (("Connect", "Disconnect",
                                 "ConfigureCameraSensor"), ())}
"""`cover` can be configured, which is what puts two guide-port instruments on
one device that holds a value."""


def binning(x: float, **kw) -> CommandRequest:
    """A setting with a value in it, so two of them can disagree."""
    return CommandRequest(command=ConfigureCameraSensor(binning=Binning(x=x,
                                                                        y=x)),
                          **kw)


# expand


def test_a_request_expands_to_one_unit_per_count(at):
    units = asking("a", count=3, seconds=2.5).expand(at("cam-sci"), 0, 0)

    assert [u.acquisition.index for u in units] == [0, 1, 2]
    assert [u.estimated_duration_s for u in units] == [2.5, 2.5, 2.5]


def test_units_number_from_the_frame_base_they_are_given(at):
    units = asking("a", count=2).expand(at("cam-sci"), 7, 0)

    assert [u.acquisition.frame_number for u in units] == [7, 8]
    assert [u.acquisition.index for u in units] == [0, 1]


def test_requests_without_collect_metadata_add_no_keywords(at):
    units = asking("a", count=2).expand(at("cam-sci"), 0, 0)

    assert all(not u.acquisition.keywords for u in units)
    assert all(u.acquisition.request == "a" for u in units)


def test_each_frame_copies_and_numbers_its_collect_metadata(at):
    metadata = CollectMetadata(target=TARGET,
                               params=CameraParameterSet(integration_time_seconds=1.0,
                                                         frame_count=2))
    units = asking("a", count=2, collect=metadata).expand(at("cam-sci"), 5, 0)

    assert [u.acquisition.keywords[CollectMetadata].frame_number for u in units] == [5, 6]
    assert metadata.frame_number == 0
    units[0].acquisition.keywords[CollectMetadata].params.gain = 5.0
    assert units[1].acquisition.keywords[CollectMetadata].params.gain is None
    assert metadata.params.gain is None


def test_a_unit_command_is_whole_but_for_the_header(at):
    unit = asking("a", seconds=4.0).expand(at("cam-sci"), 0, 0)[0]

    assert unit.command.integration_time == 4.0
    assert unit.command.context is None


def test_a_unit_carries_the_authored_deadline(at):
    unit = asking("a", timeout_s=12.0).expand(at("cam-sci"), 0, 0)[0]

    assert unit.timeout_s == 12.0


# resolve_setting


def test_a_setting_routes_to_the_deepest_candidate(facts, at):
    (routed,) = CommandRequest(command=Home(), device="wheel").resolve(
        (at("cam-sci"),), facts)

    assert routed.target == at("wheel")


def test_a_sensor_scope_setting_routes_to_the_shallowest(facts, at):
    (routed,) = CommandRequest(command=Home(), subject="sensor").resolve(
        (at("cam-sci"), at("cam-guide")), facts)

    assert routed.target == at("mount")


def test_a_resolved_setting_carries_its_authored_deadline(facts, at):
    request = CommandRequest(command=Home(), subject="sensor", timeout_s=45.0)
    (routed,) = request.resolve((at("cam-sci"),), facts)

    assert routed.timeout_s == 45.0


def test_a_command_nothing_on_the_chains_supports_is_unsatisfiable(facts, at):
    with pytest.raises(SettingUnsatisfiable,
                       match="supports 'ConfigureCameraSensor'"):
        binning(2.0).resolve((at("cam-sci"),), facts)


def test_two_candidates_at_one_position_is_a_routing_error(facts, at):
    with pytest.raises(ValueError, match="at the same position"):
        CommandRequest(command=Home()).resolve((at("cam-sci"),), facts)


def test_a_named_device_off_the_chains_reports_itself(facts, at):
    with pytest.raises(ValueError, match="'wheel' is not on"):
        CommandRequest(command=Home(), device="wheel").resolve(
            (at("cam-guide"),), facts)


def test_a_named_device_is_not_obscured_by_the_support_precheck(facts, at):
    # `cover` is on the chain and supports nothing of the kind, and naming it
    # is a different mistake from asking for a value nothing can establish.
    with pytest.raises(ValueError, match="'cover' does not support 'Home'"):
        CommandRequest(command=Home(), device="cover").resolve(
            (at("cam-sci"),), facts)


# Choosing participants


def test_a_request_is_assigned_whole_to_one_instrument(facts):
    collect = pack(intent_of(RequestEpoch(units=(asking("a", count=3),))),
                   facts)

    assert taken(collect) == [["cam-sci"] * 3]


def test_fan_out_gives_every_eligible_instrument_the_whole_count(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", count=2, distribute="each",
                   select=HasTag(tag="guiding")),))),
        facts)

    assert taken(collect) == [["cam-guide", "cam-guide"]]


def test_select_is_answered_at_the_instrument(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-acq")),))), facts)

    assert taken(collect) == [["cam-acq"]]


def test_requires_is_answered_over_the_chain(facts):
    # Nothing on the guide port focuses, so only the science chain qualifies.
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", requires=(Supports(supports="Home"),),
                   select=IsRef(device="cam-sci")),))), facts)

    assert taken(collect) == [["cam-sci"]]


def test_prefers_ranks_without_excluding(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", prefers=(IsRef(device="cam-acq"),)),))), facts)

    assert taken(collect) == [["cam-acq"]]


def test_a_request_nothing_admits_raises(facts):
    with pytest.raises(ValueError, match="no instrument satisfies 'a'"):
        pack(intent_of(RequestEpoch(units=(
            asking("a", select=HasTag(tag="absent")),))), facts)


def test_the_least_loaded_candidate_is_preferred(facts):
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", count=4,
                                       select=IsInstrument()),)),
            RequestEpoch(units=(asking("b", select=IsInstrument()),))),
        facts)

    assert taken(collect) == [["cam-sci"] * 4, ["cam-guide"]]


def test_a_divided_request_weighs_its_whole_length(facts):
    # The first segment is short and the second long. Charged by segment,
    # 'c' would join 'a' on the instrument its whole length is about to fill.
    guide_port = AnyOf(any_of=(IsRef(device="cam-guide"),
                               IsRef(device="cam-acq")))
    collect = pack(
        intent_of(
            RequestEpoch(units=(
                asking("a", seconds=10.0, assignment="a", select=guide_port),
                asking("b", seconds=50.0, select=guide_port),
                asking("c", select=guide_port))),
            RequestEpoch(units=(asking("a", count=9, seconds=10.0,
                                       assignment="a", select=guide_port),))),
        facts)

    assert taken(collect) == [["cam-guide", "cam-acq", "cam-acq"],
                              ["cam-guide"] * 9]


# Epochs


def test_an_authored_boundary_is_never_crossed(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(asking("a"),)),
                  RequestEpoch(units=(asking("b", select=IsRef(
                      device="cam-sci")),))),
        facts)

    assert taken(collect) == [["cam-sci"], ["cam-sci"]]


def test_incompatible_participants_open_a_child_epoch(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-sci")),
            asking("b", select=IsRef(device="cam-guide"))))),
        facts)

    assert taken(collect) == [["cam-sci"], ["cam-guide"]]


def test_a_child_keeps_its_parent_alignment_and_settings(facts):
    collect = pack(
        intent_of(RequestEpoch(
            align="midpoint",
            settings=(CommandRequest(command=Home(), subject="sensor"),),
            units=(asking("a", select=IsRef(device="cam-sci")),
                   asking("b", select=IsRef(device="cam-guide"))))),
        facts)

    assert [epoch.align for epoch in collect.epochs] == ["midpoint",
                                                         "midpoint"]
    assert settings_of(collect) == [["mount"], ["mount"]]


def test_one_device_asked_for_two_values_opens_a_child(topology):
    sensor, _ = BoundSensor.bind(topology, details_of(BINNED))
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-guide"),
                   settings=(binning(1.0),)),
            asking("b", select=IsRef(device="cam-acq"),
                   settings=(binning(2.0),))))),
        sensor)

    assert taken(collect) == [["cam-guide"], ["cam-acq"]]
    assert settings_of(collect) == [["cover"], ["cover"]]


def test_one_device_asked_for_two_values_alone_raises(topology):
    sensor, _ = BoundSensor.bind(topology, details_of(BINNED))

    with pytest.raises(PackingConflict, match="holds one value per epoch"):
        pack(intent_of(RequestEpoch(units=(
            asking("a", distribute="each",
                   select=IsRef(device="cam-guide"),
                   settings=(binning(1.0), binning(2.0))),))), sensor)


def test_two_deadlines_for_one_command_is_a_planning_error(topology):
    sensor, _ = BoundSensor.bind(topology, details_of(BINNED))

    with pytest.raises(ValueError, match="nothing here chooses between them"):
        pack(intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-guide"),
                   settings=(binning(1.0, timeout_s=5.0),)),
            asking("b", select=IsRef(device="cam-acq"),
                   settings=(binning(1.0, timeout_s=9.0),))))), sensor)


def test_a_setting_commanding_a_positioning_selector_is_rejected(facts):
    with pytest.raises(ValueError, match="do not command 'pickoff'"):
        pack(intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-sci"),
                   settings=(CommandRequest(command=Home(),
                                            device="pickoff"),)),))), facts)


def test_settings_are_flat_and_kept_once(facts):
    collect = pack(
        intent_of(RequestEpoch(
            settings=(CommandRequest(command=Home(), subject="sensor"),),
            units=(asking("a", distribute="each",
                          select=HasTag(tag="guiding"),
                          settings=(CommandRequest(command=Home(),
                                                   device="mount"),)),))),
        facts)

    assert settings_of(collect) == [["mount"]]


TWO_MOUNTS = """
    name: pair
    components:
      - unit: east
        components:
          - device: mount-e
          - device: cam-e
            instrument: true
      - unit: west
        components:
          - device: mount-w
          - device: cam-w
            instrument: true
    """

PAIRED = {
    "mount-e": (("Connect", "Home"), ()),
    "mount-w": (("Connect", "Home"), ()),
    "cam-e": (("Connect",), ()),
    "cam-w": (("Connect",), ()),
}


def test_participants_with_no_common_device_open_a_child_epoch():
    sensor = sensor_of(TWO_MOUNTS, PAIRED)
    collect = pack(
        intent_of(RequestEpoch(
            settings=(CommandRequest(command=Home(), subject="sensor"),),
            units=(asking("a", select=IsRef(device="cam-e")),
                   asking("b", select=IsRef(device="cam-w"))))),
        sensor)

    assert taken(collect) == [["cam-e"], ["cam-w"]]
    assert settings_of(collect) == [["mount-e"], ["mount-w"]]


# Frame numbering, prepare and cleanup


def test_frame_numbers_run_per_instrument_across_epochs(facts):
    on_science = IsRef(device="cam-sci")
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", count=2, select=on_science),)),
            RequestEpoch(units=(asking("b", count=2, select=on_science),))),
        facts)
    numbers = [unit.acquisition.frame_number
               for epoch in collect.epochs for unit in epoch.units]

    assert numbers == [0, 1, 2, 3]


def test_two_instruments_number_their_own_frames(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", count=2, select=IsRef(device="cam-sci")),
            asking("b", count=2, select=IsRef(device="cam-guide"))))),
        facts)

    assert [[u.acquisition.frame_number for u in epoch.units]
            for epoch in collect.epochs] == [[0, 1], [0, 1]]


def test_prepare_and_cleanup_route_against_every_participant(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(asking("a"),)),
                  prepare=(CommandRequest(command=Home(), subject="sensor",
                                          timeout_s=30.0),),
                  cleanup=(CommandRequest(command=Home(), subject="sensor"),)),
        facts)

    assert [r.target.device for r in collect.prepare] == ["mount"]
    assert collect.prepare[0].timeout_s == 30.0
    assert [r.target.device for r in collect.cleanup] == ["mount"]


def test_a_request_taking_no_acquisition_raises(facts):
    with pytest.raises(ValueError, match="drop the request instead"):
        pack(intent_of(RequestEpoch(units=(asking("a", count=0),))), facts)


def test_a_negative_integration_raises(facts):
    with pytest.raises(ValueError, match="integrates for -1.0 seconds"):
        pack(intent_of(RequestEpoch(units=(asking("a", seconds=-1.0),))),
             facts)


# Assignment groups


def test_the_segments_of_one_request_share_an_instrument(facts):
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", count=2, assignment="whole"),)),
            RequestEpoch(units=(asking("a", count=3, assignment="whole"),))),
        facts)

    assert taken(collect) == [["cam-sci"] * 2, ["cam-sci"] * 3]


def test_a_group_weighs_every_segment_before_it_chooses(facts):
    # The first segment alone would take `cam-sci`, which the second cannot
    # use. Nothing is reassigned to make up for that.
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", assignment="whole"),)),
            RequestEpoch(units=(asking("a", assignment="whole",
                                       requires=(IsRef(device="cam-guide"),)),
                                ))),
        facts)

    assert taken(collect) == [["cam-guide"], ["cam-guide"]]


def test_a_group_no_instrument_satisfies_rejects_planning(facts):
    with pytest.raises(ValueError, match="no instrument satisfies"):
        pack(intent_of(
            RequestEpoch(units=(asking("a", assignment="whole",
                                       requires=(IsRef(device="cam-sci"),)),)),
            RequestEpoch(units=(asking("a", assignment="whole",
                                       requires=(IsRef(device="cam-guide"),)),
                                ))), facts)


def test_fan_out_fixes_one_participant_set_for_a_group(facts):
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", distribute="each",
                                       assignment="whole",
                                       select=IsRef(device="cam-guide")),)),
            RequestEpoch(units=(asking("a", count=2, distribute="each",
                                       assignment="whole",
                                       select=IsInstrument()),))),
        facts)

    assert taken(collect) == [["cam-guide"], ["cam-guide"] * 2]


def test_a_group_keeps_its_counts_per_segment(facts):
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", count=2, assignment="whole"),)),
            RequestEpoch(units=(asking("a", count=3, assignment="whole"),))),
        facts)
    numbers = [u.acquisition.frame_number
               for epoch in collect.epochs for u in epoch.units]

    assert numbers == [0, 1, 2, 3, 4]


def test_segments_disagreeing_about_fan_out_raise(facts):
    with pytest.raises(ValueError, match="mixes distribute values"):
        pack(intent_of(
            RequestEpoch(units=(asking("a", assignment="whole"),)),
            RequestEpoch(units=(asking("a", assignment="whole",
                                       distribute="each"),))), facts)


def test_two_unnamed_requests_are_their_own_assignment_units(facts):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-sci")),
            asking("b", select=IsRef(device="cam-guide"))))),
        facts)

    assert taken(collect) == [["cam-sci"], ["cam-guide"]]


def test_segments_continue_one_request_sequence(facts):
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", count=2, assignment="whole"),)),
            RequestEpoch(units=(asking("a", count=3, assignment="whole"),))),
        facts)
    units = [u for epoch in collect.epochs for u in epoch.units]

    assert {u.acquisition.request for u in units} == {"a"}
    assert {u.target.device for u in units} == {"cam-sci"}
    assert [u.acquisition.index for u in units] == [0, 1, 2, 3, 4]


def test_fan_out_segments_number_each_instrument_whole(facts):
    guide_port = AnyOf(any_of=(IsRef(device="cam-guide"),
                               IsRef(device="cam-acq")))
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", count=2, distribute="each",
                                       assignment="whole",
                                       select=guide_port),)),
            RequestEpoch(units=(asking("a", count=3, distribute="each",
                                       assignment="whole",
                                       select=guide_port),))),
        facts)
    whole = [(n, n) for n in range(5)]

    assert numbered(collect, "a", "cam-guide") == whole
    assert numbered(collect, "a", "cam-acq") == whole


def test_earlier_work_advances_frames_but_not_a_request_ordinal(facts):
    on_science = IsRef(device="cam-sci")
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("b", count=2, select=on_science),)),
            RequestEpoch(units=(asking("a", count=2, assignment="whole",
                                       select=on_science),)),
            RequestEpoch(units=(asking("a", assignment="whole",
                                       select=on_science),))),
        facts)

    assert numbered(collect, "a", "cam-sci") == [(0, 2), (1, 3), (2, 4)]


def test_interleaved_groups_keep_their_own_ordinals(facts):
    on_science = IsRef(device="cam-sci")
    collect = pack(
        intent_of(
            RequestEpoch(units=(
                asking("a", count=2, assignment="a", select=on_science),
                asking("b", assignment="b", select=on_science))),
            RequestEpoch(units=(
                asking("a", assignment="a", select=on_science),
                asking("b", count=2, assignment="b", select=on_science)))),
        facts)

    assert numbered(collect, "a", "cam-sci") == [(0, 0), (1, 1), (2, 3)]
    assert numbered(collect, "b", "cam-sci") == [(0, 2), (1, 4), (2, 5)]


# Epoch settings in eligibility


SPLIT_ROLES = {
    **REPORTED,
    "wheel": (("Connect", "Disconnect", "Home", "SetFilter"), ()),
    "cam-guide": (("Connect", "Disconnect", "Init", "ConfigureCameraSensor"),
                  ()),
}
"""Only the science chain changes a filter and only `cam-guide` bins, so an
epoch setting of either kind rules out every chain but one."""

TIED = {
    **SPLIT_ROLES,
    "foc-sci": (("Connect", "Disconnect", "Home", "ConfigureCameraSensor"),
                ()),
    "wheel": (("Connect", "Disconnect", "Home", "ConfigureCameraSensor"), ()),
}
"""Binning ties between two devices on the science chain and routes cleanly on
the guide camera."""


def filter_of(name: str, **kw) -> CommandRequest:
    return CommandRequest(command=SetFilter(filter=name), **kw)


def test_an_epoch_setting_passes_over_a_candidate_that_cannot_take_it(
        topology):
    sensor, _ = BoundSensor.bind(topology, details_of(SPLIT_ROLES))
    collect = pack(
        intent_of(RequestEpoch(settings=(binning(2.0),),
                               units=(asking("a"),))),
        sensor)

    assert taken(collect) == [["cam-guide"]]
    assert settings_of(collect) == [["cam-guide"]]


def test_a_later_epoch_setting_decides_a_group_assignment(topology):
    sensor, _ = BoundSensor.bind(topology, details_of(SPLIT_ROLES))
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", assignment="whole"),)),
            RequestEpoch(settings=(binning(2.0),),
                         units=(asking("a", assignment="whole"),))),
        sensor)

    assert taken(collect) == [["cam-guide"], ["cam-guide"]]


def test_a_group_no_instrument_can_be_set_up_for_rejects_planning(topology):
    sensor, _ = BoundSensor.bind(topology, details_of(SPLIT_ROLES))

    with pytest.raises(SettingUnsatisfiable):
        pack(intent_of(
            RequestEpoch(settings=(filter_of("g"),),
                         units=(asking("a", assignment="whole"),)),
            RequestEpoch(settings=(binning(2.0),),
                         units=(asking("a", assignment="whole"),))), sensor)


@pytest.fixture(scope="module")
def bench() -> BoundSensor:
    """Two cameras behind one wheel, one behind its own, and one behind none."""
    return sensor_of("""
        name: bench
        components:
          - device: cam-bare
            instrument: true
            tags: [survey]
          - unit: east
            components:
              - device: wheel-e
              - device: cam-e1
                instrument: true
                tags: [survey]
              - device: cam-e2
                instrument: true
                tags: [survey]
          - unit: west
            components:
              - device: wheel-w
              - device: cam-w
                instrument: true
        """, {
        "cam-bare": (("Connect",), ()),
        "wheel-e": (("Connect", "SetFilter"), ()),
        "cam-e1": (("Connect",), ()),
        "cam-e2": (("Connect",), ()),
        "wheel-w": (("Connect", "SetFilter"), ()),
        "cam-w": (("Connect",), ()),
    })


def test_fan_out_takes_only_instruments_satisfying_the_whole_group(bench):
    survey = HasTag(tag="survey")
    collect = pack(
        intent_of(
            RequestEpoch(units=(asking("a", distribute="each",
                                       assignment="whole", select=survey),)),
            RequestEpoch(settings=(filter_of("r"),),
                         units=(asking("a", distribute="each",
                                       assignment="whole", select=survey),))),
        bench)

    assert taken(collect) == [["cam-e1", "cam-e2"], ["cam-e1", "cam-e2"]]
    assert settings_of(collect) == [[], ["wheel-e"]]


def test_an_epoch_setting_lands_on_each_participant_chain(bench):
    collect = pack(
        intent_of(RequestEpoch(
            settings=(filter_of("r"),),
            units=(asking("a", select=IsRef(device="cam-e1")),
                   asking("b", select=IsRef(device="cam-w"))))),
        bench)

    assert taken(collect) == [["cam-e1", "cam-w"]]
    assert settings_of(collect) == [["wheel-e", "wheel-w"]]


def test_a_fanned_out_epoch_setting_commands_every_private_device(bench):
    collect = pack(
        intent_of(RequestEpoch(
            settings=(filter_of("r"),),
            units=(asking("a", count=2, distribute="each",
                          select=AnyOf(any_of=(IsRef(device="cam-e1"),
                                               IsRef(device="cam-w")))),))),
        bench)

    assert taken(collect) == [["cam-e1", "cam-e1", "cam-w", "cam-w"]]
    assert settings_of(collect) == [["wheel-e", "wheel-w"]]


def test_an_instrument_prepare_lands_on_each_participant_chain(bench):
    collect = pack(
        intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-e1")),
            asking("b", select=IsRef(device="cam-w")))),
            prepare=(filter_of("r"),)),
        bench)

    assert [r.target.device for r in collect.prepare] == ["wheel-e",
                                                          "wheel-w"]


def test_an_instrument_prepare_one_chain_cannot_take_is_unsatisfiable(bench):
    with pytest.raises(SettingUnsatisfiable, match="cam-bare"):
        pack(intent_of(RequestEpoch(units=(
            asking("a", select=IsRef(device="cam-bare")),
            asking("b", select=IsRef(device="cam-w")))),
            prepare=(filter_of("r"),)), bench)


@pytest.mark.parametrize(("reported", "setting", "match"), [
    (TIED, binning(2.0), "at the same position"),
    (SPLIT_ROLES, filter_of("g", device="wheel"), "'wheel' is not on"),
    (SPLIT_ROLES, filter_of("g", select=HasTag(tag="absent")),
     "satisfies the selection"),
], ids=["ambiguous", "named-off-chain", "selected-away"])
def test_a_malformed_epoch_setting_raises_rather_than_passing_over(
        topology, reported, setting, match):
    # Another instrument could take each of these, so a failure read as
    # ineligibility would quietly pick it instead.
    sensor, _ = BoundSensor.bind(topology, details_of(reported))

    with pytest.raises(ValueError, match=match) as raised:
        pack(intent_of(RequestEpoch(settings=(setting,),
                                    units=(asking("a"),))), sensor)

    assert not isinstance(raised.value, SettingUnsatisfiable)
