# SPDX-License-Identifier: Apache-2.0
"""Translating a standard collect task, and packing what it becomes.

Translation reads no sensor, so its cases take no fixture. Packing cases bind a
two-instrument bench under one mount, where only the east chain changes a
filter and both cameras take a sensor configuration, so which exposure goes
where follows from what it asks for.
"""
from __future__ import annotations

import pytest

from sensorkit.astro.coords import Equatorial
from sensorkit.astro.target import CatalogTarget, ICRSTarget
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import (
    BoundCollect,
    CollectIntent,
    SettingUnsatisfiable,
    pack,
)
from sensorkit.sensor.standard_task import SIDEREAL, translate
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import Binning, ConfigureCameraSensor
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import SetFilter
from sensorkit.std.traits import Stop

from .common import TARGET, filters, sensor_of

STAR = ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0))

BENCH = """
    name: bench
    components:
      - device: mount
      - unit: east
        components:
          - device: wheel-e
          - device: cam-e
            instrument: true
      - unit: west
        components:
          - device: cam-w
            instrument: true
    """

REPORTED = {
    "mount": (("Connect", "FollowTarget", "Stop"), ()),
    "wheel-e": (("Connect", "SetFilter"), ()),
    "cam-e": (("Connect", "ConfigureCameraSensor"), ()),
    "cam-w": (("Connect", "ConfigureCameraSensor"), ()),
}
"""Only the east chain changes a filter, and both cameras bin."""


@pytest.fixture(scope="module")
def bench() -> BoundSensor:
    """The bench bound to what its devices report."""
    return sensor_of(BENCH, REPORTED)


def params(frame_count: int = 1, **kw) -> CameraParameterSet:
    return CameraParameterSet(integration_time_seconds=1.0,
                              frame_count=frame_count, **kw)


def task(*exposures: CameraParameterSet, target=TARGET,
         sidereal_frames: tuple[int, ...] = (), **kw) -> StandardCollectTask:
    return StandardCollectTask(target=target,
                               camera_params=list(exposures) or params(),
                               sidereal_frames=list(sidereal_frames), **kw)


SEVERAL = task(params(4, filter_name="r"),
               params(2, binning_x=2, binning_y=2, gain=1.5),
               params(3, filter_name="r"),
               sidereal_frames=(2,))
"""Three exposures of four, two and three frames, the third frame sidereal.

The first and third need the east wheel. The second can go either way, and
the east camera is already committed to the whole of the first."""


def counts(intent: CollectIntent) -> list[list[tuple[str, int]]]:
    """Each epoch's segments, as request id and frame count."""
    return [[(request.id, request.acquisition.count) for request in epoch.units]
            for epoch in intent.epochs]


def numbered(collect: BoundCollect, request: str) -> list[tuple[int, int]]:
    """One request's units in the order taken, as ordinal and frame number."""
    return [(u.acquisition.index, u.acquisition.frame_number)
            for epoch in collect.epochs for u in epoch.units
            if u.acquisition.request == request]


def units_of(collect: BoundCollect):
    return [unit for epoch in collect.epochs for unit in epoch.units]


# Translation


def test_exposures_share_each_tracking_segment():
    assert counts(translate(SEVERAL)) == [
        [("exposure-0", 2), ("exposure-1", 2), ("exposure-2", 2)],
        [("exposure-0", 1), ("exposure-2", 1)],
        [("exposure-0", 1)],
    ]


def test_each_segment_follows_its_scheduled_target():
    intent = translate(SEVERAL)

    assert [[s.command for s in epoch.settings] for epoch in intent.epochs] == [
        [FollowTarget(target=TARGET)], [FollowTarget(target=SIDEREAL)],
        [FollowTarget(target=TARGET)]]
    assert all(s.subject == "sensor"
               for epoch in intent.epochs for s in epoch.settings)


def test_every_segment_keeps_its_exposure_identity():
    requests = [r for epoch in translate(SEVERAL).epochs for r in epoch.units]

    assert all(r.assignment == r.id for r in requests)
    assert all(r.acquisition.distribute == "one" for r in requests)
    assert all(r.requires == () and r.prefers == () for r in requests)


def test_a_sidereal_target_is_one_segment():
    intent = translate(task(params(3), target=STAR, sidereal_frames=(1,)))

    assert counts(intent) == [[("exposure-0", 3)]]


def test_pointing_is_preparation_and_stopping_it_is_cleanup():
    intent = translate(task(params(2), sidereal_frames=(0,)))

    assert [r.command for r in intent.prepare] == [FollowTarget(target=TARGET)]
    assert [r.command for r in intent.cleanup] == [Stop()]
    assert intent.epochs[0].settings[0].command == FollowTarget(
        target=SIDEREAL)


def test_a_collect_taking_no_frames_points_nowhere():
    intent = translate(task(params(0)))

    assert (intent.epochs, intent.prepare, intent.cleanup) == ((), (), ())


@pytest.mark.parametrize(("exposure", "commands"), [
    (params(), []),
    (params(filter_name="r"), [SetFilter(filter="r")]),
    (params(gain=1.5), [ConfigureCameraSensor(gain=1.5)]),
    (params(binning_x=2, binning_y=1, gain=1.5),
     [ConfigureCameraSensor(binning=Binning(x=2, y=1), gain=1.5)]),
], ids=["nothing", "filter", "gain", "binning-and-gain"])
def test_requested_camera_parameters_are_commanded(exposure, commands):
    request = translate(task(exposure)).epochs[0].units[0]

    assert [s.command for s in request.settings] == commands
    assert all(s.subject == "instrument" for s in request.settings)
    # Binning and gain settings carry no target requirements or preferences.
    assert all(not (s.requires or s.prefers) for s in request.settings
               if not isinstance(s.command, SetFilter))


def test_collect_metadata_holds_the_requested_target_and_parameters():
    exposure = params(filter_name="r", gain=1.5)
    request = translate(task(exposure, target_id="probe")).epochs[0].units[0]
    planned = request.collect

    assert isinstance(planned, Collect)
    assert (planned.target, planned.target_id, planned.params) == (
        TARGET, "probe", exposure)


@pytest.mark.parametrize(("target", "expected"), [
    (CatalogTarget(object="M31"), "M31"),
    (STAR, None),
], ids=["catalog-name", "nothing-to-infer"])
def test_a_target_id_is_inferred_where_the_task_does_not_say(target, expected):
    request = translate(task(target=target)).epochs[0].units[0]

    assert request.collect.target_id == expected


@pytest.mark.parametrize(("collect", "match"), [
    (task(params(binning_x=2)), "give both axes"),
    (task(params(3), sidereal_frames=(3,)), r"sidereal_frames \[3\]"),
    (task(params(3), sidereal_frames=(-1,)), r"sidereal_frames \[-1\]"),
], ids=["half-a-binning", "beyond-every-exposure", "negative"])
def test_an_inconsistent_task_is_rejected(collect, match):
    with pytest.raises(ValueError, match=match):
        translate(collect)


def test_each_capture_is_bounded_by_integration_and_a_readout_margin():
    long = CameraParameterSet(integration_time_seconds=30.0, frame_count=1)
    exposures = task(params(2), long)

    def bounds(intent: CollectIntent) -> dict[str, float | None]:
        return {request.id: request.acquisition.timeout_s
                for epoch in intent.epochs for request in epoch.units}

    assert bounds(translate(exposures)) == {"exposure-0": 61.0,
                                            "exposure-1": 90.0}
    assert bounds(translate(exposures, readout_margin_s=5.0)) == {
        "exposure-0": 6.0, "exposure-1": 35.0}


# Through packing


def test_each_exposure_stays_on_one_instrument(bench):
    collect = pack(translate(SEVERAL), bench)
    placed = {}

    for unit in units_of(collect):
        placed.setdefault(unit.acquisition.request, set()).add(
            unit.target.device)

    assert placed == {"exposure-0": {"cam-e"}, "exposure-1": {"cam-w"},
                      "exposure-2": {"cam-e"}}


def test_ordinals_continue_and_frame_numbers_are_per_instrument(bench):
    collect = pack(translate(SEVERAL), bench)

    assert numbered(collect, "exposure-0") == [(0, 0), (1, 1), (2, 4), (3, 6)]
    assert numbered(collect, "exposure-1") == [(0, 0), (1, 1)]
    assert numbered(collect, "exposure-2") == [(0, 2), (1, 3), (2, 5)]


def test_each_frame_publishes_its_own_frame_number(bench):
    for unit in units_of(pack(translate(SEVERAL), bench)):
        cards = dict(unit.acquisition.keywords[Collect].get_fits_cards())

        assert cards["FRAMENUM"][0] == unit.acquisition.frame_number


def test_numbering_leaves_the_translated_metadata_untouched(bench):
    intent = translate(SEVERAL)
    requests = [r for epoch in intent.epochs for r in epoch.units]
    units = units_of(pack(intent, bench))

    assert {r.collect.frame_number for r in requests} == {0}
    assert len({id(u.acquisition.keywords) for u in units}) == len(units)


def test_a_frame_records_the_target_its_segment_followed(bench):
    targets = [u.acquisition.keywords[Collect].target
               for u in units_of(pack(translate(SEVERAL), bench))
               if u.acquisition.request == "exposure-0"]

    assert targets == [TARGET, TARGET, SIDEREAL, TARGET]


def test_requested_configuration_is_routed_to_the_chain_taking_it(bench):
    epoch = pack(translate(SEVERAL), bench).epochs[0]

    assert {(s.target.device, s.command.model_tag())
            for s in epoch.settings} == {
        ("mount", "FollowTarget"), ("wheel-e", "SetFilter"),
        ("cam-w", "ConfigureCameraSensor")}
    assert next(s.command for s in epoch.settings
                if s.target.device == "cam-w") == ConfigureCameraSensor(
        binning=Binning(x=2, y=2), gain=1.5)


def test_an_opening_sidereal_frame_still_acquires_the_target(bench):
    collect = pack(translate(task(params(2), sidereal_frames=(0,))), bench)

    assert [(s.target.device, s.command) for s in collect.prepare] == [
        ("mount", FollowTarget(target=TARGET))]
    assert [s.command for s in collect.epochs[0].settings] == [
        FollowTarget(target=SIDEREAL)]


def test_cleanup_stops_the_mount_that_tracked(bench):
    collect = pack(translate(task()), bench)

    assert [(s.target.device, s.command) for s in collect.cleanup] == [
        ("mount", Stop())]


@pytest.fixture(scope="module")
def wheels() -> BoundSensor:
    """Two camera branches with a filter wheel on each chain."""
    return sensor_of("""
        name: wheels
        components:
          - device: mount
          - unit: first
            components:
              - device: wheel-a
              - device: cam-a
                instrument: true
          - unit: second
            components:
              - device: wheel-b
              - device: cam-b
                instrument: true
        """, {
        "mount": (("Connect", "FollowTarget", "Stop"), ()),
        "wheel-a": (("SetFilter",), ("Filters",)),
        "cam-a": (("Connect",), ()),
        "wheel-b": (("SetFilter",), ("Filters",)),
        "cam-b": (("Connect",), ()),
    })


@pytest.mark.parametrize(("held", "camera"), [
    ({}, "cam-a"),
    ({"wheel-b": filters("g", "r")}, "cam-b"),
    ({"wheel-a": filters("g")}, "cam-b"),
    ({"wheel-a": filters("g"), "wheel-b": filters()}, "cam-b"),
], ids=["unknown", "reported-holder", "reported-lack", "empty-list"])
def test_a_requested_filter_goes_to_a_wheel_that_may_hold_it(wheels, held, camera):
    collect = pack(translate(task(params(filter_name="r"))), wheels, device_keywords=held)

    assert {unit.target.device for unit in units_of(collect)} == {camera}


def test_a_filter_every_wheel_reports_lacking_rejects_the_collect(wheels):
    held = {"wheel-a": filters("g"), "wheel-b": filters("g")}

    with pytest.raises(SettingUnsatisfiable, match="does not satisfy"):
        pack(translate(task(params(filter_name="r"))), wheels, device_keywords=held)


def test_a_filter_no_chain_can_change_rejects_the_collect():
    fixed = {**REPORTED, "wheel-e": (("Connect",), ())}

    with pytest.raises(SettingUnsatisfiable):
        pack(translate(task(params(filter_name="r"))), sensor_of(BENCH, fixed))
