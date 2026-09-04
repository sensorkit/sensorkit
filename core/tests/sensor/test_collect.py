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
from sensorkit.sensor.collect import AcquisitionRequest, CommandRequest, InstrumentRequest, SettingUnsatisfiable
from sensorkit.std.collect import CameraParameterSet
from sensorkit.std.collect import Collect as CollectMetadata
from sensorkit.std.instrument import Binning, ConfigureCameraSensor
from sensorkit.std.traits import Home

from .common import TARGET, sensor_of


def asking(id: str, count: int = 1, seconds: float = 1.0, **kw
           ) -> InstrumentRequest:
    """One instrument request, with the acquisition fields inlined."""
    acquisition = AcquisitionRequest(
        integration_time_s=seconds, count=count,
        distribute=kw.pop("distribute", "one"),
        timeout_s=kw.pop("timeout_s", None))

    return InstrumentRequest(id=id, acquisition=acquisition, **kw)










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


















# Epochs























# Frame numbering, prepare and cleanup












# Assignment groups
























# Epoch settings in eligibility


"""Only the science chain changes a filter and only `cam-guide` bins, so an
epoch setting of either kind rules out every chain but one."""

"""Binning ties between two devices on the science chain and routes cleanly on
the guide camera."""










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
