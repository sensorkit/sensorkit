# SPDX-License-Identifier: Apache-2.0
"""Placements, the indexes derived from them, and the structural rules
construction enforces."""
from __future__ import annotations

import pytest

from sensorkit.sensor.topology import (
    Device,
    Placement,
    Port,
    Selector,
    Structure,
    Topology,
    Unit,
    format_path,
)


def keys(placements) -> list[str]:
    return [p.device for p in placements]


def test_the_walk_is_root_first_with_devices_before_containers(topology):
    assert keys(topology.placements()) == [
        "mount", "dome", "cover", "pickoff", "foc-sci", "wheel", "cam-sci",
        "cam-guide", "cam-acq"]


def test_a_device_holds_one_placement(topology, at):
    assert len(topology.placements()) == len({p.device
                                              for p in topology.placements()})
    assert at("mount").path == ()
    assert at("cam-sci").path == ("ota", "science", "cam-sci")


def test_depth_is_the_length_of_the_path(at):
    assert at("mount").depth == 0
    assert at("cover").depth == 1
    assert at("foc-sci").depth == 2
    assert at("cam-sci").depth == 3


def test_an_instrument_contributes_its_key_as_a_segment(topology, at):
    assert keys(topology.instruments()) == ["cam-sci", "cam-guide", "cam-acq"]
    assert at("cam-guide").path == ("ota", "guide", "cam-guide")


def test_a_chain_is_what_an_instrument_looks_through(topology, at):
    assert keys(topology.chain(at("cam-sci"))) == [
        "mount", "dome", "cover", "pickoff", "foc-sci", "wheel", "cam-sci"]
    assert keys(topology.chain(at("cam-guide"))) == [
        "mount", "dome", "cover", "pickoff", "cam-guide"]


def test_a_selector_is_on_the_chain_and_contributes_no_segment(topology, at):
    assert at("pickoff").path == ("ota",)
    assert at("pickoff") in topology.chain(at("cam-sci"))


def test_private_is_own_position_and_sole_reader_both(topology, at):
    # foc-sci and wheel sit beside cam-sci and nothing else reads them.
    assert sorted(p.device for p in topology.private(at("cam-sci"))) == [
        "cam-sci", "foc-sci", "wheel"]
    # cam-guide and cam-acq share their port, so neither owns the other.
    assert sorted(p.device for p in topology.private(at("cam-guide"))) == [
        "cam-guide"]


def test_private_and_the_rest_of_the_chain_partition_it(topology):
    for instrument in topology.instruments():
        chain = set(topology.chain(instrument))
        private = topology.private(instrument)
        assert private <= chain
        assert private | (chain - private) == chain


def test_readers_of_is_the_inverse_of_chain(topology, at):
    assert keys(topology.readers_of(at("cover"))) == [
        "cam-sci", "cam-guide", "cam-acq"]
    assert keys(topology.readers_of(at("foc-sci"))) == ["cam-sci"]


def test_readers_of_is_empty_where_no_instrument_reaches():
    structure = Structure(components=(Device(device="orphan"),))

    assert Topology(structure).readers_of(Placement("orphan", ())) == ()


def test_selector_states_name_the_port_reaching_an_instrument(topology, at):
    assert topology.selector_states(at("cam-sci")) == ((at("pickoff"),
                                                        "science"),)
    assert topology.selector_states(at("cam-guide")) == ((at("pickoff"),
                                                          "guide"),)


def test_different_ports_are_mutually_exclusive(topology, at):
    assert topology.mutually_exclusive(at("cam-sci"),
                                       at("cam-guide")) == at("pickoff")


def test_one_port_holds_two_instruments_that_are_not_exclusive(topology, at):
    assert topology.mutually_exclusive(at("cam-guide"), at("cam-acq")) is None


def test_record_answers_what_the_structure_says(topology, at):
    assert topology.record(at("mount")).traits == ("MustConnect",)
    assert topology.record(at("cam-sci")).instrument is True
    assert isinstance(topology.record(at("pickoff")), Selector)


def test_record_raises_where_nothing_sits(topology):
    with pytest.raises(KeyError):
        topology.record(Placement("nobody", ()))


def test_chain_answers_for_instruments_only(topology, at):
    with pytest.raises(KeyError):
        topology.chain(at("mount"))


def build(*components) -> Topology:
    return Topology(Structure(components=components))


def test_a_device_named_twice_anywhere_is_a_structural_error():
    with pytest.raises(ValueError, match="'mount' is placed twice"):
        build(Device(device="mount"),
              Unit(unit="ota", components=(Device(device="mount"),)))


def test_a_device_named_twice_at_one_position_is_a_structural_error():
    with pytest.raises(ValueError, match="placed twice"):
        build(Device(device="mount"), Device(device="mount"))


def test_an_instrument_key_collides_with_a_sibling_segment():
    with pytest.raises(ValueError, match="positions named twice"):
        build(Unit(unit="cam"), Device(device="cam", instrument=True))


def test_a_port_is_a_sibling_of_the_units_beside_its_selector():
    with pytest.raises(ValueError, match="positions named twice under"):
        build(Unit(unit="a"),
              Selector(selector="sel", ports=(Port(name="a"),)))


def test_two_selectors_at_one_level_share_the_segment_namespace():
    with pytest.raises(ValueError, match="positions named twice"):
        build(Selector(selector="s1", ports=(Port(name="a"),)),
              Selector(selector="s2", ports=(Port(name="a"),)))


def test_an_error_names_the_position_it_was_found_at():
    with pytest.raises(ValueError, match="under 'ota/bench'"):
        build(Unit(unit="ota", components=(
            Unit(unit="bench", components=(Unit(unit="x"), Unit(unit="x"))),)))


def test_an_empty_structure_is_well_formed():
    topology = build()

    assert topology.placements() == ()
    assert topology.instruments() == ()


def test_format_path_names_the_root(at):
    assert format_path(at("mount").path, "<root>") == "<root>"
    assert format_path(at("cam-sci").path) == "ota/science/cam-sci"
