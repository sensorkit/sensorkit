# SPDX-License-Identifier: Apache-2.0
"""Structural model: the flat walk, uniqueness rules, derived topology."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from sensorkit.workflow import (
    Assembly,
    InstrumentAssembly,
    SelectorAssembly,
    SensorModel,
    Topology,
)


def test_iter_devices_yields_one_node_per_capability_claim(sensor):
    nodes = list(sensor.iter_devices())
    # tcs-1 is the mount at the root AND the main OTA's focuser: one
    # ref, two claims. Traits are what get walked, not devices.
    tcs = [n for n in nodes if n.ref == "tcs-1"]
    assert sorted((n.trait, n.path) for n in tcs) == [
        ("focuser", ("main-ota",)),
        ("mount", ()),
    ]


def test_duplicate_claim_is_rejected():
    with pytest.raises(ValidationError, match="duplicate claims in sensor"):
        SensorModel.model_validate({
            "name": "dupe",
            "attachments": {"tcs-1": "mount"},
            "parts": [
                {"name": "ota", "attachments": {"tcs-1": "mount"}}],
        })


def test_same_ref_may_claim_different_traits():
    SensorModel.model_validate({
        "name": "multi-trait",
        "attachments": {"tcs-1": "mount"},
        "parts": [
            {"name": "ota", "attachments": {"tcs-1": "focuser"}}],
    })


@pytest.mark.parametrize("trait", ["instrument", "selector"])
def test_no_trait_label_is_spoken_for(trait):
    """The structural kinds occupy no part of the trait vocabulary, so a
    deployment sharing this namespace with the rest of SensorKit may
    define these labels for its own purposes."""
    sensor = SensorModel.model_validate(
        {"name": "s", "attachments": {"x-1": trait}})
    (node,) = sensor.iter_devices()
    assert (node.trait, node.kind) == (trait, "attachment")


def test_structural_claims_carry_a_kind_and_no_trait(sensor):
    by_ref = {n.ref: n for n in sensor.iter_devices()}
    assert (by_ref["cam-sci"].kind, by_ref["cam-sci"].trait) == (
        "instrument", None)
    assert (by_ref["sel-1"].kind, by_ref["sel-1"].trait) == ("selector", None)


def test_part_class_is_discriminated_by_shape_alone(sensor):
    """No node in demo.yaml carries a kind tag: `selector` makes a
    selector, `instrument` an instrument, bare `parts` an assembly.
    `extra="forbid"` is what keeps exactly one branch able to match."""
    (main_ota, _, _) = sensor.parts
    (sel,) = main_ota.parts
    assert type(main_ota) is Assembly
    assert type(sel) is SelectorAssembly
    assert {type(p) for p in sel.parts} == {InstrumentAssembly}
    assert ("main-ota", "sel", "port-a") in Topology(sensor).instruments


@pytest.mark.parametrize("part, missing", [
    ({"name": "sel", "selector": "s-1", "parts": []}, "at least 1 item"),
    ({"name": "sel", "selector": "s-1"}, "Field required"),
])
def test_a_selector_must_have_something_to_select_between(part, missing):
    """The bound is on the field, not in a validator: `SelectorAssembly`
    redeclares `parts` as required and non-empty."""
    with pytest.raises(ValidationError, match=missing):
        SensorModel.model_validate({"name": "s", "parts": [part]})


def test_an_assembly_nests_to_any_depth():
    """Assemblies carry no fixed meaning, so a deployment may group by
    aperture, then bench, then cryostat. Paths are the only thing the
    depth changes."""
    sensor = SensorModel.model_validate({
        "name": "deep",
        "parts": [{"name": "ota", "attachments": {"foc": "focuser"},
                   "parts": [{"name": "bench", "attachments": {"col": "lens"},
                              "parts": [{"name": "cryo", "parts": [
                                  {"name": "cam", "instrument": "cam-1"}]}]}]}],
    })
    view = Topology(sensor).instruments[("ota", "bench", "cryo", "cam")]
    assert view.private == frozenset({"cam-1"})
    assert view.shared == ("foc", "col")


def test_a_selector_port_may_be_an_assembly():
    """A fold mirror feeding a bench with two instruments on it: both
    are behind port `bench`, so both are excluded by the same position
    and neither is exclusive of the other."""
    sensor = SensorModel.model_validate({
        "name": "bench-port",
        "parts": [{"name": "sel", "selector": "sel-1", "parts": [
            {"name": "bench", "attachments": {"col": "collimator"}, "parts": [
                {"name": "blue", "instrument": "cam-blue"},
                {"name": "red", "instrument": "cam-red"}]},
            {"name": "direct", "instrument": "cam-direct"}]}],
    })
    topo = Topology(sensor)
    blue = ("sel", "bench", "blue")
    red = ("sel", "bench", "red")
    direct = ("sel", "direct")
    assert topo.instruments[blue].selector_ports == (("sel-1", "bench"),)
    assert topo.mutually_exclusive(blue, red) is None
    assert topo.mutually_exclusive(blue, direct) == "sel-1"
    # The bench's own collimator is shared by the two cameras on it.
    assert "col" in topo.instruments[blue].shared


def test_nested_selectors_compose_their_positions():
    """Two selectors in series: reaching an instrument commands both,
    and exclusion is per selector rather than per depth."""
    sensor = SensorModel.model_validate({
        "name": "series",
        "parts": [{"name": "s1", "selector": "sel-1", "parts": [
            {"name": "p1", "selector": "sel-2", "parts": [
                {"name": "x", "instrument": "cam-x"},
                {"name": "y", "instrument": "cam-y"}]},
            {"name": "p2", "instrument": "cam-z"}]}],
    })
    topo = Topology(sensor)
    x, y = ("s1", "p1", "x"), ("s1", "p1", "y")
    assert topo.instruments[x].selector_ports == (
        ("sel-1", "p1"), ("sel-2", "x"))
    # Same sel-1 port, different sel-2 port: the inner one excludes.
    assert topo.mutually_exclusive(x, y) == "sel-2"
    assert topo.mutually_exclusive(x, ("s1", "p2")) == "sel-1"


def test_shared_chain_is_root_to_leaf_and_deduped(topo):
    view = topo.instruments[("main-ota", "sel", "port-a")]
    assert view.shared == (
        "tcs-1", "dome-1", "chiller-a", "chiller-b",  # root
        "cover-1",                                    # main-ota (tcs-1 elided)
        "sel-1", "fw-shared",                         # the selector
    )
    assert view.private == frozenset({"cam-sci", "fw-a", "rot-a"})


def test_selector_makes_sibling_ports_mutually_exclusive(topo):
    a = ("main-ota", "sel", "port-a")
    b = ("main-ota", "sel", "port-b")
    wide = ("piggyback", "wide")
    assert topo.mutually_exclusive(a, b) == "sel-1"
    assert topo.mutually_exclusive(a, wide) is None


def test_roles_split_collect_targets_from_services(topo):
    assert {v.assembly.instrument for v in topo.collect_targets()} == {
        "cam-sci", "spec-1", "cam-wide"}
    assert {v.assembly.instrument for v in topo.services()} == {"cam-guide"}
