# SPDX-License-Identifier: Apache-2.0
"""
compile_table: the phase-table frontend. Pure data in, pure data out —
no event loop, no simulated hardware.
"""
from __future__ import annotations

import pytest

from sensorkit.workflow import (
    Entry,
    Op,
    OpSpec,
    Phase,
    PhaseTable,
    Require,
    compile_table,
    format_graph,
)


def table(*phases: Phase, on_failure: str = "stop") -> PhaseTable:
    return PhaseTable(name="t", phases=phases, on_failure=on_failure)


def payloads(graph):
    return [(n.group, n.payload.ref, n.payload.op) for n in graph.nodes]


# ---- golden graphs ------------------------------------------------------

def test_init_graph(devices, init_table, assert_golden):
    assert_golden("init", format_graph(compile_table(devices, init_table)))


def test_deinit_graph(devices, deinit_table, assert_golden):
    assert_golden("deinit", format_graph(compile_table(devices, deinit_table)))


# ---- matching semantics -------------------------------------------------

def test_trait_entry_matches_every_claim(devices):
    g = compile_table(devices, table(
        Phase(name="p", entries=(Entry(trait="filter_wheel", ops="home"),))))
    assert sorted(n.payload.ref for n in g.nodes) == [
        "fw-a", "fw-shared", "fw-wide"]
    assert all(n.payload.trait == "filter_wheel" for n in g.nodes)


def test_match_all_matches_each_ref_once(devices):
    g = compile_table(devices, table(
        Phase(name="p", entries=(Entry(match="all", ops="connect"),))))
    refs = [n.payload.ref for n in g.nodes]
    assert len(refs) == len(set(refs))
    # tcs-1 claims two traits but connects once.
    assert refs.count("tcs-1") == 1
    # Instruments and selectors are devices too, however they got here.
    assert {"cam-sci", "sel-1"} <= set(refs)
    # ...and an `all` match carries no capability.
    assert all(n.payload.trait is None for n in g.nodes)
    assert all(n.payload.match == "all" for n in g.nodes)


def test_structural_kinds_match_without_a_trait(devices):
    """An instrument and a selector are addressed by what they are, and
    occupy no part of the deployment's trait vocabulary."""
    g = compile_table(devices, table(Phase(name="p", entries=(
        Entry(match="instrument", ops="start_cooling"),
        Entry(match="selector", ops="home"),
    ))))
    by_op = {n.payload.op: n for n in g.nodes}
    assert by_op["home"].payload.ref == "sel-1"
    assert by_op["home"].payload.match == "selector"
    assert all(n.payload.trait is None for n in g.nodes)
    assert sorted(n.payload.ref for n in g.nodes
                  if n.payload.op == "start_cooling") == [
        "cam-guide", "cam-sci", "cam-wide", "spec-1"]


def test_instrument_match_narrows_on_role(devices):
    g = compile_table(devices, table(Phase(name="p", entries=(
        Entry(match="instrument", role="guide", ops="start_guiding"),))))
    assert [n.payload.ref for n in g.nodes] == ["cam-guide"]


def test_device_entry_claims_the_device_from_trait_entries(devices):
    g = compile_table(devices, table(Phase(name="p", entries=(
        Entry(trait="filter_wheel", ops="home"),
        Entry(device="fw-a", ops="special_home"),
    ))))
    assert sorted(payloads(g)) == [
        ("p", "fw-a", "special_home"),
        ("p", "fw-shared", "home"),
        ("p", "fw-wide", "home"),
    ]


def test_payload_carries_every_trait_the_ref_claims(devices):
    g = compile_table(devices, table(
        Phase(name="p", entries=(Entry(device="tcs-1", ops="home"),))))
    op: Op = g.nodes[0].payload
    assert op.match == "device" and op.trait is None
    assert op.traits == ("focuser", "mount")   # sorted; what dispatch needs


# ---- edge semantics -----------------------------------------------------

def test_consecutive_phases_are_ordered_but_do_not_propagate_failure(devices):
    g = compile_table(devices, table(
        Phase(name="a", entries=(Entry(trait="chiller", ops="start"),)),
        Phase(name="b", entries=(Entry(trait="mount", ops="unpark"),)),
    ))
    a = {n.id for n in g.nodes if n.group == "a"}
    b = next(n.id for n in g.nodes if n.group == "b")
    assert g.deps[b] == frozenset(a)
    assert g.hard[b] == frozenset()          # a phase orders, it does not skip


def test_require_is_the_hard_link(devices):
    g = compile_table(devices, table(
        Phase(name="a", entries=(
            Entry(id="homed", trait="filter_wheel", ops="home"),)),
        Phase(name="b", entries=(Entry(trait="mount", ops="unpark",
                                       require=("homed",)),)),
    ))
    a = {n.id for n in g.nodes if n.group == "a"}
    b = next(n.id for n in g.nodes if n.group == "b")
    assert g.hard[b] == frozenset(a)


def test_same_device_join_narrows_to_a_hard_self_edge(devices):
    g = compile_table(devices, table(
        Phase(name="a", entries=(
            Entry(id="homed", trait="filter_wheel", ops="home"),)),
        Phase(name="b", entries=(Entry(
            trait="filter_wheel", ops="configure",
            require=(Require(name="homed", join="same-device"),)),)),
    ))
    by = {(n.group, n.payload.ref): n.id for n in g.nodes}
    for ref in ("fw-a", "fw-shared", "fw-wide"):
        assert g.hard[by[("b", ref)]] == frozenset({by[("a", ref)]})
        assert g.deps[by[("b", ref)]] == frozenset({by[("a", ref)]})


def test_same_chain_join_narrows_to_the_devices_own_chain(devices):
    """rot-a is behind the main OTA, so the focuser claim above it joins
    and the piggyback's does not."""
    g = compile_table(devices, table(
        Phase(name="a", entries=(
            Entry(id="focused", trait="focuser", ops="home"),)),
        Phase(name="b", entries=(Entry(
            trait="rotator", ops="configure",
            require=(Require(name="focused", join="same-chain"),)),)),
    ))
    by = {(n.group, n.payload.ref): n.id for n in g.nodes}
    assert g.hard[by[("b", "rot-a")]] == frozenset({by[("a", "tcs-1")]})


def test_same_chain_join_reaches_every_claim_above_the_device(devices):
    """A wheel shared on the selector is on cam-sci's chain exactly as
    its own is; fw-wide, on a sibling assembly, is not."""
    g = compile_table(devices, table(
        Phase(name="a", entries=(
            Entry(id="homed", trait="filter_wheel", ops="home"),)),
        Phase(name="b", entries=(Entry(
            device="cam-sci", ops="configure",
            require=(Require(name="homed", join="same-chain"),)),)),
    ))
    by = {(n.group, n.payload.ref): n.id for n in g.nodes}
    assert g.hard[by[("b", "cam-sci")]] == frozenset(
        {by[("a", "fw-shared")], by[("a", "fw-a")]})


def test_a_clause_replaces_the_phase_link_it_targets(devices):
    """Naming a dependency precisely says the blanket wait was not
    meant — so the mount's steps do not hold up the wheels'."""
    g = compile_table(devices, table(
        Phase(name="a", entries=(
            Entry(id="homed", trait="filter_wheel", ops="home"),
            Entry(trait="mount", ops="unpark"),
        )),
        Phase(name="b", entries=(Entry(
            trait="filter_wheel", ops="configure",
            require=(Require(name="homed", join="same-device"),)),)),
    ))
    by = {(n.group, n.payload.ref): n.id for n in g.nodes}
    assert g.deps[by[("b", "fw-a")]] == frozenset({by[("a", "fw-a")]})


def test_a_clause_elsewhere_leaves_the_phase_link_alone(devices):
    """chiller-b requires its peer inside its own phase, so it keeps
    the soft wait on the phase it follows."""
    g = compile_table(devices, table(
        Phase(name="a", entries=(Entry(trait="mount", ops="unpark"),)),
        Phase(name="b", entries=(
            Entry(id="first", device="chiller-a", ops="start"),
            Entry(device="chiller-b", ops="start", require=("first",)),
        )),
    ))
    by = {n.payload.ref: n.id for n in g.nodes}
    assert g.hard[by["chiller-b"]] == frozenset({by["chiller-a"]})
    assert by["tcs-1"] in g.deps[by["chiller-b"]]


def test_require_reaches_across_phases(devices):
    g = compile_table(devices, table(
        Phase(name="a", entries=(Entry(id="m", trait="mount", ops="unpark"),)),
        Phase(name="b", entries=(Entry(trait="dome", ops="open"),)),
        Phase(name="c", entries=(Entry(trait="mirror_cover", ops="open",
                                       require=("m",)),)),
    ))
    by = {n.payload.ref: n.id for n in g.nodes}
    assert g.hard[by["cover-1"]] == frozenset({by["tcs-1"]})


def test_multi_op_entry_serializes_per_device_and_keeps_devices_concurrent(devices):
    g = compile_table(devices, table(Phase(name="p", entries=(
        Entry(trait="filter_wheel", ops=["home", "park"]),))))
    by = {(n.payload.ref, n.payload.op): n.id for n in g.nodes}
    for ref in ("fw-a", "fw-shared", "fw-wide"):
        assert g.hard[by[(ref, "park")]] == frozenset({by[(ref, "home")]})
        assert g.deps[by[(ref, "home")]] == frozenset()


def test_empty_phase_falls_through_to_its_own_predecessor(devices):
    # "b" matches nothing on this sensor, so "c" must barrier on "a".
    g = compile_table(devices, table(
        Phase(name="a", entries=(Entry(trait="mount", ops="unpark"),)),
        Phase(name="b", entries=(Entry(trait="nonesuch", ops="poke"),)),
        Phase(name="c", entries=(Entry(trait="dome", ops="open"),)),
    ))
    a = next(n.id for n in g.nodes if n.group == "a")
    c = next(n.id for n in g.nodes if n.group == "c")
    assert g.deps[c] == frozenset({a})


def test_op_spec_carries_the_optional_flag(devices):
    g = compile_table(devices, table(Phase(name="p", entries=(
        Entry(trait="filter_wheel", ops=OpSpec(op="home", optional=True)),))))
    assert all(n.optional for n in g.nodes)


def test_on_failure_resolves_op_then_phase_then_table(devices):
    g = compile_table(devices, table(
        Phase(name="p", entries=(
            Entry(trait="filter_wheel", ops="home"),
            Entry(trait="dome", ops=OpSpec(op="close", on_failure="continue")),
        )),
        Phase(name="q", on_failure="stop",
              entries=(Entry(trait="mount", ops="park"),)),
        on_failure="skip"))
    assert {n.payload.op: n.on_failure for n in g.nodes} == {
        "home": "skip", "close": "continue", "park": "stop"}


def test_nothing_declares_a_reversal(devices, init_table):
    """A node is one action, not an action and its inverse: nothing on a
    node or its payload declares how to undo it. Recovery is another
    table, run deliberately."""
    g = compile_table(devices, init_table)
    assert not any(hasattr(n, "undo_payload") for n in g.nodes)
    assert not any(hasattr(n.payload, "undo") for n in g.nodes)


def test_lifecycle_ops_are_nullary(devices, init_table):
    """A phase table has no syntax for arguments, so the shared payload's
    params stay empty here — the field exists for collect's verbs."""
    g = compile_table(devices, init_table)
    assert all(n.payload.params == {} for n in g.nodes)


# ---- config errors are load-time errors ---------------------------------

@pytest.mark.parametrize("phases, message", [
    ([Phase(name="p", entries=(Entry(device="nope", ops="x"),))],
     "unknown device"),
    ([Phase(name="p", entries=(Entry(trait="mount", ops="x"),)),
      Phase(name="p", entries=(Entry(trait="mount", ops="y"),))],
     "duplicate phase name"),
    ([Phase(name="p", entries=(Entry(trait="mount", ops="x"),),
            after=("later",))],
     "after names unknown or later"),
    ([Phase(name="p", entries=(Entry(device="tcs-1", ops="x"),
                               Entry(device="tcs-1", ops="y")))],
     "multiple entries claim the same device"),
    ([Phase(name="p", entries=(
        Entry(device="chiller-a", ops="x", require=("nowhere",)),))],
     "require names unknown or later"),
    ([Phase(name="p", entries=(Entry(id="dup", trait="mount", ops="x"),
                               Entry(id="dup", trait="dome", ops="y")))],
     "duplicate entry id"),
    ([Phase(name="p", entries=(
        Entry(id="none", trait="nonesuch", ops="x"),
        Entry(device="chiller-a", ops="y", require=("none",))))],
     "matches no step on this sensor"),
    ([Phase(name="a", entries=(Entry(trait="nonesuch", ops="x"),)),
      Phase(name="b", entries=(
          Entry(trait="mount", ops="y", require=("a",)),))],
     "matches no step on this sensor"),
    ([Phase(name="a", entries=(
        Entry(id="homed", trait="filter_wheel", ops="x"),)),
      Phase(name="b", entries=(Entry(
          trait="mount", ops="y",
          require=(Require(name="homed", join="same-device"),)),))],
     "join='same-device' matches no step for tcs-1"),
    # cam-guide has no wheel anywhere on its chain, so the clause is
    # unsatisfiable for it however well it reads for cam-sci.
    ([Phase(name="a", entries=(
        Entry(id="homed", trait="filter_wheel", ops="x"),)),
      Phase(name="b", entries=(Entry(
          device="cam-guide", ops="y",
          require=(Require(name="homed", join="same-chain"),)),))],
     "join='same-chain' matches no step for cam-guide"),
])
def test_compile_rejects_bad_tables(devices, phases, message):
    with pytest.raises(ValueError, match=message):
        compile_table(devices, table(*phases))


def test_require_cycle_is_caught(devices):
    with pytest.raises(ValueError, match="dependency cycle"):
        compile_table(devices, table(Phase(name="p", entries=(
            Entry(id="a", device="chiller-a", ops="x", require=("b",)),
            Entry(id="b", device="chiller-b", ops="y", require=("a",)),
        ))))


def test_entry_must_select_devices():
    with pytest.raises(ValueError, match="both trait and device"):
        Entry(ops="x", trait="mount", device="tcs-1")
    with pytest.raises(ValueError, match="must select devices"):
        Entry(ops="x")
    with pytest.raises(ValueError, match="needs a trait field"):
        Entry(ops="x", match="trait")
    with pytest.raises(ValueError, match="role applies only"):
        Entry(ops="x", trait="mount", role="guide")
