# SPDX-License-Identifier: Apache-2.0
"""
compile_collect: derived per-device barriers. The claim under test is
that a device may change once nobody is exposing through it, and that
the *global* barrier appears only where it is physically real.
"""
from __future__ import annotations

from functools import cache

import pytest

from sensorkit.workflow import (
    OP_APPLY,
    OP_EXPOSE,
    Collect,
    FramePlan,
    Op,
    Step,
    SyncPoint,
    compile_collect,
    format_graph,
    validate_collect,
)

CAM = ("main-ota", "sel", "port-a")      # cam-sci, behind selector sel-1
SPEC = ("main-ota", "sel", "port-b")     # spec-1, the other port
WIDE = ("piggyback", "wide")             # cam-wide, its own assembly
GUIDE = ("guide-scope", "guider")        # a service, not a collect target

SAT = Collect(name="sat", steps=(
    Step(settings={"tcs-1": "rate:42931", "fw-shared": "clear"},
         plans={CAM: FramePlan(5, n_frames=4, settings={"fw-a": "clear"})}),
    Step(settings={"tcs-1": "sidereal:42931-field"},
         plans={CAM: FramePlan(5, n_frames=1)}),
))

SWEEP = Collect(name="sweep", steps=(
    Step(settings={"tcs-1": "M51", "foc-2": 0.0},
         plans={CAM: FramePlan(0.30),
                WIDE: FramePlan(0.02, target_type="focus")}),
    Step(settings={"foc-2": 0.1},
         plans={WIDE: FramePlan(0.02, target_type="focus")}),
    Step(settings={"foc-2": 0.2},
         plans={WIDE: FramePlan(0.02, target_type="focus")}),
))


def is_op(node, verb: str) -> bool:
    return isinstance(node.payload, Op) and node.payload.op == verb


def applies(graph, group: str | None = None):
    """ref -> apply node. Scope to a group when a later step re-commands
    the same device, or the last one wins."""
    return {n.payload.ref: n for n in graph.nodes
            if is_op(n, OP_APPLY) and (group is None or n.group == group)}


def frames(graph, path=None):
    """Exposure nodes, in compile order."""
    return [n for n in graph.nodes if is_op(n, OP_EXPOSE)
            and (path is None or n.payload.path == path)]


def blocks(graph):
    """First frame of each camera's block, per step — the node the
    barriers actually attach to."""
    out, seen = [], set()
    for n in frames(graph):
        key = (n.group, n.payload.path)
        if key not in seen:
            seen.add(key)
            out.append(n)
    return out


# ---- golden graphs ------------------------------------------------------

def test_rate_sidereal_graph(topo, devices, assert_golden):
    assert_golden("collect-sat", format_graph(compile_collect(topo, devices, SAT)))


def test_focus_sweep_graph(topo, devices, assert_golden):
    assert_golden("collect-sweep", format_graph(compile_collect(topo, devices, SWEEP)))


# ---- derived barriers ---------------------------------------------------

def test_selector_position_is_derived_from_the_participants(topo, devices):
    g = compile_collect(topo, devices, SAT)
    assert applies(g)["sel-1"].payload.params["value"] == "port-a"


def test_a_device_off_the_peer_path_never_waits_for_it(topo, devices):
    """The whole point of the sweep: foc-2 is not on cam-sci's optical
    path, so no sweep step depends on cam-sci's long exposure."""
    g = compile_collect(topo, devices, SWEEP)
    cam_sci_block = next(n for n in blocks(g) if n.payload.path == CAM)
    foc_applies = [n for n in g.nodes
                   if is_op(n, OP_APPLY) and n.payload.ref == "foc-2"]
    assert len(foc_applies) == 3
    for n in foc_applies:
        assert cam_sci_block.id not in g.deps[n.id]


def test_a_shared_device_waits_for_every_camera_exposing_through_it(topo, devices):
    """The mount is on every path, so a re-target drains everyone —
    the global barrier, appearing exactly where it is real."""
    two_cams = Collect(steps=(
        Step(settings={"tcs-1": "A"},
             plans={CAM: FramePlan(1), WIDE: FramePlan(1)}),
        Step(settings={"tcs-1": "B"},
             plans={CAM: FramePlan(1), WIDE: FramePlan(1)}),
    ))
    g = compile_collect(topo, devices, two_cams)
    slew_b = [n for n in g.nodes
              if is_op(n, OP_APPLY) and n.payload.params["value"] == "B"][0]
    step0 = {n.id for n in blocks(g) if n.group == "step 0"}
    assert step0 <= g.deps[slew_b.id]
    assert g.hard[slew_b.id] == frozenset()   # draining is ordering, not skipping


def test_frame_block_hard_depends_on_its_governing_applies(topo, devices):
    g = compile_collect(topo, devices, SAT)
    block0 = next(n for n in blocks(g) if n.group == "step 0")
    a = applies(g, "step 0")
    assert g.hard[block0.id] == frozenset(
        {a[r].id for r in ("sel-1", "tcs-1", "fw-shared", "fw-a")})


def test_unchanged_recommands_are_elided_and_the_governing_apply_persists(topo, devices):
    g = compile_collect(topo, devices, SAT)
    # fw-shared and fw-a are commanded in step 0 only; step 1 re-uses them
    # implicitly, so step 1's block still hard-depends on the step-0 apply
    # — a stuck wheel keeps skipping frames it invalidated.
    assert len([n for n in g.nodes
                if is_op(n, OP_APPLY) and n.payload.ref == "fw-shared"]) == 1
    block1 = next(n for n in blocks(g) if n.group == "step 1")
    assert applies(g)["fw-shared"].id in g.hard[block1.id]


def test_same_camera_serializes_softly_across_steps(topo, devices):
    """The next step's block waits for the camera's *latest* frame."""
    g = compile_collect(topo, devices, SAT)
    last_of_step0 = [f for f in frames(g, CAM) if f.group == "step 0"][-1]
    b1 = next(n for n in blocks(g) if n.group == "step 1")
    assert last_of_step0.id in g.deps[b1.id]
    assert last_of_step0.id not in g.hard[b1.id]   # soft: it may try again


def test_midpoint_alignment_adds_a_sync_node_and_start_offsets(topo, devices):
    aligned = Collect(steps=(
        Step(settings={"tcs-1": "M51"}, align="midpoint",
             plans={CAM: FramePlan(0.05, n_frames=2),   # 0.10s
                    WIDE: FramePlan(0.30)}),            # 0.30s
    ))
    g = compile_collect(topo, devices, aligned)
    offsets = {n.payload.path: n.delay_s for n in blocks(g)}
    assert offsets[WIDE] == pytest.approx(0.0)
    assert offsets[CAM] == pytest.approx(0.10)          # (0.30 - 0.10) / 2
    sync = [n for n in g.nodes if isinstance(n.payload, SyncPoint)]
    assert len(sync) == 1
    assert all(sync[0].id in g.deps[n.id] for n in blocks(g))


def test_single_participant_needs_no_sync_node(topo, devices):
    g = compile_collect(topo, devices, Collect(steps=(
        Step(align="midpoint", plans={CAM: FramePlan(1)}),)))
    assert not [n for n in g.nodes if isinstance(n.payload, SyncPoint)]


# ---- validation ---------------------------------------------------------

@pytest.mark.parametrize("step, message", [
    (Step(plans={}), "no frame plans"),
    (Step(plans={("nope",): FramePlan(1)}), "unknown instrument"),
    (Step(plans={GUIDE: FramePlan(1)}), "not a collect target"),
    (Step(plans={CAM: FramePlan(1), SPEC: FramePlan(1)}),
     "different ports of selector"),
    (Step(plans={CAM: FramePlan(1, settings={"tcs-1": "x"})}),
     "plan settings must be private"),
    (Step(plans={CAM: FramePlan(1)}, settings={"sel-1": "port-a"}),
     "selector positions are derived"),
    (Step(plans={CAM: FramePlan(1)}, settings={"foc-2": 0.0}),
     "not shared by any participant"),
])
def test_validate_rejects_malformed_steps(topo, step, message):
    with pytest.raises(ValueError, match=message):
        validate_collect(topo, Collect(steps=(step,)))


def test_on_failure_uses_dags_vocabulary_and_reaches_every_node(topo, devices):
    assert Collect(steps=()).on_failure == "stop"
    g = compile_collect(topo, devices, Collect(
        steps=(Step(settings={"tcs-1": "M51"},
                    plans={CAM: FramePlan(1.0, n_frames=2)}),),
        on_failure="skip"))
    assert {n.on_failure for n in g.nodes} == {"skip"}


# ---- the shared op payload ----------------------------------------------

def test_an_apply_is_addressed_by_device_and_carries_its_traits(topo, devices):
    """A step names devices, not capabilities, so the hook resolves
    which one it is — exactly the lifecycle `device:` case."""
    g = compile_collect(topo, devices, SAT)
    fw = applies(g)["fw-a"].payload
    assert (fw.op, fw.match, fw.trait) == (OP_APPLY, "device", None)
    assert fw.traits == ("filter_wheel",)
    assert fw.params == {"value": "clear"}
    assert fw.path == ("main-ota", "sel", "port-a")


def test_an_exposure_is_addressed_as_the_instrument_kind(topo, devices):
    """Structural, not a trait claim: nothing in the deployment's trait
    vocabulary stands for an instrument."""
    g = compile_collect(topo, devices, SAT)
    f = frames(g)[0].payload
    assert (f.op, f.match, f.trait) == (OP_EXPOSE, "instrument", None)
    assert f.ref == "cam-sci"                    # the device, not the path
    assert f.path == CAM
    assert f.params == {"exposure_s": 5, "target_type": "science",
                        "frame": 0, "n_frames": 4}


def test_every_frame_is_its_own_node(topo, devices):
    g = compile_collect(topo, devices, SAT)
    assert [f.payload.params["frame"] for f in frames(g)] == [0, 1, 2, 3, 0]


def test_frames_chain_softly_but_all_carry_the_blocks_hard_deps(topo, devices):
    """A failed filter move invalidates every frame it governs, not just
    the first; a failed *frame* does not skip its successors."""
    g = compile_collect(topo, devices, SAT)
    block = [f for f in frames(g) if f.group == "step 0"]
    govern = {applies(g, "step 0")[r].id
              for r in ("sel-1", "tcs-1", "fw-shared", "fw-a")}
    assert all(g.hard[f.id] == frozenset(govern) for f in block)
    for prev, nxt in zip(block, block[1:], strict=False):
        assert prev.id in g.deps[nxt.id]
        assert prev.id not in g.hard[nxt.id]


def test_concurrent_blocks_never_couple_frame_to_frame(topo, devices):
    """Peer cameras' frames are independent chains.

    `format_graph` groups nodes by topological level, so two blocks of
    equal length render as interleaved levels — `[1/3]` beside `[1/3]`,
    then `[2/3]` beside `[2/3]`. That is the *renderer*
    reporting equal depth, not a barrier: a level is the earliest a node
    could start, and nothing synchronises the two chains. Asserted on
    transitive reachability, which is what the display cannot show."""
    pair = Collect(steps=(
        Step(settings={"tcs-1": "M51"},
             plans={CAM: FramePlan(0.30, n_frames=3),
                    WIDE: FramePlan(0.05, n_frames=3)}),))
    g = compile_collect(topo, devices, pair)

    @cache
    def reaches(nid: int) -> frozenset[int]:
        return frozenset().union(
            g.deps[nid], *(reaches(d) for d in g.deps[nid]))

    fs = frames(g)
    assert len(fs) == 6
    assert not [(a.label, b.label) for a in fs for b in fs
                if a.payload.path != b.payload.path and b.id in reaches(a.id)]
    # Each chain depends only on its own predecessor, plus applies.
    for path in (CAM, WIDE):
        chain = frames(g, path)
        peers = {f.id for f in fs if f.payload.path != path}
        for prev, nxt in zip(chain, chain[1:], strict=False):
            assert prev.id in g.deps[nxt.id]
            assert not peers & g.deps[nxt.id]


def test_only_the_first_frame_of_a_block_takes_the_alignment_offset(topo, devices):
    aligned = Collect(steps=(
        Step(settings={"tcs-1": "M51"}, align="midpoint",
             plans={CAM: FramePlan(0.05, n_frames=2), WIDE: FramePlan(0.30)}),))
    g = compile_collect(topo, devices, aligned)
    assert [f.delay_s for f in frames(g, CAM)] == pytest.approx([0.10, 0.0])


def test_sync_points_are_the_only_non_op_payload(topo, devices):
    g = compile_collect(topo, devices, SWEEP)
    assert all(isinstance(n.payload, Op | SyncPoint) for n in g.nodes)
