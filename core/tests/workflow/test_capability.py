# SPDX-License-Identifier: Apache-2.0
"""
The external-task frontend: the manifest a task source discovers, the
two selector forms, the derivation that keeps refs and structure out
of the task, and the chronology that is authored rather than derived.
"""
from __future__ import annotations

import pytest

from sensorkit.workflow import (
    Bindings,
    ByCapability,
    ByRef,
    DeviceIndex,
    DimensionBinding,
    ExposureReq,
    ExternalTask,
    InstrumentRole,
    RequestResolver,
    SensorModel,
    SensorPlan,
    TargetSpec,
    TaskStep,
    Topology,
    build_manifest,
    compile_collect,
    format_graph,
    resolve_target_ref,
    validate_collect,
)

BINDINGS = Bindings(
    dimensions={"filter": DimensionBinding(trait="filter_wheel",
                                           scope="private")},
    encodings={"filter_wheel": {b: f"pos:{b}" for b in "grizy"}},
    target_trait="mount",
    aliases={"wide": "cam-wide"},
)


@pytest.fixture
def resolver(topo, devices) -> RequestResolver:
    return RequestResolver(topo, devices, BINDINGS)


# ---- the manifest -------------------------------------------------------

def test_manifest_publishes_one_entry_per_instrument(resolver):
    assert {e.ref for e in resolver.manifest} == {
        "cam-sci", "spec-1", "cam-guide", "cam-wide"}


def test_alias_becomes_the_published_handle(resolver):
    handles = {e.ref: e.handle for e in resolver.manifest}
    assert handles["cam-wide"] == "wide"
    assert handles["cam-sci"] == "main-ota/sel/port-a"   # no alias: the path


def test_passbands_are_derived_from_the_filter_wheels_vocabulary(resolver):
    """Not authored twice: the wheel's encoded value set IS the
    instrument's passband set."""
    by_ref = {e.ref: e for e in resolver.manifest}
    assert by_ref["cam-sci"].attributes["passbands"] == sorted("grizy")
    # cam-guide's chain carries no filter wheel, so it claims none.
    assert "passbands" not in by_ref["cam-guide"].attributes


def test_incoherent_passband_dimension_fails_at_load(topo, devices):
    bad = Bindings(dimensions={}, passband_dimension="filter")
    with pytest.raises(ValueError, match="is not a declared dimension"):
        build_manifest(topo, devices, bad)


# ---- selection ----------------------------------------------------------

def test_by_ref_resolves_aliases(resolver):
    assert resolver.resolve_paths(ByRef("wide")) == (("piggyback", "wide"),)
    assert resolver.resolve_paths(ByRef("cam-wide")) == (("piggyback", "wide"),)


def test_by_ref_that_matches_nothing_is_an_error(resolver):
    with pytest.raises(ValueError, match="matches no instrument"):
        resolver.resolve_paths(ByRef("nope"))


def test_by_capability_matches_on_role_and_attributes(resolver):
    paths = resolver.resolve_paths(ByCapability(
        role=InstrumentRole.SCIENCE, requires={"passbands": {"has": ["g", "r"]}},
        count=2))
    assert len(paths) == 2
    assert ("guide-scope", "guider") not in paths


def test_by_capability_reports_insufficient_cardinality(resolver):
    with pytest.raises(ValueError, match="matched 3 instrument"):
        resolver.resolve_paths(ByCapability(role=InstrumentRole.SCIENCE,
                                            count=99))


# ---- resolution ---------------------------------------------------------

def test_shared_and_private_settings_land_on_the_right_side(topo, resolver):
    """The task says {filter: r}; whether that becomes a Step setting or
    a FramePlan setting is derived from structure, never asked."""
    collect = resolver.to_collect(ExternalTask.single(
        TargetSpec("M51"),
        [ExposureReq(select=ByRef("cam-wide"), exposure_s=30,
                     config={"filter": "r"})],
        name="t"))
    (step,) = collect.steps
    assert repr(step.settings["tcs-1"]) == "sidereal:M51"      # the mount
    plan = step.plans[("piggyback", "wide")]
    assert plan.settings == {"fw-wide": "pos:r"}               # private wheel
    validate_collect(topo, collect)                            # and it is legal


def test_unknown_dimension_is_rejected(resolver):
    with pytest.raises(ValueError, match="unknown dimension"):
        resolver.to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByRef("cam-wide"), exposure_s=1,
                         config={"grating": "x"})]))


def test_unencodable_value_is_rejected(resolver):
    with pytest.raises(ValueError, match="no encoding for"):
        resolver.to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByRef("cam-wide"), exposure_s=1,
                         config={"filter": "ultraviolet"})]))


def test_ambiguous_dimension_needs_a_scope(topo, devices):
    """cam-sci's chain carries both fw-shared and fw-a; with scope=any
    the choice is genuinely ambiguous and must be an error."""
    ambiguous = BINDINGS.model_copy(update={
        "dimensions": {"filter": DimensionBinding(trait="filter_wheel")}})
    with pytest.raises(ValueError, match="is ambiguous across"):
        RequestResolver(topo, devices, ambiguous).to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByRef("cam-sci"), exposure_s=1,
                         config={"filter": "r"})]))


def test_two_requests_may_not_target_the_same_instrument(resolver):
    with pytest.raises(ValueError, match="more than one\n?\\s*exposure request"):
        resolver.to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByRef("wide"), exposure_s=1),
             ExposureReq(select=ByRef("cam-wide"), exposure_s=2)],
            name="clash"))


# ---- one device, one value per step -------------------------------------

# Two co-schedulable cameras behind one shared filter wheel — a shape the
# demo cannot express (its shared wheel sits on a selector, so the two
# cameras under it are mutually exclusive and never appear in one step).
SHARED_WHEEL = SensorModel.model_validate({
    "name": "shared-wheel",
    "attachments": {"tcs-1": "mount"},
    "parts": [{
        "name": "ota",
        "attachments": {"fw-shared": "filter_wheel"},
        "parts": [{"name": "cam-a", "instrument": "cam-a"},
                  {"name": "cam-b", "instrument": "cam-b"}]}]})

SHARED_BINDINGS = BINDINGS.model_copy(update={
    "dimensions": {"filter": DimensionBinding(trait="filter_wheel")},
    "aliases": {}})


def shared_wheel_task(filter_a: str, filter_b: str) -> ExternalTask:
    return ExternalTask.single(
        TargetSpec("M51"),
        [ExposureReq(select=ByRef("cam-a"), exposure_s=10,
                     config={"filter": filter_a}),
         ExposureReq(select=ByRef("cam-b"), exposure_s=10,
                     config={"filter": filter_b})],
        name="pair")


def test_instruments_sharing_a_device_may_agree_on_it():
    """Agreement is the whole point of a Step setting: two cameras
    exposing through one wheel in the same band is one apply."""
    topo = Topology(SHARED_WHEEL)
    resolver = RequestResolver(topo, DeviceIndex(SHARED_WHEEL), SHARED_BINDINGS)
    collect = resolver.to_collect(shared_wheel_task("r", "r"))
    (step,) = collect.steps
    assert step.settings["fw-shared"] == "pos:r"
    assert all(not plan.settings for plan in step.plans.values())
    validate_collect(topo, collect)


def test_instruments_sharing_a_device_may_not_differ_on_it():
    """A step is one configuration epoch, so the wheel holds one value
    across it. Keeping the last write would hand back frames labelled
    with a band they were not taken in."""
    resolver = RequestResolver(Topology(SHARED_WHEEL), DeviceIndex(SHARED_WHEEL),
                            SHARED_BINDINGS)
    with pytest.raises(ValueError, match="already commanded this step"):
        resolver.to_collect(shared_wheel_task("r", "g"))


def test_a_dimension_may_not_clobber_the_pointing_setpoint(topo, devices):
    """The target reaches the mount as a Step setting; a dimension bound
    to the same trait would overwrite it silently."""
    rate = BINDINGS.model_copy(update={
        "dimensions": {**BINDINGS.dimensions,
                       "tracking": DimensionBinding(trait="mount")},
        "encodings": {**BINDINGS.encodings, "mount": {"lunar": "rate:lunar"}}})
    with pytest.raises(ValueError, match="already commanded this step"):
        RequestResolver(topo, devices, rate).to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByRef("wide"), exposure_s=1,
                         config={"tracking": "lunar"})]))


def test_missing_target_trait_fails_at_construction(topo, devices):
    """A binding-coherence error, so it surfaces when the resolver is
    built — not on the first task of the night. No task involved."""
    no_mount = BINDINGS.model_copy(update={"target_trait": "nonesuch"})
    with pytest.raises(ValueError, match="no root device claims target trait"):
        RequestResolver(topo, devices, no_mount)


def test_two_root_devices_may_not_both_carry_the_setpoint(devices):
    two_mounts = DeviceIndex(SensorModel.model_validate({
        "name": "two-mounts",
        "attachments": {"tcs-1": "mount", "tcs-2": "mount"},
        "parts": [{
            "name": "ota",
            "parts": [{"name": "cam", "instrument": "cam-1"}]}]}))
    with pytest.raises(ValueError, match="exactly one root device"):
        resolve_target_ref(two_mounts, BINDINGS)


def test_incoherent_bindings_fail_at_yaml_load(config):
    """SensorPlan runs the same two checks the resolver does, so the failure
    lands at load — earlier than the resolver, and long before a task."""
    with pytest.raises(ValueError, match="no root device claims target trait"):
        SensorPlan.model_validate({
            "sensor": config.sensor.model_dump(),
            "bindings": BINDINGS.model_copy(
                update={"target_trait": "nonesuch"}).model_dump()})


# ---- authored chronology ------------------------------------------------

def wide(exposure_s: float, band: str) -> ExposureReq:
    return ExposureReq(select=ByRef("wide"), exposure_s=exposure_s,
                       config={"filter": band})


def test_steps_resolve_in_the_order_authored(resolver):
    collect = resolver.to_collect(ExternalTask(
        name="two-target",
        steps=(TaskStep(TargetSpec("M51"), (wide(30, "r"),)),
               TaskStep(TargetSpec("M101"), (wide(60, "g"),)))))
    assert len(collect.steps) == 2
    assert [repr(s.settings["tcs-1"]) for s in collect.steps] == [
        "sidereal:M51", "sidereal:M101"]
    plans = [s.plans[("piggyback", "wide")] for s in collect.steps]
    assert [p.exposure_s for p in plans] == [30, 60]
    assert [p.settings["fw-wide"] for p in plans] == ["pos:r", "pos:g"]


def test_a_step_boundary_is_the_licence_to_change_a_device(resolver):
    """The one-value-per-step rule is per step, not per task: the same
    wheel takes a different band in the next step, which is the whole
    reason to author the ordering."""
    collect = resolver.to_collect(ExternalTask(
        name="sweep",
        steps=tuple(TaskStep(TargetSpec("M51"), (wide(10, b),))
                    for b in "griz")))
    assert [s.plans[("piggyback", "wide")].settings["fw-wide"]
            for s in collect.steps] == ["pos:g", "pos:r", "pos:i", "pos:z"]


def test_conflicting_requests_name_the_step_they_conflict_in(resolver):
    with pytest.raises(ValueError, match=r"pair, step 1: .*more than one"):
        resolver.to_collect(ExternalTask(
            name="pair",
            steps=(TaskStep(TargetSpec("M51"), (wide(1, "r"),)),
                   TaskStep(TargetSpec("M51"),
                            (ExposureReq(select=ByRef("wide"), exposure_s=1),
                             ExposureReq(select=ByRef("cam-wide"),
                                         exposure_s=2))))))


def test_align_reaches_the_resolved_step(resolver):
    (step,) = resolver.to_collect(ExternalTask(steps=(
        TaskStep(TargetSpec("M51"),
                 (wide(30, "r"), ExposureReq(select=ByRef("cam-sci"),
                                             exposure_s=10)),
                 align="midpoint"),))).steps
    assert step.align == "midpoint"


def test_single_is_the_one_step_case(resolver):
    task = ExternalTask.single(TargetSpec("M51"), [wide(30, "r")], name="t")
    assert task.steps == (TaskStep(TargetSpec("M51"), (wide(30, "r"),)),)
    assert task.name == "t"


def test_on_failure_reaches_the_collect(resolver):
    collect = resolver.to_collect(ExternalTask(
        steps=(TaskStep(TargetSpec("M51"), (wide(1, "r"),)),),
        on_failure="continue"))
    assert collect.on_failure == "continue"


# ---- a resolved collect compiles ----------------------------------------

# Selection matches on role, modality and attributes; none of those say
# anything about whether an instrument may be exposed, or about whether
# two of them can be exposed at once. So resolution has to check.

def test_selecting_a_non_collect_target_is_caught_at_resolution(resolver):
    """cam-guide is published in the manifest — a task source may
    legitimately discover it — but it is not a collect target."""
    with pytest.raises(ValueError, match="not a collect target"):
        resolver.to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByRef("cam-guide"), exposure_s=1)]))


def test_mutually_exclusive_selection_is_caught_at_resolution(resolver):
    """ByCapability(role=SCIENCE, count=2) matches port-a and port-b,
    which sit on opposite ports of one selector. Nothing in the match
    predicate could have known that; the resolver reads the topology
    and does."""
    with pytest.raises(ValueError, match="different ports of selector"):
        resolver.to_collect(ExternalTask.single(
            TargetSpec("M51"),
            [ExposureReq(select=ByCapability(role=InstrumentRole.SCIENCE,
                                             count=2), exposure_s=1)]))


def test_a_step_with_no_exposures_is_caught_at_resolution(resolver):
    with pytest.raises(ValueError, match="step has no frame plans"):
        resolver.to_collect(ExternalTask(
            steps=(TaskStep(TargetSpec("M51"), ()),)))


def test_every_resolved_collect_compiles(topo, devices, resolver):
    """The contract, stated as a test: what to_collect returns is
    compilable, so a task source never learns a topology rule from the
    layer below."""
    collect = resolver.to_collect(ExternalTask(
        name="ok",
        steps=(TaskStep(TargetSpec("M51"), (wide(30, "r"),)),
               TaskStep(TargetSpec("M101"), (wide(60, "g"),)))))
    validate_collect(topo, collect)
    compile_collect(topo, devices, collect)


# ---- the golden: authored chronology, derived everything else -----------

def sci(exposure_s: float, band: str, n: int = 1) -> ExposureReq:
    return ExposureReq(select=ByRef("cam-sci"), exposure_s=exposure_s,
                       n_frames=n, config={"filter": band})


# Two co-schedulable science cameras (different optical assemblies, so no
# selector between them) over three authored steps. The steps are the
# only thing the task source states; the barriers, the elisions and the
# overlap are all read off the topology.
SEQUENCE = ExternalTask(
    name="seq",
    steps=(
        # cam-sci r / cam-wide g under one slew.
        TaskStep(TargetSpec("M51"), (sci(20, "r", 2), wide(45, "g"))),
        # Same target, same wide band: both re-commands elide, so only
        # fw-a moves, and it moves under cam-wide's next exposure.
        TaskStep(TargetSpec("M51"), (sci(20, "i", 2), wide(45, "g"))),
        # A new target: the mount is on every path, so this one waits
        # for everybody — the global barrier, where it is real.
        TaskStep(TargetSpec("M101"), (sci(30, "i"),)),
    ),
)


def test_authored_sequence_graph(topo, devices, resolver, assert_golden):
    assert_golden("collect-sequence",
                  format_graph(compile_collect(topo, devices,
                                               resolver.to_collect(SEQUENCE))))
