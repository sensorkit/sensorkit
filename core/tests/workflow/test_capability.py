# SPDX-License-Identifier: Apache-2.0
"""
The external-request frontend: the manifest a task source discovers,
the two selector forms, the routing that derives a device from a
command, and the chronology that is authored rather than derived.

The vocabulary below stands in for a deployment's: commands carrying
their own registered id, keyword models registered the way any other
keyword is, and a capability index of the kind a device layer projects
from what its devices publish. Nothing in the library knows what any of
it means, which is the property these tests are written to hold.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pytest
from pydantic import BaseModel, ValidationError

from sensorkit.common.keyword import KeywordDict, declare_keyword
from sensorkit.common.predicate import contains, eq, exists, ge, gt, le
from sensorkit.workflow import (
    ByCapability,
    ByRef,
    CapabilityIndex,
    CommandRequest,
    DeviceCapabilities,
    DeviceIndex,
    ExposureRequest,
    InstrumentRole,
    KeywordMatch,
    RequestResolver,
    RequestStep,
    SensorModel,
    SensorPlan,
    Topology,
    compile_collect,
    format_graph,
    portability,
    validate_collect,
)


class Band(BaseModel):
    name: str
    position: int


@declare_keyword(key="demo_filters")
class DemoFilters(BaseModel):
    filters: tuple[Band, ...] = ()


@declare_keyword(key="demo_optics")
class DemoOptics(BaseModel):
    fov_deg: float | None = None
    pixel_scale: float | None = None


# Commands are opaque to the library: it reads `command_id` off them and
# compares them for equality, and that is the whole of the contract.


@dataclass(frozen=True)
class FollowTarget:
    command_id: ClassVar[str] = "follow_target"
    target: str


@dataclass(frozen=True)
class SetFilter:
    command_id: ClassVar[str] = "set_filter"
    filter: str


@dataclass(frozen=True)
class ConfigureSensor:
    command_id: ClassVar[str] = "configure_sensor"
    binning: int = 1


@dataclass(frozen=True)
class SetFocus:
    command_id: ClassVar[str] = "set_focus"
    position: float


@dataclass(frozen=True)
class HomeAxis:
    command_id: ClassVar[str] = "home_axis"


def bands(*names: str) -> DemoFilters:
    return DemoFilters(filters=tuple(
        Band(name=n, position=i) for i, n in enumerate(names)))


# What the demo sensor's devices publish about themselves. Only the
# structure is authored; every tier of a descriptor is read from here.
CAPS: CapabilityIndex = {
    "tcs-1": DeviceCapabilities(
        traits=frozenset({"tracker"}),
        commands=frozenset({"follow_target", "set_focus"})),
    "sel-1": DeviceCapabilities(commands=frozenset({"select_port"})),
    "fw-shared": DeviceCapabilities(
        commands=frozenset({"set_filter"}),
        keywords=KeywordDict(bands("g", "r"))),
    "cam-sci": DeviceCapabilities(
        traits=frozenset({"imager"}),
        commands=frozenset({"configure_sensor"}),
        keywords=KeywordDict(DemoOptics(fov_deg=0.3, pixel_scale=0.2))),
    "fw-a": DeviceCapabilities(
        commands=frozenset({"set_filter", "home_axis"}),
        keywords=KeywordDict(bands("r", "i", "z"))),
    "rot-a": DeviceCapabilities(commands=frozenset({"home_axis"})),
    "spec-1": DeviceCapabilities(
        traits=frozenset({"spectrograph"}),
        commands=frozenset({"configure_sensor"}),
        keywords=KeywordDict(DemoOptics(fov_deg=0.05))),
    "cam-guide": DeviceCapabilities(
        traits=frozenset({"imager"}),
        commands=frozenset({"configure_sensor"})),
    "foc-2": DeviceCapabilities(
        commands=frozenset({"set_focus"}),
        keywords=KeywordDict(DemoOptics(fov_deg=1.0, pixel_scale=0.9))),
    "cam-wide": DeviceCapabilities(
        traits=frozenset({"imager"}),
        commands=frozenset({"configure_sensor"}),
        keywords=KeywordDict(DemoOptics(fov_deg=2.5))),
    "fw-wide": DeviceCapabilities(
        commands=frozenset({"set_filter"}),
        keywords=KeywordDict(bands("g", "r"))),
}

ALIASES = {"wide": "cam-wide"}

WIDE = ("piggyback", "wide")
SCI = ("main-ota", "sel", "port-a")


@pytest.fixture
def resolver(topo, devices) -> RequestResolver:
    return RequestResolver(topo, devices, CAPS, ALIASES)


@pytest.fixture
def entries(resolver) -> dict[str, object]:
    return {e.ref: e for e in resolver.manifest}


def follow(target: str) -> CommandRequest:
    return CommandRequest(command=FollowTarget(target))


def step(*exposures: ExposureRequest, target: str = "M51",
         align: str = "start") -> RequestStep:
    return RequestStep(exposures=exposures, settings=(follow(target),),
                       align=align)


def wide(integration_time: float, band: str = "r",
         frame_count: int = 1) -> ExposureRequest:
    return ExposureRequest(
        select=ByRef("wide"), integration_time=integration_time,
        frame_count=frame_count,
        settings=(CommandRequest(command=SetFilter(band)),))


def sci(integration_time: float, band: str = "r",
        frame_count: int = 1) -> ExposureRequest:
    return ExposureRequest(
        select=ByRef("cam-sci"), integration_time=integration_time,
        frame_count=frame_count,
        settings=(CommandRequest(command=SetFilter(band)),))


# ---- the manifest -------------------------------------------------------

def test_manifest_publishes_one_entry_per_instrument(resolver):
    assert {e.ref for e in resolver.manifest} == {
        "cam-sci", "spec-1", "cam-guide", "cam-wide"}


def test_alias_becomes_the_published_handle(entries):
    assert entries["cam-wide"].handle == "wide"
    assert entries["cam-sci"].handle == "main-ota/sel/port-a"   # no alias: the path


def test_traits_span_the_structure_and_what_devices_declare(entries):
    """Both halves of a claim: the deployment attaches fw-a as a filter
    wheel, and cam-sci says for itself that it is an imager."""
    assert {"filter_wheel", "mount", "imager"} <= entries["cam-sci"].traits
    assert "imager" not in entries["spec-1"].traits


def test_commands_are_the_union_over_the_chain(entries):
    """A descriptor asks what the chain can do, not what the detector
    can: the wheel's set_filter is the instrument's to offer."""
    assert {"set_filter", "configure_sensor", "follow_target"} <= (
        entries["cam-sci"].commands)
    assert "set_filter" not in entries["cam-guide"].commands


def test_chain_keywords_prefer_the_deepest_publisher(entries):
    """cam-sci's chain carries two wheels; the private one is the one
    the instrument would be commanded on, so it is the one it reports."""
    assert [b.name for b in entries["cam-sci"].keywords[DemoFilters].filters] == [
        "r", "i", "z"]


def test_the_deepest_publisher_wins_whole(entries):
    """Merging is per keyword, not per field: cam-wide's optics replace
    the focuser's rather than filling in around them."""
    assert entries["cam-wide"].keywords[DemoOptics] == DemoOptics(fov_deg=2.5)


def test_a_trait_narrows_the_reading_to_its_claimants(entries):
    """Two devices on one chain describe the optics; naming the trait
    asks the one that was meant."""
    entry = entries["cam-wide"]
    assert entry.keywords_for("focuser")[DemoOptics].pixel_scale == 0.9
    assert entry.keywords_for(None)[DemoOptics].pixel_scale is None


def test_a_trait_nobody_claims_reads_nothing(entries):
    assert not entries["cam-wide"].keywords_for("spectrograph")


# ---- static keywords ----------------------------------------------------

STATIC = SensorModel.model_validate({
    "name": "static",
    "attachments": [{"ref": "tcs-1", "trait": "mount"}],
    "parts": [{
        "name": "ota",
        "attachments": [{"ref": "fw-1", "trait": "filter_wheel",
                         "keywords": {"demo_filters": {
                             "filters": [{"name": "n", "position": 0}]}}}],
        "parts": [{"name": "cam", "instrument": "cam-1",
                   "keywords": {"demo_optics": {"fov_deg": 0.4}}}]}]})


def static_resolver(caps: CapabilityIndex) -> RequestResolver:
    return RequestResolver(Topology(STATIC), DeviceIndex(STATIC), caps)


def test_a_site_may_describe_a_device_that_does_not_publish():
    (entry,) = static_resolver({}).manifest
    assert entry.keywords[DemoOptics].fov_deg == 0.4
    assert [b.name for b in entry.keywords[DemoFilters].filters] == ["n"]


def test_what_a_device_publishes_wins_over_what_a_site_supplied():
    """Static keywords are defaults for a driver that reports nothing,
    never an override of one that reports something."""
    live = {"cam-1": DeviceCapabilities(
        keywords=KeywordDict(DemoOptics(fov_deg=0.9)))}
    (entry,) = static_resolver(live).manifest
    assert entry.keywords[DemoOptics].fov_deg == 0.9


def test_static_keywords_go_through_the_registry_at_load():
    with pytest.raises(ValidationError, match="not a declared keyword"):
        SensorModel.model_validate({
            "name": "typo",
            "parts": [{"name": "cam", "instrument": "cam-1",
                       "keywords": {"demo_optcs": {"fov_deg": 0.4}}}]})


def test_a_static_keyword_is_validated_against_its_model():
    with pytest.raises(ValidationError):
        SensorModel.model_validate({
            "name": "bad",
            "parts": [{"name": "cam", "instrument": "cam-1",
                       "keywords": {"demo_optics": {"fov_deg": "wide"}}}]})


# ---- keyword matches ----------------------------------------------------

def test_a_path_naming_no_field_is_rejected_where_it_is_written():
    """The alternative is a task that resolves to nothing at 2am and
    says only that it matched no instrument."""
    with pytest.raises(ValidationError, match="no field 'fov_dgree'"):
        KeywordMatch(keyword=DemoOptics, field="fov_dgree", predicate=gt(1))


def test_an_operand_that_could_never_compare_is_rejected():
    with pytest.raises(ValidationError, match="cannot equal an element of str"):
        KeywordMatch(keyword=DemoFilters, field="filters.name",
                     predicate=contains(3))


def test_an_unregistered_keyword_is_rejected():
    with pytest.raises(ValidationError, match="not a declared keyword"):
        KeywordMatch(keyword="nonesuch", predicate=exists())


def test_the_keyword_may_be_authored_as_its_type():
    assert KeywordMatch(keyword=DemoOptics, field="fov_deg",
                        predicate=le(1)).keyword == "demo_optics"


# ---- selection ----------------------------------------------------------

def test_by_ref_resolves_aliases(resolver):
    assert resolver.resolve_paths(ByRef("wide")) == (WIDE,)
    assert resolver.resolve_paths(ByRef("cam-wide")) == (WIDE,)


def test_by_ref_that_matches_nothing_is_an_error(resolver):
    with pytest.raises(ValueError, match="matches no instrument"):
        resolver.resolve_paths(ByRef("nope"))


def test_selection_by_declared_trait(resolver):
    assert resolver.resolve_paths(
        ByCapability(traits=("spectrograph",))) == (("main-ota", "sel", "port-b"),)


def test_selection_by_supported_command(resolver):
    """The tier for a capability no declared trait names: an instrument
    that can be filtered, whatever the device doing it is called."""
    paths = resolver.resolve_paths(
        ByCapability(role=InstrumentRole.SCIENCE, capabilities=("set_filter",),
                     count=3))
    assert set(paths) == {SCI, ("main-ota", "sel", "port-b"), WIDE}


def test_selection_by_keyword_predicate(resolver):
    assert resolver.resolve_paths(ByCapability(
        role=InstrumentRole.SCIENCE,
        requires=(KeywordMatch(keyword=DemoOptics, field="fov_deg",
                               predicate=ge(1.0)),))) == (WIDE,)


def test_a_predicate_projects_over_a_collection(resolver):
    """'has an r filter' reads the names of a list of filters, which is
    the shape a real vocabulary has."""
    assert resolver.resolve_paths(ByCapability(
        traits=("imager",),
        requires=(KeywordMatch(keyword=DemoFilters, field="filters.name",
                               predicate=contains("i")),))) == (SCI,)


def test_a_match_may_name_the_trait_it_reads_from(resolver):
    assert resolver.resolve_paths(ByCapability(
        requires=(KeywordMatch(keyword=DemoOptics, field="pixel_scale",
                               predicate=eq(0.9), trait="focuser"),))) == (WIDE,)


def test_an_instrument_publishing_nothing_matches_no_predicate(resolver):
    """cam-guide's chain reports no optics at all, and absence is not a
    match — a driver that says nothing cannot be selected by accident."""
    with pytest.raises(ValueError, match="matched 0 instrument"):
        resolver.resolve_paths(ByCapability(
            role=InstrumentRole.GUIDE,
            requires=(KeywordMatch(keyword=DemoOptics, field="fov_deg",
                                   predicate=le(99)),)))


def test_the_tiers_conjoin(resolver):
    with pytest.raises(ValueError, match="matched 0 instrument"):
        resolver.resolve_paths(ByCapability(
            traits=("spectrograph",), capabilities=("home_axis",)))


def test_by_capability_reports_insufficient_cardinality(resolver):
    with pytest.raises(ValueError, match=r"traits=\['imager'\].*matched 3"):
        resolver.resolve_paths(ByCapability(traits=("imager",), count=99))


# ---- routing: a command finds its own device ----------------------------

def test_instrument_scope_takes_the_deepest_claim(resolver):
    """'Set this instrument's filter' means its own wheel, not the
    selector wheel it shares with the far port."""
    (step_,) = resolver.to_collect([step(sci(10, "i"))]).steps
    assert step_.plans[SCI].settings == {"fw-a": SetFilter("i")}
    assert "fw-shared" not in step_.settings


def test_a_scope_picks_the_other_side(resolver):
    (step_,) = resolver.to_collect([step(ExposureRequest(
        select=ByRef("cam-sci"), integration_time=10,
        settings=(CommandRequest(command=SetFilter("g"), scope="shared"),)))
    ]).steps
    assert step_.settings["fw-shared"] == SetFilter("g")
    assert not step_.plans[SCI].settings


def test_sensor_scope_takes_the_shallowest_claim(resolver):
    """The mount is above every participant and the piggyback focuser is
    not, so the pointing setpoint lands on the mount without anything
    naming it."""
    (step_,) = resolver.to_collect([step(wide(30), sci(20))]).steps
    assert step_.settings["tcs-1"] == FollowTarget("M51")


def test_instrument_scope_prefers_the_instruments_own_focuser(resolver):
    """tcs-1 focuses the main OTA and foc-2 the piggyback; both are on
    cam-wide's chain, and depth is what tells them apart.

    foc-2 hangs above the instrument rather than on it, so it is shared
    by construction and the command lands on the step — routing picks
    the device, the structure places it.
    """
    (step_,) = resolver.to_collect([step(ExposureRequest(
        select=ByRef("wide"), integration_time=5,
        settings=(CommandRequest(command=SetFocus(0.1)),)))]).steps
    assert step_.settings["foc-2"] == SetFocus(0.1)
    assert "tcs-1" not in step_.plans[WIDE].settings


def test_a_tie_at_the_winning_depth_is_an_error(resolver):
    """fw-a and rot-a both home, both at port-a. Picking one would
    command a device the request did not mean."""
    with pytest.raises(ValueError, match=r"'home_axis' is supported by \['fw-a', 'rot-a'\]"):
        resolver.to_collect([step(ExposureRequest(
            select=ByRef("cam-sci"), integration_time=1,
            settings=(CommandRequest(command=HomeAxis()),)))])


def test_a_command_nothing_supports_is_an_error(resolver):
    with pytest.raises(ValueError, match="no device on guide-scope/guider supports"):
        resolver.resolve_step(RequestStep(exposures=(ExposureRequest(
            select=ByRef("cam-guide"), integration_time=1,
            settings=(CommandRequest(command=SetFilter("r")),)),)))


def test_a_scope_that_selects_nothing_says_so(resolver):
    with pytest.raises(ValueError, match="supports 'set_filter' as a shared device"):
        resolver.to_collect([step(ExposureRequest(
            select=ByRef("wide"), integration_time=1,
            settings=(CommandRequest(command=SetFilter("r"), scope="shared"),)))])


def test_two_roots_may_not_both_carry_the_setpoint():
    """Sensor scope is shallowest-wins, so two mounts tie at the root —
    a config error rather than a coin flip."""
    two = SensorModel.model_validate({
        "name": "two-mounts",
        "attachments": {"tcs-1": "mount", "tcs-2": "mount"},
        "parts": [{"name": "ota", "parts": [{"name": "cam",
                                             "instrument": "cam-1"}]}]})
    caps = {r: DeviceCapabilities(commands=frozenset({"follow_target"}))
            for r in ("tcs-1", "tcs-2")}
    resolver = RequestResolver(Topology(two), DeviceIndex(two), caps)

    with pytest.raises(ValueError, match=r"\['tcs-1', 'tcs-2'\] at the same"):
        resolver.to_collect([step(ExposureRequest(
            select=ByRef("cam-1"), integration_time=1))])


# ---- escape hatches -----------------------------------------------------

def test_a_named_device_still_has_to_support_the_command(resolver):
    with pytest.raises(ValueError, match="rot-a does not support 'set_filter'"):
        resolver.to_collect([step(ExposureRequest(
            select=ByRef("cam-sci"), integration_time=1,
            settings=(CommandRequest(command=SetFilter("r"), ref="rot-a"),)))])


def test_a_named_device_is_still_placed_by_the_structure(resolver):
    """Addressing is what `ref` bypasses. Whether the command lands on
    the step or on the frame plan is read off the topology either way."""
    (step_,) = resolver.to_collect([step(ExposureRequest(
        select=ByRef("cam-sci"), integration_time=1,
        settings=(CommandRequest(command=SetFilter("g"), ref="fw-shared"),
                  CommandRequest(command=HomeAxis(), ref="rot-a"))))]).steps
    assert step_.settings["fw-shared"] == SetFilter("g")
    assert step_.plans[SCI].settings == {"rot-a": HomeAxis()}


def test_portability_counts_what_a_request_named():
    portable = RequestStep(exposures=(ExposureRequest(
        select=ByCapability(traits=("imager",)), integration_time=1),))
    assert portability([portable]).portable

    report = portability([step(sci(1), wide(1)),
                          RequestStep(exposures=(ExposureRequest(
                              select=ByCapability(traits=("imager",)),
                              integration_time=1,
                              settings=(CommandRequest(command=HomeAxis(),
                                                       ref="rot-a"),)),))])
    assert (report.instruments, report.devices) == (2, 1)
    assert not report.portable


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

SHARED_CAPS: CapabilityIndex = {
    "tcs-1": DeviceCapabilities(commands=frozenset({"follow_target"})),
    "fw-shared": DeviceCapabilities(commands=frozenset({"set_filter"})),
}


def shared_wheel_step(filter_a: str, filter_b: str) -> RequestStep:
    return step(*(
        ExposureRequest(select=ByRef(ref), integration_time=10,
                        settings=(CommandRequest(command=SetFilter(band)),))
        for ref, band in (("cam-a", filter_a), ("cam-b", filter_b))))


@pytest.fixture
def shared_wheel() -> RequestResolver:
    return RequestResolver(Topology(SHARED_WHEEL), DeviceIndex(SHARED_WHEEL),
                           SHARED_CAPS)


def test_instruments_sharing_a_device_may_agree_on_it(shared_wheel):
    """Agreement is the whole point of a Step setting: two cameras
    exposing through one wheel in the same band is one apply."""
    (resolved,) = shared_wheel.to_collect([shared_wheel_step("r", "r")]).steps
    assert resolved.settings["fw-shared"] == SetFilter("r")
    assert all(not plan.settings for plan in resolved.plans.values())
    validate_collect(Topology(SHARED_WHEEL), shared_wheel.to_collect(
        [shared_wheel_step("r", "r")]))


def test_instruments_sharing_a_device_may_not_differ_on_it(shared_wheel):
    """A step is one configuration epoch, so the wheel holds one value
    across it. Keeping the last write would hand back frames labelled
    with a band they were not taken in."""
    with pytest.raises(ValueError, match="already commanded this step"):
        shared_wheel.to_collect([shared_wheel_step("r", "g")])


def test_two_unequal_commands_on_one_device_conflict(resolver):
    """Two different commands are as much a conflict as two values of
    one: a device's configuration for a step is whatever it was sent."""
    with pytest.raises(ValueError, match="already commanded this step"):
        resolver.to_collect([step(ExposureRequest(
            select=ByRef("cam-sci"), integration_time=1,
            settings=(CommandRequest(command=SetFilter("r"), ref="fw-a"),
                      CommandRequest(command=HomeAxis(), ref="fw-a"))))])


def test_two_requests_may_not_target_the_same_instrument(resolver):
    with pytest.raises(ValueError, match="more than one\n?\\s*exposure request"):
        resolver.to_collect(
            [step(wide(1), ExposureRequest(select=ByRef("cam-wide"),
                                           integration_time=2))],
            name="clash")


# ---- authored chronology ------------------------------------------------

def test_steps_resolve_in_the_order_authored(resolver):
    collect = resolver.to_collect(
        [step(wide(30, "r"), target="M51"),
         step(wide(60, "g"), target="M101")], name="two-target")
    assert [s.settings["tcs-1"] for s in collect.steps] == [
        FollowTarget("M51"), FollowTarget("M101")]
    plans = [s.plans[WIDE] for s in collect.steps]
    assert [p.exposure_s for p in plans] == [30, 60]
    assert [p.settings["fw-wide"] for p in plans] == [
        SetFilter("r"), SetFilter("g")]


def test_a_step_boundary_is_the_licence_to_change_a_device(resolver):
    """The one-value-per-step rule is per step, not per request: the
    same wheel takes a different band in the next step, which is the
    whole reason to author the ordering."""
    collect = resolver.to_collect(
        [step(wide(10, b)) for b in "griz"], name="sweep")
    assert [s.plans[WIDE].settings["fw-wide"] for s in collect.steps] == [
        SetFilter(b) for b in "griz"]


def test_conflicting_requests_name_the_step_they_conflict_in(resolver):
    with pytest.raises(ValueError, match=r"pair, step 1: .*more than one"):
        resolver.to_collect(
            [step(wide(1)),
             step(wide(1), ExposureRequest(select=ByRef("cam-wide"),
                                           integration_time=2))],
            name="pair")


def test_align_reaches_the_resolved_step(resolver):
    (resolved,) = resolver.to_collect(
        [step(wide(30), sci(10), align="midpoint")]).steps
    assert resolved.align == "midpoint"


def test_on_failure_reaches_the_collect(resolver):
    assert resolver.to_collect(
        [step(wide(1))], on_failure="continue").on_failure == "continue"


# ---- a resolved collect compiles ----------------------------------------

# Selection matches on capability, which says nothing about whether an
# instrument may be exposed, or about whether two of them can be exposed
# at once. So resolution has to check.

def test_selecting_a_non_collect_target_is_caught_at_resolution(resolver):
    """cam-guide is published in the manifest — a task source may
    legitimately discover it — but it is not a collect target."""
    with pytest.raises(ValueError, match="not a collect target"):
        resolver.to_collect([step(ExposureRequest(select=ByRef("cam-guide"),
                                                  integration_time=1))])


def test_mutually_exclusive_selection_is_caught_at_resolution(resolver):
    """A descriptor matching both ports of one selector cannot have
    known they are mutually exclusive; the resolver reads the topology
    and does."""
    with pytest.raises(ValueError, match="different ports of selector"):
        resolver.to_collect([step(ExposureRequest(
            select=ByCapability(capabilities=("configure_sensor",),
                                requires=(KeywordMatch(
                                    keyword=DemoOptics, field="fov_deg",
                                    predicate=le(0.5)),),
                                count=2),
            integration_time=1))])


def test_a_step_with_no_exposures_is_caught_at_resolution(resolver):
    with pytest.raises(ValueError, match="step has no frame plans"):
        resolver.to_collect([RequestStep(exposures=())])


def test_every_resolved_collect_compiles(topo, devices, resolver):
    """The contract, stated as a test: what to_collect returns is
    compilable, so a task source never learns a topology rule from the
    layer below."""
    collect = resolver.to_collect(
        [step(wide(30, "r")), step(wide(60, "g"), target="M101")], name="ok")
    validate_collect(topo, collect)
    compile_collect(topo, devices, collect)


# ---- aliases are the whole of what a deployment authors -----------------

def test_an_alias_naming_no_device_fails_at_load(config):
    with pytest.raises(ValueError, match="aliases name unknown device"):
        SensorPlan.model_validate({"sensor": config.sensor.model_dump(),
                                   "aliases": {"wide": "nonesuch"}})


# ---- the golden: authored chronology, derived everything else -----------

# Two co-schedulable science cameras (different optical assemblies, so no
# selector between them) over three authored steps. The steps are the
# only thing the task source states; the barriers, the elisions and the
# overlap are all read off the topology.
SEQUENCE = [
    # cam-sci r / cam-wide g under one slew.
    step(sci(20, "r", 2), wide(45, "g")),
    # Same target, same wide band: both re-commands elide, so only fw-a
    # moves, and it moves under cam-wide's next exposure.
    step(sci(20, "i", 2), wide(45, "g")),
    # A new target: the mount is on every path, so this one waits for
    # everybody — the global barrier, where it is real.
    step(sci(30, "i"), target="M101"),
]


def test_authored_sequence_graph(topo, devices, resolver, assert_golden):
    assert_golden("collect-sequence",
                  format_graph(compile_collect(
                      topo, devices,
                      resolver.to_collect(SEQUENCE, name="seq"))))
