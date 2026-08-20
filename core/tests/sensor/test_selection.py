# SPDX-License-Identifier: Apache-2.0
"""Selection: what a predicate reads, and how a composition means different
things of a chain and of a placement."""
from __future__ import annotations

import pytest
from pydantic import TypeAdapter

from sensorkit.core.trait import Trait
from sensorkit.sensor.selection import (
    AllOf,
    AnyOf,
    AnySelection,
    HasTag,
    HasTrait,
    IsInstrument,
    IsKind,
    IsRef,
    Not,
    Publishes,
    SelectionError,
    Supports,
)
from sensorkit.std.traits import Connect, Enabled, MustConnect, MustEnable

PARSE = TypeAdapter(AnySelection).validate_python


def test_a_bare_string_is_a_trait_name():
    assert PARSE("MustConnect") == HasTrait(trait="MustConnect")


def test_a_command_type_stands_for_its_id():
    assert PARSE({"supports": Connect}) == Supports(supports="Connect")


def test_a_keyword_type_stands_for_its_key():
    assert PARSE({"publishes": Enabled}) == Publishes(publishes="Enabled")


def test_a_keyword_that_was_never_declared_is_rejected():
    with pytest.raises(ValueError, match="is not a declared keyword"):
        PARSE({"publishes": int})


def test_one_key_per_question():
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        PARSE({"trait": "MustConnect", "tag": "primary"})


def test_a_mapping_naming_no_question_is_rejected():
    with pytest.raises(ValueError, match="discriminator"):
        PARSE({"nonsense": 1})


def test_a_composition_needs_a_member():
    with pytest.raises(ValueError, match="at least 1 item"):
        PARSE({"all_of": []})


def test_a_selection_is_frozen():
    with pytest.raises(ValueError, match="frozen"):
        PARSE("MustConnect").trait = "other"


def test_a_composition_distributes_over_a_chain(topology, facts, at):
    # The wheel homes and the camera initializes; no one device does both.
    both = AllOf(all_of=(Supports(supports="Home"), Supports(supports="Init")))
    chain = topology.chain(at("cam-sci"))

    assert both.reaches(chain, facts)
    assert not any(both.matches(p, facts) for p in chain)


def test_a_composition_conjoins_over_a_placement(facts, at):
    both = AllOf(all_of=(Supports(supports="Connect"),
                         Supports(supports="Home")))

    assert both.matches(at("mount"), facts)
    assert not both.matches(at("cam-sci"), facts)


def test_negation_of_a_chain_composition(topology, facts, at):
    absent = Not(negated=Supports(supports="Deinit"))

    # cam-sci deinitializes, so the chain does and its negation does not.
    assert not absent.reaches(topology.chain(at("cam-sci")), facts)
    assert absent.reaches(topology.chain(at("cam-guide")), facts)


def test_negation_distinguishes_a_chain_from_a_placement(topology, facts, at):
    absent = Not(negated=Supports(supports="Deinit"))
    chain = topology.chain(at("cam-sci"))

    # No single device on the chain deinitializes except the camera, so the
    # placement question and the chain question answer differently.
    assert absent.matches(at("mount"), facts)
    assert not absent.reaches(chain, facts)


def test_nested_compositions(topology, facts, at):
    nested = AnyOf(any_of=(
        AllOf(all_of=(Supports(supports="Home"),
                      Supports(supports="Nothing"))),
        Not(negated=Supports(supports="Nothing")),
    ))

    assert nested.reaches(topology.chain(at("cam-sci")), facts)


def test_matching_yields_one_placement_per_device_admitted(placements, facts):
    admitted = HasTrait(trait="MustEnable").matching(placements, facts)

    assert [p.device for p in admitted] == ["dome"]


def test_matching_is_scoped_to_the_universe_it_is_given(topology, facts, at):
    homes = Supports(supports="Home")
    everywhere = homes.matching(topology.placements(), facts)
    on_one_chain = homes.matching(topology.chain(at("cam-guide")), facts)

    assert [p.device for p in everywhere] == [
        "mount", "pickoff", "foc-sci", "wheel"]
    assert [p.device for p in on_one_chain] == ["mount", "pickoff"]


def test_refs_answers_in_walk_order(placements, facts):
    assert IsKind(kind="any").refs(placements, facts)[:3] == (
        "mount", "dome", "cover")


def test_is_kind_answers_from_the_placement_surface(placements, facts):
    assert IsKind(kind="selector").refs(placements, facts) == ("pickoff",)
    assert IsKind(kind="instrument").refs(placements, facts) == (
        "cam-sci", "cam-guide", "cam-acq")


def test_is_instrument_answers_from_the_record_not_from_capability(
        placements, facts):
    # cam-acq reports almost nothing and is still a collect target.
    assert "cam-acq" in IsInstrument().refs(placements, facts)


def test_is_instrument_false_admits_everything_that_is_not_one(placements,
                                                               facts):
    assert IsInstrument(instrument=False).refs(placements, facts) == (
        "mount", "dome", "cover", "pickoff", "foc-sci", "wheel")


def test_is_ref_answers_without_facts(at):
    assert IsRef(device="mount").matches(at("mount"))
    assert not IsRef(device="mount").matches(at("dome"))


def test_a_tag_is_its_own_namespace(placements, facts):
    assert HasTag(tag="primary").refs(placements, facts) == ("mount",)
    assert HasTrait(trait="primary").refs(placements, facts) == ()


def test_has_trait_reads_every_trait_the_device_satisfies(facts, at):
    # The dome's record declares both; cover's declares none.
    assert HasTrait(trait="MustEnable").matches(at("dome"), facts)
    assert HasTrait(trait="MustConnect").matches(at("cover"), facts)


def test_a_record_declaring_a_subset_changes_no_answer(topology, facts, at):
    # mount's record declares MustConnect alone, and it establishes the same.
    assert topology.record(at("mount")).traits == ("MustConnect",)
    assert HasTrait(trait="MustConnect").matches(at("mount"), facts)
    # The selector declares MustConnect and is asked outside it all the same.
    assert Supports(supports="Home").matches(at("pickoff"), facts)


def test_every_question_but_device_needs_facts(at):
    asked = (HasTrait(trait="MustConnect"), HasTag(tag="primary"),
             IsKind(kind="any"), IsInstrument(), Supports(supports="Connect"),
             Publishes(publishes="Enabled"))

    for selection in asked:
        with pytest.raises(SelectionError, match="none was given"):
            selection.matches(at("mount"))


def test_a_capability_predicate_raises_rather_than_answering_false(at):
    with pytest.raises(SelectionError):
        Supports(supports="Connect").matches(at("mount"))


def test_an_unanswerable_predicate_under_negation_still_raises(at):
    with pytest.raises(SelectionError):
        Not(negated=Supports(supports="Connect")).matches(at("mount"))


def test_supporting_asks_for_every_command_a_trait_requires():
    selection = AllOf.supporting(MustConnect)

    assert selection == AllOf(all_of=(Supports(supports="Connect"),
                                      Supports(supports="Disconnect")))


def test_supporting_asks_for_published_keywords_too():
    members = AllOf.supporting(MustEnable).all_of

    assert Supports(supports="Enable") in members
    assert Supports(supports="Disable") in members


def test_supporting_a_trait_that_requires_nothing_is_an_error():
    empty = Trait(name="Empty")

    with pytest.raises(ValueError, match="nothing to ask a device for"):
        AllOf.supporting(empty)
