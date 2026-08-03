# SPDX-License-Identifier: Apache-2.0
import enum

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from sensorkit.common.predicate import (
    MISSING,
    AnyPredicate,
    FieldMatch,
    PathType,
    PredicateError,
    all_of,
    any_of,
    between,
    contains,
    eq,
    exists,
    ge,
    gt,
    is_in,
    le,
    lt,
    ne,
    not_,
    read_path,
    resolve_path,
)


class Band(enum.StrEnum):
    RED = "r"
    GREEN = "g"


class Filter(BaseModel):
    name: str
    position: int | None = None


class Filters(BaseModel):
    filters: list[Filter] = []
    label: str = ""


class Optics(BaseModel):
    fov_deg: float
    detector: str | None = None
    band: Band = Band.RED
    filters: Filters = Filters()


@pytest.fixture
def optics():
    return Optics(
        fov_deg=0.3,
        detector="cmos",
        filters=Filters(
            label="wheel-a",
            filters=[Filter(name="r", position=1), Filter(name="g", position=2)],
        ),
    )


def test_comparisons():
    assert eq(3).test(3)
    assert not eq(3).test(4)
    assert ne(3).test(4)
    assert lt(3).test(2) and not lt(3).test(3)
    assert le(3).test(3)
    assert gt(3).test(4) and not gt(3).test(3)
    assert ge(3).test(3)


def test_between_is_inclusive():
    assert between(1.0, 2.0).test(1.0)
    assert between(1.0, 2.0).test(2.0)
    assert not between(1.0, 2.0).test(2.5)


def test_membership_and_containment():
    assert is_in("r", "g").test("r")
    assert not is_in("r", "g").test("i")
    assert contains("r").test(["r", "g"])
    assert contains("r", "g").test(["r", "g"])
    assert not contains("r", "i").test(["r", "g"])
    assert not contains("r").test("r")


def test_exists_counts_an_empty_collection_as_absent():
    assert exists().test(0)
    assert exists().test(False)
    assert not exists().test(None)
    assert not exists().test(())
    assert exists(False).test(MISSING)
    assert exists(False).test([])


def test_composites():
    assert all_of(gt(1), lt(5)).test(3)
    assert not all_of(gt(1), lt(5)).test(7)
    assert any_of(eq(1), eq(5)).test(5)
    assert not any_of(eq(1), eq(5)).test(3)
    assert not_(eq(1)).test(2)


def test_a_value_that_is_missing_or_none_matches_nothing():
    for predicate in (eq(1), ne(1), gt(1), between(0, 2), is_in(1), contains(1), not_(eq(1))):
        assert not predicate.test(MISSING)
        assert not predicate.test(None)


def test_absence_is_selectable_only_by_saying_so():
    absent = Optics(fov_deg=0.3)

    assert not FieldMatch(field="detector", predicate=ne("cmos")).test(absent)
    assert not FieldMatch(field="detector", predicate=not_(eq("cmos"))).test(absent)
    assert FieldMatch(field="detector", predicate=exists(False)).test(absent)
    assert FieldMatch(field="detector", predicate=any_of(exists(False), ne("cmos"))).test(absent)


def test_negating_exists_is_rejected_as_unsatisfiable():
    # Raised inside a model validator, so it reaches the author as a pydantic error either way.
    with pytest.raises(ValidationError, match="use exists\\(False\\)"):
        not_(exists())

    with pytest.raises(ValidationError, match="use exists\\(False\\)"):
        TypeAdapter(AnyPredicate).validate_python({"op": "not", "predicate": {"op": "exists"}})


def test_an_incomparable_value_is_no_match():
    assert not gt(1).test("text")
    assert not between(1, 2).test("text")
    assert not contains("r").test(7)


def test_read_path_walks_nested_models(optics):
    assert read_path(optics, "fov_deg") == 0.3
    assert read_path(optics, "filters.label") == "wheel-a"
    assert read_path(optics, "detector") == "cmos"


def test_read_path_projects_over_a_collection(optics):
    assert read_path(optics, "filters.filters.name") == ("r", "g")
    assert read_path(optics, "filters.filters.position") == (1, 2)


def test_read_path_returns_the_collection_it_ends_on(optics):
    assert read_path(optics, "filters.filters") == optics.filters.filters


def test_read_path_reads_nothing_for_an_absent_or_severed_path(optics):
    assert read_path(optics, "nonesuch") is MISSING
    assert read_path(Optics(fov_deg=0.3), "detector.anything") is MISSING


def test_resolve_path_carries_annotations_and_projection():
    assert resolve_path(Optics, "fov_deg") == PathType((float,))
    assert resolve_path(Optics, "detector") == PathType((str,))
    assert resolve_path(Optics, "filters.filters.name") == PathType((str,), projected=True)
    assert not resolve_path(Optics, "filters.filters").projected


def test_resolve_path_rejects_a_typo():
    with pytest.raises(PredicateError, match="no field 'fov_degs'"):
        resolve_path(Optics, "fov_degs")

    with pytest.raises(PredicateError, match="no field 'nmae'"):
        resolve_path(Optics, "filters.filters.nmae")


def test_validate_against_accepts_a_compatible_operand():
    FieldMatch(field="fov_deg", predicate=le(0.5)).validate_against(Optics)
    FieldMatch(field="fov_deg", predicate=le(1)).validate_against(Optics)
    FieldMatch(field="detector", predicate=eq("cmos")).validate_against(Optics)
    FieldMatch(field="filters.filters.name", predicate=contains("r")).validate_against(Optics)
    FieldMatch(field="filters.filters", predicate=exists()).validate_against(Optics)


def test_validate_against_rejects_a_mistyped_operand():
    with pytest.raises(PredicateError, match="cannot compare against str"):
        FieldMatch(field="detector", predicate=gt(5)).validate_against(Optics)

    with pytest.raises(PredicateError, match="cannot compare against float"):
        FieldMatch(field="fov_deg", predicate=eq("wide")).validate_against(Optics)

    with pytest.raises(PredicateError, match="cannot compare against float"):
        FieldMatch(field="fov_deg", predicate=between(0.1, "wide")).validate_against(Optics)


def test_an_enum_annotation_accepts_its_own_values():
    FieldMatch(field="band", predicate=eq("r")).validate_against(Optics)
    FieldMatch(field="band", predicate=is_in("r", "g")).validate_against(Optics)

    with pytest.raises(PredicateError, match="cannot compare against Band"):
        FieldMatch(field="band", predicate=eq("ultraviolet")).validate_against(Optics)


def test_a_scalar_predicate_rejects_a_collection_path():
    with pytest.raises(PredicateError, match="use 'contains'"):
        FieldMatch(field="filters.filters.name", predicate=eq("r")).validate_against(Optics)

    with pytest.raises(PredicateError, match="use 'contains'"):
        FieldMatch(field="filters.filters", predicate=gt(1)).validate_against(Optics)


def test_contains_needs_a_collection_path():
    with pytest.raises(PredicateError, match="'contains' needs a collection"):
        FieldMatch(field="detector", predicate=contains("cmos")).validate_against(Optics)

    with pytest.raises(PredicateError, match="cannot equal an element of str"):
        FieldMatch(field="filters.filters.name", predicate=contains(5)).validate_against(Optics)


def test_composites_validate_their_children():
    with pytest.raises(PredicateError, match="cannot compare against float"):
        FieldMatch(field="fov_deg", predicate=all_of(gt(0.1), eq("wide"))).validate_against(Optics)

    with pytest.raises(PredicateError, match="cannot compare against float"):
        FieldMatch(field="fov_deg", predicate=not_(eq("wide"))).validate_against(Optics)


def test_a_match_without_a_field_addresses_the_whole_value():
    match = FieldMatch(predicate=contains("r"))
    match.validate_against(list[str])
    assert match.test(["r", "g"])


def test_field_match_evaluates_end_to_end(optics):
    assert FieldMatch(field="fov_deg", predicate=le(0.5)).test(optics)
    assert not FieldMatch(field="fov_deg", predicate=le(0.1)).test(optics)
    assert FieldMatch(field="filters.filters.name", predicate=contains("r")).test(optics)
    assert not FieldMatch(field="filters.filters.name", predicate=contains("i")).test(optics)
    assert FieldMatch(field="band", predicate=eq("r")).test(optics)


def test_evaluation_tolerates_an_object_the_match_was_not_validated_against():
    match = FieldMatch(field="fov_deg", predicate=le(0.5))
    match.validate_against(Optics)

    assert not match.test(Filters())
    assert not match.test("not a model")
    assert not match.test(None)


def test_predicates_round_trip_through_the_wire():
    original = FieldMatch(
        field="filters.filters.name",
        predicate=any_of(contains("r"), all_of(exists(), not_(contains("i")))),
    )
    restored = FieldMatch.model_validate_json(original.model_dump_json())

    assert restored == original
    assert restored.test(Optics(fov_deg=0.3, filters=Filters(filters=[Filter(name="r")])))


def test_the_wire_form_rejects_an_unknown_operator():
    with pytest.raises(ValidationError):
        FieldMatch.model_validate({"field": "fov_deg", "predicate": {"op": "approx", "value": 1}})


def test_the_union_validates_each_operator():
    adapter = TypeAdapter(AnyPredicate)

    for predicate in (
        eq(1),
        lt(1),
        between(0, 1),
        is_in(1, 2),
        contains("r"),
        exists(),
        all_of(eq(1)),
        any_of(eq(1)),
        not_(eq(1)),
    ):
        assert adapter.validate_python(predicate.model_dump()) == predicate
