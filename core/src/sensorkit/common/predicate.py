# SPDX-License-Identifier: Apache-2.0
"""Pure predicates over values, and predicate-at-a-path matching against pydantic models.

A predicate asks a static question about a value: no previous value, no edges, no deadband.
`sensorkit.common.condition` holds the streaming form of the same idea.

A `FieldMatch` binds a predicate to a dot path within a model. Because a path resolves against
`model_fields` rather than against an instance, a match can be checked before any value exists:
`FieldMatch.validate_against` rejects a path naming no field and an operand that could never
compare against the annotation it lands on. That is the difference between a mistyped match
failing where it is authored and one that silently never matches.

Evaluation is deliberately more forgiving than validation, since the object read at runtime may
not be an instance of the model the match was validated against. A path that reads nothing, or a
value that cannot compare, resolves to `False` rather than raising.

A value that is absent and one that is `None` are the same thing here -- nothing to compare --
and no predicate matches either, `ne` and `not` included. `exists(False)` is the one way to
select on absence, so a driver that publishes nothing can never satisfy a match by accident.

A path segment applied to a collection projects over its elements, so `filters.name` on a
`list[Filter]` reads the names as a collection. Predicates comparing against a single value
reject such a path at validation; `contains` requires one.
"""

from __future__ import annotations

import operator
from abc import ABC, abstractmethod
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from enum import Enum
from types import NoneType, UnionType
from typing import Any, Literal, Self, Union, get_args, get_origin, override

from pydantic import BaseModel, model_validator


class PredicateError(ValueError):
    """A path names no field, or an operand cannot compare against the value it addresses."""


class Missing:
    """Type of the `MISSING` sentinel."""

    def __repr__(self):
        return "MISSING"


MISSING = Missing()
"""Returned by `read_path` when a path reads nothing."""


def candidates(annotation: Any) -> tuple[Any, ...]:
    """Flatten `Annotated` wrappers and unions into the concrete annotations they admit."""
    while hasattr(annotation, "__metadata__"):
        annotation = annotation.__origin__

    if get_origin(annotation) in (Union, UnionType):
        return tuple(
            candidate
            for arg in get_args(annotation)
            if arg is not NoneType
            for candidate in candidates(arg)
        )

    return (annotation,)


def collection_element(annotation: Any) -> tuple[Any, ...]:
    """Return the element annotations of a collection annotation, or empty if it is not one."""
    origin = get_origin(annotation)
    base = origin or annotation

    if not isinstance(base, type) or issubclass(base, (str, bytes, Mapping)):
        return ()

    if not issubclass(base, Collection):
        return ()

    if origin is None:
        return (Any,)

    return tuple(arg for arg in get_args(annotation) if arg is not Ellipsis) or (Any,)


def is_collection(value: object) -> bool:
    """Whether a read value should be treated as a collection of elements."""
    return isinstance(value, Collection) and not isinstance(value, (str, bytes, Mapping))


def accepts(annotation: Any, operand: object) -> bool:
    """Whether `operand` could compare meaningfully against a value annotated `annotation`."""
    base = get_origin(annotation) or annotation

    if base is Any or not isinstance(base, type):
        return True

    if isinstance(operand, base):
        return True

    if issubclass(base, Enum):
        return any(operand == member.value for member in base)

    return _numeric(base) and _numeric(type(operand))


def orderable(annotation: Any) -> bool:
    """Whether values annotated `annotation` support `<`."""
    base = get_origin(annotation) or annotation

    if base is Any or not isinstance(base, type):
        return True

    return base.__lt__ is not object.__lt__


def _numeric(type_: type) -> bool:
    return issubclass(type_, (int, float)) and not issubclass(type_, bool)


def _names(annotations: tuple[Any, ...]) -> str:
    return " | ".join(getattr(a, "__name__", str(a)) for a in annotations)


@dataclass(frozen=True, slots=True)
class PathType:
    """The static type of the value a path reads.

    `annotations` are the candidates that value may take. `projected` marks a walk that crossed a
    collection, which makes the value a collection of those candidates rather than one of them.
    """

    annotations: tuple[Any, ...]
    projected: bool = False

    @classmethod
    def of(cls, annotation: Any) -> Self:
        """Build a `PathType` for a value annotated `annotation`, with no projection."""
        return cls(candidates(annotation))

    def elements(self) -> tuple[Any, ...]:
        """The annotations of the value's elements, treating it as a collection."""
        if self.projected:
            return self.annotations

        return tuple(element for a in self.annotations for element in collection_element(a))

    def collection(self) -> bool:
        """Whether the value is a collection rather than a single comparable."""
        return self.projected or any(collection_element(a) for a in self.annotations)


def _step(annotation: Any, segment: str) -> tuple[tuple[Any, ...], bool]:
    """Resolve one path segment, crossing a collection annotation to its elements."""
    elements = collection_element(annotation)
    resolved: list[Any] = []

    for source in elements or (annotation,):
        base = get_origin(source) or source

        if isinstance(base, type) and issubclass(base, BaseModel):
            if field := base.model_fields.get(segment):
                resolved.extend(candidates(field.annotation))

    return tuple(resolved), bool(elements)


def resolve_path(model_type: type, path: str) -> PathType:
    """Resolve a dot path against a model, returning the static type of the value it reads.

    Args:
        model_type: The model the path is rooted at.
        path: Dot-separated field names. A segment applied to a collection projects over its
            elements.

    Raises:
        PredicateError: A segment names no field on anything the path can reach.
    """
    current = PathType.of(model_type)

    for segment in path.split("."):
        annotations: list[Any] = []
        projected = current.projected

        for annotation in current.annotations:
            resolved, crossed = _step(annotation, segment)
            annotations.extend(resolved)
            projected = projected or crossed

        if not annotations:
            raise PredicateError(
                f"'{path}': no field '{segment}' on {_names(current.annotations)}"
            )

        current = PathType(tuple(annotations), projected)

    return current


def read_path(obj: object, path: str) -> object:
    """Read a dot path from an object, projecting over any collection the walk crosses.

    Returns:
        The value read, a collection of values if the walk crossed one, or `MISSING` if the path
        reads nothing.
    """
    values: tuple[Any, ...] = (obj,)
    projected = False

    for segment in path.split("."):
        read: list[Any] = []

        for value in values:
            projected = projected or is_collection(value)
            items = value if is_collection(value) else (value,)

            read.extend(
                got for item in items if (got := getattr(item, segment, MISSING)) is not MISSING
            )

        values = tuple(read)

    if projected:
        return values

    return values[0] if values else MISSING


def no_value(value: object) -> bool:
    """Whether a read value carries nothing to compare against."""
    return value is MISSING or value is None


def _apply(fn: Any, value: object, operand: object) -> bool:
    if no_value(value):
        return False

    try:
        return bool(fn(value, operand))
    except TypeError:
        return False


class Predicate(BaseModel, ABC, extra="forbid"):
    """A static question about a value, discriminated on `op`."""

    op: str

    @abstractmethod
    def test(self, value: object) -> bool:
        """Evaluate against a value read from a path.

        Returns `False` for a missing value or one that cannot compare, never raising.
        """
        ...

    def check_operand(self, path_type: PathType):
        """Raise if this predicate's operands could never compare against `path_type`.

        Raises:
            PredicateError: The operands are incompatible with the annotated value.
        """


class ScalarPredicate(Predicate, ABC):
    """A predicate comparing the read value against authored operands, one value at a time."""

    @abstractmethod
    def operands(self) -> tuple[Any, ...]:
        """The authored values this predicate compares against."""
        ...

    def ordered(self) -> bool:
        """Whether this predicate needs a value supporting `<`."""
        return False

    @override
    def check_operand(self, path_type: PathType):
        if path_type.collection():
            raise PredicateError(f"'{self.op}' addresses a collection; use 'contains'")

        for operand in self.operands():
            if not any(accepts(a, operand) for a in path_type.annotations):
                raise PredicateError(
                    f"'{self.op}' operand {operand!r} cannot compare against "
                    f"{_names(path_type.annotations)}"
                )

        if self.ordered() and not all(orderable(a) for a in path_type.annotations):
            raise PredicateError(
                f"'{self.op}' needs an ordered value, not {_names(path_type.annotations)}"
            )


_COMPARISONS = {
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "le": operator.le,
    "gt": operator.gt,
    "ge": operator.ge,
}


class Compare(ScalarPredicate):
    """Compare the read value against a single operand."""

    op: Literal["eq", "ne", "lt", "le", "gt", "ge"]
    value: Any

    @override
    def ordered(self):
        """Only the inequalities need an ordered value."""
        return self.op not in ("eq", "ne")

    @override
    def operands(self):
        """Return the single comparison operand."""
        return (self.value,)

    @override
    def test(self, value):
        """Apply the operator, treating a missing or incomparable value as no match."""
        return _apply(_COMPARISONS[self.op], value, self.value)


class Between(ScalarPredicate):
    """The read value falls within an inclusive range."""

    op: Literal["between"] = "between"
    low: Any
    high: Any

    @override
    def ordered(self):
        """Bounds are only meaningful against an ordered value."""
        return True

    @override
    def operands(self):
        """Return both bounds."""
        return (self.low, self.high)

    @override
    def test(self, value):
        """Match values at or between the bounds."""
        return _apply(operator.ge, value, self.low) and _apply(operator.le, value, self.high)


class In(ScalarPredicate):
    """The read value is one of an authored set."""

    op: Literal["in"] = "in"
    values: tuple[Any, ...]

    @override
    def operands(self):
        """Return the authored set."""
        return self.values

    @override
    def test(self, value):
        """Match a value equal to any member of the set."""
        return not no_value(value) and any(value == operand for operand in self.values)


class Contains(Predicate):
    """The read collection contains every authored operand."""

    op: Literal["contains"] = "contains"
    values: tuple[Any, ...]

    @override
    def check_operand(self, path_type: PathType):
        """Raise unless the path reads a collection whose elements the operands could equal."""
        elements = path_type.elements()

        if not elements:
            raise PredicateError(
                f"'contains' needs a collection, not {_names(path_type.annotations)}"
            )

        for operand in self.values:
            if not any(accepts(element, operand) for element in elements):
                raise PredicateError(
                    f"'contains' operand {operand!r} cannot equal an element of {_names(elements)}"
                )

    @override
    def test(self, value):
        """Match when every operand equals some element of the read collection."""
        if not is_collection(value):
            return False

        items = tuple(value)
        return all(any(item == operand for item in items) for operand in self.values)


class Exists(Predicate):
    """The path reads a value at all. An empty collection counts as absent.

    The only predicate that sees a missing value, and so the only way to select on absence.
    """

    op: Literal["exists"] = "exists"
    present: bool = True

    @override
    def test(self, value):
        """Match on presence, or on absence when `present` is false."""
        if no_value(value):
            found = False
        elif is_collection(value):
            found = bool(value)
        else:
            found = True

        return found is self.present


class AllOf(Predicate):
    """Every child predicate holds."""

    op: Literal["all_of"] = "all_of"
    predicates: tuple[AnyPredicate, ...]

    @override
    def check_operand(self, path_type: PathType):
        """Check every child against the same path."""
        for predicate in self.predicates:
            predicate.check_operand(path_type)

    @override
    def test(self, value):
        """Match when every child matches."""
        return all(predicate.test(value) for predicate in self.predicates)


class AnyOf(Predicate):
    """At least one child predicate holds."""

    op: Literal["any_of"] = "any_of"
    predicates: tuple[AnyPredicate, ...]

    @override
    def check_operand(self, path_type: PathType):
        """Check every child against the same path."""
        for predicate in self.predicates:
            predicate.check_operand(path_type)

    @override
    def test(self, value):
        """Match when any child matches."""
        return any(predicate.test(value) for predicate in self.predicates)


class Not(Predicate):
    """The child predicate does not hold, against a value that was read."""

    op: Literal["not"] = "not"
    predicate: AnyPredicate

    @model_validator(mode="after")
    def check_child(self) -> Self:
        """Reject negating `exists`, which cannot hold: `not` needs a value to invert."""
        if isinstance(self.predicate, Exists):
            raise PredicateError("'not' cannot negate 'exists'; use exists(False)")

        return self

    @override
    def check_operand(self, path_type: PathType):
        """Check the child against the same path."""
        self.predicate.check_operand(path_type)

    @override
    def test(self, value):
        """Match when the child does not, against a value that was read."""
        return not no_value(value) and not self.predicate.test(value)


type AnyPredicate = Compare | Between | In | Contains | Exists | AllOf | AnyOf | Not


class FieldMatch(BaseModel, extra="forbid"):
    """A predicate at a dot path within some model.

    The model is supplied by whoever owns the anchor, so this type stays agnostic about where the
    object comes from: `validate_against` takes the type, `test` takes an instance.
    """

    field: str | None = None
    predicate: AnyPredicate

    def validate_against(self, model_type: type):
        """Raise if this match could never hold against instances of `model_type`.

        Raises:
            PredicateError: The path names no field, or the predicate's operands cannot compare
                against the value it addresses.
        """
        if self.field is None:
            self.predicate.check_operand(PathType.of(model_type))
        else:
            self.predicate.check_operand(resolve_path(model_type, self.field))

    def test(self, obj: object) -> bool:
        """Evaluate the predicate against the value this match's path reads from `obj`."""
        return self.predicate.test(obj if self.field is None else read_path(obj, self.field))


AllOf.model_rebuild()
AnyOf.model_rebuild()
Not.model_rebuild()
FieldMatch.model_rebuild()


def eq(value: Any) -> Compare:
    """The read value equals `value`."""
    return Compare(op="eq", value=value)


def ne(value: Any) -> Compare:
    """The read value differs from `value`."""
    return Compare(op="ne", value=value)


def lt(value: Any) -> Compare:
    """The read value is less than `value`."""
    return Compare(op="lt", value=value)


def le(value: Any) -> Compare:
    """The read value is at most `value`."""
    return Compare(op="le", value=value)


def gt(value: Any) -> Compare:
    """The read value is greater than `value`."""
    return Compare(op="gt", value=value)


def ge(value: Any) -> Compare:
    """The read value is at least `value`."""
    return Compare(op="ge", value=value)


def between(low: Any, high: Any) -> Between:
    """The read value lies within `low` and `high`, inclusive."""
    return Between(low=low, high=high)


def is_in(*values: Any) -> In:
    """The read value is one of `values`."""
    return In(values=values)


def contains(*values: Any) -> Contains:
    """The read collection contains every one of `values`."""
    return Contains(values=values)


def exists(present: bool = True) -> Exists:
    """The path reads a value, or reads nothing when `present` is false."""
    return Exists(present=present)


def all_of(*predicates: AnyPredicate) -> AllOf:
    """Every one of `predicates` holds."""
    return AllOf(predicates=predicates)


def any_of(*predicates: AnyPredicate) -> AnyOf:
    """At least one of `predicates` holds."""
    return AnyOf(predicates=predicates)


def not_(predicate: AnyPredicate) -> Not:
    """`predicate` does not hold."""
    return Not(predicate=predicate)
