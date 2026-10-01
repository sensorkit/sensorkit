# SPDX-License-Identifier: Apache-2.0
"""Select devices by placement properties and reported capabilities.

`matches` tests one placement; `matching` filters a supplied list. `reaches`
tests an instrument chain. Compositions evaluate each member over the whole
chain: a wheel may satisfy `Supports(SetFilter)` while its camera satisfies
`Supports(CameraCapture)`. At one placement, both must hold on that device.

Except for device-key comparisons, predicates require `PlacementFacts`,
implemented by `BoundSensor`. Missing facts raise rather than count as false,
including under negation. Facts contain capability identifiers, not current
device values.

Authored selections use a distinguishing key; a bare string names a trait.
Routing chooses a command target from matching placements in `binding`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, ClassVar, Literal, Protocol

from pydantic import (
    BaseModel,
    BeforeValidator,
    Discriminator,
    Field,
    Tag,
    model_validator,
)

from sensorkit.common.keyword import get_keyword_info
from sensorkit.core.device import DeviceCommand
from sensorkit.core.trait import Trait
from sensorkit.sensor.topology import DeviceKey, Placement, TagKey, TraitKey


class SelectionError(ValueError):
    """A predicate requires placement facts that were not supplied."""


class DeviceFacts(Protocol):
    """Reported command and keyword identifiers for each device."""

    def commands(self, device: DeviceKey) -> frozenset[str]:
        """Return the command identifiers reported by a device."""

    def keywords(self, device: DeviceKey) -> frozenset[str]:
        """Return the keyword identifiers reported by a device."""


class PlacementFacts(DeviceFacts, Protocol):
    """Device capabilities and structural properties used by selections.

    `BoundSensor` implements this protocol. Binding establishes traits from
    capabilities; tags, kind and instrument status come from the structure.
    """

    def traits(self, placement: Placement) -> frozenset[TraitKey]:
        """Return all traits established for the placement."""

    def tags(self, placement: Placement) -> frozenset[TagKey]:
        """Return the tags declared on the placement record."""

    def kind(self, placement: Placement) -> Literal["device", "instrument", "selector"]:
        """Return the record kind: device, instrument or selector."""

    def instrument(self, placement: Placement) -> bool:
        """Return whether the record marks this device as a collection
        target.

        Instrument status is configured, not inferred from camera capabilities.
        """


def _known(facts: PlacementFacts | None, question: str) -> PlacementFacts:
    """Require facts for a predicate, naming it in the error.

    Raises:
        SelectionError: No facts were supplied.
    """
    if facts is None:
        raise SelectionError(f"'{question}' is answered from PlacementFacts, and none was given")

    return facts


def _command_tag(v: object) -> object:
    """Convert a command class to its registered identifier."""
    if isinstance(v, type) and issubclass(v, DeviceCommand):
        return v.model_tag()

    return v


def _keyword_key(v: object) -> object:
    """Convert a declared keyword class to its registered key."""
    if not isinstance(v, type):
        return v

    info = get_keyword_info(v)
    if info is None:
        raise ValueError(f"'{v.__name__}' is not a declared keyword")

    return info.key


class SelectionType(StrEnum):
    """Authored key that identifies each selection type.

    A mapping naming several keys takes the first in definition order.
    """

    trait = "trait"
    tag = "tag"
    device = "device"
    kind = "kind"
    instrument = "instrument"
    supports = "supports"
    publishes = "publishes"
    all_of = "all_of"
    any_of = "any_of"
    negated = "not"


class Selection(BaseModel, ABC, frozen=True, extra="forbid"):
    """A predicate over one placement or an instrument chain.

    Compositions apply their members to the same placement in `matches`, and
    independently to the whole chain in `reaches`.
    """

    type: ClassVar[SelectionType]

    @abstractmethod
    def matches(self, placement: Placement, facts: PlacementFacts | None = None) -> bool:
        """Test whether one placement satisfies this predicate.

        Raises:
            SelectionError: The predicate requires facts and none were
                supplied.
        """
        ...

    def matching(
        self, placements: tuple[Placement, ...], facts: PlacementFacts | None = None
    ) -> tuple[Placement, ...]:
        """Return matching placements in the supplied order.

        Callers supply the search set, such as a whole sensor or a
        participant's chain. The facts provider does not enumerate placements.
        """
        return tuple(p for p in placements if self.matches(p, facts))

    def refs(
        self, placements: tuple[Placement, ...], facts: PlacementFacts | None = None
    ) -> tuple[DeviceKey, ...]:
        """Return matching device keys in the supplied placement order."""
        return tuple(p.device for p in self.matching(placements, facts))

    def reaches(self, chain: tuple[Placement, ...], facts: PlacementFacts | None = None) -> bool:
        """Test whether the chain satisfies this predicate.

        Simple predicates match if any placement does. Compositions evaluate
        each member over the chain: `AllOf` may use different devices for its
        members, and `Not` requires that its member fail for the chain as a
        whole.
        """
        return any(self.matches(p, facts) for p in chain)


class HasTrait(Selection):
    """Match a trait established at binding, regardless of authored
    assertions.
    """

    type = SelectionType.trait
    trait: TraitKey

    @model_validator(mode="before")
    @classmethod
    def _str_shorthand(cls, v: object) -> object:
        return {"trait": v} if isinstance(v, str) else v

    def matches(self, placement, facts=None):
        known = _known(facts, f"trait: {self.trait}")

        return self.trait in known.traits(placement)


class HasTag(Selection):
    """Match a grouping tag declared on the device or selector record."""

    type = SelectionType.tag
    tag: TagKey

    def matches(self, placement, facts=None):
        known = _known(facts, f"tag: {self.tag}")

        return self.tag in known.tags(placement)


class IsRef(Selection):
    """Match a device key; no facts provider is required."""

    type = SelectionType.device
    device: DeviceKey

    def matches(self, placement, facts=None):
        return self.device == placement.device


type KindQuery = Literal["device", "instrument", "selector", "any"]
"""Record kinds accepted by `IsKind`; `any` matches every kind."""


class IsKind(Selection):
    """Match the record kind, or every placement when `kind` is `any`."""

    type = SelectionType.kind
    kind: KindQuery

    def matches(self, placement, facts=None):
        known = _known(facts, f"kind: {self.kind}")

        return self.kind in ("any", known.kind(placement))


class IsInstrument(Selection):
    """Match configured instrument status.

    `instrument: false` selects placements that are not collection targets.
    """

    type = SelectionType.instrument
    instrument: bool = True

    def matches(self, placement, facts=None):
        known = _known(facts, f"instrument: {self.instrument}")

        return known.instrument(placement) == self.instrument


class Supports(Selection):
    """Match a reported command identifier.

    A command class may be supplied in place of its identifier.
    """

    type = SelectionType.supports
    supports: Annotated[str, BeforeValidator(_command_tag)]

    def matches(self, placement, facts=None):
        known = _known(facts, f"supports: {self.supports}")

        return self.supports in known.commands(placement.device)


class Publishes(Selection):
    """Match a reported keyword identifier, regardless of its current value.

    A declared keyword class may be supplied in place of its identifier.
    """

    type = SelectionType.publishes
    publishes: Annotated[str, BeforeValidator(_keyword_key)]

    def matches(self, placement, facts=None):
        known = _known(facts, f"publishes: {self.publishes}")

        return self.publishes in known.keywords(placement.device)


class AllOf(Selection):
    """Require every member to match.

    On a chain, different placements may satisfy different members.
    """

    type = SelectionType.all_of
    all_of: tuple[AnySelection, ...] = Field(min_length=1)

    @classmethod
    def supporting(cls, trait: Trait) -> AllOf:
        """Build predicates for every command and keyword required by a
        trait.

        Use `matches` to check that a single device satisfies the trait.

        Raises:
            ValueError: The trait requires no commands or keywords.
        """
        members: list[AnySelection] = [
            Supports(supports=command) for command in sorted(trait.effective_command_ids())
        ]
        members += [
            Publishes(publishes=keyword) for keyword in sorted(trait.effective_keyword_ids())
        ]

        if not members:
            raise ValueError(
                f"trait '{trait.name}' requires no command or keyword, so "
                f"there is nothing to ask a device for"
            )

        return cls(all_of=tuple(members))

    def matches(self, placement, facts=None):
        return all(s.matches(placement, facts) for s in self.all_of)

    def reaches(self, chain, facts=None):
        return all(s.reaches(chain, facts) for s in self.all_of)


class AnyOf(Selection):
    """Require at least one member to match the placement or chain."""

    type = SelectionType.any_of
    any_of: tuple[AnySelection, ...] = Field(min_length=1)

    def matches(self, placement, facts=None):
        return any(s.matches(placement, facts) for s in self.any_of)

    def reaches(self, chain, facts=None):
        return any(s.reaches(chain, facts) for s in self.any_of)


class Not(Selection, populate_by_name=True):
    """Negate a member at the placement or whole-chain level."""

    type = SelectionType.negated
    negated: AnySelection = Field(alias="not")

    def matches(self, placement, facts=None):
        return not self.negated.matches(placement, facts)

    def reaches(self, chain, facts=None):
        return not self.negated.reaches(chain, facts)


def _selection_type(v: object) -> SelectionType | None:
    """Identify a selection from its model, mapping key or trait shorthand."""
    match v:
        case str():
            return SelectionType.trait
        case Selection():
            return v.type
        case Mapping():
            return next((k for k in SelectionType if k in v), None)

    return None


type AnySelection = Annotated[
    Annotated[HasTrait, Tag(HasTrait.type)]
    | Annotated[HasTag, Tag(HasTag.type)]
    | Annotated[IsRef, Tag(IsRef.type)]
    | Annotated[IsKind, Tag(IsKind.type)]
    | Annotated[IsInstrument, Tag(IsInstrument.type)]
    | Annotated[Supports, Tag(Supports.type)]
    | Annotated[Publishes, Tag(Publishes.type)]
    | Annotated[AllOf, Tag(AllOf.type)]
    | Annotated[AnyOf, Tag(AnyOf.type)]
    | Annotated[Not, Tag(Not.type)],
    Discriminator(_selection_type),
]

# Resolve recursive annotations now that `AnySelection` is defined.
AllOf.model_rebuild()
AnyOf.model_rebuild()
Not.model_rebuild()
