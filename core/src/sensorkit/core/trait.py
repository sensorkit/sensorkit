"""Device trait infrastructure.

Traits are high-level interfaces that describe device capabilities by specifying
which commands a device must implement. They use structural typing: a device matches
a trait if it implements all required commands.

Two variants exist:
  - Trait: a device can satisfy many; specifies only required commands.
  - Archetype (subclass of Trait): a device should match at most one; additionally
    supports required and optional sub-traits, and optional commands.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sensorkit.core.device import DeviceCommand
    from sensorkit.core.entity import DeviceDetails

# Global registries, auto-populated by declare_trait() and declare_archetype().
_trait_registry: set[Trait] = set()
_archetype_registry: set[Archetype] = set()


@dataclass(frozen=True, eq=False)
class Trait:
    """A named set of required commands and/or keywords that a device may structurally satisfy."""

    name: str
    required_commands: tuple[type[DeviceCommand], ...] = ()
    required_keywords: tuple[str, ...] = ()

    def effective_command_ids(self) -> frozenset[str]:
        """Return all command IDs required by this trait."""
        return frozenset(cmd.model_tag() for cmd in self.required_commands)

    def effective_keyword_ids(self) -> frozenset[str]:
        """Return all keyword IDs required by this trait."""
        return frozenset(self.required_keywords)

    def match(self, details: DeviceDetails) -> bool:
        """Return True if the given device details satisfy this trait's commands and keywords."""
        if not (self.effective_command_ids() <= details.supported_commands):
            return False
        if not (self.effective_keyword_ids() <= details.published_keywords):
            return False
        return True

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, Trait) and self.name == other.name

    def __repr__(self):
        return f"Trait({self.name!r})"


@dataclass(frozen=True, eq=False)
class Archetype(Trait):
    """A trait archetype: a device should match at most one.

    Extends Trait with support for required and optional sub-traits, and optional commands.
    Optional traits and commands do not affect matching but describe extended capabilities.
    """

    required_traits: tuple[Trait, ...] = ()
    optional_commands: tuple[type[DeviceCommand], ...] = ()

    def effective_command_ids(self) -> frozenset[str]:
        """Return all command IDs required by this archetype and its required sub-traits."""
        return frozenset(
            itertools.chain(
                (cmd.model_tag() for cmd in self.required_commands),
                *(sub_trait.effective_command_ids() for sub_trait in self.required_traits),
            )
        )

    def effective_keyword_ids(self) -> frozenset[str]:
        """Return all keyword IDs required by this archetype and its required sub-traits."""
        return frozenset(
            itertools.chain(
                self.required_keywords,
                *(sub_trait.effective_keyword_ids() for sub_trait in self.required_traits),
            )
        )

    def __repr__(self):
        return f"Archetype({self.name!r})"


def declare_trait(
    name: str,
    *,
    required_commands: tuple[type[DeviceCommand], ...] = (),
    required_keywords: tuple[str, ...] = (),
) -> Trait:
    """Declare a device trait and add it to the global registry."""
    trait = Trait(
        name=name,
        required_commands=required_commands,
        required_keywords=required_keywords,
    )
    _trait_registry.add(trait)
    return trait


def declare_archetype(
    name: str,
    *,
    required_commands: tuple[type[DeviceCommand], ...] = (),
    required_keywords: tuple[str, ...] = (),
    required_traits: tuple[Trait, ...] = (),
    optional_commands: tuple[type[DeviceCommand], ...] = (),
) -> Archetype:
    """Declare an archetype and add it to the global registries.

    Archetypes extend traits with required and optional sub-traits, and optional commands.
    Sub-traits must themselves be plain Traits, not Archetypes.
    """
    for t in required_traits:
        if isinstance(t, Archetype):
            raise ValueError(
                f"Sub-trait '{t.name}' is an archetype and cannot be used as a sub-trait"
            )

    archetype = Archetype(
        name=name,
        required_commands=required_commands,
        required_keywords=required_keywords,
        required_traits=required_traits,
        optional_commands=optional_commands,
    )

    _trait_registry.add(archetype)
    _archetype_registry.add(archetype)

    return archetype


def get_registered_traits() -> frozenset[Trait]:
    """Return all registered traits (including archetypes)."""
    return frozenset(_trait_registry)


def get_registered_archetypes() -> frozenset[Archetype]:
    """Return all registered archetypes."""
    return frozenset(_archetype_registry)


def match_traits(
    details: DeviceDetails,
    traits: Iterable[Trait] | None = None,
    *,
    exclude_archetypes: bool = False,
) -> list[Trait]:
    """Return all traits from the given iterable that the device satisfies."""
    if traits is None:
        traits = _trait_registry

    output = []

    for trait in traits:
        if exclude_archetypes and isinstance(trait, Archetype):
            continue

        if trait.match(details):
            output.append(trait)

    return output


def match_archetype(details: DeviceDetails) -> Archetype | None:
    """Return the first matching archetype for the given device details, or None."""
    for archetype in _archetype_registry:
        if archetype.match(details):
            return archetype

    return None
