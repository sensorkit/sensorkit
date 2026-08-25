# SPDX-License-Identifier: Apache-2.0
"""Shared workflow models and lowering for lifecycle and collect compilers.

Compilers emit named `PlannedStep` records with dependencies. `lower` checks
capabilities, resolves deadlines and operator rules, and builds graphs for
`sensorkit.common.dag`. Nodes carry an `Operation` or `None` for ordering only.

Success dependencies become hard edges; completion dependencies become soft
edges. Fail-fast failures stop the whole run, so a soft edge cannot guarantee
subsequent work. Cleanup uses separate graphs run after the main graph drains.

Unsupported steps marked `omit` produce no node. Their dependents inherit their
dependencies, with success required only when both links require it. When
several paths reach the same predecessor, any success requirement wins.
Omissions are recorded separately from operator-imposed outcomes.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, model_validator

from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.topology import DeviceKey, Placement, TraitKey

"""Authored request label shared by all acquisitions expanded from it."""

"""Readable operation label derived from its origin and target device.

Used in logs and audits. Cleanup and execution track operations by object
identity; graph node identifiers belong to `sensorkit.common.dag`.
"""

"""Step identifier unique within one compilation, used to resolve
dependencies.
"""

type Scope = Literal["any", "private", "shared"]
"""Filter routing candidates by their relation to an instrument.

`private` uses the topology's local, sole-reader placements; `shared` uses the
rest of the chain. `any` accepts both. Ancestor infrastructure remains shared
even when it has only one reader.
"""

type Subject = Literal["sensor", "instrument"]
"""Choose how participants are grouped and targets are ranked for routing.

`sensor` chooses the shallowest supporting device common to all participants.
`instrument` chooses the deepest supporting device on each participant's chain.
Shared targets are commanded once. `Scope` filters candidates independently.
"""


def partition(subject: Subject, participants: tuple[Placement, ...]
              ) -> tuple[tuple[Placement, ...], ...]:
    """Group all sensor participants together, or each instrument
    separately.
    """
    match subject:
        case "sensor":
            return (participants,)
        case "instrument":
            return tuple((p,) for p in participants)






@dataclass(frozen=True)
class RoutedCommand:
    """A command with its target placement and optional explicit timeout.

    Routing chooses the target; lowering resolves the timeout against deadline
    rules. The timeout is orchestration metadata, not part of the device
    command.
    """

    target: Placement
    command: DeviceCommand
    timeout_s: float | None = None

    @property
    def governs(self) -> tuple[Placement, type[DeviceCommand]]:
        """Return the collect state key: target placement and command type."""
        return self.target, type(self.command)










type DeadlineTarget = (
    tuple[Literal["device"], DeviceKey]
    | tuple[Literal["trait"], TraitKey]
    | tuple[Literal["any"], None]
)
"""A deadline target tagged as a device key, trait name or universal match."""

_NAMESPACES = ("device", "trait", "any")


class DeadlineRule(BaseModel, frozen=True, extra="forbid"):
    """A time limit for one command, or every command, on a device, trait or
    any target.

    Author targets as `device: cam-1`, `trait: MustConnect` or `any: true`.
    More specific rules take precedence during lowering. A rule without a
    command is a default that yields to every rule naming the command.
    """

    target: DeadlineTarget
    command: str | None = None
    seconds: float

    @model_validator(mode="before")
    @classmethod
    def _namespaced(cls, v: object) -> object:
        if not isinstance(v, Mapping) or "target" in v:
            return v

        named = [k for k in _NAMESPACES if k in v]

        if len(named) != 1:
            raise ValueError(
                f"a deadline rule names exactly one of {', '.join(_NAMESPACES)}")

        key = named[0]
        rest = {k: v[k] for k in v if k not in _NAMESPACES}

        return {**rest, "target": (key, None if key == "any" else v[key])}
