# SPDX-License-Identifier: Apache-2.0
"""Runtime overrides: rules that edit what a compiled graph says about the ops it
dispatches.

An override is not a second failure vocabulary. Two of the three things it can say
— `on_failure` and `optional` — are the vocabulary the authoring surface already
speaks, bound late by a caller instead of early by an author. The third, `outcome`,
is the one rung no author has: *do not dispatch this at all, and record this
instead.* So a rule is best read as an amendment to a step rather than as a new
kind of thing.

An override is deliberately **not** config. A site's YAML says what its observatory
is; whether tonight is a daytime test with the enclosure shut is an operational
fact with a lifetime of hours, and putting it in the document that describes the
hardware means remembering to take it out again. Rules are supplied to
`compile_table`, so a change of operating state is a recompile, and the graph — the
thing that gets inspected, pinned and dry-run — is what carries the consequences.

Matching is over `ops.Op`, not over the authoring surface, so a rule is written
once against the dispatch vocabulary and any frontend that builds an `Op` can
consult it. `Op` already carries everything a rule needs to select on: the ref, the
verb, the capability being addressed and the ones its device claims.

Application is **first match wins**, in declaration order — the same "most specific
first" ordering a deployment already writes by hand in its dispatcher, and the
alternative (merging every matching rule) makes the effect of a list something a
reader has to compute.

A rule that addresses nothing is inert, and a rule saying `dome_1` where the sensor
says `dome-1` is the failure mode an operational input actually has. Checking for
it is the caller's: it is two lines over a compiled graph (`Override.matches`
against each `node.payload`), and a set spanning several tables is legitimately
inert against any one of them, so there is no way to know here which silence is
wrong.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, model_validator

from sensorkit.workflow.dag import NodeOverride, OnFailure
from sensorkit.workflow.ops import Match, Op
from sensorkit.workflow.structure import DeviceRef, Trait


def _one_or_many(v: object) -> object:
    return [v] if isinstance(v, str) else v


class Override(BaseModel, frozen=True, extra="forbid"):
    """One rule: which ops it addresses, and what it changes about them.

    Selection is a conjunction of whichever of `trait` / `device` / `match` / `ops`
    are set, and at least one must be — a rule matching every op in the graph is
    not something anyone means to write.

    Effects are `outcome` (don't dispatch; record this), `on_failure` and
    `optional`. `outcome` excludes the other two: a step that will not run cannot
    fail, so setting a failure policy alongside it is a misunderstanding worth
    rejecting rather than ignoring.
    """

    # Why. Required, and it travels all the way into `RunReport` — "the dome did not
    # open" is only useful next to who decided that.
    reason: str

    trait: Trait | None = None
    device: DeviceRef | None = None
    match: Match | None = None
    # Empty: every op on the selection. YAML/dict shorthand: a bare string is the
    # one-op tuple.
    ops: Annotated[tuple[str, ...], BeforeValidator(_one_or_many)] = ()

    outcome: Literal["ok", "skipped"] | None = None
    on_failure: OnFailure | None = None
    optional: bool | None = None

    @model_validator(mode="after")
    def _well_formed(self) -> Override:
        if not (self.trait or self.device or self.match or self.ops):
            raise ValueError(
                "override selects nothing: set trait, device, match, or ops")

        if (self.outcome, self.on_failure, self.optional) == (None, None, None):
            raise ValueError(
                "override changes nothing: set outcome, on_failure, or optional")

        if self.outcome is not None and (
                self.on_failure is not None or self.optional is not None):
            raise ValueError(
                "override sets outcome and a failure policy; a step that "
                "will not be dispatched cannot fail")

        return self

    def matches(self, op: Op) -> bool:
        """Whether this rule addresses `op`.

        A `trait` clause reads the capability the op addresses; for an op
        addressing none — a `device`, `all`, `instrument` or `selector` match — it
        falls back to every capability the device claims. So "nothing on the dome"
        reaches the dome's `open` and its `connect` alike, while "nothing on the
        mount" leaves the focuser ops of a ref that is both alone.
        """
        if self.trait is not None and self.trait not in (
                (op.trait,) if op.trait is not None else op.traits):
            return False

        if self.device is not None and self.device != op.ref:
            return False

        if self.match is not None and self.match != op.match:
            return False

        return not self.ops or op.op in self.ops


@dataclass(frozen=True)
class NodeEffects:
    """What a compiler should hand `GraphBuilder.add` for one op."""

    on_failure: OnFailure
    optional: bool
    override: NodeOverride | None


def resolve_effects(overrides: Sequence[Override], op: Op, *,
                    on_failure: OnFailure, optional: bool) -> NodeEffects:
    """Amend the values an authoring surface resolved for `op` with the first rule
    addressing it."""
    rule = next((o for o in overrides if o.matches(op)), None)

    if rule is None:
        return NodeEffects(on_failure, optional, None)

    return NodeEffects(
        on_failure=rule.on_failure or on_failure,
        optional=optional if rule.optional is None else rule.optional,
        override=(NodeOverride(rule.outcome, rule.reason)
                  if rule.outcome is not None else None),
    )
