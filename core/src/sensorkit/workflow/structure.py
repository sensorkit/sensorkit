# SPDX-License-Identifier: Apache-2.0
"""Structural model of an observatory sensor system: the tree, and nothing derived
from it (see `views.py` for that).

* **Traits are open string labels** ("focuser", "chiller", "pdu") attached as
  `Attachment(ref=..., trait=...)`. This package never interprets them; only phase
  tables do. The vocabulary is wholly the deployment's: no label anywhere is spoken
  for.

* **Two claims are structural, and carry no trait at all.** An instrument (leaf
  collect target) and a selector (makes sibling parts mutually exclusive) are
  declared by dedicated fields on their node classes, because they define what the
  node *is*. Each yields a `DeviceNode` whose `kind` says so and whose `trait` is
  `None`, and the authoring surfaces address them by that kind (`Entry.match`,
  `ops.Op.match`) rather than by a label.

    Routing them through the trait vocabulary instead would mean reserving two
    words in it — a namespace shared with the rest of SensorKit. Selection is a
    `kind`, so a deployment stays free to define traits named "instrument" or
    "selector" for its own purposes.

* **A trait is a capability claim, not a distinct box.** The same ref may appear in
  any number of attachments — e.g. a TCS ref attached at the root as "mount" and on
  an optical assembly as "focuser". Uniqueness is per claim, not per ref. Ops that
  are inherently per-device rather than per-capability (connect, disconnect) match
  distinct refs once via `lifecycle.Entry(match="all")`.

* **What a device *is* is described in keywords, and mostly not here.** A device
  publishes its own description at runtime; `keywords` on a claim is what a site
  knows and the driver does not report, validated against the same registry and
  read as that device's defaults. There is no second attribute vocabulary to
  reconcile — see `capability.py` for what reads them.

* **Parts have names; attachments have refs.** A part is a position in the
  structure — an `Assembly` nests to whatever depth a deployment organizes by
  (aperture, bench, cryostat), and nothing is read into the grouping beyond the
  path it contributes. An attachment is a capability claim on a device at that
  position. So a filter wheel bolted to an OTA is an attachment, not a part.

* **Names identify positions in the structure; refs identify devices.** There are
  no named slots: a mount is a root attachment with trait "mount", and nothing
  special-cases it. There is likewise no node class per observatory concept.

Out of scope here: site-level infrastructure above the sensor. Weather interlocks,
e-stops and emergency closes are the caller's concern and reach this package only
as an external abort.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    field_validator,
    model_validator,
)

from sensorkit.common.keyword import KeywordDict, is_keyword, validate_keyword

type DeviceRef = str
"""Opaque device identifier, resolved at runtime against a device registry. The
structural model never touches device clients."""

type Trait = str
"""Open vocabulary, defined entirely by config; no label means anything here."""

type InstrumentPath = tuple[str, ...]
"""A position in the structure, root -> leaf: `("main-ota", "sel", "port-a")`.
Names identify positions; refs identify devices."""


def on_chain(above: InstrumentPath, below: InstrumentPath) -> bool:
    """Whether `above` is at or above `below` on one root-to-leaf chain.

    The whole of what "shared with" means structurally: a device at `above` is
    reached by everything at `below`, and the root is on every chain.
    """
    return below[:len(above)] == above


type ClaimKind = Literal["attachment", "instrument", "selector"]
"""What kind of claim put a device in the structure. "attachment" is a trait claim
and carries one; the other two are structural — declared by a field on their node
class, and so addressed by kind rather than by a label, which is what keeps the
trait vocabulary entirely the deployment's."""


class InstrumentRole(str, Enum):
    """Workflow contract of an instrument.

    The operational split:

    * **collect target** (science, calibration): takes part in a `collect.Step` as
      a `collect.FramePlan`. `CALIBRATION` marks instruments *dedicated* to
      calibration products — e.g. a transparency monitor whose frames are
      extinction measurements, never science. What an individual *frame* is (dark,
      flat, bias, science) is per-exposure rather than per-instrument: see
      `collect.FramePlan.target_type`.
    * **service** (guide, wavefront): runs continuously across steps, and the
      collect layer does not schedule it. A service may issue *corrective* writes
      to a shared device (guider -> mount pulses) that hold the commanded setpoint
      rather than change it, so they are not the value changes the derived barriers
      order against.
    """

    SCIENCE = "science"
    GUIDE = "guide"
    WAVEFRONT = "wavefront"
    CALIBRATION = "calibration"

    @property
    def is_collect_target(self) -> bool:
        return self in (InstrumentRole.SCIENCE, InstrumentRole.CALIBRATION)


def _known_keywords(v: object) -> object:
    """Config-supplied keywords go through the registry exactly as published ones do.

    `KeywordDict` leaves an unrecognized key as a plain mapping, which would make a typo a
    value nobody ever reads; here it is a load error.
    """
    if isinstance(v, Mapping):
        for key, value in v.items():
            if not is_keyword(key):
                raise ValueError(f"'{key}' is not a declared keyword")
            validate_keyword(key, value)

    return v


type StaticKeywords = Annotated[KeywordDict, BeforeValidator(_known_keywords)]
"""What a site knows about a device that the device does not publish. Read as that
device's defaults, under anything it reports live."""


class Attachment(BaseModel, extra="forbid"):
    """One capability claim: device `ref` serves as `trait` at the node this
    attachment hangs on."""

    ref: DeviceRef
    trait: Trait
    keywords: StaticKeywords = Field(default_factory=KeywordDict)

    @field_validator("trait")
    @classmethod
    def _trait_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("trait must be a non-empty label")
        return v


def _coerce_attachments(v: object) -> object:
    """YAML shorthand: `{tcs-1: mount}` or `{tcs-1: [focuser, rotator]}` instead of
    a list of `{ref, trait}` objects. Both forms parse; dumps normalize to the list
    form."""
    if isinstance(v, Mapping):
        return [{"ref": ref, "trait": t}
                for ref, traits in v.items()
                for t in ([traits] if isinstance(traits, str) else traits)]
    return v


type Attachments = Annotated[
    list[Attachment], BeforeValidator(_coerce_attachments)]


class BaseAssembly(BaseModel, extra="forbid"):
    """A named position in the structure that may claim capabilities.

    Common to every node: the `name` contributing one segment to the
    `InstrumentPath`, and the `attachments` claiming devices at that position.
    Subclasses decide whether the position terminates (`InstrumentAssembly`) or
    holds further parts (`Assembly`).

    `extra="forbid"` is inherited by all three, and is what makes the `Part` union
    discriminate structurally.
    """

    name: str
    attachments: Attachments = Field(default_factory=list)


class Assembly(BaseAssembly):
    """An organizational unit of the sensor: an aperture, an instrument bench, a
    cryostat, a coudé path — whatever a deployment groups by.

    Assemblies nest to any depth, and nothing is read into the grouping beyond the
    path segment it contributes.

    Attachments at this level are shared across everything below it, which is the
    whole of what an assembly *means* structurally: a mirror cover on an OTA is
    shared by every instrument behind it.
    """

    parts: list[Part] = Field(default_factory=list)


class SelectorAssembly(Assembly):
    """An assembly whose parts are mutually exclusive: a pick-off mirror, fold
    mirror or fiber selector routing the beam to one of them.

    Strictly an `Assembly` that adds a device — anywhere an assembly's attachments
    are gathered and its parts walked, a selector behaves the same and merely means
    more, so consumers add to the shared case rather than branching away from it.

    The two semantics it carries, and the only two:

    * each part is reachable only to the exclusion of its siblings
      (`views.Topology.mutually_exclusive`);
    * a part's *name* is the value commanded on `selector` to reach it, so
      positions are derived from a step's participants and may not be commanded by
      hand (`collect.compile_collect`).

    Attachments here sit upstream of the selector and so are shared across all
    parts, exactly as on any other assembly.
    """

    selector: DeviceRef
    # Redeclared for the bound: a selector with nothing to select between is not a
    # selector. Its parts stay `Part`, so a port may be an instrument or a further
    # assembly (a fold mirror feeding a bench with several instruments on it).
    parts: list[Part] = Field(min_length=1)


class InstrumentAssembly(BaseAssembly):
    """A single detector at the end of an optical path, with any devices associated
    with it specifically (as opposed to devices shared with sibling instruments,
    which live on an assembly above it).

    Terminal node of the structural graph — the one node class with no `parts`,
    which is the whole of how it differs from an `Assembly`.
    """

    instrument: DeviceRef
    role: InstrumentRole = InstrumentRole.SCIENCE
    # The instrument device's own static keywords: this node *is* that device's
    # position, so a site describing the detector describes it here.
    keywords: StaticKeywords = Field(default_factory=KeywordDict)


# Discrimination is structural, not tagged. `extra="forbid"` closes every branch,
# so exactly one can ever validate a given mapping — `selector` present means the
# assembly branch rejects it, absent means the selector branch is missing a
# required field — and pydantic reports the failures per branch. A `kind:` tag would
# buy only error localization, at the price of a hand-written shape heuristic to
# infer it, which misreports precisely the typo it is most likely to see (a
# misspelled `parts` reads as an instrument missing its `instrument`). It can be
# added later without breaking configs if a fourth node kind ever makes the
# required-field sets overlap.
type Part = SelectorAssembly | Assembly | InstrumentAssembly

# `parts` is annotated with `Part`, declared below the classes it names: the
# recursion has to be closed by hand.
Assembly.model_rebuild()
SelectorAssembly.model_rebuild()


class SensorModel(BaseModel, extra="forbid"):
    """Top-level structural model of an observatory sensor system: the complete set
    of hardware participating in a single observation workflow.

    Root attachments are sensor-wide devices — mount, dome, chillers, power —
    expressed purely as traits. They appear on every instrument's shared chain (a
    chiller *is* shared infrastructure whose state belongs in frame metadata).

    Field-for-field an `Assembly`, and deliberately not one: the root's name is the
    sensor's, not a path segment, so every path is relative to it and
    `root_claims()` is the claims at `()`. Subclassing would make a sensor
    substitutable for a part, which it is not.
    """

    name: str
    attachments: Attachments = Field(default_factory=list)
    parts: list[Part] = Field(default_factory=list)

    @model_validator(mode="after")
    def _claims_unique(self) -> SensorModel:
        # Keyed on the kind as well, so one ref may be an instrument and claim
        # traits elsewhere while two instruments sharing a ref still collide. Per
        # sensor only; entity names are globally unique above this layer.
        seen: set[tuple[DeviceRef, ClaimKind, Trait | None]] = set()
        dupes: set[tuple[DeviceRef, ClaimKind, Trait | None]] = set()

        for node in self.iter_devices():
            key = (node.ref, node.kind, node.trait)
            (dupes if key in seen else seen).add(key)

        if dupes:
            raise ValueError(f"duplicate claims in sensor: {sorted(dupes)}")

        return self

    def iter_devices(self) -> Iterator[DeviceNode]:
        """Root-first flat walk: every claim with its kind, trait and structural
        path.

        One ref may yield several nodes — one per trait it satisfies, plus one if
        it is an instrument or a selector.
        """
        for a in self.attachments:
            yield DeviceNode(a.ref, a.trait, (), keywords=a.keywords)
        for part in self.parts:
            yield from _iter_part(part, ())


@dataclass(frozen=True)
class DeviceNode:
    """One claim on a device in the structural tree.

    `trait` is the claimed capability, and is `None` for the two structural kinds —
    an instrument and a selector are what their node *is*, declared by a field
    rather than labelled, so they occupy no part of the trait vocabulary.
    """

    ref: DeviceRef
    trait: Trait | None
    path: InstrumentPath
    kind: ClaimKind = "attachment"
    instrument_role: InstrumentRole | None = None   # only for instrument nodes
    # Out of the comparison: two claims that agree on ref, trait and position are the
    # same claim, and a `KeywordDict` is unhashable besides.
    keywords: KeywordDict = field(default_factory=KeywordDict, compare=False)

    def __str__(self) -> str:
        what = self.trait or self.kind
        return f"{self.ref} ({what} @ {'/'.join(self.path) or '<root>'})"


def _iter_part(node: Part, path: InstrumentPath) -> Iterator[DeviceNode]:
    path = path + (node.name,)

    # A node's own structural ref precedes its attachments, and both precede its
    # parts: the walk is root-first at every level, and `DeviceIndex.refs` order is
    # that order. A plain `Assembly` matches neither arm and contributes no ref of
    # its own — so the arms are additions to the shared tail below, not a partition
    # of the cases, and `Assembly` is deliberately absent from them.
    match node:
        case InstrumentAssembly():
            yield DeviceNode(node.instrument, None, path, "instrument",
                             node.role, node.keywords)
        case SelectorAssembly():
            yield DeviceNode(node.selector, None, path, "selector")

    for a in node.attachments:
        yield DeviceNode(a.ref, a.trait, path, keywords=a.keywords)

    if isinstance(node, Assembly):
        for part in node.parts:
            yield from _iter_part(part, path)
