# SPDX-License-Identifier: Apache-2.0
"""External-request frontend: bind deployment-agnostic observing requests to the
structural model, then translate them into `collect.Collect`s.

This is the layer above `collect.py`. Where `collect.Collect` speaks the
deployment's private vocabulary — structural paths, device refs, opaque `Setting`s
— an external task source (a survey scheduler, a TOM, a proposal system) should not
have to. It addresses instruments by what they *are* and commands by what they
*mean*, and the resolver here derives everything else.

* **A command is already portable, so there are no encoding tables.** A `Setting`
  is a whole command with its values inside it, and its identity — whatever
  `command_id` reads off it — is a name both sides already share. "Set the filter
  to r" means the same thing at every site; what a deployment contributes is which
  of its devices can do it, and that is published, not authored.

* **A command routes itself.** Given a command and a scope, the receiving device
  falls out of `DeviceCapabilities.commands` along the instrument's chain: the
  **deepest** claim wins at instrument scope (a private wheel beats a shared
  selector wheel — that is what "this instrument's filter" means), the
  **shallowest** at sensor scope (pointing lands on the mount). A tie at the
  winning depth is an error naming the candidates, because silently picking one
  returns frames configured differently from how they are labelled.

* **Two selector forms, no third.** An instrument is addressed either by
  **identity** or by **capability** — never by a positional ordinal, which is
  identity that has forgotten it is identity.

    - `ByRef(ref)` — identity. The fully-coupled source that knows the
      observatory's shape (or introspects it). `ref` may be a real device ref or a
      deployment **alias**.
    - `ByCapability(...)` — properties plus cardinality, over three tiers of
      increasing resolution: declared traits, supported commands, and typed
      predicates over the keywords the chain's devices publish about themselves.
      The *same request* resolves on any deployment whose manifest satisfies it.
      `count` > 1 absorbs the "give me N science cameras" case a bare ordinal only
      pretended to serve.

* **Selection and routing read the same values in the same order.** A predicate
  reads a keyword from the chain merged root-first, so the deepest publisher wins;
  routing sends the command to the deepest claimant. The device a request selected
  *on* is the device it commands, and (one layer up) the device whose values reach
  the frame's header. Nothing here may put those on different orders.

* **The manifest is the discovery surface.** `build_manifest` projects the
  structure plus what each device claims about itself into one entry per
  instrument. A task source reasons against that — a stable, observatory-
  independent vocabulary — never the model internals. Nothing in it is authored:
  the traits, the commands and the keywords are all things devices publish, and the
  structure only says which chain they land on.

* **Everything else is derived from the views, not named by the request.** Given a
  resolved instrument path, the resolver derives which device serves a command and
  whether the resulting `(ref, Setting)` is shared or private — so it lands on the
  `Step` or the `FramePlan`, the split `collect.validate_step` enforces.

    This is the one consumer whose question spans both view axes, and it needs both
    for that reason: the *routing* lookup is device-major (`DeviceIndex.chain`),
    the *ownership* test is instrument-major (`InstrumentView.private`).

* **A device holds one value per step.** Two instruments sharing a device may agree
  on it — that is what a `Step` setting is for — but may not differ, and two
  unequal commands on one ref raise rather than keeping the last write, whose
  frames would carry a config they were not taken in.

* **Chronology is authored; everything under it is derived.** A request is an
  ordered tuple of `RequestStep`s. A within-step conflict is not resolved by
  inventing a boundary, because which side goes first is a science question — so it
  raises, and the boundary is the task source's answer. What stays derived is the
  whole of what makes this layer worth having: which device serves a command,
  whether a setting is shared or private, and (one layer down) the per-device
  barriers the ordering implies.

* **A resolved collect compiles.** `resolve_step` validates each step as it
  produces it, so `to_collect` cannot hand back something that only fails at
  `compile_collect`. It has to: selection matches on capability, which does not
  stop it selecting a guider or two instruments on opposite ports of one selector.

* **Escape hatches are opt-in and still validated.** `ByRef` addresses an
  instrument, `CommandRequest.ref` a device. Both bypass *addressing* and nothing
  else — support is still checked, placement is still derived, the one-value rule
  still applies — and `portability` reports that a request used them.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from operator import attrgetter
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, model_validator

from sensorkit.common.keyword import KeywordDict, get_keyword_info, get_keyword_type
from sensorkit.common.predicate import MISSING, FieldMatch, PredicateError
from sensorkit.workflow.collect import Collect, FramePlan, Setting, Step, validate_step
from sensorkit.workflow.dag import OnFailure
from sensorkit.workflow.structure import (
    DeviceRef,
    InstrumentPath,
    InstrumentRole,
    Trait,
)
from sensorkit.workflow.views import DeviceIndex, Topology

type CommandId = str
"""The registered name of a command type — the op vocabulary a deployment's devices
publish support for, and the whole of what this layer needs to know about a
`Setting`."""

type CommandIdHook = Callable[[Setting], CommandId]
"""How a `Setting` names its command type. A hook rather than a method call so the
compilers keep their opacity: nothing here knows what a command is."""

DEFAULT_COMMAND_ID: CommandIdHook = attrgetter("command_id")
"""The reader for a command carrying its own registered name, which is what a
registry-backed command vocabulary produces."""

type Aliases = Mapping[str, DeviceRef]
"""Deployment-published names for devices: sugar over `ByRef`, and the only naming
a deployment authors."""

NO_ALIASES: Aliases = MappingProxyType({})
"""A deployment that publishes no names of its own, which is every deployment until
one prefers a word to a device ref."""


@dataclass(frozen=True)
class DeviceCapabilities:
    """What one device claims about itself, live.

    All three tiers of `ByCapability`, and the routing table besides. Projected from
    whatever a deployment's device layer publishes; the structure contributes only
    which chain the device sits on.
    """

    traits: frozenset[Trait] = frozenset()
    commands: frozenset[CommandId] = frozenset()
    keywords: KeywordDict = field(default_factory=KeywordDict)


type CapabilityIndex = Mapping[DeviceRef, DeviceCapabilities]

NO_CAPABILITIES = DeviceCapabilities()
"""What a device the index says nothing about claims: nothing. A device is matched
and routed to on what it publishes, so silence is an absence of capability rather
than an error — the structure is free to carry devices this layer never addresses."""


def capabilities_of(caps: CapabilityIndex, ref: DeviceRef) -> DeviceCapabilities:
    """What one device claims, or nothing at all for a device the index omits."""
    return caps.get(ref, NO_CAPABILITIES)


def _keyword_key(v: object) -> object:
    """Authoring shorthand: a registered keyword type stands for its own key."""
    if not isinstance(v, type):
        return v

    info = get_keyword_info(v)
    if info is None:
        raise ValueError(f"'{v.__name__}' is not a declared keyword")

    return info.key


class KeywordMatch(FieldMatch):
    """A predicate at a path within one keyword a device publishes.

    The keyword registry is the description vocabulary — typed, documented, and
    already shared with frame metadata and constraints — so a capability question
    is asked in the same terms a header answers in. `keyword` may be authored as
    the type itself and serializes to its registered key.

    Validated on construction against the model that key names, both passes:
    the path must name a field, and the predicate's operands must be able to
    compare against it. A capability match that could never hold is a mistake where
    it is written, not a task that silently selects nothing at 2am.

    `trait` narrows the reading to the devices on the chain claiming that trait,
    for a chain where two kinds of device publish the same keyword. Absent, the
    whole chain is read and the deepest publisher wins.
    """

    keyword: Annotated[str, BeforeValidator(_keyword_key)]
    trait: Trait | None = None

    @model_validator(mode="after")
    def _check_against_keyword(self) -> Self:
        model = get_keyword_type(self.keyword)

        if model is None:
            raise PredicateError(f"'{self.keyword}' is not a declared keyword")

        self.validate_against(model)
        return self

    def matches(self, keywords: KeywordDict) -> bool:
        """Evaluate against a merged chain reading.

        A keyword nobody published reads as missing rather than as a failure, so
        `exists(False)` is the one predicate it can satisfy.
        """
        return self.test(keywords.get(self.keyword, MISSING))


# The request model below is frozen dataclasses — runtime objects a task source
# constructs, mirroring `collect.Collect` / `Step` / `FramePlan` one level up.


@dataclass(frozen=True)
class ByRef:
    """Address an instrument by identity: a device ref or an alias."""

    ref: DeviceRef


@dataclass(frozen=True)
class ByCapability:
    """Address instruments by capability plus cardinality.

    Three tiers over registries a deployment already keeps, in increasing
    resolution and decreasing coarseness:

    * `traits` — declared trait names the chain must satisfy. Free: already
      declared, already matched.
    * `capabilities` — individual command ids the chain must support, for when no
      declared trait says what is meant. A descriptor is in effect a trait nobody
      had to declare; one written often enough is worth promoting to a real trait.
    * `requires` — value-level predicates over what the chain's devices publish
      about themselves. "Has an r filter", "field of view under half a degree".

    `role` stays structural rather than keyword-borne because it is a workflow
    contract rather than a description: it decides whether an instrument may appear
    in a `FramePlan` at all.
    """

    role: InstrumentRole | None = None
    traits: tuple[Trait, ...] = ()
    capabilities: tuple[CommandId, ...] = ()
    requires: tuple[KeywordMatch, ...] = ()
    count: int = 1


type Selector = ByRef | ByCapability

type Scope = Literal["any", "private", "shared"]


@dataclass(frozen=True)
class CommandRequest:
    """One command, and how much help the resolver is given placing it.

    `scope` is a tie-break for a chain carrying two devices that can both perform
    the command: it picks a side rather than naming one. `ref` is the escape hatch
    that names one — addressing bypassed, validation not.
    """

    command: Setting
    scope: Scope = "any"
    ref: DeviceRef | None = None


@dataclass(frozen=True)
class ExposureRequest:
    """One instrument-chain's work: which instrument(s) to use (`select`), the
    exposure, and the commands that configure it.

    `settings` are at instrument scope, so they route within the selected chain. A
    `ByCapability` with `count` > 1 applies this request to each matched instrument.
    """

    select: Selector
    integration_time: float
    frame_count: int = 1
    frame_type: str = "science"
    settings: tuple[CommandRequest, ...] = ()


@dataclass(frozen=True)
class RequestStep:
    """One configuration epoch in request vocabulary: the exposures taken under one
    set of sensor-scope commands.

    Mirrors `collect.Step`, which is what it resolves to — the mirror is the point,
    since the layer's whole job is translating one vocabulary into the other. The
    pointing setpoint is an ordinary setting here: a target is a command value like
    any other, and the mount is wherever that command lands.
    """

    exposures: tuple[ExposureRequest, ...]
    settings: tuple[CommandRequest, ...] = ()
    align: Literal["start", "midpoint"] = "start"


@dataclass(frozen=True)
class InstrumentEntry:
    """One row of the published discovery surface.

    The structure contributes the name, the position and the role; all three
    capability tiers are read from what the chain's devices publish. That is what
    makes the manifest a projection of live state rather than a second document to
    keep in step with one.
    """

    handle: str                             # alias if published, else path string
    path: InstrumentPath
    ref: DeviceRef
    role: InstrumentRole
    traits: frozenset[Trait]                # declared anywhere on the chain
    commands: frozenset[CommandId]          # supported anywhere on the chain
    keywords: KeywordDict                   # the chain merged, deepest publisher wins
    trait_keywords: Mapping[Trait, KeywordDict]     # the same merge, per claimed trait

    def keywords_for(self, trait: Trait | None) -> KeywordDict:
        """The reading a match takes: the whole chain, or one trait's claimants."""
        if trait is None:
            return self.keywords

        return self.trait_keywords.get(trait) or KeywordDict()

    def __str__(self) -> str:
        return (f"{self.handle:<12} {self.role.value} "
                f"[{', '.join(sorted(self.traits))}]  <- {self.ref}")


@dataclass(frozen=True)
class RequestReport:
    """What a request cost in portability: the identity it named.

    A pure question about the request, asked without resolving it, so a deployment
    that accepts only portable tasks can refuse before it commits to one. Neither
    hatch is illegitimate — `ByRef` is how a local operator drives a named
    instrument — which is why this reports rather than rejects.

    A third coupling is not visible here and belongs to whoever owns the command
    vocabulary: a command that no trait requires leaves a request device-agnostic
    but resolvable only where that command is supported.
    """

    instruments: int = 0                    # ByRef selectors
    devices: int = 0                        # CommandRequest.ref

    @property
    def portable(self) -> bool:
        return not (self.instruments or self.devices)


def portability(steps: Sequence[RequestStep]) -> RequestReport:
    """Count the addressing escape hatches a request used."""
    commands = [c for s in steps
                for c in (*s.settings,
                          *(c for e in s.exposures for c in e.settings))]

    return RequestReport(
        instruments=sum(isinstance(e.select, ByRef)
                        for s in steps for e in s.exposures),
        devices=sum(c.ref is not None for c in commands))


def merge_keywords(devices: DeviceIndex, caps: CapabilityIndex,
                   refs: Iterable[DeviceRef]) -> KeywordDict:
    """Merge what a run of devices says about itself, in the order given.

    Two orderings, and they are what makes a reading answerable: `refs` root-first
    means the deepest publisher wins, and per device, what it reports live wins over
    what the structure supplies for it. Static keywords are a site's defaults for a
    driver that does not report something, never an override of one that does.
    """
    merged = KeywordDict()

    for ref in refs:
        merged.update(devices.keywords_of(ref))
        merged.update(capabilities_of(caps, ref).keywords)

    return merged


def build_manifest(topo: Topology, devices: DeviceIndex, caps: CapabilityIndex,
                   aliases: Aliases = NO_ALIASES) -> tuple[InstrumentEntry, ...]:
    """Project a sensor and what its devices publish into the discovery schema.

    One entry per instrument, whatever its role: a task source may legitimately
    discover a guider, and learning that it is not a collect target is resolution's
    job rather than discovery's.
    """
    alias_of = {ref: name for name, ref in aliases.items()}
    entries: list[InstrumentEntry] = []

    for path, view in topo.instruments.items():
        chain = tuple(devices.chain(path))
        claimants = _claimants(devices, caps, path, chain)
        ref = view.assembly.instrument

        entries.append(InstrumentEntry(
            handle=alias_of.get(ref, "/".join(path)),
            path=path, ref=ref, role=view.assembly.role,
            traits=frozenset(claimants),
            commands=frozenset(
                c for r in chain for c in capabilities_of(caps, r).commands),
            keywords=merge_keywords(devices, caps, chain),
            trait_keywords={
                t: merge_keywords(devices, caps, refs)
                for t, refs in claimants.items()}))

    return tuple(entries)


def _claimants(devices: DeviceIndex, caps: CapabilityIndex,
               path: InstrumentPath, chain: Sequence[DeviceRef],
               ) -> dict[Trait, list[DeviceRef]]:
    """trait -> the chain's devices claiming it, root-first.

    Both halves of what a trait claim can be: the structure attaches a device as a
    trait, and the device declares the traits it satisfies. The two are the same
    vocabulary, and an entry answers over their union.
    """
    structural = devices.claims_on_chain(path)
    by_ref: dict[DeviceRef, set[Trait]] = {}

    for trait, refs in structural.items():
        for ref in refs:
            by_ref.setdefault(ref, set()).add(trait)

    claimants: dict[Trait, list[DeviceRef]] = {}
    for ref in chain:
        declared = capabilities_of(caps, ref).traits
        for trait in sorted(by_ref.get(ref, set()) | declared):
            claimants.setdefault(trait, []).append(ref)

    return claimants


def matches(entry: InstrumentEntry, sel: ByCapability) -> bool:
    """Whether one manifest entry satisfies a capability descriptor."""
    return (
        (sel.role is None or entry.role == sel.role)
        and set(sel.traits) <= entry.traits
        and set(sel.capabilities) <= entry.commands
        and all(m.matches(entry.keywords_for(m.trait)) for m in sel.requires)
    )


def select(manifest: Sequence[InstrumentEntry], sel: Selector,
           aliases: Aliases = NO_ALIASES) -> tuple[InstrumentPath, ...]:
    """Resolve a selector to instrument paths.

    `ByRef` -> exactly one (or error); `ByCapability` -> the first `count` matches
    in manifest order (distinct by construction — manifest instruments are unique).

    Pure over the manifest, so a task source holding a published one selects with
    the code the deployment will select with, and learns before submitting whether
    its request resolves.
    """
    if isinstance(sel, ByRef):
        ref = aliases.get(sel.ref, sel.ref)
        hits = [e for e in manifest if e.ref == ref]
        if not hits:
            raise ValueError(
                f"ByRef({sel.ref!r}) matches no instrument"
                + (f" (alias -> {ref!r})" if ref != sel.ref else ""))
        return (hits[0].path,)

    hits = [e for e in manifest if matches(e, sel)]
    if len(hits) < sel.count:
        raise ValueError(
            f"{_describe(sel)} matched {len(hits)} instrument(s), "
            f"need {sel.count}")

    return tuple(e.path for e in hits[:sel.count])


def _describe(sel: ByCapability) -> str:
    """The descriptor, in the terms it was authored in — every tier it constrains
    and none it does not, since an empty one is not what failed."""
    parts = [f"{name}={value}" for name, value in (
        ("role", sel.role.value if sel.role else None),
        ("traits", sorted(sel.traits) or None),
        ("capabilities", sorted(sel.capabilities) or None),
        ("requires", [f"{m.keyword}.{m.field or ''}".rstrip(".")
                      for m in sel.requires] or None),
    ) if value is not None]

    return f"ByCapability({', '.join(parts)})"


def _paths(paths: Sequence[InstrumentPath]) -> str:
    return ", ".join("/".join(p) for p in paths) or "<no instrument>"


class Placement:
    """The two sides of a step under construction, and the rule keeping them apart.

    Which side a command lands on is read off the structure — private to exactly one
    participant, or shared — so `collect.validate_step`'s split is satisfied by
    construction rather than asked about. The escape hatches obey it too: they
    bypass addressing, not placement.
    """

    def __init__(self, topo: Topology, participants: Iterable[InstrumentPath]):
        self.topo = topo
        self.shared: dict[DeviceRef, Setting] = {}
        self.private: dict[InstrumentPath, dict[DeviceRef, Setting]] = {
            path: {} for path in participants}

    def put(self, ref: DeviceRef, command: Setting,
            paths: Sequence[InstrumentPath], where: str) -> None:
        """Record one routed command, or raise if the device is already commanded.

        Raises:
            ValueError: The device holds a different value this step.
        """
        owner = next((p for p in self.private
                      if ref in self.topo.instruments[p].private), None)
        bucket = self.shared if owner is None else self.private[owner]

        if ref in bucket and bucket[ref] != command:
            # A collect step is one configuration epoch, so a device holds one value
            # across it. Silently keeping the last write would return frames labelled
            # with a config they were not taken in.
            raise ValueError(
                f"{where}: {_paths(paths)} needs {ref} at {command!r}, but "
                f"{bucket[ref]!r} is already commanded this step; a device "
                f"holds one value per step — give them a step each")

        bucket[ref] = command


class RequestResolver:
    """Sole importer of both vocabularies.

    Everything upstream speaks the external request model; everything downstream
    (`Collect`, the derived views, the op hook) speaks the deployment's.

    The manifest is built here, so a resolver that exists is one that can resolve —
    but it is built from live capability, so it is only as current as the index it
    was given. A deployment rebuilds when what its devices publish changes.
    """

    def __init__(self, topo: Topology, devices: DeviceIndex,
                 caps: CapabilityIndex, aliases: Aliases = NO_ALIASES,
                 command_id: CommandIdHook = DEFAULT_COMMAND_ID):
        self.topo = topo
        self.devices = devices
        self.caps = caps
        self.aliases = aliases
        self.command_id = command_id
        self.manifest = build_manifest(topo, devices, caps, aliases)

    def resolve_paths(self, sel: Selector) -> tuple[InstrumentPath, ...]:
        return select(self.manifest, sel, self.aliases)

    def resolve_step(self, step: RequestStep, *, where: str = "step") -> Step:
        """Resolve one authored epoch into a `collect.Step`.

        The primitive of this layer: selection, command routing, the shared/private
        split and the one-value-per-device rule all live here, and `to_collect` is a
        loop over it. A caller sequencing steps some other way reuses this rather
        than reimplementing the half of the layer that reads the views.

        The returned `Step` is validated, so it compiles.
        """
        exposures: list[tuple[ExposureRequest, tuple[InstrumentPath, ...]]] = []
        participants: list[InstrumentPath] = []

        for req in step.exposures:
            paths = self.resolve_paths(req.select)
            for path in paths:
                if path in participants:
                    raise ValueError(
                        f"{where}: {'/'.join(path)} targeted by more than "
                        f"one exposure request")
                participants.append(path)
            exposures.append((req, paths))

        placement = Placement(self.topo, participants)

        # Sensor scope routes over every participant's chain at once, which is what
        # puts the pointing setpoint on the one mount above all of them — and makes
        # two mounts an error rather than a coin flip.
        for cmd in step.settings:
            self._place(placement, cmd, participants, deepest=False, where=where)

        for req, paths in exposures:
            for path in paths:
                for cmd in req.settings:
                    self._place(placement, cmd, (path,), deepest=True,
                                where=where)

        resolved = Step(
            plans={path: FramePlan(req.integration_time, req.frame_count,
                                   target_type=req.frame_type,
                                   settings=placement.private[path])
                   for req, paths in exposures for path in paths},
            settings=placement.shared, align=step.align)

        # Checked here rather than left for `compile_collect`. Selection matches on
        # capability, which says nothing about whether an instrument may be exposed
        # (a published guider is not a collect target) or whether two of them may be
        # exposed at once (two ports of one selector are not). Both are errors in the
        # request, so this layer is where they belong.
        validate_step(self.topo, resolved, where)
        return resolved

    def to_collect(self, steps: Sequence[RequestStep], *, name: str = "",
                   on_failure: OnFailure = "stop") -> Collect:
        """Resolve an authored sequence.

        Steps are independent: the same instrument may reappear, and a device may
        take a new value — that is what a boundary is for. `compile_collect` derives
        the barriers between them and elides re-commands of an unchanged value, so
        repeating an unchanged target across steps is free.
        """
        where = name or "collect"

        return Collect(
            steps=tuple(
                self.resolve_step(step, where=f"{where}, step {k}")
                for k, step in enumerate(steps)),
            name=name, on_failure=on_failure)

    def _place(self, placement: Placement, cmd: CommandRequest,
               paths: Sequence[InstrumentPath], *, deepest: bool,
               where: str) -> None:
        """Route one command and put it on the side of the step it belongs on."""
        ref = self._route(cmd, paths, deepest=deepest, where=where)
        placement.put(ref, cmd.command, paths, where)

    def _route(self, cmd: CommandRequest, paths: Sequence[InstrumentPath], *,
               deepest: bool, where: str) -> DeviceRef:
        """The device a command is for.

        Candidates are the devices supporting it on the participating chains, and
        depth decides between them: the deepest claim is the most specific one and
        wins where a request means "this instrument's", the shallowest where it
        means "the sensor's".
        """
        cid = self.command_id(cmd.command)

        if cmd.ref is not None:
            ref = self.aliases.get(cmd.ref, cmd.ref)
            if cid not in capabilities_of(self.caps, ref).commands:
                raise ValueError(
                    f"{where}: {ref} does not support {cid!r}")
            return ref

        candidates = self._candidates(cid, cmd.scope, paths, deepest=deepest)

        if not candidates:
            raise ValueError(
                f"{where}: no device on {_paths(paths)} supports {cid!r}"
                + ("" if cmd.scope == "any" else f" as a {cmd.scope} device"))

        best = (max if deepest else min)(candidates.values())
        winners = sorted(r for r, depth in candidates.items() if depth == best)

        if len(winners) > 1:
            # Keeping one silently would command a device the request did not mean.
            raise ValueError(
                f"{where}: {cid!r} is supported by {winners} at the same "
                f"position on {_paths(paths)}; name one with ref, or set a scope")

        return winners[0]

    def _candidates(self, cid: CommandId, scope: Scope,
                    paths: Sequence[InstrumentPath], *, deepest: bool
                    ) -> dict[DeviceRef, int]:
        """Supporting devices on the given chains, against the depth that decides.

        A ref reached from two chains is kept at the depth its own side of the
        comparison would pick, so merging the chains cannot promote a device over
        one that beats it on either.
        """
        pick = max if deepest else min
        found: dict[DeviceRef, int] = {}

        for path in paths:
            view = self.topo.instruments[path]
            for ref, depth in self.devices.chain(path).items():
                if cid not in capabilities_of(self.caps, ref).commands:
                    continue
                if scope == "private" and ref not in view.private:
                    continue
                if scope == "shared" and ref in view.private:
                    continue
                found[ref] = pick(found.get(ref, depth), depth)

        return found


