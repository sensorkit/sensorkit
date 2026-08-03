# SPDX-License-Identifier: Apache-2.0
"""External-request frontend: bind deployment-agnostic observing requests to the
structural model, then translate them into `collect.Collect`s.

This is the layer above `collect.py`. Where `collect.Collect` speaks the
deployment's private vocabulary — structural paths, device refs, opaque `Setting`s
— an external task source (a survey scheduler, a TOM, a proposal system) should not
have to. It addresses instruments by what they *are*, and the resolver here derives
everything else.

* **Two selector forms, no third.** An instrument is addressed either by
  **identity** or by **properties** — never by a positional ordinal, which is
  identity that has forgotten it is identity.

    - `ByRef(ref)` — identity. The fully-coupled source that knows the
      observatory's shape (or introspects it). `ref` may be a real device ref or a
      deployment **alias** (sugar over `ByRef`, resolved against
      `Bindings.aliases`).
    - `ByCapability(role, modality, requires, count)` — properties plus
      cardinality. The agnostic source describes the instrument chain it needs and
      the match happens here; the *same request* resolves on any deployment whose
      manifest satisfies the predicate. `count` > 1 absorbs the "give me N science
      cameras" case a bare ordinal only pretended to serve.

* **Binding coherence is checked before any request exists.** `build_manifest` and
  `resolve_target_ref` need the bindings and the structure and nothing else, so
  both run in `RequestResolver.__init__` and both run again at `SensorPlan` load. A
  resolver that exists can resolve; a deployment whose `target_trait` names nothing
  fails at load, not on the night's first request.

* **The manifest is the discovery surface.** `build_manifest` projects a
  `SensorModel` + `Bindings` into a standard schema (handle, role, modality,
  attributes). A task source reasons against the *schema* — a stable,
  observatory-independent vocabulary — never the model internals. Part of it is
  derived, not authored twice: an imager's `passbands` is exactly its filter
  wheel's encoded value vocabulary.

* **Everything else is derived from the views, not named by the request.** Given a
  resolved instrument path, the resolver derives: which device serves a given
  abstract dimension (by trait), whether a resolved `(ref, Setting)` is shared or
  private (so it lands on the `Step` or the `FramePlan` — the split
  `collect.validate_step` enforces), and the mount ref for the target. The request
  carries capabilities and physical quantities; the deployment carries the small
  binding tables; refs and structure are never in either.

    This is the one consumer whose question spans both view axes, and it needs both
    for that reason: the *trait* lookup is device-major
    (`DeviceIndex.claims_on_chain`), the *ownership* test is instrument-major
    (`InstrumentView.private`).

* **A device holds one value per step.** Two instruments sharing a device may agree
  on it — that is what a `Step` setting is for — but may not differ, and a dimension
  may not overwrite the pointing setpoint. Resolution raises instead of keeping the
  last write, whose frames would carry a config they were not taken in.

* **Chronology is authored; everything under it is derived.** An `ExternalTask` is
  an ordered tuple of `TaskStep`s, each with its own target. A within-step conflict
  is not resolved by inventing a boundary, because which side goes first is a
  science question — so it raises, and the boundary is the task source's answer.
  What stays derived is the whole of what makes this layer worth having: which
  device serves a dimension, whether a setting is shared or private, and (one layer
  down) the per-device barriers the ordering implies. `ExternalTask.single` is the
  one-step case.

* **A resolved collect compiles.** `resolve_step` validates each step as it produces
  it, so `to_collect` cannot hand back something that only fails at
  `compile_collect`. It has to: selection matches on role and attributes, which does
  not stop it selecting a guider or two instruments on opposite ports of one
  selector.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from sensorkit.workflow.collect import (
    Collect,
    FramePlan,
    Setting,
    Step,
    validate_step,
)
from sensorkit.workflow.structure import (
    DeviceRef,
    InstrumentPath,
    InstrumentRole,
    Trait,
)
from sensorkit.workflow.views import DeviceIndex, Topology

# The binding tables below are deployment config — pydantic models, like
# `lifecycle.PhaseTable`, validated and cross-checked at load.


class DimensionBinding(BaseModel, extra="forbid", frozen=True):
    """Maps an abstract request dimension (`filter`) onto a trait (`filter_wheel`).

    `scope` disambiguates when a path carries more than one device of that trait
    (e.g. a shared selector wheel plus a private wheel): `private` / `shared` picks
    a side, `any` requires the choice to be unambiguous.
    """

    trait: Trait
    scope: Literal["any", "private", "shared"] = "any"

    @model_validator(mode="before")
    @classmethod
    def _str_shorthand(cls, v: object) -> object:
        return {"trait": v} if isinstance(v, str) else v


class Bindings(BaseModel, extra="forbid"):
    """The entire name map, and the only deployment-specific thing a task source's
    translation depends on.

    Small and stable under instrument count: refs and structure are derived, so
    adding a camera edits nothing here.
    """

    # abstract dimension -> trait (+ scope). `filter` -> `filter_wheel`.
    dimensions: dict[str, DimensionBinding] = Field(default_factory=dict)
    # trait -> {abstract value -> Setting}. One table per trait, reused by every
    # device claiming it — the anti-friction: not one per ref.
    encodings: dict[Trait, dict[str, Setting]] = Field(default_factory=dict)
    # the trait carrying the pointing setpoint (the mount).
    target_trait: Trait = "mount"
    # deployment aliases: published name -> device ref (sugar over `ByRef`).
    aliases: dict[str, DeviceRef] = Field(default_factory=dict)
    # which dimension's encoded vocabulary populates manifest passbands (None:
    # don't derive passbands).
    passband_dimension: str | None = "filter"


# The external request model below is frozen dataclasses — runtime objects a task
# source constructs, mirroring `collect.Collect` / `Step` / `FramePlan`.


@dataclass(frozen=True)
class ByRef:
    """Address an instrument by identity: a device ref or an alias."""

    ref: DeviceRef


@dataclass(frozen=True)
class ByCapability:
    """Address instruments by properties plus cardinality.

    `requires` matches manifest attributes: a scalar means equality (or membership
    if the attribute is a collection); a mapping supports `{min, max}` bounds and
    `{has: [...]}` set-superset.
    """

    role: InstrumentRole | None = None
    modality: str | None = None
    requires: Mapping[str, object] = field(default_factory=dict)
    count: int = 1


type Selector = ByRef | ByCapability


@dataclass(frozen=True)
class TargetSpec:
    """Pointing in domain terms; the mount ref and command encoding are derived."""

    name: str
    mode: str = "sidereal"          # sidereal / rate:<id> / ... — opaque here


@dataclass(frozen=True)
class TargetCommand:
    """Opaque mount `Setting` produced from a `TargetSpec`.

    Structured so a real adapter can dispatch it; the compiler only needs `==`.
    """

    mode: str
    name: str

    def __repr__(self) -> str:      # keeps `dag.format_graph` readable
        return f"{self.mode}:{self.name}"


@dataclass(frozen=True)
class ExposureReq:
    """One instrument-chain's work: which instrument(s) to use (`select`), the
    exposure, and abstract config (`{filter: r}`).

    A `ByCapability` with `count` > 1 applies this request to each matched
    instrument.
    """

    select: Selector
    exposure_s: float
    n_frames: int = 1
    config: Mapping[str, str] = field(default_factory=dict)
    frame_type: str = "science"


@dataclass(frozen=True)
class TaskStep:
    """One configuration epoch in request vocabulary: a target, and the exposure
    requests taken under it.

    Mirrors `collect.Step`, which is what it resolves to — the mirror is the point,
    since the layer's whole job is translating one vocabulary into the other.
    """

    target: TargetSpec
    exposures: tuple[ExposureReq, ...]
    align: Literal["start", "midpoint"] = "start"


@dataclass(frozen=True)
class ExternalTask:
    """An ordered sequence of steps, mirroring `collect.Collect` field for field.

    Ordering is *authored*, not derived. Two conflicting requests are not assigned
    to different steps here, because which one goes first is a science question with
    no answer at this layer — so a conflict within a step raises, and placing the
    boundary is the task source's answer to it. What stays derived is everything
    below the boundary: which device serves a dimension, whether a setting is shared
    or private, and the per-device barriers the ordering implies.

    `single()` is the common case — one target, a flat set of exposures — and is the
    whole of what an agnostic source needs. Dithers, focus sweeps and rate-sidereal
    sequences are multi-step, and a task source states them here, in its own
    vocabulary.
    """

    steps: tuple[TaskStep, ...]
    name: str = ""
    on_failure: Literal["stop", "continue"] = "stop"   # dag's vocabulary

    @classmethod
    def single(cls, target: TargetSpec, exposures: Sequence[ExposureReq],
               name: str = "") -> ExternalTask:
        return cls((TaskStep(target, tuple(exposures)),), name)


@dataclass(frozen=True)
class InstrumentEntry:
    """One row of the published discovery surface."""

    handle: str                     # alias if published, else path string
    path: InstrumentPath
    ref: DeviceRef
    role: InstrumentRole
    modality: str
    attributes: Mapping[str, object]

    def __str__(self) -> str:
        attrs = ", ".join(f"{k}={v}" for k, v in sorted(self.attributes.items()))
        return (f"{self.handle:<12} {self.role.value}/{self.modality} "
                f"[{attrs}]  <- {self.ref}")


def build_manifest(topo: Topology, devices: DeviceIndex,
                   bindings: Bindings) -> tuple[InstrumentEntry, ...]:
    """Project a sensor + bindings into the standard capability schema.

    Raises `ValueError` on incoherent bindings (a passband dimension that names no
    known dimension), so the manifest is load-checkable.
    """
    alias_of = {ref: name for name, ref in bindings.aliases.items()}

    filter_trait: Trait | None = None
    if bindings.passband_dimension is not None:
        dim = bindings.dimensions.get(bindings.passband_dimension)
        if dim is None:
            raise ValueError(
                f"passband_dimension '{bindings.passband_dimension}' is not a "
                f"declared dimension {sorted(bindings.dimensions)}")
        filter_trait = dim.trait

    entries: list[InstrumentEntry] = []
    for path, view in topo.instruments.items():
        caps = view.assembly.capabilities
        attrs = dict(caps.attributes)
        # Derived, not authored twice: the filter wheel's value vocabulary *is* the
        # instrument's passband set.
        if filter_trait is not None and filter_trait in devices.claims_on_chain(path):
            attrs.setdefault(
                "passbands", sorted(bindings.encodings.get(filter_trait, {})))
        ref = view.assembly.instrument
        entries.append(InstrumentEntry(
            handle=alias_of.get(ref, "/".join(path)),
            path=path, ref=ref, role=view.assembly.role,
            modality=caps.modality, attributes=attrs))

    return tuple(entries)


def resolve_target_ref(devices: DeviceIndex, bindings: Bindings) -> DeviceRef:
    """The one root device carrying the pointing setpoint.

    A binding-coherence check, and a sibling of `build_manifest`: it depends on the
    bindings and the structure, not on any request, so it belongs where those are
    first put together. `SensorPlan` runs both at load and `RequestResolver` runs
    both at construction, which is why a deployment whose `target_trait` names
    nothing fails then rather than on the first request of the night.
    """
    trait = bindings.target_trait
    refs = devices.root_claims().get(trait, ())

    if not refs:
        raise ValueError(f"no root device claims target trait {trait!r}")
    if len(refs) > 1:
        # Two mounts is a config error, not a choice to make silently.
        raise ValueError(
            f"target trait {trait!r} is claimed by {sorted(refs)}; "
            f"exactly one root device may carry the pointing setpoint")

    return refs[0]


def _attr_matches(value: object, req: object) -> bool:
    if value is None:
        return False

    if isinstance(req, Mapping):
        if "min" in req and not value >= req["min"]:          # type: ignore[operator]
            return False
        if "max" in req and not value <= req["max"]:          # type: ignore[operator]
            return False
        if "has" in req and not set(req["has"]).issubset(set(value)):  # type: ignore[arg-type]
            return False
        return True

    if isinstance(value, list | tuple | set):
        return req in value

    return value == req


def select(manifest: tuple[InstrumentEntry, ...], bindings: Bindings,
           sel: Selector) -> tuple[InstrumentPath, ...]:
    """Resolve a selector to instrument paths.

    `ByRef` -> exactly one (or error); `ByCapability` -> the first `count` matches
    in manifest order (distinct by construction — manifest instruments are unique).
    """
    if isinstance(sel, ByRef):
        ref = bindings.aliases.get(sel.ref, sel.ref)
        hits = [e for e in manifest if e.ref == ref]
        if not hits:
            raise ValueError(
                f"ByRef({sel.ref!r}) matches no instrument"
                + (f" (alias -> {ref!r})" if ref != sel.ref else ""))
        return (hits[0].path,)

    hits = [
        e for e in manifest
        if (sel.role is None or e.role == sel.role)
        and (sel.modality is None or e.modality == sel.modality)
        and all(_attr_matches(e.attributes.get(k), v)
                for k, v in sel.requires.items())
    ]
    if len(hits) < sel.count:
        raise ValueError(
            f"ByCapability(role={sel.role}, modality={sel.modality}, "
            f"requires={dict(sel.requires)}) matched {len(hits)} "
            f"instrument(s), need {sel.count}")

    return tuple(e.path for e in hits[:sel.count])


class RequestResolver:
    """Sole importer of both vocabularies.

    Everything upstream speaks the external request model; everything downstream
    (`Collect`, the derived views, the op hook) speaks the deployment's.
    """

    def __init__(self, topo: Topology, devices: DeviceIndex,
                 bindings: Bindings):
        self.topo = topo
        self.devices = devices
        self.bindings = bindings
        # Both binding-coherence checks run here, so a resolver that exists is one
        # that can resolve.
        self.manifest = build_manifest(topo, devices, bindings)
        self.target_ref = resolve_target_ref(devices, bindings)

    def resolve_paths(self, sel: Selector) -> tuple[InstrumentPath, ...]:
        return select(self.manifest, self.bindings, sel)

    def resolve_step(self, step: TaskStep, *, where: str = "step") -> Step:
        """Resolve one authored epoch into a `collect.Step`.

        The primitive of this layer: selection, dimension encoding, the
        shared/private split and the one-value-per-device rule all live here, and
        `to_collect` is a loop over it. A caller sequencing steps some other way
        reuses this rather than reimplementing the half of the layer that reads the
        views.

        The returned `Step` is validated, so it compiles.
        """
        settings: dict[DeviceRef, Setting] = dict(
            self._encode_target(step.target))
        plans: dict[InstrumentPath, FramePlan] = {}

        for req in step.exposures:
            for path in self.resolve_paths(req.select):
                if path in plans:
                    raise ValueError(
                        f"{where}: {'/'.join(path)} targeted by more than "
                        f"one exposure request")
                view = self.topo.instruments[path]
                private: dict[DeviceRef, Setting] = {}
                for dim, val in req.config.items():
                    ref, setting = self._encode(path, dim, val)
                    # shared vs private derived from structure, not asked.
                    bucket = private if ref in view.private else settings
                    if ref in bucket and bucket[ref] != setting:
                        # A collect step is one configuration epoch, so a device
                        # holds one value across it. Silently keeping the last
                        # write would return frames labelled with a config they
                        # were not taken in.
                        raise ValueError(
                            f"{where}: {'/'.join(path)} needs "
                            f"{ref} at {setting!r} for dimension {dim!r}, "
                            f"but {bucket[ref]!r} is already commanded this "
                            f"step; instruments sharing a device cannot "
                            f"differ on it within one step — give them a "
                            f"step each")
                    bucket[ref] = setting
                plans[path] = FramePlan(
                    req.exposure_s, req.n_frames,
                    target_type=req.frame_type, settings=private)

        resolved = Step(plans=plans, settings=settings, align=step.align)
        # Checked here rather than left for `compile_collect`. Selection matches on
        # role, modality and attributes, none of which say whether an instrument may
        # be exposed (a published guider is not a collect target) or whether two of
        # them may be exposed at once (two ports of one selector are not). Both are
        # errors in the request, so this layer is where they belong.
        validate_step(self.topo, resolved, where)
        return resolved

    def to_collect(self, task: ExternalTask) -> Collect:
        """Resolve an authored sequence.

        Steps are independent: the same instrument may reappear, and a device may
        take a new value — that is what a boundary is for. `compile_collect` derives
        the barriers between them and elides re-commands of an unchanged value, so
        repeating an unchanged target across steps is free.
        """
        name = task.name or "task"
        return Collect(
            steps=tuple(
                self.resolve_step(step, where=f"{name}, step {k}")
                for k, step in enumerate(task.steps)),
            name=task.name, on_failure=task.on_failure)

    def _encode_target(self, target: TargetSpec) -> dict[DeviceRef, Setting]:
        return {self.target_ref: TargetCommand(target.mode, target.name)}

    def _encode(self, path: InstrumentPath, dim: str, val: str
                ) -> tuple[DeviceRef, Setting]:
        binding = self.bindings.dimensions.get(dim)
        if binding is None:
            raise ValueError(
                f"unknown dimension {dim!r}; bindings define "
                f"{sorted(self.bindings.dimensions)}")

        view = self.topo.instruments[path]
        cands = list(self.devices.claims_on_chain(path).get(binding.trait, ()))
        if binding.scope == "private":
            cands = [r for r in cands if r in view.private]
        elif binding.scope == "shared":
            cands = [r for r in cands if r in view.shared]

        if not cands:
            raise ValueError(
                f"{'/'.join(path)}: no {binding.trait!r} device for "
                f"dimension {dim!r}")
        if len(cands) > 1:
            raise ValueError(
                f"{'/'.join(path)}: dimension {dim!r} is ambiguous across "
                f"{sorted(cands)}; set a scope on the binding")

        table = self.bindings.encodings.get(binding.trait, {})
        if val not in table:
            raise ValueError(
                f"dimension {dim!r}: no encoding for {val!r} "
                f"(have {sorted(table)})")

        return cands[0], table[val]
