# SPDX-License-Identifier: Apache-2.0
"""Lifecycle workflows over the sensor structural model: bring-up, shutdown, and
anything else expressible as ordered phases of ops.

**Authoring: phase tables.** Ordered phases of entries, each entry selecting
devices by trait, by name, or by structural kind. No tables ship here — all tables
are config (pydantic models; YAML documents parse straight into them).

**Compilation.** `compile_table` expands a table against a sensor's
`views.DeviceIndex`, and the two kinds of link it emits are kept strictly apart:

* **Phases order; they do not propagate failure.** Every step in a phase takes a
  *soft* edge to every step of the phase(s) it follows. `Phase.after=(...)` names
  those phases (default: the previous phase in the table; empty phases fall through
  to their own predecessors), and is the only way to depart from config order — two
  phases following a common earlier one run in parallel.

* **`Entry.require=(...)` is the hard link,** and the only one an author writes.
  Each clause names a phase or an entry `id`; if that target did not succeed, this
  entry's steps skip. A `join` narrows the cross product to the steps that are the
  matched device's business — `same-device` to its own (one camera waits on its own
  cooling), `same-chain` to its position and everything above it (one unit of an
  array waits on its own focuser, and on a wheel shared above it).

    A clause targeting a phase this entry already follows — or an entry inside one
    — **replaces** that phase's soft edge rather than adding to it. Naming a
    dependency precisely is a statement that the blanket wait was not what was
    meant, and the same "explicit beats implicit" rule already governs a `device:`
    entry against a `trait:` one.

    Which is why a clause resolving to no step is a config error rather than a
    weaker clause, and why a join that empties it is the same error: having
    displaced the link it named, adding nothing in its place would leave the entry
    unordered.

* An `Entry` with several ops runs them serially per matched device — the same
  relation `join="same-device"` expresses, within one entry. Distinct devices remain
  concurrent.

* Within a phase, a `device`-keyed entry *claims* its device: `trait` and `all`
  entries in the same phase skip it (device = override, trait = default policy).

**Dispatch** is `ops.py`'s, unchanged and unextended: a phase table's steps become
`ops.Op` payloads and go to the deployment's one `OpHook`, the same hook a collect
uses. A table's op names are whatever that hook resolves; no vocabulary ships here.
Ops in a table are nullary — `Op.params` stays empty, because a phase table has no
syntax for arguments.

**Failure**, over `dag`'s two independent questions:

* `on_failure: stop | skip | continue` — how far this op's failure spreads,
  resolved op -> phase -> table. The table's value is required: a table's tolerance
  for failure is worth one line at the top of it, and is not something to inherit
  by accident. `continue` is how a teardown step says "attempt what follows me
  regardless": the dome's halt-before-close, or one chiller that must stop even if
  its peer would not.
* `optional` — whether this op's failure fails the run. Per op only, because it is
  the flag that can keep a failure out of the report, and independent of the ladder
  in both directions: a chiller that will not stop is worth reporting even though
  the teardown carries on past it, and a filter wheel that will not home is not,
  even though its own configure step is skipped.

**Overrides** are the same two questions bound late by the caller of `compile_table`
rather than by the table's author, plus the one thing no author can say: *don't
dispatch this at all*. A site running daytime tests compiles its init table with
the enclosure ops overridden to `ok` and everything downstream proceeds against a
shut dome. See `override.py`; nothing in a table mentions them.

**Raise policy**, applied here because `dag.DagRunner` never raises for step
failures:

* Any non-optional step that failed *or was skipped* raises `LifecycleError`
  carrying the `RunReport`. A required step that never ran leaves the sensor in the
  same state as one that ran and failed, so it is reported the same way — which is
  what keeps a mis-authored `optional` from silently swallowing everything
  downstream of it.
* Optional-step failures degrade: recorded, never fatal, never raised.
* External abort: report with `aborted=True`, and no raise — an abort is an
  outcome, not a failure.

Nothing is reversed automatically. A step is one action, and a table that reverses
another is just a table: a deployment wanting recovery writes one and runs it
deliberately, where it shows up in a report like any other work.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    model_validator,
)

from sensorkit.workflow.abort import AbortSignal
from sensorkit.workflow.dag import DagRunner, Graph, GraphBuilder, OnFailure, RunReport
from sensorkit.workflow.ops import Match, Op, OpContext, OpHook, RunContext
from sensorkit.workflow.override import Override, resolve_effects
from sensorkit.workflow.structure import (
    DeviceRef,
    InstrumentPath,
    InstrumentRole,
    Trait,
    on_chain,
)
from sensorkit.workflow.views import DeviceIndex

# The authoring surface below is pydantic models, so a YAML config layer parses
# straight into them. Shorthands: an op may be a bare string; an entry's `ops` may
# be a single item instead of a list; a table's name comes from its key in the
# config's `tables` mapping.


class OpSpec(BaseModel, frozen=True, extra="forbid"):
    op: str                            # what the hook is asked to perform
    optional: bool = False             # True: failure degrades, not fails
    on_failure: OnFailure | None = None    # None: the table's

    @model_validator(mode="before")
    @classmethod
    def _str_shorthand(cls, v: object) -> object:
        return {"op": v} if isinstance(v, str) else v


def _one_or_many(v: object) -> object:
    return [v] if isinstance(v, str | Mapping | OpSpec) else v


class Require(BaseModel, frozen=True, extra="forbid"):
    """One hard dependency: if `name` did not succeed, this entry skips.

    `name` is a phase name or an entry `id`, and may only be something already
    declared — a table reads forwards. `join` narrows the clause from the cross
    product to the steps that are this device's business:

    * `same-device` — the target's steps on the same ref, which is how a camera
      waits on its own cooling.
    * `same-chain` — the target's steps at or above this device's position, which
      is how one unit of an array waits on its own focuser without waiting on its
      peers'. A device shared above the unit is on that chain and joins too; a
      sibling's is not.

    A join that selects nothing is a config error, like a clause naming a target
    that matched nothing.

    YAML shorthand: a bare string is the clause with the default join.
    """

    name: str
    join: Literal["all", "same-device", "same-chain"] = "all"

    @model_validator(mode="before")
    @classmethod
    def _str_shorthand(cls, v: object) -> object:
        return {"name": v} if isinstance(v, str) else v


def _requires(v: object) -> object:
    return [v] if isinstance(v, str | Mapping | Require) else v


class Entry(BaseModel, frozen=True, extra="forbid"):
    """One row of a phase: which devices it matches and what runs.

    Selection is `match`, which may be left implicit when a `trait` or `device`
    field says it:

    * `trait`: every attachment claiming that trait.
    * `device`: one specific device — overrides `trait` and `all` entries in the
      same phase for that device.
    * `instrument`: every instrument, optionally narrowed by `role`.
    * `selector`: every selector.
    * `all`: every distinct ref once, whatever kind of claim put it there — for
      inherently per-device ops like connect/disconnect.

    The last three name a structural kind rather than a label, which is what keeps
    "instrument" and "selector" out of the deployment's trait vocabulary.

    `ops` run serially per matched device. `require` adds hard edges; see the module
    docstring for how a clause interacts with the soft phase link it may replace.
    """

    ops: Annotated[tuple[OpSpec, ...], BeforeValidator(_one_or_many)]
    id: str | None = None              # so a require clause can name this
    match: Match | None = None         # None: inferred from trait / device
    trait: Trait | None = None
    device: DeviceRef | None = None
    role: InstrumentRole | None = None     # match="instrument" only
    require: Annotated[
        tuple[Require, ...], BeforeValidator(_requires)] = ()

    @model_validator(mode="after")
    def _well_formed(self) -> Entry:
        if not self.ops:
            raise ValueError("entry declares no ops")

        named = (self.trait is not None) + (self.device is not None)
        if named + (self.match is not None) == 0:
            raise ValueError(
                "entry must select devices: set trait, device, or match")
        if named > 1:
            raise ValueError("entry sets both trait and device")

        implied = "trait" if self.trait is not None else (
            "device" if self.device is not None else None)
        if implied is not None and self.match not in (None, implied):
            raise ValueError(f"entry sets {implied} but match='{self.match}'")
        if implied is None and self.match in ("trait", "device"):
            raise ValueError(f"match='{self.match}' needs a {self.match} field")

        if self.role is not None and self.matcher != "instrument":
            raise ValueError("role applies only to match='instrument'")

        return self

    @property
    def matcher(self) -> Match:
        """The selection mode, with the field shorthands resolved."""
        if self.trait is not None:
            return "trait"
        if self.device is not None:
            return "device"
        assert self.match is not None       # _well_formed
        return self.match


class Phase(BaseModel, frozen=True, extra="forbid"):
    name: str
    entries: tuple[Entry, ...]
    after: tuple[str, ...] | None = None    # None: the previous phase
    on_failure: OnFailure | None = None     # None: the table's


class PhaseTable(BaseModel, frozen=True, extra="forbid"):
    # Injected from the mapping key in the config document; excluded from dumps so
    # the key stays the single source of the name.
    name: str = Field(default="", exclude=True)
    phases: tuple[Phase, ...]
    # dag's vocabulary, and the default every op takes. Required rather than
    # defaulted: what a table does when a step fails is the first thing a reader
    # needs and the last thing to inherit silently. `stop` suits bring-up (a
    # half-built sensor is not worth continuing into); `skip` suits teardown, where
    # every step not invalidated by the failure is still worth attempting;
    # `continue` suits a recovery table, where nothing should hold anything else
    # back.
    on_failure: OnFailure


type PhaseSteps = dict[str, list[int]]
type PhaseAfter = dict[str, tuple[str, ...]]
type EntryPhase = dict[str, str]
type EntrySteps = dict[str, list[int]]
type Target = tuple[DeviceRef, Trait | None, InstrumentPath]


def compile_table(devices: DeviceIndex, table: PhaseTable,
                  overrides: Sequence[Override] = ()) -> Graph:
    """Expand a phase table against a sensor's device index into the dag IR.

    Pure data in, pure data out — dry-runnable, no device access. Raises
    `ValueError` on config errors (unknown devices, bad `after` or `require`
    targets, duplicate claims, dependency cycles).

    `overrides` amends what the table says about the steps they address — see
    `override.py`. They change no edge and no selection: a step an override answers
    for is still a node, still in its phase, and still named by whatever required
    it, so the graph a caller inspects is the graph the run will follow.
    """
    g = GraphBuilder()
    phase_steps: PhaseSteps = {}
    phase_after: PhaseAfter = {}
    entry_steps: EntrySteps = {}                # entry id -> its step ids
    entry_phase: EntryPhase = {}                # entry id -> declaring phase

    prev: str | None = None
    for phase in table.phases:
        after = _declare_phase(phase, prev, phase_steps, phase_after, entry_phase)
        claimed = _claimed_devices(devices, phase)
        on_failure = phase.on_failure or table.on_failure

        steps: list[int] = []
        heads: list[tuple[Entry, DeviceRef, int]] = []

        for e in phase.entries:
            soft = _soft_links(e, after, phase.name, phase_steps, phase_after,
                               entry_phase)
            entry_heads, entry_ids = _emit_entry(
                g, devices, overrides, e, phase.name, soft, on_failure,
                _select_targets(devices, e, claimed))
            heads += [(e, ref, head) for ref, head in entry_heads]
            steps += entry_ids
            if e.id is not None:
                entry_steps.setdefault(e.id, []).extend(entry_ids)

        phase_steps[phase.name] = steps
        _resolve_requires(g, phase.name, heads, entry_steps, phase_steps,
                          phase_after, entry_phase)
        prev = phase.name

    return g.build()    # raises on cycles (only reachable via require)


def _effective_steps(label: str, phase_steps: PhaseSteps,
                     phase_after: PhaseAfter) -> set[int]:
    """Steps an edge to `label` means: the phase's own steps, or, if it produced
    none for this sensor, its predecessors'."""
    ids = phase_steps[label]
    if ids:
        return set(ids)

    out: set[int] = set()
    for a in phase_after[label]:
        out |= _effective_steps(a, phase_steps, phase_after)
    return out


def _declaring_phase(target: str, where: str, entry_phase: EntryPhase,
                     phase_after: PhaseAfter) -> str:
    """The phase a require clause's target belongs to — itself if it names a phase.

    Only already-declared names resolve, so a table reads forwards exactly as
    `after` does.
    """
    if target in entry_phase:
        return entry_phase[target]
    if target in phase_after:
        return target

    raise ValueError(
        f"phase '{where}': require names unknown or later "
        f"phase/entry '{target}'")


def _declare_phase(phase: Phase, prev: str | None, phase_steps: PhaseSteps,
                   phase_after: PhaseAfter, entry_phase: EntryPhase
                   ) -> tuple[str, ...]:
    """Register the phase's name and its entries' ids, and resolve what it follows.

    Entry ids are registered before any entry is built, so a clause may name a peer
    declared later in this phase; resolution happens once the whole phase exists.
    """
    if phase.name in phase_steps:
        raise ValueError(f"duplicate phase name '{phase.name}'")

    after = phase.after if phase.after is not None else (
        (prev,) if prev else ())
    unknown = [a for a in after if a not in phase_steps]
    if unknown:
        raise ValueError(
            f"phase '{phase.name}': after names unknown or later "
            f"phase(s) {unknown} — phases may only follow earlier ones")
    phase_after[phase.name] = after

    for e in phase.entries:
        if e.id is not None:
            if e.id in entry_phase:
                raise ValueError(f"duplicate entry id '{e.id}'")
            entry_phase[e.id] = phase.name

    return after


def _claimed_devices(devices: DeviceIndex, phase: Phase) -> set[DeviceRef]:
    """The devices this phase's `device` entries claim, which `trait` and `all`
    entries in it then skip."""
    claims = [e.device for e in phase.entries if e.device is not None]

    if len(claims) != len(set(claims)):
        raise ValueError(
            f"phase '{phase.name}': multiple entries claim the same device")

    missing = [r for r in claims if r not in devices]
    if missing:
        raise ValueError(
            f"phase '{phase.name}': entries name unknown device(s) {missing}")

    return set(claims)


def _soft_links(entry: Entry, after: tuple[str, ...], where: str,
                phase_steps: PhaseSteps, phase_after: PhaseAfter,
                entry_phase: EntryPhase) -> set[int]:
    """The phase-ordering edges this entry takes.

    A clause naming a phase this entry already follows says the blanket wait was
    not what was meant, so it replaces it.
    """
    shadowed = {_declaring_phase(c.name, where, entry_phase, phase_after)
                for c in entry.require} & set(after)

    soft: set[int] = set()
    for a in after:
        if a not in shadowed:
            soft |= _effective_steps(a, phase_steps, phase_after)

    return soft


def _select_targets(devices: DeviceIndex, entry: Entry,
                    claimed: set[DeviceRef]) -> list[Target]:
    """The devices one entry runs its ops on."""
    match entry.matcher:
        case "trait":
            assert entry.trait is not None
            return [(n.ref, n.trait, n.path)
                    for n in devices.claiming(entry.trait)
                    if n.ref not in claimed]
        case "device":
            assert entry.device is not None
            return [(entry.device, None, devices.path_of(entry.device))]
        case "all":
            return [(r, None, n.path)
                    for r, n in devices.refs.items()
                    if r not in claimed]
        case kind:      # "instrument" / "selector": a structural kind
            return [(n.ref, None, n.path)
                    for n in devices.of_kind(kind)
                    if n.ref not in claimed
                    and (entry.role is None or n.instrument_role == entry.role)]


def _emit_entry(g: GraphBuilder, devices: DeviceIndex,
                overrides: Sequence[Override], entry: Entry, group: str,
                soft: set[int], on_failure: OnFailure, targets: list[Target],
                ) -> tuple[list[tuple[DeviceRef, int]], list[int]]:
    """Add one entry's nodes: its ops serially per matched device.

    Returns the per-device head ids — the ones a `require` clause attaches to — and
    every id emitted, in order.
    """
    heads: list[tuple[DeviceRef, int]] = []
    ids: list[int] = []

    for ref, trait, path in targets:
        prev_id: int | None = None
        for spec in entry.ops:
            payload = Op(ref=ref, op=spec.op, trait=trait,
                         traits=devices.traits_of(ref),
                         path=path, match=entry.matcher)
            eff = resolve_effects(
                overrides, payload,
                on_failure=spec.on_failure or on_failure,
                optional=spec.optional)
            sid = g.add(
                label=f"{spec.op:<18} {payload.describe()}",
                group=group,
                payload=payload,
                soft=soft if prev_id is None else (),
                hard=() if prev_id is None else {prev_id},
                optional=eff.optional,
                on_failure=eff.on_failure,
                override=eff.override)
            if prev_id is None:
                heads.append((ref, sid))
            prev_id = sid
            ids.append(sid)

    return heads, ids


def _apply_join(g: GraphBuilder, clause: Require, dep_ids: list[int],
                ref: DeviceRef, head: int) -> list[int]:
    """Narrow a clause's steps to the ones that are this device's business."""
    match clause.join:
        case "same-device":
            return [i for i in dep_ids if g[i].payload.ref == ref]
        case "same-chain":
            here = g[head].payload.path
            return [i for i in dep_ids if on_chain(g[i].payload.path, here)]
        case _:
            return dep_ids


def _resolve_requires(g: GraphBuilder, where: str,
                      heads: list[tuple[Entry, DeviceRef, int]],
                      entry_steps: EntrySteps, phase_steps: PhaseSteps,
                      phase_after: PhaseAfter, entry_phase: EntryPhase) -> None:
    """Add the hard edges a phase's `require` clauses ask for.

    Resolved after the phase is built, so a clause may name a peer in it. Only the
    entry's head takes the edge — the serial ops behind it inherit through their
    own chain.
    """
    for entry, ref, head in heads:
        for clause in entry.require:
            target = clause.name
            dep_ids = (entry_steps.get(target, []) if target in entry_phase
                       else sorted(_effective_steps(target, phase_steps,
                                                    phase_after)))
            if not dep_ids:
                raise ValueError(
                    f"phase '{where}': require '{target}' matches "
                    f"no step on this sensor")

            dep_ids = _apply_join(g, clause, dep_ids, ref, head)
            # A clause that survives selection but not its own join is still
            # unsatisfiable, and adding nothing would be worse than a missing edge:
            # the clause has already displaced the soft link it named, so the entry
            # would be left with no dependency at all.
            if not dep_ids:
                raise ValueError(
                    f"phase '{where}': require '{target}' with "
                    f"join='{clause.join}' matches no step for {ref}")

            g.require(head, dep_ids)


class LifecycleError(Exception):
    def __init__(self, message: str, report: RunReport):
        super().__init__(message)
        self.report = report


class LifecycleRunner:
    """Lifecycle frontend to `dag.DagRunner`: hands `ops.Op` payloads to the op hook
    and applies the raise policy.

    `run()` = compile + execute; `execute` is public because the graph is the API's
    IR — callers may compile, inspect, verify, then execute the same graph.

    Holds nothing but the hook: per-run state lives in the `RunContext`, so one
    runner serves any number of sensors and concurrent runs.
    """

    def __init__(self, perform: OpHook):
        self.perform = perform

    async def run(self, devices: DeviceIndex, table: PhaseTable, *,
                  abort: AbortSignal | None = None,
                  overrides: Sequence[Override] = ()) -> RunReport:
        graph = compile_table(devices, table, overrides)
        return await self.execute(graph, name=table.name, abort=abort)

    async def execute(self, graph: Graph, *, name: str = "",
                      abort: AbortSignal | None = None) -> RunReport:
        run = RunContext(name=name, graph=graph, abort=abort)
        runner = DagRunner(
            lambda node: self.perform(OpContext(node.payload, node, run)))
        report = await runner.execute(graph, name=name, abort=abort)

        # An abort is an outcome, not a failure: it never raises. A required step
        # that failed — or that never ran — always does.
        if report.failures and not report.aborted:
            raise LifecycleError(f"{name}: {_describe(report)}", report)

        return report


def _describe(report: RunReport) -> str:
    """Name the causes, not the cascade: the skipped steps are what the failures
    did, and listing them buries the one line that matters."""
    causes = report.causes
    missing = len(report.failures) - sum(not n.optional for n, _ in causes)

    parts = [f"{n.label.strip()} ({e})" for n, e in causes[:3]]
    if len(causes) > 3:
        parts.append(f"and {len(causes) - 3} more")
    if missing:
        parts.append(f"{missing} required step(s) did not run")

    return "; ".join(parts)
