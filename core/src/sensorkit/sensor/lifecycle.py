# SPDX-License-Identifier: Apache-2.0
"""Author and compile lifecycle tables such as bring-up and shutdown.

Tables contain phases of entries. Each entry selects placements and runs an
ordered list of commands on each; entries in one phase must select disjoint
placements. Compilation emits planned steps for shared workflow lowering.

A phase normally follows the preceding phase with completion dependencies.
Explicit `after` lists change that order. `require` clauses can refine a wait
on a followed phase by selecting operations on the same device or chain and
requiring success or completion. Other inherited phase waits remain intact.

`sequence` controls each command's dependency on the preceding command at its
placement. `fail_fast` controls whether failure stops the whole run. Teardown
that should keep going needs completion sequencing and non-fail-fast policy.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, model_validator

from sensorkit.common.dag import GraphBuilder
from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.selection import AnySelection
from sensorkit.sensor.topology import Placement
from sensorkit.sensor.workflow import (
    CleanupPlan,
    DeadlineRule,
    Dependency,
    ExecutableWorkflow,
    OperatorRule,
    Origin,
    PlannedStep,
    StepName,
    lower,
)


def _accept_str_command(v: object) -> object:
    """Convert a bare command name to the tagged command model input."""
    return {"command_id": v} if isinstance(v, str) else v


class Join(BaseModel, frozen=True, extra="forbid"):
    """A dependency on a named phase or entry, optionally narrowed by
    placement.

    `on` requires success or completion. `join` selects all target operations,
    operations on the same device, or operations at the same path or above it
    (`same-chain`). A bare string uses `all` and `success`.
    """

    name: str
    join: Literal["all", "same-device", "same-chain"] = "all"
    on: Literal["success", "completion"] = "success"

    @model_validator(mode="before")
    @classmethod
    def _str_shorthand(cls, v: object) -> object:
        if isinstance(v, str):
            return {"name": v}

        if not isinstance(v, Mapping) or True not in v:
            return v

        # Restore `on` after YAML 1.1 parses the key as boolean True.
        return {("on" if key is True else key): value for key, value in v.items()}


class OpSpec(BaseModel, frozen=True, extra="forbid"):
    """A command and execution policy applied at each selected placement.

    A bare command name is shorthand for a command using default arguments.
    `sequence` requires success or completion of the preceding command at the
    same placement; it has no effect on the first command.

    `unsupported` chooses compilation error or omission. `optional` controls
    whether an unsuccessful result fails the run. `fail_fast` controls whether
    failure stops dispatch and inherits from the phase, then table, when unset.
    `timeout_s` overrides configured deadlines; `None` inherits them.
    """

    command: Annotated[DeviceCommand, BeforeValidator(_accept_str_command)]
    optional: bool = False
    unsupported: Literal["error", "omit"] = "error"
    fail_fast: bool | None = None
    sequence: Literal["success", "completion"] = "success"
    timeout_s: float | None = None

    @model_validator(mode="before")
    @classmethod
    def _shorthand(cls, v: object) -> object:
        return {"command": v} if isinstance(v, str | DeviceCommand) else v

    @property
    def op(self) -> str:
        """Return the command identifier used by reports and rules."""
        return self.command.model_tag()

    def effective_fail_fast(self, phase: bool | None, table: bool) -> bool:
        """Resolve failure policy from the operation, phase, then table."""
        if self.fail_fast is not None:
            return self.fail_fast

        return table if phase is None else phase


def _one_or_many(v: object) -> object:
    """Accept one operation wherever a sequence of operations is expected."""
    return [v] if isinstance(v, str | Mapping | OpSpec | DeviceCommand) else v


def _accept_bare_require(v: object) -> object:
    """Accept one dependency clause wherever a sequence is expected."""
    return [v] if isinstance(v, str | Mapping | Join) else v


class Entry(BaseModel, frozen=True, extra="forbid"):
    """A selection of placements and the commands to run on each.

    `exclude` removes placements from `select`. Entries within a phase or
    cleanup must not overlap. Commands are serial per placement; different
    placements may run concurrently.

    `require` adds dependencies or refines inherited waits on named phases.
    Each clause specifies its join and success or completion condition.
    """

    select: AnySelection
    ops: Annotated[tuple[OpSpec, ...], BeforeValidator(_one_or_many)]
    exclude: AnySelection | None = None
    id: str | None = None
    require: Annotated[
        tuple[Join, ...],
        BeforeValidator(
            _accept_bare_require, json_schema_input_type=Join | str | tuple[Join | str, ...]
        ),
    ] = ()

    @model_validator(mode="after")
    def _declares_ops(self) -> Entry:
        if not self.ops:
            raise ValueError("entry declares no ops")

        return self

    def describe(self) -> str:
        """Serialize the authored selection for diagnostic messages."""
        return self.select.model_dump_json(by_alias=True)

    def targets(self, sensor: BoundSensor) -> tuple[Placement, ...]:
        """Return selected placements after exclusions, in topology order.

        This query may return no placements; compilation rejects empty entries.
        """
        return tuple(
            placement
            for placement in self.select.matching(sensor.topology.placements(), sensor)
            if self.exclude is None or not self.exclude.matches(placement, sensor)
        )


class Phase(BaseModel, frozen=True, extra="forbid"):
    """A named group of entries with inherited completion dependencies.

    `after=None` follows the preceding phase; an empty tuple follows none.
    `after` names earlier phases. Entry requirements may narrow their inherited
    waits. An unset `fail_fast` inherits the table default.
    """

    name: str
    entries: tuple[Entry, ...]
    after: tuple[str, ...] | None = None
    fail_fast: bool | None = None


class CleanupSpec(BaseModel, frozen=True, extra="forbid"):
    """A separate cleanup graph selected after the main workflow drains.

    Entries are ordered only by their own `require` clauses, which may
    reference entries in this spec. Each spec has its own entry-id namespace.

    `armed_by` names main-workflow entries: any attempted operation from those
    entries arms cleanup. `None` is unconditional; an empty tuple never arms
    it. `when` independently selects the run outcome, with `cancelled` meaning
    a domain abort. Hard cancellation skips cleanup. `timeout_s` bounds the
    graph.

    Keep cleanup to stopping ongoing work; it cannot provide crash recovery.
    """

    name: str
    entries: tuple[Entry, ...]
    when: Literal["always", "failure", "cancelled", "failure_or_cancelled"] = "always"
    timeout_s: float = 60.0
    armed_by: tuple[str, ...] | None = None


class LifecycleWorkflow(BaseModel, frozen=True, extra="forbid"):
    """A named lifecycle table with phases, cleanup and a failure-policy
    default.

    `fail_fast` is required: true stops the run on failure, while false allows
    other work to continue subject to dependencies. Operations can override it.
    Definition mappings supply `name` from the table's mapping key.
    """

    name: str
    phases: tuple[Phase, ...]
    fail_fast: bool
    cleanup: tuple[CleanupSpec, ...] = ()

    def follows(self, index: int) -> tuple[str, ...]:
        """Return explicit predecessors, or the preceding phase when `after`
        is unset.
        """
        after = self.phases[index].after

        if after is not None:
            return after

        return (self.phases[index - 1].name,) if index else ()

    def check(self) -> None:
        """Check names, references and symbolic cycles without device facts.

        Compilation additionally checks declaration order and selected
        operations.

        Raises:
            ValueError: Names repeat, references are invalid, or dependencies
                cycle. The error includes the table name.
        """
        try:
            self._check()
        except ValueError as e:
            raise ValueError(f"table '{self.name}': {e}") from e

    def _check(self) -> None:
        """Validate phase, entry and cleanup namespaces and dependencies.

        Raises:
            ValueError: Names collide, references are invalid, or dependencies
                cycle.
        """
        phases = [phase.name for phase in self.phases]
        rows = [entry for phase in self.phases for entry in phase.entries]
        ids = [entry.id for entry in rows if entry.id is not None]

        _unique(phases, "a phase is named twice")
        _unique([spec.name for spec in self.cleanup], "a cleanup is named twice")
        _unique(ids, "an entry id is used twice")
        _disjoint(phases, ids)
        _acyclic(self._dependencies(set(phases) | set(ids), set(ids)))

        for spec in self.cleanup:
            _check_cleanup(spec, set(ids))

    def _dependencies(
        self, known: set[str], ids: set[str]
    ) -> dict[tuple[str, str], set[tuple[str, str]]]:
        """Build symbolic phase and entry dependencies, validating
        references.

        A phase depends on its entries so cycles through an entry include its
        phase.
        """
        phases = {phase.name for phase in self.phases}
        deps: dict[tuple[str, str], set[tuple[str, str]]] = {}

        for index, phase in enumerate(self.phases):
            follows = self.follows(index)
            _resolves(follows, phases, f"phase '{phase.name}' after")
            node = ("phase", phase.name)
            deps[node] = set()

            for position, entry in enumerate(phase.entries):
                row = ("entry", entry.id or f"{phase.name}[{position}]")
                deps[node].add(row)
                deps[row] = {("phase", name) for name in follows} | _required(entry, known, ids)

        return deps


def compile_lifecycle(
    workflow: LifecycleWorkflow,
    sensor: BoundSensor,
    *,
    deadlines: tuple[DeadlineRule, ...] = (),
    rules: tuple[OperatorRule, ...] = (),
) -> ExecutableWorkflow:
    """Compile a lifecycle table against a bound sensor without device
    calls.

    The result can be inspected before running.

    Raises:
        ValueError: References or declaration order are invalid, entries
            overlap or select nothing, joins match no operations, a required
            command is unsupported, or shared lowering rejects the workflow.
    """
    return TableCompiler(workflow, sensor, deadlines=deadlines, rules=rules).run()


class TableCompiler:
    """Single-use state for expanding a table into planned steps.

    Phases may reference earlier phases; entries may also reference peers in
    their own phase. Dependencies are accumulated separately, then attached to
    frozen steps before shared lowering.
    """

    def __init__(
        self,
        table: LifecycleWorkflow,
        sensor: BoundSensor,
        *,
        deadlines: tuple[DeadlineRule, ...] = (),
        rules: tuple[OperatorRule, ...] = (),
    ):
        self.table = table
        self.sensor = sensor
        self.deadlines = deadlines
        self.rules = rules
        self.steps: list[PlannedStep] = []
        self.deps: dict[StepName, list[Dependency]] = {}
        self.where: dict[StepName, Placement] = {}
        self.phase_steps: dict[str, list[StepName]] = {}
        self.phase_after: dict[str, tuple[str, ...]] = {}
        self.entry_steps: dict[str, list[StepName]] = {}
        self.entry_phase: dict[str, str] = {}
        self.emitted = 0

    def run(self) -> ExecutableWorkflow:
        """Expand phases and cleanup specs, then lower their planned steps."""
        previous: str | None = None

        for phase in self.table.phases:
            self._compile_phase(phase, previous)
            previous = phase.name

        # Compile cleanup after all main-workflow trigger entries are indexed.
        plans = tuple(self._cleanup(spec) for spec in self.table.cleanup)

        return lower(
            self.table.name,
            self._settled(self.steps),
            self.sensor,
            cleanup=plans,
            deadlines=self.deadlines,
            rules=self.rules,
        )

    def _compile_phase(self, phase: Phase, previous: str | None) -> None:
        """Emit a phase and attach inherited and explicit dependencies."""
        where = f"phase '{phase.name}'"
        after = self._declare_phase(phase, previous)
        chosen = self._selected(phase.entries, where)
        heads: list[tuple[Entry, Placement, StepName]] = []

        for position, entry in enumerate(phase.entries):
            emitted, entry_heads = self._emit_entry(
                entry,
                chosen[position],
                self._soft_links(entry, after, where),
                group=phase.name,
                origin=(phase.name, entry.id or position),
                stated=phase.fail_fast,
            )
            self.steps += emitted
            heads += [(entry, placement, head) for placement, head in entry_heads]
            names = [step.name for step in emitted]
            self.phase_steps[phase.name] += names

            if entry.id is not None:
                self.entry_steps.setdefault(entry.id, []).extend(names)

        self._resolve_requires(heads, where)

    def _declare_phase(self, phase: Phase, previous: str | None) -> tuple[str, ...]:
        """Register a phase and its entry ids before emitting operations.

        Registering peer ids first allows requirements to reference later
        entries in the same phase.

        Raises:
            ValueError: Names repeat or `after` references an undeclared phase.
        """
        if phase.name in self.phase_steps:
            raise ValueError(f"duplicate phase name '{phase.name}'")

        after = phase.after if phase.after is not None else (previous,) if previous else ()
        unknown = [name for name in after if name not in self.phase_steps]

        if unknown:
            raise ValueError(
                f"phase '{phase.name}': after names unknown or later phase(s) "
                f"{unknown}; phases may only follow earlier ones"
            )

        self.phase_after[phase.name] = after
        self.phase_steps[phase.name] = []

        for entry in phase.entries:
            if entry.id is None:
                continue

            if entry.id in self.entry_phase:
                raise ValueError(f"duplicate entry id '{entry.id}'")

            self.entry_phase[entry.id] = phase.name

        return after

    def _selected(self, entries: tuple[Entry, ...], where: str) -> list[tuple[Placement, ...]]:
        """Select targets for a group and check that entries do not overlap.

        Check selection before unsupported-command omission.

        Raises:
            ValueError: An entry selects nothing or entries share a placement.
        """
        chosen = [self._targets(entry, where) for entry in entries]
        reached: dict[Placement, Entry] = {}

        for entry, targets in zip(entries, chosen, strict=True):
            for placement in targets:
                held = reached.setdefault(placement, entry)

                if held is not entry:
                    raise ValueError(
                        f"{where}: '{placement.device}' is reached by two "
                        f"entries, {held.describe()} and {entry.describe()}; "
                        f"narrow one with exclude"
                    )

        return chosen

    def _targets(self, entry: Entry, where: str) -> tuple[Placement, ...]:
        """Return an entry's targets in topology order.

        Raises:
            ValueError: The entry selects no placement on this sensor.
        """
        targets = entry.targets(self.sensor)

        if not targets:
            raise ValueError(
                f"{where}: entry selects no device on this sensor ({entry.describe()})"
            )

        return targets

    def _soft_links(self, entry: Entry, after: tuple[str, ...], where: str) -> list[StepName]:
        """Collect inherited phase waits that explicit requirements do not
        replace.

        A requirement on a followed phase, or one of its entries, replaces that
        phase's blanket completion wait for this entry.

        Raises:
            ValueError: A requirement names an unknown or later phase or entry.
        """
        required = {self._declaring_phase(clause.name, where) for clause in entry.require}
        shadowed = required & set(after)

        return list(
            dict.fromkeys(
                name
                for followed in after
                if followed not in shadowed
                for name in self._effective_steps(followed)
            )
        )

    def _declaring_phase(self, target: str, where: str) -> str:
        """Return a target phase or the phase declaring a target entry.

        Raises:
            ValueError: The target has not been declared.
        """
        if target in self.entry_phase:
            return self.entry_phase[target]

        if target in self.phase_after:
            return target

        raise ValueError(f"{where}: require names unknown or later phase/entry '{target}'")

    def _effective_steps(self, phase: str) -> list[StepName]:
        """Return a phase's steps, or recursively its predecessors when
        empty.
        """
        names = self.phase_steps[phase]

        if names:
            return names

        return list(
            dict.fromkeys(
                name
                for followed in self.phase_after[phase]
                for name in self._effective_steps(followed)
            )
        )

    def _emit_entry(
        self,
        entry: Entry,
        targets: tuple[Placement, ...],
        soft: list[StepName],
        *,
        group: str,
        origin: tuple[str | int, ...],
        stated: bool | None,
    ) -> tuple[list[PlannedStep], list[tuple[Placement, StepName]]]:
        """Emit serial commands per placement and return each placement's
        first step.

        Entry requirements attach to the first step; subsequent steps inherit
        the wait through their sequence dependencies.
        """
        steps: list[PlannedStep] = []
        heads: list[tuple[Placement, StepName]] = []

        for placement in targets:
            previous: StepName | None = None

            for index, spec in enumerate(entry.ops):
                step = self._step(
                    spec, placement, group=group, origin=origin + (index,), stated=stated
                )
                self.deps[step.name] = (
                    list(Dependency.completion(soft))
                    if previous is None
                    else [Dependency(on=previous, kind=spec.sequence)]
                )

                if previous is None:
                    heads.append((placement, step.name))

                previous = step.name
                steps.append(step)

        return steps, heads

    def _step(
        self,
        spec: OpSpec,
        placement: Placement,
        *,
        group: str,
        origin: tuple[str | int, ...],
        stated: bool | None,
    ) -> PlannedStep:
        """Build a planned command with a unique internal name and resolved
        failure policy.
        """
        self.emitted += 1
        step = PlannedStep(
            name=f"{group}/{placement.device}/{self.emitted}",
            origin=Origin(source=self.table.name, path=origin),
            group=group,
            target=placement,
            command=spec.command,
            optional=spec.optional,
            fail_fast=spec.effective_fail_fast(stated, self.table.fail_fast),
            unsupported=spec.unsupported,
            timeout_s=spec.timeout_s,
        )
        self.where[step.name] = placement

        return step

    def _resolve_requires(
        self, heads: list[tuple[Entry, Placement, StepName]], where: str
    ) -> None:
        """Resolve entry requirements after emitting the whole phase,
        including peers.
        """
        for entry, placement, head in heads:
            for clause in entry.require:
                named = (
                    self.entry_steps.get(clause.name, [])
                    if clause.name in self.entry_phase
                    else self._effective_steps(clause.name)
                )
                self._join(clause, named, placement, head, where)

    def _join(
        self, clause: Join, named: list[StepName], placement: Placement, head: StepName, where: str
    ) -> None:
        """Filter a requirement's target steps and attach dependencies to
        the entry head.

        Raises:
            ValueError: The target has no steps or the join selects none.
        """
        if not named:
            raise ValueError(f"{where}: require '{clause.name}' matches no step on this sensor")

        narrowed = self._narrowed(clause, named, placement)

        # An empty join would remove the inherited phase wait without replacing it.
        if not narrowed:
            raise ValueError(
                f"{where}: require '{clause.name}' with join='{clause.join}' "
                f"matches no step for '{placement.device}'"
            )

        self.deps[head] += [Dependency(on=name, kind=clause.on) for name in narrowed]

    def _narrowed(
        self, clause: Join, named: list[StepName], placement: Placement
    ) -> list[StepName]:
        """Filter target steps by the clause's device or path relationship."""
        match clause.join:
            case "same-device":
                return [name for name in named if self.where[name].device == placement.device]
            case "same-chain":
                return [
                    name
                    for name in named
                    if placement.path[: len(self.where[name].path)] == self.where[name].path
                ]

        return named

    def _cleanup(self, spec: CleanupSpec) -> CleanupPlan:
        """Compile a cleanup spec with internal dependencies and main-graph
        triggers.

        No phase-order waits apply inside cleanup.

        Raises:
            ValueError: Entry ids or references are invalid, entries overlap or
                select nothing, or a trigger names no main-workflow entry.
        """
        where = f"cleanup '{spec.name}'"
        declared = self._declare_cleanup(spec, where)
        chosen = self._selected(spec.entries, where)
        steps: list[PlannedStep] = []
        heads: list[tuple[Entry, Placement, StepName]] = []

        for position, entry in enumerate(spec.entries):
            emitted, entry_heads = self._emit_entry(
                entry,
                chosen[position],
                [],
                group=spec.name,
                origin=("cleanup", spec.name, entry.id or position),
                stated=None,
            )
            steps += emitted
            heads += [(entry, placement, head) for placement, head in entry_heads]

            if entry.id is not None:
                declared[entry.id] += [step.name for step in emitted]

        for entry, placement, head in heads:
            for clause in entry.require:
                self._join(clause, declared[clause.name], placement, head, where)

        return CleanupPlan(
            steps=self._settled(steps),
            origin=Origin(source=spec.name),
            when=spec.when,
            timeout_s=spec.timeout_s,
            armed_by=self._arming(spec, where),
        )

    def _declare_cleanup(self, spec: CleanupSpec, where: str) -> dict[str, list[StepName]]:
        """Register entry ids and validate requirements within one cleanup
        spec.

        Raises:
            ValueError: Ids repeat or requirements reference entries outside
                the spec.
        """
        declared: dict[str, list[StepName]] = {}

        for entry in spec.entries:
            if entry.id is None:
                continue

            if entry.id in declared:
                raise ValueError(f"{where}: duplicate entry id '{entry.id}'")

            declared[entry.id] = []

        outside = sorted(
            {clause.name for entry in spec.entries for clause in entry.require} - set(declared)
        )

        if outside:
            raise ValueError(
                f"{where}: require names entries outside it, "
                f"{', '.join(outside)}; a cleanup is ordered against itself "
                f"alone"
            )

        return declared

    def _arming(self, spec: CleanupSpec, where: str) -> tuple[StepName, ...] | None:
        """Expand main-workflow entry ids into cleanup trigger step names.

        Preserve `None` as unconditional and an empty tuple as never armed.

        Raises:
            ValueError: A trigger names an entry absent from the main workflow.
        """
        if spec.armed_by is None:
            return None

        unknown = sorted(set(spec.armed_by) - set(self.entry_phase))

        if unknown:
            raise ValueError(
                f"{where}: armed_by names entries no phase declares, {', '.join(unknown)}"
            )

        return tuple(name for entry in spec.armed_by for name in self.entry_steps[entry])

    def _settled(self, steps: list[PlannedStep]) -> tuple[PlannedStep, ...]:
        """Copy emitted steps with their accumulated dependencies attached."""
        return tuple(replace(step, deps=tuple(self.deps[step.name])) for step in steps)


def _unique(names: list[str], what: str) -> None:
    """Reject repeated names in a namespace.

    Raises:
        ValueError: A name occurs more than once.
    """
    dupes = sorted({n for n in names if names.count(n) > 1})

    if dupes:
        raise ValueError(f"{what}: {', '.join(dupes)}")


def _disjoint(phases: list[str], ids: list[str]) -> None:
    """Require phase names and entry ids to occupy separate namespaces.

    Raises:
        ValueError: A phase and an entry share a name.
    """
    shared = sorted(set(phases) & set(ids))

    if shared:
        raise ValueError(f"a phase and an entry share a name: {', '.join(shared)}")


def _resolves(names: Iterable[str], known: set[str], what: str) -> None:
    """Check that each referenced name is declared.

    Raises:
        ValueError: A reference names no declaration.
    """
    unknown = sorted(n for n in names if n not in known)

    if unknown:
        raise ValueError(f"{what} names nothing: {', '.join(unknown)}")


def _required(entry: Entry, known: set[str], ids: set[str]) -> set[tuple[str, str]]:
    """Resolve requirement names to symbolic phase or entry nodes."""
    named = [clause.name for clause in entry.require]
    _resolves(named, known, "require")

    return {("entry", name) if name in ids else ("phase", name) for name in named}


def _check_cleanup(spec: CleanupSpec, armable: set[str]) -> None:
    """Validate a cleanup's local references and main-workflow trigger ids.

    Requirements stay inside the spec; only arming references main entries.

    Raises:
        ValueError: Ids repeat, references are invalid, or dependencies cycle.
    """
    ids = [entry.id for entry in spec.entries if entry.id is not None]
    named = {clause.name for entry in spec.entries for clause in entry.require}

    _unique(ids, f"cleanup '{spec.name}' uses an entry id twice")
    _resolves(spec.armed_by or (), armable, f"cleanup '{spec.name}' armed_by")

    outside = sorted(named - set(ids))

    if outside:
        raise ValueError(
            f"cleanup '{spec.name}' require names entries outside it: {', '.join(outside)}"
        )

    deps: dict[tuple[str, str], set[tuple[str, str]]] = {}

    for position, entry in enumerate(spec.entries):
        row = ("entry", entry.id or f"{spec.name}[{position}]")
        deps[row] = {("entry", clause.name) for clause in entry.require}

    _acyclic(deps)


def _acyclic(deps: dict[tuple[str, str], set[tuple[str, str]]]) -> None:
    """Check symbolic dependencies with the DAG builder, without device
    facts.

    Raises:
        ValueError: Dependencies contain a cycle.
    """
    builder = GraphBuilder()
    ids = {
        (kind, name): builder.add(f"{kind} '{name}'", kind, None) for kind, name in sorted(deps)
    }

    for node, following in deps.items():
        builder.order(ids[node], (ids[f] for f in following))

    builder.build()
