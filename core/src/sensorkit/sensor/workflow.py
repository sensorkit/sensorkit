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

import itertools
import math
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, model_validator

from sensorkit.common.dag import (
    Graph,
    GraphBuilder,
    NodeOverride,
    OnFailure,
)
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.selection import PlacementFacts, Selection
from sensorkit.sensor.topology import (
    DeviceKey,
    Placement,
    TraitKey,
    format_path,
)

type RequestId = str
"""Authored request label shared by all acquisitions expanded from it."""

type OperationId = str
"""Readable operation label derived from its origin and target device.

Used in logs and audits. Cleanup and execution track operations by object
identity; graph node identifiers belong to `sensorkit.common.dag`.
"""

type StepName = str
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
class Origin:
    """Authored source, path and explanatory reason used in diagnostics and
    labels.
    """

    source: str
    path: tuple[str | int, ...] = ()
    reason: str = ""

    def __str__(self) -> str:
        return "/".join(str(part) for part in (self.source, *self.path))


@dataclass(frozen=True)
class Acquisition:
    """One frame's request identity, numbering and planned keywords.

    `index` is the request ordinal, continuing across segments and repeated for
    each instrument during fan-out. `frame_number` is unique per instrument
    across the collect. The pair (instrument, frame_number) identifies a frame.
    Keywords travel with the acquisition through packing and compilation.
    """

    request: RequestId
    index: int
    frame_number: int
    keywords: KeywordDict


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


@dataclass(frozen=True)
class Dependency:
    """A named predecessor and the condition required before a step may run.

    `success` requires a successful result; `completion` waits for a terminal
    result regardless of outcome. A run-wide stop can still prevent dispatch.
    """

    on: StepName
    kind: Literal["success", "completion"] = "success"

    @classmethod
    def completion(cls, names: Iterable[StepName]) -> tuple[Dependency, ...]:
        """Build completion dependencies for the supplied step names."""
        return tuple(cls(on=name, kind="completion") for name in names)


@dataclass(frozen=True)
class PlannedStep:
    """A compiler's command or ordering step before graph construction.

    `name` is unique within the step list; dependencies may name later steps.
    `command` and `target` must be set together or both absent. With neither,
    the step becomes an ordering node; `delay_s` can provide an alignment
    delay.

    `timeout_s` overrides deadline rules. An unset value inherits those rules
    and may remain unbounded if none match. Resolved numeric deadlines must be
    finite and positive.
    """

    name: StepName
    origin: Origin
    group: str
    target: Placement | None = None
    command: DeviceCommand | None = None
    deps: tuple[Dependency, ...] = ()
    optional: bool = False
    fail_fast: bool = True
    unsupported: Literal["error", "omit"] = "error"
    timeout_s: float | None = None
    acquisition: Acquisition | None = None
    delay_s: float = 0.0

    @property
    def label(self) -> str:
        """Describe the command and target, or the origin of an ordering
        step.
        """
        if self.command is None or self.target is None:
            return self.origin.reason or self.name

        return (f"{format_command(self.command):<18} {self.target.device} "
                f"@ {format_path(self.target.path, '<root>')}")


@dataclass(frozen=True, eq=False)
class Operation:
    """A graph payload containing one command for one placement.

    Operations compare and hash by object identity, allowing execution and
    cleanup to track separate attempts even when their fields match.
    `timeout_s` is already resolved; `None` means unbounded. An acquisition
    marks a command whose header must be populated at dispatch.
    """

    id: OperationId
    target: Placement
    command: DeviceCommand
    origin: Origin
    timeout_s: float | None = None
    acquisition: Acquisition | None = None

    @classmethod
    def planned(cls, step: PlannedStep,
                timeout_s: float | None) -> Operation:
        """Build an operation with an origin-based label and a deep-copied
        command.

        Copying detaches the caller's command; it does not make exposed nested
        values immutable. Callers must not mutate compiled workflows.

        Raises:
            ValueError: The step has no command or target.
        """
        if step.command is None or step.target is None:
            raise ValueError(
                f"step '{step.name}' has no command, so there is no operation "
                f"to build")

        return cls(id=f"{step.origin}@{step.target.device}", target=step.target,
                   command=step.command.model_copy(deep=True),
                   origin=step.origin, timeout_s=timeout_s,
                   acquisition=step.acquisition)


@dataclass(frozen=True)
class Omission:
    """An unsupported step removed during lowering, with its origin and
    reason.
    """

    origin: Origin
    target: Placement
    command: str
    reason: str


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


def _named_target(target: DeadlineTarget) -> str:
    """Format a deadline target for a diagnostic message."""
    kind, name = target

    return kind if name is None else f"{kind} '{name}'"


def resolve_deadline(command: str, target: Placement,
                     stated: tuple[DeadlineRule, ...], explicit: float | None,
                     facts: PlacementFacts) -> float | None:
    """Resolve an operation timeout: explicit value, then rules naming the
    command, then rules for every command, each by device, trait, any.

    Return `None` if neither an explicit value nor a matching rule exists.
    Lowering validates the result and stores it on the operation.

    Raises:
        ValueError: Multiple rules match at the winning specificity.
    """
    # Resolve only device, trait and universal targets.
    if explicit is not None:
        return explicit

    named = tuple(r for r in stated if r.command == command)
    every = tuple(r for r in stated if r.command is None)
    traits = facts.traits(target)

    for rules, rung in itertools.product((named, every),
                                         ("device", "trait", "any")):
        if matched := _addressing(rules, rung, target, traits):
            return _the_rule(matched, command, target).seconds

    return None


def _addressing(rules: tuple[DeadlineRule, ...], rung: str, target: Placement,
                traits: frozenset[TraitKey]) -> tuple[DeadlineRule, ...]:
    """Return rules at one specificity that match the placement."""
    match rung:
        case "device":
            return tuple(r for r in rules
                         if r.target == ("device", target.device))
        case "trait":
            return tuple(r for r in rules if r.target[0] == "trait"
                         and r.target[1] in traits)

    return tuple(r for r in rules if r.target[0] == "any")


def _the_rule(matched: tuple[DeadlineRule, ...], command: str,
              target: Placement) -> DeadlineRule:
    """Require exactly one matching rule at the winning specificity.

    Raises:
        ValueError: Multiple rules match the placement.
    """
    if len(matched) == 1:
        return matched[0]

    # Multiple traits may match; a device rule can disambiguate their deadlines.
    named = ", ".join(sorted(_named_target(r.target) for r in matched))

    raise ValueError(
        f"'{command}' on '{target.device}' is given a deadline by {named}; "
        f"nothing ranks them, so name the device instead")


@dataclass(frozen=True)
class OperatorRule:
    """An explained override of selected operations' outcomes or failure
    policy.

    Set `select`, `commands`, or both; when both are set, both must match. An
    `outcome` replaces dispatch with a recorded result and cannot be combined
    with failure-policy changes. Capability omissions are recorded separately.
    """

    # Explain the operator decision in reports.
    reason: str

    select: Selection | None = None
    commands: tuple[str, ...] = ()

    outcome: Literal["ok", "skipped"] | None = None
    fail_fast: bool | None = None
    optional: bool | None = None

    def __post_init__(self) -> None:
        if self.select is None and not self.commands:
            raise ValueError(
                "an operator rule addresses nothing; set select or commands")

        if (self.outcome, self.fail_fast, self.optional) == (None, None, None):
            raise ValueError(
                "an operator rule changes nothing; set outcome, fail_fast or "
                "optional")

        if self.outcome is not None and not (self.fail_fast is None
                                             and self.optional is None):
            raise ValueError(
                "an operator rule sets an outcome and a failure policy; an "
                "operation that will not be dispatched cannot fail")

    def matches(self, step: PlannedStep, facts: PlacementFacts) -> bool:
        """Test the command and placement filters against a planned step.

        Ordering steps never match. Placement selection covers all commands on
        that placement unless the rule also limits command identifiers.
        """
        if step.command is None or step.target is None:
            return False

        if self.select is not None and not self.select.matches(step.target,
                                                               facts):
            return False

        return not self.commands or step.command.model_tag() in self.commands






@dataclass(frozen=True, eq=False)
class ExecutableWorkflow:
    """A compiled graph with cleanup graphs, omissions and capability
    provenance.

    Preparation is ordinary work in the main graph. Cleanup runs separately
    after it drains. Provenance describes the facts used to compile the
    workflow; it is not an identity or replay record.

    Workflows compare by object identity so sessions can track which they
    issued. Callers must not mutate their graphs, commands or nested metadata.
    """

    name: str
    graph: Graph
    provenance: str = ""
    cleanup: tuple[Cleanup, ...] = ()
    omissions: tuple[Omission, ...] = ()


def lower(name: str, steps: tuple[PlannedStep, ...], facts: PlacementFacts, *,
          provenance: str = "", cleanup=(), deadlines=(), rules=()) -> ExecutableWorkflow:
    if cleanup:
        raise NotImplementedError("Cleanup lowering is unavailable")
    graph, _, omissions = _compile(steps, facts, deadlines, rules)
    workflow = ExecutableWorkflow(name=name, graph=graph,
                                  provenance=provenance, omissions=omissions)
    _validated(workflow)
    return workflow


def _compile(steps: tuple[PlannedStep, ...], facts: PlacementFacts,
             deadlines: tuple[DeadlineRule, ...],
             rules: tuple[OperatorRule, ...]
             ) -> tuple[Graph, dict[StepName, Operation], tuple[Omission, ...]]:
    """Lower one step list into a graph, operation index and omission
    records.
    """
    by_name = _checked(steps)
    omitted, omissions = _fates(steps, facts)
    resolved = _resolved(by_name, omitted)

    builder = GraphBuilder()
    nodes: dict[StepName, int] = {}
    operations: dict[StepName, Operation] = {}

    for step in steps:
        if step.name in omitted:
            continue

        payload = None

        if step.command is not None and step.target is not None:
            payload = Operation.planned(
                step, resolve_deadline(step.command.model_tag(), step.target,
                                       deadlines, step.timeout_s, facts))
            operations[step.name] = payload

        on_failure, optional, override = _effects(step, rules, facts)
        nodes[step.name] = builder.add(
            step.label, step.group, payload, on_failure=on_failure,
            optional=optional, delay_s=step.delay_s, override=override)

    # Add edges after allocating all node ids to support forward references.
    for named, nid in nodes.items():
        builder.require(nid, (nodes[d.on] for d in resolved[named]
                              if d.kind == "success"))
        builder.order(nid, (nodes[d.on] for d in resolved[named]
                            if d.kind == "completion"))

    return builder.build(), operations, tuple(omissions)


def _checked(steps: tuple[PlannedStep, ...]) -> dict[StepName, PlannedStep]:
    """Index steps after checking names, command-target pairs and
    dependencies.

    Raises:
        ValueError: Names repeat, only one of command and target is set, or a
            dependency names an absent step.
    """
    by_name: dict[StepName, PlannedStep] = {}

    for step in steps:
        if step.name in by_name:
            raise ValueError(f"two steps are named '{step.name}'")

        if (step.command is None) != (step.target is None):
            missing = "target" if step.target is None else "command"
            raise ValueError(
                f"step '{step.name}' has no {missing}; a command and the "
                f"placement receiving it are set together or not at all")

        by_name[step.name] = step

    dangling = sorted({d.on for step in steps for d in step.deps}
                      - set(by_name))

    if dangling:
        raise ValueError(
            f"steps depend on names nothing emitted: {', '.join(dangling)}")

    return by_name


def _fates(steps: tuple[PlannedStep, ...], facts: PlacementFacts
           ) -> tuple[frozenset[StepName], list[Omission]]:
    """Find unsupported steps to omit and record their reasons.

    Ordering steps are always retained.

    Raises:
        ValueError: An unsupported command has `unsupported="error"`.
    """
    omitted: list[StepName] = []
    omissions: list[Omission] = []

    for step in steps:
        if step.command is None or step.target is None:
            continue

        named = step.command.model_tag()

        if named in facts.commands(step.target.device):
            continue

        reason = f"'{step.target.device}' does not support '{named}'"

        if step.unsupported == "error":
            raise ValueError(f"step '{step.name}': {reason}")

        omitted.append(step.name)
        omissions.append(Omission(origin=step.origin, target=step.target,
                                  command=named, reason=reason))

    return frozenset(omitted), omissions


def _resolved(by_name: dict[StepName, PlannedStep],
              omitted: frozenset[StepName]
              ) -> dict[StepName, tuple[Dependency, ...]]:
    """Resolve each step's dependencies through omitted predecessors."""
    resolved: dict[StepName, tuple[Dependency, ...]] = {}

    for name in by_name:
        _resolve(name, by_name, omitted, resolved, frozenset())

    return resolved


def _resolve(name: StepName, by_name: dict[StepName, PlannedStep],
             omitted: frozenset[StepName],
             resolved: dict[StepName, tuple[Dependency, ...]],
             seen: frozenset[StepName]) -> tuple[Dependency, ...]:
    """Replace omitted predecessors with their dependencies recursively.

    Inherited dependencies require success only if every link requires it. An
    omitted predecessor with no dependencies adds no constraint.

    Raises:
        ValueError: Dependency traversal encounters a cycle through omissions.
    """
    if name in resolved:
        return resolved[name]

    if name in seen:
        raise ValueError(
            f"omitted steps depend on each other, through '{name}'")

    here: list[Dependency] = []

    for dep in by_name[name].deps:
        if dep.on in omitted:
            here += [Dependency(on=inherited.on,
                                kind="success" if dep.kind == inherited.kind == "success"
                                else "completion")
                     for inherited in _resolve(dep.on, by_name, omitted,
                                               resolved, seen | {name})]
        else:
            here.append(dep)

    resolved[name] = _merged(here)

    return resolved[name]


def _merged(deps: list[Dependency]) -> tuple[Dependency, ...]:
    """Merge duplicate predecessors in first-seen order, retaining success
    requirements.
    """
    kinds: dict[StepName, Literal["success", "completion"]] = {}

    for dep in deps:
        if kinds.get(dep.on) != "success":
            kinds[dep.on] = dep.kind

    return tuple(Dependency(on=name, kind=kind)
                 for name, kind in kinds.items())


def _effects(step: PlannedStep, rules: tuple[OperatorRule, ...],
             facts: PlacementFacts
             ) -> tuple[OnFailure, bool, NodeOverride | None]:
    """Resolve node failure policy and the first matching operator rule.

    Fail-fast maps to `stop`; otherwise failures map to `skip`. Order operator
    rules by precedence, since only the first match applies.
    """
    on_failure: OnFailure = "stop" if step.fail_fast else "skip"
    rule = next((r for r in rules if r.matches(step, facts)), None)

    if rule is None:
        return on_failure, step.optional, None

    if rule.fail_fast is not None:
        on_failure = "stop" if rule.fail_fast else "skip"

    return (on_failure,
            step.optional if rule.optional is None else rule.optional,
            NodeOverride(rule.outcome, rule.reason)
            if rule.outcome is not None else None)




def _validated(workflow: ExecutableWorkflow) -> None:
    """Check operation labels, deadlines and cleanup trigger membership.

    Graph construction already checks cycles. Ordering nodes have no operation.

    Raises:
        ValueError: Operation labels repeat, a numeric deadline is invalid, or
            cleanup references an operation outside the main graph.
    """
    operations = tuple(_operations(workflow.graph))
    counted = Counter(op.id for op in operations)
    dupes = sorted(i for i, n in counted.items() if n > 1)

    if dupes:
        raise ValueError(f"operations share an id: {', '.join(dupes)}")

    _deadlines_within(operations, "the run")
    held = set(operations)

    for plan in workflow.cleanup:
        _bounded(plan)
        _armed_within(plan, held)


def _usable(seconds: float) -> bool:
    """Test whether a timeout is finite and positive."""
    return math.isfinite(seconds) and seconds > 0


def _deadlines_within(operations: Iterable[Operation], graph: str) -> None:
    """Validate numeric operation deadlines, allowing `None` for unbounded
    work.

    Raises:
        ValueError: A numeric deadline is not finite and positive.
    """
    for operation in operations:
        if operation.timeout_s is not None and not _usable(operation.timeout_s):
            raise ValueError(
                f"operation '{operation.id}' in {graph} resolved to a deadline "
                f"of {operation.timeout_s} seconds")






def _operations(graph: Graph) -> Iterator[Operation]:
    """Yield operation payloads, skipping ordering nodes."""
    for node in graph.nodes:
        if isinstance(node.payload, Operation):
            yield node.payload


def format_command(command: DeviceCommand) -> str:
    """Format a command name and its nondefault arguments for logs and
    audits.

    Serialize nested models as values and exclude the command discriminator.
    """
    args = ", ".join(
        f"{k}={v!r}" for k, v in
        command.model_dump(mode="json", exclude={"command_id"},
                           exclude_defaults=True).items())

    return f"{command.model_tag()}({args})" if args else command.model_tag()
