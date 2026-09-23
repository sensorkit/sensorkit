# SPDX-License-Identifier: Apache-2.0
"""Explain sensor definitions and compiled workflows without contacting
hardware.

Definition audits check structure and references, rendering selections
symbolically and deferring capability-dependent answers to binding. Workflow
audits describe stored nodes, edges, deadlines, overrides, omissions and
cleanup conditions. Provenance describes compilation facts, not current state.

Audits read their inputs without modifying them. Compiled workflows are
rendered as supplied rather than revalidated.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.lifecycle import (
    CleanupSpec,
    Entry,
    Join,
    LifecycleWorkflow,
    OpSpec,
    Phase,
)
from sensorkit.sensor.selection import (
    AllOf,
    AnyOf,
    HasTag,
    HasTrait,
    IsInstrument,
    IsKind,
    IsRef,
    Not,
    Publishes,
    Selection,
    Supports,
)
from sensorkit.sensor.topology import Component, Device, Selector, Structure, Topology, Unit
from sensorkit.sensor.workflow import DeadlineRule, DeadlineTarget, Origin, format_command


class When(StrEnum):
    """Human-readable descriptions indexed by cleanup eligibility."""

    always = "however the run ended"
    failure = "after a required failure"
    cancelled = "after a domain abort"
    failure_or_cancelled = ("after a required failure, a domain abort or "
                            "both, and once where both hold")


@dataclass(frozen=True)
class Finding:
    """A validation result or a question deferred until device facts are
    available.

    `deferred` indicates an unanswered question, not a pass or failure.
    """

    status: Literal["valid", "invalid", "deferred"]
    message: str
    origin: Origin


@dataclass(frozen=True)
class AuditReport:
    """Structured findings and a deterministic, human-readable description."""

    findings: tuple[Finding, ...]
    description: str


def audit_definition(definition: SensorDefinition) -> AuditReport:
    """Check an existing definition and describe its authored structure and
    tables.

    Report structural and reference checks as valid or invalid. Defer target
    selection, trait assertions, command support and trait deadlines to
    binding. Render predicates symbolically, including negation, without
    guessing targets.

    Parsing errors belong to the loader; this audit can report structural
    errors in Python-built definitions and checks individual parts separately.
    """
    topology, structure = _structure_checked(definition.sensor)
    checked = (structure, *(_table_checked(t) for t in definition.tables),
               *_deadlines_checked(definition, topology))
    findings = (
        *checked,
        *_document_checked(definition, checked),
        *_asserted(topology),
        *(finding for table in definition.tables
          for finding in _entries_deferred(table)),
        *_deadlines_deferred(definition.deadlines),
    )

    lines = [
        f"definition of sensor '{definition.sensor.name}', without device facts",
        "  Which placements a selection reaches and what each device supports "
        "are answered at binding, so selections are shown as authored.",
        "",
        f"structure '{definition.sensor.name}'",
        *_structure(definition.sensor.components, 1),
        "",
        *_deadlines(definition.deadlines),
    ]

    for table in definition.tables:
        lines += ["", *_table(table)]

    return AuditReport(findings=findings, description="\n".join(lines))




# TODO: Support concrete offline previews from saved capability snapshots,
# showing their source and age.


def _structure_checked(structure: Structure
                       ) -> tuple[Topology | None, Finding]:
    """Try building topology and return it with a structural validation
    finding.
    """
    origin = Origin(source="structure")

    try:
        topology = Topology(structure)
    except ValueError as e:
        return None, Finding("invalid", str(e), origin)

    return topology, Finding(
        "valid", f"structure '{structure.name}' places every device once and "
        f"names every position once", origin)


def _table_checked(table: LifecycleWorkflow) -> Finding:
    """Report a table's symbolic name, reference and cycle checks."""
    origin = Origin(source=table.name)

    try:
        table.check()
    except ValueError as e:
        return Finding("invalid", str(e), origin)

    return Finding(
        "valid", f"table '{table.name}' names resolve and nothing it orders "
        f"is circular", origin)


def _deadlines_checked(definition: SensorDefinition,
                       topology: Topology | None) -> list[Finding]:
    """Report device-target membership when a valid topology is available.

    Invalid structure is reported elsewhere; non-device rules need no
    membership check.
    """
    if topology is None or not any(r.target[0] == "device"
                                   for r in definition.deadlines):
        return []

    origin = Origin(source="deadlines")

    try:
        definition.check_deadlines(topology)
    except ValueError as e:
        return [Finding("invalid", str(e), origin)]

    return [Finding("valid", "every device a deadline rule targets is in the "
                    "structure", origin)]


def _document_checked(definition: SensorDefinition,
                      found: Iterable[Finding]) -> list[Finding]:
    """Report the whole-definition error unless a part already reported the
    same message.
    """
    try:
        definition.check()
    except ValueError as e:
        if str(e) not in {f.message for f in found}:
            return [Finding("invalid", str(e), Origin(source="definition"))]

    return []


def _asserted(topology: Topology | None) -> list[Finding]:
    """Defer authored trait assertions for placements in a valid topology."""
    if topology is None:
        return []

    findings: list[Finding] = []

    for placement in topology.placements():
        traits = topology.record(placement).traits

        if traits:
            findings.append(Finding(
                "deferred", f"'{placement.device}' asserts {', '.join(traits)}; "
                f"whether it satisfies them is answered at binding",
                Origin(source="structure", path=(placement.device,))))

    return findings


def _rows(table: LifecycleWorkflow
          ) -> Iterator[tuple[tuple[str | int, ...], Entry]]:
    """Yield phase and cleanup entries with their authored origin paths."""
    for phase in table.phases:
        for position, entry in enumerate(phase.entries):
            yield (phase.name, entry.id or position), entry

    for spec in table.cleanup:
        for position, entry in enumerate(spec.entries):
            yield ("cleanup", spec.name, entry.id or position), entry


def _entries_deferred(table: LifecycleWorkflow) -> list[Finding]:
    """Record deferred target selection and command-support checks for each
    entry.
    """
    return [
        Finding("deferred",
                f"which placements match {_targets(entry)}, and whether each "
                f"supports {', '.join(spec.op for spec in entry.ops)}, is "
                f"answered at binding",
                Origin(source=table.name, path=path))
        for path, entry in _rows(table)]


def _deadlines_deferred(rules: tuple[DeadlineRule, ...]) -> list[Finding]:
    """Record deferred trait matching for each trait-specific deadline rule."""
    return [
        Finding("deferred",
                f"which placements satisfy trait {rule.target[1]}, and so take "
                f"this {_commanded(rule)} deadline, is answered at binding",
                Origin(source="deadlines", path=(index,)))
        for index, rule in enumerate(rules) if rule.target[0] == "trait"]


def _symbolic(selection: Selection) -> str:
    """Format an authored selection without evaluating it."""
    match selection:
        case HasTrait():
            return f"trait {selection.trait}"
        case HasTag():
            return f"tag {selection.tag}"
        case IsRef():
            return f"device {selection.device}"
        case IsKind():
            return f"kind {selection.kind}"
        case IsInstrument():
            return f"instrument {str(selection.instrument).lower()}"
        case Supports():
            return f"supports {selection.supports}"
        case Publishes():
            return f"publishes {selection.publishes}"
        case AllOf():
            return f"all of ({', '.join(map(_symbolic, selection.all_of))})"
        case AnyOf():
            return f"any of ({', '.join(map(_symbolic, selection.any_of))})"
        case Not():
            return f"not ({_symbolic(selection.negated)})"

    return selection.model_dump_json(by_alias=True)


def _targets(entry: Entry) -> str:
    """Describe an entry's selection and exclusion symbolically."""
    if entry.exclude is None:
        return _symbolic(entry.select)

    return f"{_symbolic(entry.select)} excluding {_symbolic(entry.exclude)}"


def _structure(components: tuple[Component, ...], depth: int) -> Iterator[str]:
    """Render components as an indented tree in authored order."""
    indent = "  " * depth

    for node in components:
        match node:
            case Device():
                yield indent + _record(node.device, node.traits, node.tags,
                                       node.instrument)
            case Unit():
                yield f"{indent}unit {node.unit}"
                yield from _structure(node.components, depth + 1)
            case Selector():
                yield f"{indent}selector " + _record(node.selector, node.traits,
                                                     node.tags, False)

                for port in node.ports:
                    yield f"{indent}  port {port.name}"
                    yield from _structure(port.components, depth + 2)


def _record(key: str, traits: tuple[str, ...], tags: tuple[str, ...],
            instrument: bool) -> str:
    """Describe a device key, instrument status, trait assertions and tags."""
    parts = [key]

    if instrument:
        parts.append("instrument")

    if traits:
        parts.append(f"asserting {', '.join(traits)}")

    if tags:
        parts.append(f"tagged {', '.join(tags)}")

    return ", ".join(parts)


def _seconds(value: float) -> str:
    return f"{value:g} s"


def _addressed(target: DeadlineTarget) -> str:
    """Format a deadline target for the audit description."""
    kind, name = target

    return "any placement" if name is None else f"{kind} {name}"


def _deadlines(rules: tuple[DeadlineRule, ...]) -> Iterator[str]:
    """Describe authored deadline rules in their supplied order."""
    if not rules:
        yield "no deadline rules"
        return

    yield ("deadline rules, where an operation states no timeout, device "
           "first, then trait, then any, and rules for every command last")

    for rule in rules:
        yield (f"  {_commanded(rule)} on {_addressed(rule.target)}, "
               f"{_seconds(rule.seconds)}")


def _commanded(rule: DeadlineRule) -> str:
    """Name the command a deadline rule limits."""
    return "every command" if rule.command is None else rule.command


def _policy(fail_fast: bool) -> str:
    return "fail-fast" if fail_fast else "not fail-fast"


def _table(table: LifecycleWorkflow) -> Iterator[str]:
    """Describe a table's phases, entries and cleanup specs."""
    declared = {entry.id: phase.name for phase in table.phases
                for entry in phase.entries if entry.id is not None}

    yield (f"table '{table.name}', {_policy(table.fail_fast)} unless a phase "
           f"or an operation says otherwise")

    for index, phase in enumerate(table.phases):
        follows = table.follows(index)
        yield f"  phase '{phase.name}', {_ordered(phase, follows)}"

        for position, entry in enumerate(phase.entries):
            yield from _entry(entry, position, follows, declared,
                              phase.fail_fast, table.fail_fast)

    for spec in table.cleanup:
        yield from _cleanup_spec(spec, table.fail_fast)


def _ordered(phase: Phase, follows: tuple[str, ...]) -> str:
    """Describe phase ordering and any explicit failure-policy override."""
    if not follows:
        text = "after nothing"
    elif phase.after is None:
        text = f"after the completion of '{follows[0]}', which precedes it"
    else:
        text = f"after the completion of {_quoted(follows)}"

    if phase.fail_fast is not None:
        text += f", {_policy(phase.fail_fast)} unless an operation says otherwise"

    return text


def _quoted(names: Iterable[str]) -> str:
    return ", ".join(f"'{name}'" for name in names)


def _entry(entry: Entry, position: int, follows: tuple[str, ...],
           declared: dict[str, str], phase: bool | None,
           table: bool) -> Iterator[str]:
    """Describe an entry's authored selection, requirements and commands."""
    yield f"    entry '{entry.id}'" if entry.id else f"    entry {position}"
    yield f"      select {_symbolic(entry.select)}"

    if entry.exclude is not None:
        yield f"      exclude {_symbolic(entry.exclude)}"

    yield "      targets answered at binding"

    for clause in entry.require:
        yield f"      {_clause(clause, follows, declared)}"

    yield "      operations, serial at each target"

    for index, spec in enumerate(entry.ops):
        yield f"        {_op(spec, index, phase, table)}"


def _clause(clause: Join, follows: tuple[str, ...],
            declared: dict[str, str]) -> str:
    """Describe a requirement and any inherited phase wait it replaces."""
    text = f"requires '{clause.name}' on {clause.on}"

    if clause.join != "all":
        text += f", joining {clause.join}"

    phase = declared.get(clause.name, clause.name)

    if phase in follows:
        text += f", in place of the wait on all of '{phase}'"

    return text


def _op(spec: OpSpec, index: int, phase: bool | None, table: bool) -> str:
    """Describe an authored command with sequencing, failure policy and
    timeout source.
    """
    parts = [format_command(spec.command),
             "first at each target" if index == 0
             else f"after the {spec.sequence} of the previous",
             _effective(spec.fail_fast, phase, table)]

    if spec.optional:
        parts.append("optional")

    parts.append("omitted where unsupported" if spec.unsupported == "omit"
                 else "an error where unsupported")
    parts.append("deadline from the rules" if spec.timeout_s is None
                 else f"deadline {_seconds(spec.timeout_s)}, explicit")

    return "; ".join(parts)


def _effective(operation: bool | None, phase: bool | None, table: bool) -> str:
    """Describe failure policy resolved from operation, phase, then table."""
    match operation, phase:
        case bool(), _:
            return f"{_policy(operation)}, as the operation states"
        case None, bool():
            return f"{_policy(phase)}, from the phase"

    return f"{_policy(table)}, from the table"


def _cleanup_spec(spec: CleanupSpec, table: bool) -> Iterator[str]:
    """Describe a cleanup spec's eligibility, arming, timeout and entries."""
    declared = {entry.id: spec.name for entry in spec.entries
                if entry.id is not None}

    yield f"  cleanup '{spec.name}', runs {When[spec.when]}"
    yield f"    total deadline {_seconds(spec.timeout_s)}"
    yield f"    {_armed_by_entries(spec.armed_by)}"

    for position, entry in enumerate(spec.entries):
        yield from _entry(entry, position, (), declared, None, table)


def _armed_by_entries(armed_by: tuple[str, ...] | None) -> str:
    match armed_by:
        case None:
            return "armed unconditionally"
        case ():
            return "never armed, so it never runs"

    return f"armed once any operation of {_quoted(armed_by)} is attempted"
