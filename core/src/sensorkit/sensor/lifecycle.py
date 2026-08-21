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

from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, model_validator

from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.selection import AnySelection
from sensorkit.sensor.topology import Placement


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
        return {("on" if key is True else key): value
                for key, value in v.items()}


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
        BeforeValidator(_accept_bare_require,
                        json_schema_input_type=Join | str | tuple[Join | str, ...]),
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
            for placement in self.select.matching(sensor.topology.placements(),
                                                  sensor)
            if self.exclude is None
            or not self.exclude.matches(placement, sensor))


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
    when: Literal["always", "failure", "cancelled",
                  "failure_or_cancelled"] = "always"
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
