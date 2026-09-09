# SPDX-License-Identifier: Apache-2.0
"""Expand acquisition requests, assign instruments and compile collect
workflows.

`pack` chooses participants, routes settings and splits incompatible requests
into child epochs. `compile_collect` emits planned steps with dependencies;
shared lowering builds the executable graphs. Current expansion produces camera
captures, with planned keywords carried on each acquisition.

Settings must describe absolute state. State is keyed by placement and command
type; later commands replace earlier ones without merging partial fields. Equal
commands with equal authored timeouts are elided, not retried. Relative
commands such as offsets are unsuitable for settings because repeats may be
elided. Preparation seeds the same state and has the same constraints.

Dependencies follow devices, not epoch boundaries. Settings wait for preceding
operations on their device and acquisitions reading through it. Acquisitions
require successful governing settings and preparation on their chain, and
serialize per instrument. Independent chains may overlap across epochs.

Midpoint alignment centers estimated acquisition blocks within a child epoch;
the estimates include integration time only. Teardown uses a separate cleanup
graph, armed by attempted preparation or, without preparation, any attempted
command. It should stop ongoing work; recovery belongs to the agent.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Literal

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.selection import Selection
from sensorkit.sensor.topology import Placement, format_paths
from sensorkit.sensor.workflow import (
    Acquisition,
    CleanupPlan,
    DeadlineRule,
    Dependency,
    ExecutableWorkflow,
    OperatorRule,
    Origin,
    PlannedStep,
    RequestId,
    RoutedCommand,
    Scope,
    StepName,
    Subject,
    format_command,
    lower,
    partition,
)
from sensorkit.std.collect import Collect as CollectKeyword
from sensorkit.std.instrument import CameraCapture
from sensorkit.std.optics import SelectPort


class PackingConflict(ValueError):
    """Requests or participants cannot share one epoch.

    The packer may open another epoch for this error. Other planning failures
    propagate because splitting an epoch would not fix them.
    """


class SettingUnsatisfiable(ValueError):
    """No supporting command is available to establish a requested setting.

    This tests command availability, not whether hardware can achieve the
    value.
    """


@dataclass(frozen=True)
class AcquisitionRequest:
    """Frame integration time, count, distribution and optional command
    timeout.

    Zero integration is allowed for bias frames. `distribute="one"` assigns all
    `count` frames to one chosen instrument. `each` assigns `count` frames to
    every eligible instrument. Splitting a count across instruments is
    unsupported.

    `timeout_s` limits each capture separately, not the whole request or its
    integration estimate. `None` inherits configured deadline rules.
    """

    integration_time_s: float
    count: int = 1
    distribute: Literal["one", "each"] = "one"
    timeout_s: float | None = None


@dataclass(frozen=True)
class CommandRequest:
    """A command request with routing filters and an optional explicit
    timeout.

    `subject` chooses sensor-wide or per-instrument routing. `scope`, `select`
    and `device` narrow the targets. Requests can configure an epoch,
    individual instruments, preparation or cleanup.

    Collect settings must be absolute, with one state per command type. Restate
    all required fields in partial-update commands; compilation does not merge
    them. Requests merged on one target must agree on both command and timeout.
    """

    command: DeviceCommand
    subject: Subject = "instrument"
    select: Selection | None = None
    scope: Scope = "any"
    device: str | None = None
    timeout_s: float | None = None

    def resolve(self, participants: tuple[Placement, ...],
                sensor: BoundSensor) -> tuple[RoutedCommand, ...]:
        """Route this command for the participants and attach its authored
        timeout.

        Different requests colliding on a shared device are checked during
        packing. Explicit device references skip the support precheck to retain
        routing errors.

        Raises:
            SettingUnsatisfiable: A participant group has no supporting device.
            ValueError: Routing is ambiguous, filters remove all candidates, or
                a named device is unreachable or unsupported.
        """
        named = type(self.command).model_tag()
        unsupported = next((group for group in partition(self.subject,
                                                         participants)
                            if not sensor.supported_on(named, group)), None)

        if self.device is None and unsupported is not None:
            raise SettingUnsatisfiable(
                f"no device on {format_paths(unsupported)} supports '{named}', so "
                f"nothing can establish what it was asked for")

        routed = sensor.route(self.command, self.subject, participants,
                              select=self.select, scope=self.scope,
                              device=self.device)

        return tuple(replace(r, timeout_s=self.timeout_s) for r in routed)

    def resolve_shared(self, participants: tuple[Placement, ...],
                       sensor: BoundSensor) -> tuple[RoutedCommand, ...]:
        """Route an epoch setting, distinguishing combined-participant
        conflicts.

        Raises:
            PackingConflict: Combined routing fails although each participant
                routes successfully alone.
            SettingUnsatisfiable: A participant group has no supporting device.
            ValueError: Routing fails for an individual participant.
        """
        try:
            return self.resolve(participants, sensor)
        except SettingUnsatisfiable:
            raise
        except ValueError as conflict:
            if len(participants) > 1 and all(self._alone(p, sensor)
                                             for p in participants):
                raise PackingConflict(str(conflict)) from conflict

            raise

    def _alone(self, participant: Placement, sensor: BoundSensor) -> bool:
        """Test whether the request routes successfully for one participant."""
        try:
            self.resolve((participant,), sensor)
        except ValueError:
            return False

        return True


@dataclass(frozen=True)
class InstrumentRequest:
    """An acquisition request with instrument criteria, settings and collect
    metadata.

    `select` filters instrument placements. `requires` filters their chains;
    `prefers` ranks eligible chains without excluding them. Settings must route
    successfully for the chosen instruments.

    `collect` carries the standard task's target and requested camera parameters.
    Expansion fills its per-instrument frame number. Device keywords are sampled
    from subscriptions at dispatch.

    A named `assignment` keeps segments on the same instruments across the
    collect. Every segment must agree on distribution and be eligible there.
    Ordinals continue across segments; an unnamed request forms its own group.
    """

    id: RequestId
    acquisition: AcquisitionRequest
    collect: CollectKeyword | None = None
    select: Selection | None = None
    settings: tuple[CommandRequest, ...] = ()
    requires: tuple[Selection, ...] = ()
    prefers: tuple[Selection, ...] = ()
    assignment: str | None = None

    def expand(self, target: Placement, first_frame: int,
               first_index: int) -> tuple[PlannedAcquisition, ...]:
        """Expand the full request into camera captures for one chosen
        instrument.

        `first_frame` starts per-instrument numbering across the collect.
        `first_index` continues the assignment group's ordinals. Packing
        maintains both counters. Each capture carries its acquisition keywords
        and explicit timeout; dispatch supplies its header later. Duration
        estimates include integration only.
        """
        asked = self.acquisition
        frames = range(first_frame, first_frame + asked.count)

        # Dispatch samples a fresh header and attaches it to a command copy.
        return tuple(
            PlannedAcquisition(
                acquisition=Acquisition(request=self.id,
                                        index=first_index + offset,
                                        frame_number=frame,
                                        keywords=KeywordDict(
                                            self.collect.model_copy(
                                                deep=True,
                                                update={"frame_number": frame}))
                                        if self.collect is not None else KeywordDict()),
                target=target,
                command=CameraCapture(integration_time=asked.integration_time_s,
                                      context=None),
                estimated_duration_s=asked.integration_time_s,
                timeout_s=asked.timeout_s)
            for offset, frame in enumerate(frames))

    def assignment_key(self, epoch: int, position: int) -> tuple[bool, str]:
        """Return the named assignment group or a unique key for an unnamed
        request.

        Named groups span the collect; unnamed requests are keyed by epoch and
        position.
        """
        if self.assignment is None:
            return False, f"{epoch}/{position}"

        return True, self.assignment

    def check(self, where: str) -> None:
        """Check that the request has a positive count and nonnegative
        integration.

        Raises:
            ValueError: Count is below one or integration time is negative.
        """
        asked = self.acquisition

        if asked.count < 1:
            # Omit the request to collect no frames.
            raise ValueError(
                f"{where}: request '{self.id}' takes {asked.count} "
                f"acquisition(s); drop the request instead")

        # Bias frames permit zero integration time.
        if asked.integration_time_s < 0:
            raise ValueError(
                f"{where}: request '{self.id}' integrates for "
                f"{asked.integration_time_s} seconds")


@dataclass(frozen=True)
class PlannedAcquisition:
    """A capture command for a chosen instrument, with frame metadata and
    estimate.

    The dispatcher fills the header on a copy of the command. Acquisition
    metadata passes unchanged to the operation. `timeout_s` retains the
    request's override.
    """

    acquisition: Acquisition
    target: Placement
    command: DeviceCommand
    estimated_duration_s: float
    timeout_s: float | None = None


@dataclass(frozen=True)
class Epoch[Setting, Unit]:
    """A group of acquisitions under one configuration, before or after
    binding.

    Packing preserves authored boundaries but may split incompatible requests
    into child epochs. Each child retains epoch settings and alignment;
    requests retain their instrument settings. Alignment applies separately to
    each child. Bound settings are a flat tuple of commands routed to
    placements.
    """

    units: tuple[Unit, ...]
    settings: tuple[Setting, ...] = ()
    align: Literal["start", "midpoint"] = "start"


@dataclass(frozen=True)
class Collect[Setting, Unit]:
    """An ordered set of configuration epochs with preparation and cleanup.

    Preparation runs in the main graph and gates acquisitions. Cleanup runs
    separately after it drains, unless hard cancellation skips it. Any
    attempted preparation arms cleanup; without preparation, any attempted
    command does. A workflow with no attempted command never arms its cleanup.

    `fail_fast` defaults main-workflow operations. Cleanup is non-fail-fast and
    bounded by `cleanup_timeout_s`, independently of command deadlines.
    """

    name: str
    epochs: tuple[Epoch[Setting, Unit], ...]
    prepare: tuple[Setting, ...] = ()
    cleanup: tuple[Setting, ...] = ()
    cleanup_timeout_s: float = 60.0
    fail_fast: bool = True


RequestEpoch = Epoch[CommandRequest, InstrumentRequest]
"""An authored configuration epoch before participant assignment."""

CollectIntent = Collect[CommandRequest, InstrumentRequest]
"""An authored collect before participant assignment and routing."""

BoundEpoch = Epoch[RoutedCommand, PlannedAcquisition]
"""A configuration epoch with chosen instruments and routed settings."""

BoundCollect = Collect[RoutedCommand, PlannedAcquisition]
"""A collect with chosen instruments, numbered acquisitions and routed
commands.
"""


def pack(intent: CollectIntent, sensor: BoundSensor) -> BoundCollect:
    """Choose instruments, route commands and split incompatible
    configuration epochs.

    Rank assignment options by preferences, then integration-time load,
    choosing the first that fits the current child epoch. Otherwise open a new
    child. Charge each assignment group's full duration when first placed.
    Preserve authored epoch boundaries, order, settings and per-child
    alignment.

    Raises:
        SettingUnsatisfiable: No eligible instrument can route a required
            setting.
        ValueError: Requests or assignments are invalid, routing fails, or a
            request cannot fit even by itself. `PackingConflict` is a subclass.
    """
    groups = _grouped(intent)
    chosen: dict[tuple[bool, str], tuple[Placement, ...]] = {}
    load: dict[Placement, float] = {}
    frames: dict[Placement, int] = {}
    ordinals: dict[tuple[str, Placement], int] = {}
    packed: list[BoundEpoch] = []

    for index, epoch in enumerate(intent.epochs):
        packed += _packed(epoch, index, sensor, groups, chosen, load, frames,
                          ordinals)

    participants = tuple(dict.fromkeys(unit.target for epoch in packed
                                       for unit in epoch.units))

    return BoundCollect(
        name=intent.name, epochs=tuple(packed),
        prepare=tuple(routed for r in intent.prepare
                      for routed in r.resolve(participants, sensor)),
        cleanup=tuple(routed for r in intent.cleanup
                      for routed in r.resolve(participants, sensor)),
        cleanup_timeout_s=intent.cleanup_timeout_s,
        fail_fast=intent.fail_fast)


def _packed(epoch: RequestEpoch, index: int, sensor: BoundSensor,
            groups: Mapping[tuple[bool, str],
                            tuple[tuple[InstrumentRequest, RequestEpoch], ...]],
            chosen: dict[tuple[bool, str], tuple[Placement, ...]],
            load: dict[Placement, float], frames: dict[Placement, int],
            ordinals: dict[tuple[str, Placement], int]) -> list[BoundEpoch]:
    """Pack one authored epoch into compatible child epochs without crossing
    its boundary.
    """
    where = f"epoch {index}"
    children: list[list[tuple[InstrumentRequest, tuple[Placement, ...]]]] = [[]]

    for position, request in enumerate(epoch.units):
        key = request.assignment_key(index, position)
        options = ((chosen[key],) if key in chosen
                   else _options(groups[key], sensor, load, where))
        targets = next(
            (option for option in options
             if _fits([*children[-1], (request, option)], epoch, sensor,
                      where)),
            None)

        if targets is None:
            children.append([])
            targets = options[0]
            # A request that fails alone cannot be repaired by splitting epochs.
            _resolved([(request, targets)], epoch, sensor, where)

        _bind(chosen, load, key, targets, groups[key])
        children[-1].append((request, targets))

    return [_child(child, epoch, sensor, where, frames, ordinals)
            for child in children if child]


def _bind(chosen: dict[tuple[bool, str], tuple[Placement, ...]],
          load: dict[Placement, float], key: tuple[bool, str],
          targets: tuple[Placement, ...],
          members: tuple[tuple[InstrumentRequest, RequestEpoch], ...]) -> None:
    """Record a group's first assignment and charge its full duration to
    each target.
    """
    if key in chosen:
        return

    chosen[key] = targets

    for target in targets:
        load[target] = load.get(target, 0.0) + _duration(members)


def _child(assigned: list[tuple[InstrumentRequest, tuple[Placement, ...]]],
           epoch: RequestEpoch, sensor: BoundSensor, where: str,
           frames: dict[Placement, int],
           ordinals: dict[tuple[str, Placement], int]) -> BoundEpoch:
    """Expand assigned requests and route the child epoch's settings.

    Frame numbers continue per instrument across the collect. Ordinals continue
    per named assignment group and instrument; unnamed requests start at zero.
    """
    units: list[PlannedAcquisition] = []

    for request, targets in assigned:
        count = request.acquisition.count

        for target in targets:
            frame = frames.get(target, 0)
            frames[target] = frame + count

            match request.assignment:
                # An unnamed request starts a new ordinal sequence.
                case None:
                    ordinal = 0
                case name:
                    ordinal = ordinals.get((name, target), 0)
                    ordinals[(name, target)] = ordinal + count

            units += request.expand(target, frame, ordinal)

    return BoundEpoch(units=tuple(units), align=epoch.align,
                      settings=_resolved(assigned, epoch, sensor, where))


def _fits(assigned: list[tuple[InstrumentRequest, tuple[Placement, ...]]],
          epoch: RequestEpoch, sensor: BoundSensor, where: str) -> bool:
    """Test whether assigned requests can share an epoch.

    Return false only for `PackingConflict`; other planning errors propagate.
    """
    try:
        _resolved(assigned, epoch, sensor, where)
    except PackingConflict:
        return False

    return True


def _resolved(assigned: list[tuple[InstrumentRequest, tuple[Placement, ...]]],
              epoch: RequestEpoch, sensor: BoundSensor,
              where: str) -> tuple[RoutedCommand, ...]:
    """Route and merge epoch settings and each request's instrument
    settings.

    Raises:
        PackingConflict: Participants are exclusive or commands disagree on a
            shared state key.
        ValueError: Timeouts disagree, a setting overrides a derived selector
            position, or routing fails.
    """
    participants = tuple(dict.fromkeys(p for _, targets in assigned
                                       for p in targets))
    _exclusive(participants, sensor, where)
    held: dict[tuple[Placement, type], tuple[RoutedCommand, str]] = {}

    for setting in epoch.settings:
        for routed in setting.resolve_shared(participants, sensor):
            _put(held, routed, "the epoch", where)

    for request, targets in assigned:
        for setting in request.settings:
            for routed in setting.resolve(targets, sensor):
                _put(held, routed, f"request '{request.id}'", where)

    _positioned(held, participants, sensor, where)

    return tuple(routed for routed, _ in held.values())


def _put(held: dict[tuple[Placement, type], tuple[RoutedCommand, str]],
         routed: RoutedCommand, origin: str, where: str) -> None:
    """Merge a setting by placement and command type, requiring equal
    requests.

    Raises:
        PackingConflict: Commands disagree for the same key.
        ValueError: Equal commands have different authored timeouts.
    """
    key = (routed.target, type(routed.command))
    seen = held.get(key)

    if seen is None:
        held[key] = (routed, origin)

        return

    if seen[0].command != routed.command:
        raise PackingConflict(
            f"{where}: '{routed.target.device}' is asked for "
            f"{format_command(routed.command)} by {origin} and for "
            f"{format_command(seen[0].command)} by {seen[1]}; a device holds "
            f"one value per epoch")

    if seen[0].timeout_s != routed.timeout_s:
        raise ValueError(
            f"{where}: {origin} and {seen[1]} ask '{routed.target.device}' for "
            f"{format_command(routed.command)} under deadlines of "
            f"{routed.timeout_s} and {seen[0].timeout_s} seconds; nothing here "
            f"chooses between them")


def _positioned(held: Mapping[tuple[Placement, type],
                              tuple[RoutedCommand, str]],
                participants: tuple[Placement, ...], sensor: BoundSensor,
                where: str) -> None:
    """Reject authored settings on selectors already positioned by
    participants.

    Raises:
        ValueError: A setting targets a selector on a participating port path.
    """
    positioned = {selector for participant in participants
                  for selector, _ in
                  sensor.topology.selector_states(participant)}
    commanded = sorted({target.device for target, _ in held
                        if target in positioned})

    if commanded:
        raise ValueError(
            f"{where}: selector positions follow from the participants; do not "
            f"command {', '.join(repr(device) for device in commanded)}")


def _options(members: tuple[tuple[InstrumentRequest, RequestEpoch], ...],
             sensor: BoundSensor, load: Mapping[Placement, float],
             where: str) -> tuple[tuple[Placement, ...], ...]:
    """Rank an assignment group's eligible instrument sets.

    Fan-out has one option containing every eligible instrument. Single-target
    options rank by preference count, then accumulated integration-time load.
    """
    eligible = _eligible(members, sensor, where)
    first, _ = members[0]

    if first.acquisition.distribute == "each":
        return (eligible,)

    ranked = sorted(eligible,
                    key=lambda p: (-_preferred(members, p, sensor),
                                   load.get(p, 0.0)))

    return tuple((placement,) for placement in ranked)


def _eligible(members: tuple[tuple[InstrumentRequest, RequestEpoch], ...],
              sensor: BoundSensor, where: str) -> tuple[Placement, ...]:
    """Find instruments satisfying every segment and routing all group
    settings.

    Check all segments before choosing so later segments keep the same
    assignment.

    Raises:
        SettingUnsatisfiable: No admitted instrument can route all settings.
        ValueError: No instrument meets selection and requirements, or routing
            fails structurally on an admitted instrument.
    """
    admitted = tuple(placement for placement in sensor.topology.instruments()
                     if all(_admits(member, placement, sensor)
                            for member, _ in members))

    if not admitted:
        raise ValueError(
            f"{where}: no instrument satisfies {_asked(members)}")

    refused: SettingUnsatisfiable | None = None
    settled: list[Placement] = []

    for placement in admitted:
        try:
            _establishes(members, placement, sensor)
        except SettingUnsatisfiable as unmet:
            refused = refused or unmet
        else:
            settled.append(placement)

    if not settled:
        raise SettingUnsatisfiable(f"{where}: {refused}")

    return tuple(settled)


def _establishes(members: tuple[tuple[InstrumentRequest, RequestEpoch], ...],
                 placement: Placement, sensor: BoundSensor) -> None:
    """Check all segment and epoch settings against one instrument's chain.

    Packing checks compatibility with other participants separately.

    Raises:
        SettingUnsatisfiable: A setting has no supporting device on this chain.
        ValueError: Routing fails structurally.
    """
    for member, epoch in members:
        for setting in (*epoch.settings, *member.settings):
            setting.resolve((placement,), sensor)


def _admits(request: InstrumentRequest, placement: Placement,
            sensor: BoundSensor) -> bool:
    """Test instrument selection at the placement and requirements over its
    chain.
    """
    if request.select is not None and not request.select.matches(placement,
                                                                 sensor):
        return False

    chain = sensor.topology.chain(placement)

    return all(requirement.reaches(chain, sensor)
               for requirement in request.requires)


def _preferred(members: tuple[tuple[InstrumentRequest, RequestEpoch], ...],
               placement: Placement, sensor: BoundSensor) -> int:
    """Count satisfied chain preferences across every segment of an
    assignment group.
    """
    chain = sensor.topology.chain(placement)

    return sum(preference.reaches(chain, sensor)
               for member, _ in members for preference in member.prefers)


def _grouped(intent: CollectIntent
             ) -> dict[tuple[bool, str],
                       tuple[tuple[InstrumentRequest, RequestEpoch], ...]]:
    """Validate requests and group them with their containing authored
    epochs.

    Raises:
        ValueError: A request has invalid count or integration, or a named
            group mixes distribution modes.
    """
    groups: dict[tuple[bool, str],
                 list[tuple[InstrumentRequest, RequestEpoch]]] = {}

    for index, epoch in enumerate(intent.epochs):
        for position, request in enumerate(epoch.units):
            request.check(f"epoch {index}")
            groups.setdefault(request.assignment_key(index, position),
                              []).append((request, epoch))

    for (named, name), members in groups.items():
        if named and len({m.acquisition.distribute
                          for m, _ in members}) > 1:
            raise ValueError(
                f"assignment '{name}' mixes distribute values; every "
                f"segment of one request takes the same instruments")

    return {key: tuple(members) for key, members in groups.items()}


def _duration(members: tuple[tuple[InstrumentRequest, RequestEpoch], ...]
              ) -> float:
    """Sum group integration time for load ranking, excluding readout and
    overhead.
    """
    return sum(member.acquisition.integration_time_s * member.acquisition.count
               for member, _ in members)


def _exclusive(participants: tuple[Placement, ...], sensor: BoundSensor,
               where: str) -> None:
    """Reject participants requiring different ports of one selector.

    Raises:
        PackingConflict: The instruments cannot share a selector position.
    """
    for a, b in itertools.combinations(participants, 2):
        selector = sensor.topology.mutually_exclusive(a, b)

        if selector is not None:
            raise PackingConflict(
                f"{where}: '{a.device}' and '{b.device}' are on different "
                f"ports of selector '{selector.device}'")


def _asked(members: tuple[tuple[InstrumentRequest, RequestEpoch], ...]) -> str:
    """Format unique request ids in a group for diagnostics."""
    return ", ".join(sorted({f"'{member.id}'" for member, _ in members}))


def compile_collect(collect: BoundCollect, sensor: BoundSensor, *,
                    deadlines: tuple[DeadlineRule, ...] = (),
                    rules: tuple[OperatorRule, ...] = ()
                    ) -> ExecutableWorkflow:
    """Compile a bound collect into executable graphs without contacting
    devices.

    Dependencies follow previously emitted steps. The result includes
    capability provenance and can be inspected before execution.

    Raises:
        ValueError: Bound settings or participants conflict, commands are
            unsupported, or shared lowering rejects the workflow.
    """
    return CollectCompiler(collect, sensor, deadlines=deadlines,
                           rules=rules).run()


class CollectCompiler:
    """Single-use state for deriving collect dependencies and planned steps.

    Track governing settings by placement and command type, latest operations
    per device, and latest acquisitions per instrument. Shared lowering handles
    omissions, deadlines, operator rules and graph construction.
    """

    def __init__(self, collect: BoundCollect, sensor: BoundSensor, *,
                 deadlines: tuple[DeadlineRule, ...] = (),
                 rules: tuple[OperatorRule, ...] = ()):
        self.collect = collect
        self.sensor = sensor
        self.topology = sensor.topology
        self.deadlines = deadlines
        self.rules = rules
        self.steps: list[PlannedStep] = []
        # Governing command and its step, keyed by placement and command type.
        self.governing: dict[tuple[Placement, type[DeviceCommand]],
                             tuple[StepName, RoutedCommand]] = {}
        self.latest: dict[Placement, StepName] = {}
        self.last_unit: dict[Placement, StepName] = {}
        self.prepared: list[tuple[Placement, StepName]] = []
        self.emitted = 0

    def run(self) -> ExecutableWorkflow:
        """Emit preparation, epochs and cleanup, then lower their planned
        steps.
        """
        for position, routed in enumerate(self.collect.prepare):
            name = self._setting(routed, "prepare", ("prepare", position))
            self.prepared.append((routed.target, name))

        for index, epoch in enumerate(self.collect.epochs):
            self._compile_epoch(epoch, index)

        cleanup = (self._cleanup(),) if self.collect.cleanup else ()

        return lower(self.collect.name, tuple(self.steps), self.sensor,
                     provenance=self.sensor.capabilities.provenance,
                     cleanup=cleanup, deadlines=self.deadlines,
                     rules=self.rules)

    def _compile_epoch(self, epoch: BoundEpoch, index: int) -> None:
        """Emit changed settings and each instrument's acquisition block."""
        where = f"epoch {index}"
        participants = tuple(dict.fromkeys(unit.target for unit in epoch.units))
        _consistent(epoch, participants, self.sensor, where)

        for routed in (*self._ports(participants), *epoch.settings):
            if not self._in_force(routed):
                self._setting(routed, where,
                              (index, routed.command.model_tag()))

        blocks: dict[Placement, list[PlannedAcquisition]] = {}

        for unit in epoch.units:
            blocks.setdefault(unit.target, []).append(unit)

        needs = {instrument: self._needs(instrument) for instrument in blocks}
        aligned, offsets = self._align(epoch, blocks, needs, index)

        for instrument, units in blocks.items():
            hard, soft = needs[instrument]
            self._emit_units(units, index, hard,
                             soft if aligned is None else (*soft, aligned),
                             offsets[instrument])

    def _ports(self, participants: tuple[Placement, ...]
               ) -> tuple[RoutedCommand, ...]:
        """Derive distinct selector-position commands from participant port
        paths.
        """
        states = dict.fromkeys(
            state for participant in participants
            for state in self.topology.selector_states(participant))

        return tuple(RoutedCommand(target=selector,
                                   command=SelectPort(port=port))
                     for selector, port in states)

    def _in_force(self, routed: RoutedCommand) -> bool:
        """Test whether an equal command and authored timeout already govern
        this key.

        Equality elides a new setting but retains the earlier success
        requirement. It does not request a retry after that earlier setting
        fails.
        """
        held = self.governing.get(routed.governs)

        return held is not None and held[1] == routed

    def _setting(self, routed: RoutedCommand, group: str,
                 path: tuple[str | int, ...]) -> StepName:
        """Emit a governing setting after preceding readers and device work
        complete.

        Completion dependencies serialize the device without requiring prior
        success.
        """
        target = routed.target
        waits = [self.last_unit[reader]
                 for reader in self.topology.readers_of(target)
                 if reader in self.last_unit]

        if target in self.latest:
            waits.append(self.latest[target])

        step = self._planned(group, path, target=target,
                             command=routed.command, timeout_s=routed.timeout_s,
                             deps=Dependency.completion(waits))
        self.steps.append(step)
        self.governing[routed.governs] = (step.name, routed)
        self.latest[target] = step.name

        return step.name

    def _needs(self, instrument: Placement
               ) -> tuple[tuple[StepName, ...], tuple[StepName, ...]]:
        """Return required successes and completion waits for an
        instrument's next block.

        Require governing settings and all preparation on its chain. Follow its
        last acquisition on completion. Preparation remains required after
        later settings replace its governing state.
        """
        chain = frozenset(self.topology.chain(instrument))
        hard = [name for (placement, _), (name, _) in self.governing.items()
                if placement in chain]
        hard += [name for placement, name in self.prepared
                 if placement in chain]
        soft = ((self.last_unit[instrument],) if instrument in self.last_unit
                else ())

        return tuple(dict.fromkeys(hard)), soft

    def _align(self, epoch: BoundEpoch,
               blocks: Mapping[Placement, list[PlannedAcquisition]],
               needs: Mapping[Placement, tuple[tuple[StepName, ...],
                                               tuple[StepName, ...]]],
               index: int) -> tuple[StepName | None, dict[Placement, float]]:
        """Build a common readiness point and offsets for estimated block
        midpoints.

        Spans sum acquisition estimates, currently integration time only.
        Readout and transfer overhead are excluded, so actual block midpoints
        may differ. Start alignment and single-instrument blocks add no common
        ordering node.
        """
        if epoch.align != "midpoint" or len(blocks) <= 1:
            return None, dict.fromkeys(blocks, 0.0)

        waits = [name for hard, soft in needs.values() for name in (*hard, *soft)]
        step = self._planned(f"epoch {index}", (index, "align"),
                             deps=Dependency.completion(dict.fromkeys(waits)),
                             reason=f"align midpoints of epoch {index}")
        self.steps.append(step)
        spans = {instrument: sum(unit.estimated_duration_s for unit in units)
                 for instrument, units in blocks.items()}
        longest = max(spans.values())

        return step.name, {instrument: (longest - span) / 2
                           for instrument, span in spans.items()}

    def _emit_units(self, units: list[PlannedAcquisition], index: int,
                    hard: tuple[StepName, ...], soft: tuple[StepName, ...],
                    delay_s: float) -> None:
        """Emit one instrument's acquisition block with serial completion
        dependencies.

        Every unit requires successful block settings. A failed capture alone
        does not skip the next, though fail-fast policy can stop the whole run.
        """
        previous: StepName | None = None

        for unit in units:
            acquisition = unit.acquisition
            follows = soft if previous is None else (previous,)
            step = self._planned(
                f"epoch {index}",
                (index, acquisition.request, acquisition.frame_number),
                target=unit.target, command=unit.command,
                timeout_s=unit.timeout_s, acquisition=acquisition,
                delay_s=delay_s if previous is None else 0.0,
                deps=(*(Dependency(on=name) for name in hard),
                      *Dependency.completion(follows)))
            self.steps.append(step)
            previous = step.name

        if previous is not None:
            self.last_unit[units[0].target] = previous

    def _cleanup(self) -> CleanupPlan:
        """Plan teardown armed by preparation, or by any command if
        preparation is absent.

        Ordering steps do not arm it. Commands serialize per device on
        completion and otherwise run independently. Non-fail-fast policy lets
        teardown continue after a failed command.
        """
        steps: list[PlannedStep] = []
        latest: dict[Placement, StepName] = {}

        for position, routed in enumerate(self.collect.cleanup):
            target = routed.target
            step = self._planned(
                "cleanup", ("cleanup", position), target=target,
                command=routed.command, timeout_s=routed.timeout_s,
                deps=Dependency.completion(
                    (latest[target],) if target in latest else ()),
                fail_fast=False)
            steps.append(step)
            latest[target] = step.name

        armed_by = (tuple(name for _, name in self.prepared)
                    if self.collect.prepare else
                    tuple(step.name for step in self.steps
                          if step.command is not None))

        return CleanupPlan(
            steps=tuple(steps),
            origin=Origin(source=self.collect.name, path=("cleanup",)),
            when="always", timeout_s=self.collect.cleanup_timeout_s,
            armed_by=armed_by)

    def _planned(self, group: str, path: tuple[str | int, ...], *,
                 target: Placement | None = None,
                 command: DeviceCommand | None = None,
                 deps: tuple[Dependency, ...] = (),
                 timeout_s: float | None = None,
                 acquisition: Acquisition | None = None, delay_s: float = 0.0,
                 reason: str = "", fail_fast: bool | None = None
                 ) -> PlannedStep:
        """Build a uniquely named step, inheriting collect failure policy
        unless overridden.
        """
        self.emitted += 1
        where = "ordering" if target is None else target.device

        return PlannedStep(
            name=f"{group}/{where}/{self.emitted}",
            origin=Origin(source=self.collect.name, path=path, reason=reason),
            group=group, target=target, command=command, deps=deps,
            fail_fast=self.collect.fail_fast if fail_fast is None else fail_fast,
            timeout_s=timeout_s, acquisition=acquisition, delay_s=delay_s)


def _consistent(epoch: BoundEpoch, participants: tuple[Placement, ...],
                sensor: BoundSensor, where: str) -> None:
    """Check bound-epoch compatibility, including for manually built
    collects.

    Raises:
        ValueError: Participants are exclusive, commands or timeouts conflict,
            or settings override derived selector positions.
    """
    _exclusive(participants, sensor, where)
    held: dict[tuple[Placement, type], tuple[RoutedCommand, str]] = {}

    for routed in epoch.settings:
        _put(held, routed, "the epoch", where)

    _positioned(held, participants, sensor, where)
