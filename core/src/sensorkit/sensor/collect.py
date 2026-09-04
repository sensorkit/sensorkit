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

from dataclasses import dataclass, replace
from typing import Literal

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.selection import Selection
from sensorkit.sensor.topology import Placement, format_paths
from sensorkit.sensor.workflow import Acquisition, RequestId, RoutedCommand, Scope, Subject, partition
from sensorkit.std.collect import Collect as CollectKeyword
from sensorkit.std.instrument import CameraCapture


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
