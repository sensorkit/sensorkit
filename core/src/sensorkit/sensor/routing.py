# SPDX-License-Identifier: Apache-2.0
"""Route commands to supporting placements on participating instrument chains.

Sensor subjects use one common target; instrument subjects route per chain,
merging duplicate targets. Scope and selection filters narrow the candidates.
Capability checks and selections read the bound sensor given to the router,
including any device keywords it holds for planning.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from sensorkit.core.device import DeviceCommand
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.selection import Selection
from sensorkit.sensor.topology import Placement, format_paths

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


def partition(
    subject: Subject, participants: tuple[Placement, ...]
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
        """Return the target placement and command type governing collect state."""
        return self.target, type(self.command)


class CommandRouter:
    """Choose command targets on a bound sensor."""

    def __init__(self, sensor: BoundSensor):
        self.sensor = sensor

    def supported_on(self, command: str, participants: tuple[Placement, ...]) -> bool:
        """Test whether any device on the participating chains supports the
        command.
        """
        return any(
            command in self.sensor.supported_commands(placement.device)
            for placement in self._reachable(participants)
        )

    def route(
        self,
        command: DeviceCommand,
        subject: Subject,
        participants: tuple[Placement, ...],
        *,
        select: Selection | None = None,
        scope: Scope = "any",
        device: str | None = None,
    ) -> tuple[RoutedCommand, ...]:
        """Choose targets for a command and return one routed command per
        target.

        A sensor subject chooses the shallowest supporting placement common to
        all participants. An instrument subject chooses the deepest supporting
        placement on each participant's chain. Repeated targets are commanded
        once.

        `scope` filters private or shared placements; `select` applies an
        additional predicate. `device` restricts the search to a named device,
        which must still satisfy capability, scope and selection checks.

        Raises:
            ValueError: No eligible target supports the command, candidates tie
                at the winning depth, or a named device is outside the relevant
                chains.
        """
        targets = dict.fromkeys(
            self._winner(command, subject, group, select, scope, device)
            for group in partition(subject, participants)
        )

        return tuple(RoutedCommand(target=target, command=command) for target in targets)

    def _winner(
        self,
        command: DeviceCommand,
        subject: Subject,
        participants: tuple[Placement, ...],
        select: Selection | None,
        scope: Scope,
        device: str | None,
    ) -> Placement:
        """Choose one target for a participant group using filters and
        depth.
        """
        sensor = self.sensor
        named = type(command).model_tag()
        reachable = self._reachable(participants)

        if device is not None:
            reachable = self._named(device, reachable, named, participants)

        candidates = tuple(
            p
            for p in reachable
            if named in sensor.supported_commands(p.device)
            and any(self._eligible(p, q, scope) for q in participants)
            and (select is None or select.matches(p, sensor))
        )

        if not candidates:
            raise ValueError(
                f"no device on {format_paths(participants)} supports '{named}'"
                + ("" if scope == "any" else f" as a {scope} device")
                + ("" if select is None else " and satisfies the selection")
            )

        if subject == "sensor":
            candidates = self._covering(candidates, participants, named)

        best = (min if subject == "sensor" else max)(p.depth for p in candidates)
        winners = sorted(
            (p for p in candidates if p.depth == best), key=lambda p: (p.path, p.device)
        )

        if len(winners) > 1:
            # Equal depths are ambiguous; require an explicit routing choice.
            raise ValueError(
                f"'{named}' is supported by "
                f"{', '.join(repr(p.device) for p in winners)} at the same "
                f"position on {format_paths(participants)}; name one with device, or "
                f"set a scope"
            )

        return winners[0]

    def _reachable(self, participants: tuple[Placement, ...]) -> tuple[Placement, ...]:
        """Combine participant chains, retaining each placement on its first
        occurrence.
        """
        topology = self.sensor.topology

        return tuple(dict.fromkeys(p for q in participants for p in topology.chain(q)))

    def _named(
        self,
        device: str,
        reachable: tuple[Placement, ...],
        named: str,
        participants: tuple[Placement, ...],
    ) -> tuple[Placement, ...]:
        """Restrict reachable placements to an explicitly named device.

        Raises:
            ValueError: The device is unreachable or does not support the
                command.
        """
        placement = next((p for p in reachable if p.device == device), None)

        if placement is None:
            # An explicit reference must still belong to a participating chain.
            raise ValueError(f"'{device}' is not on {format_paths(participants)}")

        if named not in self.sensor.supported_commands(device):
            raise ValueError(f"'{device}' does not support '{named}'")

        return (placement,)

    def _eligible(self, placement: Placement, participant: Placement, scope: Scope) -> bool:
        """Test chain membership and scope for one participant.

        Private or shared status is relative to this participant's topology.
        """
        topology = self.sensor.topology

        if placement not in topology.chain(participant):
            return False

        match scope:
            case "private":
                return placement in topology.private(participant)
            case "shared":
                return placement not in topology.private(participant)

        return True

    def _covering(
        self, candidates: tuple[Placement, ...], participants: tuple[Placement, ...], named: str
    ) -> tuple[Placement, ...]:
        """Keep only candidates present on every participant's chain.

        Raises:
            ValueError: No supporting candidate is common to all chains.
        """
        topology = self.sensor.topology
        common = tuple(p for p in candidates if all(p in topology.chain(q) for q in participants))

        if not common:
            raise ValueError(
                f"'{named}' is supported on {format_paths(participants)}, but by no "
                f"device every one of them looks through; a sensor-scope "
                f"command lands on one device or on none"
            )

        return common
