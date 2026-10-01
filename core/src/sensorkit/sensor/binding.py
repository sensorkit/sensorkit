# SPDX-License-Identifier: Apache-2.0
"""Bind reported capabilities to placements and route commands.

Binding requires reports for every configured device and verifies authored
trait assertions. Established traits come from reported commands and keywords;
assertions do not restrict which capabilities compilation may use.

Routing selects supporting placements on participating instrument chains.
Sensor subjects use one common target; instrument subjects route per chain,
merging duplicate targets. Scope and selection filters narrow the candidates.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

from sensorkit.common.keyword import KeywordDict
from sensorkit.core.device import DeviceCommand
from sensorkit.core.entity import DeviceDetails
from sensorkit.core.trait import get_trait, match_traits
from sensorkit.sensor.selection import Selection
from sensorkit.sensor.topology import (
    Device,
    DeviceKey,
    Placement,
    Selector,
    TagKey,
    Topology,
    TraitKey,
    format_path,
    format_paths,
)
from sensorkit.sensor.workflow import (
    RoutedCommand,
    Scope,
    Subject,
    partition,
)


class BoundSensor:
    """A topology with reported device details and established traits.

    Implements `PlacementFacts` for selection and routing. `bind` validates
    report coverage and trait assertions. Direct construction skips validation.

    `device_keywords` supplies device keyword values for planning. `None`
    means no mapping was supplied. The values may describe capabilities or
    current state and may become stale before execution.
    """

    def __init__(
        self,
        topology: Topology,
        details: Mapping[DeviceKey, DeviceDetails],
        *,
        device_keywords: Mapping[DeviceKey, KeywordDict] | None = None,
    ):
        self.topology = topology
        self.details = dict(details)
        self._device_keywords = device_keywords

        # Keep every matching trait, including multiple archetypes.
        self._traits = {
            placement: frozenset(t.name for t in match_traits(found))
            for placement in topology.placements()
            if (found := self.details.get(placement.device)) is not None
        }

    @classmethod
    def bind(cls, topology: Topology, details: Mapping[DeviceKey, DeviceDetails]) -> BoundSensor:
        """Check device reports and trait assertions, then return a sensor.

        Trait matching uses the same capability predicates as device
        declarations. Unsupported workflow commands are found later, during
        compilation.

        Raises:
            ValueError: A configured device has no report, or an authored trait
                is unregistered or not satisfied by its device.
        """
        silent = tuple(p for p in topology.placements() if p.device not in details)

        if silent:
            raise ValueError(
                "the structure names devices that reported nothing: "
                + ", ".join(f"'{p.device}' at '{format_path(p.path, '<root>')}'" for p in silent)
            )

        sensor = cls(topology, details)
        unmet = tuple(
            f"'{p.device}' at '{format_path(p.path, '<root>')}' {reason}"
            for p in topology.placements()
            for reason in _unmet(topology.record(p), sensor.traits(p))
        )

        if unmet:
            raise ValueError("declared traits were not established: " + ", ".join(unmet))

        return sensor

    def supported_commands(self, device: DeviceKey) -> frozenset[str]:
        """Return reported command identifiers, or an empty set for an
        unknown device.
        """
        found = self.details.get(device)

        return found.supported_commands if found else frozenset()

    def published_keywords(self, device: DeviceKey) -> frozenset[str]:
        """Return reported keyword identifiers, or an empty set for an
        unknown device.
        """
        found = self.details.get(device)

        return found.published_keywords if found else frozenset()

    def device_keywords(self, device: DeviceKey) -> KeywordDict | None:
        """Return keywords copied from a device, or `None` if none were
        supplied.

        A device missing from a supplied mapping has an empty keyword set.
        """
        if self._device_keywords is None:
            return None

        return self._device_keywords.get(device, KeywordDict())

    def traits(self, placement: Placement) -> frozenset[TraitKey]:
        """Return established traits, or an empty set for an unknown
        placement.
        """
        return self._traits.get(placement, frozenset())

    def tags(self, placement: Placement) -> frozenset[TagKey]:
        """Return grouping tags from the authored placement record."""
        return frozenset(self.topology.record(placement).tags)

    def kind(self, placement: Placement) -> Literal["device", "instrument", "selector"]:
        """Return the placement's device, instrument or selector kind."""
        match self.topology.record(placement):
            case Selector():
                return "selector"
            case Device(instrument=True):
                return "instrument"

        return "device"

    def instrument(self, placement: Placement) -> bool:
        """Test whether the record marks this placement as a collection
        target.
        """
        record = self.topology.record(placement)

        return isinstance(record, Device) and record.instrument

    def supported_on(self, command: str, participants: tuple[Placement, ...]) -> bool:
        """Test whether any device on the participating chains supports the
        command.
        """
        return any(
            command in self.supported_commands(placement.device)
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
        device: DeviceKey | None = None,
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
        device: DeviceKey | None,
    ) -> Placement:
        """Choose one target for a participant group using filters and
        depth.
        """
        named = type(command).model_tag()
        reachable = self._reachable(participants)

        if device is not None:
            reachable = self._named(device, reachable, named, participants)

        candidates = tuple(
            p
            for p in reachable
            if named in self.supported_commands(p.device)
            and any(self._eligible(p, q, scope) for q in participants)
            and (select is None or select.matches(p, self))
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
        return tuple(dict.fromkeys(p for q in participants for p in self.topology.chain(q)))

    def _named(
        self,
        device: DeviceKey,
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

        if named not in self.supported_commands(device):
            raise ValueError(f"'{device}' does not support '{named}'")

        return (placement,)

    def _eligible(self, placement: Placement, participant: Placement, scope: Scope) -> bool:
        """Test chain membership and scope for one participant.

        Private or shared status is relative to this participant's topology.
        """
        if placement not in self.topology.chain(participant):
            return False

        match scope:
            case "private":
                return placement in self.topology.private(participant)
            case "shared":
                return placement not in self.topology.private(participant)

        return True

    def _covering(
        self, candidates: tuple[Placement, ...], participants: tuple[Placement, ...], named: str
    ) -> tuple[Placement, ...]:
        """Keep only candidates present on every participant's chain.

        Raises:
            ValueError: No supporting candidate is common to all chains.
        """
        common = tuple(
            p for p in candidates if all(p in self.topology.chain(q) for q in participants)
        )

        if not common:
            raise ValueError(
                f"'{named}' is supported on {format_paths(participants)}, but by no "
                f"device every one of them looks through; a sensor-scope "
                f"command lands on one device or on none"
            )

        return common


def _unmet(record: Device | Selector, established: frozenset[TraitKey]) -> tuple[str, ...]:
    """Describe declared traits that binding could not establish.

    Distinguish unregistered names from registered traits the device lacks.
    """
    return tuple(
        f"declares trait '{name}', which is not registered"
        if get_trait(name) is None
        else f"declares trait '{name}', which its device does not satisfy"
        for name in record.traits
        if name not in established
    )
