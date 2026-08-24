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

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sensorkit.core.entity import DeviceDetails
from sensorkit.core.trait import get_trait, match_traits
from sensorkit.sensor.topology import Device, DeviceKey, Placement, Selector, TagKey, Topology, TraitKey, format_path


@dataclass(frozen=True)
class CapabilitySnapshot:
    """Device reports with their discovery time and source.

    Missing devices are unknown, not devices with no capabilities. Binding
    rejects a snapshot that lacks a configured device. Discovery may read
    devices sequentially; `taken` labels the snapshot, not simultaneous
    hardware state.
    """

    devices: tuple[tuple[DeviceKey, DeviceDetails], ...]
    taken: datetime
    source: str = ""

    @property
    def provenance(self) -> str:
        """Describe the snapshot source, timestamp and covered devices for
        reports.
        """
        source = self.source or "an unnamed source"
        covered = ", ".join(f"'{device}'" for device, _ in self.devices)

        return (f"capabilities of {covered or 'no device'} from {source}, "
                f"taken {self.taken.isoformat()}")


@dataclass(frozen=True)
class BindingReport:
    """Trait descriptions from a successful binding, one per placement.

    Missing reports and unmet assertions raise instead of producing a report.
    Unsupported workflow commands are handled later, during compilation.
    """

    established: tuple[str, ...] = ()


class BoundSensor:
    """A topology with reported capabilities and established traits.

    Implements `PlacementFacts` for selection and routing. Use `bind` to check
    snapshot coverage and trait assertions; direct construction skips those
    checks.
    """

    def __init__(self, topology: Topology, capabilities: CapabilitySnapshot):
        self.topology = topology
        self.capabilities = capabilities
        self._details = dict(capabilities.devices)

        # Keep every matching trait, including multiple archetypes.
        self._traits = {
            placement: frozenset(t.name for t in match_traits(details))
            for placement in topology.placements()
            if (details := self._details.get(placement.device)) is not None
        }

    @classmethod
    def bind(cls, topology: Topology,
             capabilities: CapabilitySnapshot) -> tuple[BoundSensor,
                                                        BindingReport]:
        """Check device reports and trait assertions, then return a sensor
        and report.

        Trait matching uses the same capability predicates as device
        declarations.

        Raises:
            ValueError: A configured device has no report, or an authored trait
                is unregistered or not satisfied by its device.
        """
        reported = dict(capabilities.devices)
        silent = tuple(p for p in topology.placements()
                       if p.device not in reported)

        if silent:
            raise ValueError(
                "the structure names devices that reported nothing: "
                + ", ".join(f"'{p.device}' at '{format_path(p.path, "<root>")}'" for p in silent))

        sensor = cls(topology, capabilities)
        unmet = tuple(f"'{p.device}' at '{format_path(p.path, "<root>")}' {reason}"
                      for p in topology.placements()
                      for reason in _unmet(topology.record(p),
                                           sensor.traits(p)))

        if unmet:
            raise ValueError("declared traits were not established: "
                             + ", ".join(unmet))

        return sensor, BindingReport(established=tuple(sensor._established()))

    def commands(self, device: DeviceKey) -> frozenset[str]:
        """Return reported command identifiers, or an empty set for an
        unknown device.
        """
        details = self._details.get(device)

        return details.supported_commands if details else frozenset()

    def keywords(self, device: DeviceKey) -> frozenset[str]:
        """Return reported keyword identifiers, or an empty set for an
        unknown device.
        """
        details = self._details.get(device)

        return details.published_keywords if details else frozenset()

    def traits(self, placement: Placement) -> frozenset[TraitKey]:
        """Return established traits, or an empty set for an unknown
        placement.
        """
        return self._traits.get(placement, frozenset())

    def tags(self, placement: Placement) -> frozenset[TagKey]:
        """Return grouping tags from the authored placement record."""
        return frozenset(self.topology.record(placement).tags)

    def kind(self, placement: Placement) -> Literal["device", "instrument",
                                                    "selector"]:
        """Return the placement record kind: device, instrument or selector."""
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




    def _established(self) -> Iterator[str]:
        """Describe established traits for each placement in traversal
        order.
        """
        for placement in self.topology.placements():
            traits = sorted(self.traits(placement))
            yield (f"'{placement.device}' at '{format_path(placement.path, "<root>")}' satisfies "
                   f"{", ".join(traits) or 'no trait'}")






def _unmet(record: Device | Selector,
           established: frozenset[TraitKey]) -> tuple[str, ...]:
    """Describe declared traits that binding could not establish.

    Distinguish unregistered names from registered traits the device lacks.
    """
    return tuple(
        f"declares trait '{name}', which is not registered"
        if get_trait(name) is None else
        f"declares trait '{name}', which its device does not satisfy"
        for name in record.traits if name not in established)
