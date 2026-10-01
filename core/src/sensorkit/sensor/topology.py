# SPDX-License-Identifier: Apache-2.0
"""Describe a sensor's structure and index its device relationships.

Devices occupy positions within units and selector ports. Each device has one
placement, identified by its device key and path. Instruments are collection
targets; their chains contain the devices they depend on, from root to leaf.
Instruments behind different ports of the same selector are mutually exclusive.

`Topology` validates device uniqueness and sibling path names, then indexes
chains, readers, private devices and required selector states without contacting
hardware. Binding checks declared traits against reported capabilities. Tags
provide grouping labels without asserting capabilities.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Discriminator, Field, Tag

type DeviceKey = str
"""Device identifier used in the structure and capability snapshot."""

type TraitKey = str
"""Registered trait name, checked against device capabilities at binding."""

type TagKey = str
"""Grouping label, such as a rack or cooling loop, with no capability meaning."""

type StructurePath = tuple[str, ...]
"""Path of unit names, port names and an instrument key, from root to leaf.

The sensor name is excluded; root devices have an empty path.
"""


@dataclass(frozen=True)
class Placement:
    """A device key and its structural path, used to identify workflow targets."""

    device: DeviceKey
    path: StructurePath

    @property
    def depth(self) -> int:
        """Number of path segments below the sensor root."""
        return len(self.path)


def _listed(v: object) -> object:
    """Accept a single name wherever a list of names is expected."""
    return [v] if isinstance(v, str) else v


class Device(BaseModel, frozen=True, extra="forbid"):
    """A device reference with optional trait assertions and grouping tags.

    Binding requires the device to satisfy every declared trait. These
    assertions do not limit which reported capabilities the planner can use.

    Set `instrument` to make the device a collection target. Its key becomes a
    final path segment, distinguishing instruments in the same component list.
    """

    device: DeviceKey
    traits: Annotated[tuple[TraitKey, ...], BeforeValidator(_listed)] = ()
    tags: Annotated[tuple[TagKey, ...], BeforeValidator(_listed)] = ()
    instrument: bool = False

    def placement(self, path: StructurePath) -> Placement:
        """Return the placement at `path`, appending the key for an instrument."""
        return Placement(self.device, path + (self.device,) if self.instrument else path)


class Unit(BaseModel, frozen=True, extra="forbid"):
    """An organizational container, such as an aperture or an instrument bench.

    Units may nest. Non-instrument devices in a unit appear on every instrument
    chain within it, including chains in nested units and selector ports.
    """

    unit: str
    components: tuple[Component, ...] = ()


class Port(BaseModel, frozen=True, extra="forbid"):
    """A selector port containing devices reachable in one selector position.

    `name` is both a path segment and the value commanded on the selector.
    Collect compilation derives this value from the participating instruments;
    callers must not also set that selector in the same epoch.
    """

    name: str
    components: tuple[Component, ...] = ()


class Selector(BaseModel, frozen=True, extra="forbid"):
    """A device that routes the beam to one of its ports.

    The selector occupies its parent's path; each port adds a segment below it.
    At least one port is required. Traits and tags work as they do on `Device`.
    """

    selector: DeviceKey
    ports: tuple[Port, ...] = Field(min_length=1)
    traits: Annotated[tuple[TraitKey, ...], BeforeValidator(_listed)] = ()
    tags: Annotated[tuple[TagKey, ...], BeforeValidator(_listed)] = ()


def _record_kind(v: object) -> str | None:
    """Identify a component by its model type or identifying mapping key."""
    match v:
        case Device():
            return "device"
        case Unit():
            return "unit"
        case Selector():
            return "selector"
        case Mapping():
            return next((k for k in ("device", "unit", "selector") if k in v), None)

    return None


# Use the identifying key to report validation errors for the selected model.
type Component = Annotated[
    Annotated[Device, Tag("device")]
    | Annotated[Unit, Tag("unit")]
    | Annotated[Selector, Tag("selector")],
    Discriminator(_record_kind),
]


class Structure(BaseModel, frozen=True, extra="forbid"):
    """The named root and components of one sensor.

    Non-instrument devices at the root, such as a mount or dome, appear on every
    instrument's chain. The sensor name is excluded from placement paths.
    Build a `Topology` to check device uniqueness and sibling path names.
    """

    name: str
    components: tuple[Component, ...] = ()


# Resolve recursive component annotations now that `Component` is defined.
Unit.model_rebuild()
Port.model_rebuild()
Selector.model_rebuild()


def format_path(path: StructurePath, root: str = "") -> str:
    """Render a path for a label or an error message.

    Args:
        path: Path segments from root to leaf, joined with slashes.
        root: Label to return for an empty path.
    """
    return "/".join(path) or root


def format_paths(placements: Iterable[Placement]) -> str:
    """Render participant paths for routing diagnostics."""
    return ", ".join(format_path(p.path, "<root>") for p in placements) or "<no instrument>"


class Topology:
    """Validated indexes of placements, instrument chains and shared devices.

    All relationships come from the structure; construction requires no device
    clients or capability facts.

    Raises:
        ValueError: A device key occurs more than once, or sibling units, ports
            or instruments contribute duplicate path segments.
    """

    def __init__(self, structure: Structure):
        self.structure = structure
        self._records: dict[Placement, Device | Selector] = {}
        self._chains: dict[Placement, tuple[Placement, ...]] = {}
        self._ports: dict[Placement, tuple[tuple[Placement, str], ...]] = {}
        self._placed: dict[DeviceKey, Placement] = {}

        self._visit(structure.components, (), (), ())

        # Preserve traversal order for discovery and other per-device operations.
        self._placements = tuple(self._records)
        self._instruments = tuple(self._chains)

        readers: dict[Placement, list[Placement]] = {}

        for instrument, chain in self._chains.items():
            for placement in chain:
                readers.setdefault(placement, []).append(instrument)

        self._readers = {p: tuple(insts) for p, insts in readers.items()}
        self._private = {
            instrument: frozenset(p for p in chain if self._owned(p, instrument))
            for instrument, chain in self._chains.items()
        }

    def placements(self) -> tuple[Placement, ...]:
        """Return one placement per device, in structure traversal order."""
        return self._placements

    def record(self, placement: Placement) -> Device | Selector:
        """Return the authored device or selector record for a placement.

        Raises:
            KeyError: The placement is not in this topology.
        """
        return self._records[placement]

    def instruments(self) -> tuple[Placement, ...]:
        """Return collection targets in structure traversal order."""
        return self._instruments

    def chain(self, instrument: Placement) -> tuple[Placement, ...]:
        """Return the instrument's device chain, from root to instrument.

        The chain includes non-instrument devices at each containing level,
        selectors and the instrument itself.

        Raises:
            KeyError: The placement is not an instrument.
        """
        return self._chains[instrument]

    def readers_of(self, placement: Placement) -> tuple[Placement, ...]:
        """Return instruments whose chains include this placement.

        Collect barriers use these readers to wait for acquisitions before
        changing a device's settings. Return an empty tuple if there are none.
        """
        return self._readers.get(placement, ())

    def private(self, instrument: Placement) -> frozenset[Placement]:
        """Return chain placements local to this instrument and used only by it.

        A placement is local if it shares the instrument's path or sits directly
        beside it in the parent component list. Ancestor devices remain shared
        infrastructure even when this instrument is their only reader.

        Raises:
            KeyError: The placement is not an instrument.
        """
        return self._private[instrument]

    def selector_states(self, instrument: Placement) -> tuple[tuple[Placement, str], ...]:
        """Return selector placements and required port names, from root to leaf.

        Raises:
            KeyError: The placement is not an instrument.
        """
        return self._ports[instrument]

    def mutually_exclusive(self, a: Placement, b: Placement) -> Placement | None:
        """Return the first selector requiring different ports for the instruments.

        Return `None` if both instruments can be reached at once.

        Raises:
            KeyError: Either placement is not an instrument in this topology.
        """
        ports = dict(self._ports[b])

        for selector, port in self._ports[a]:
            if selector in ports and ports[selector] != port:
                return selector

        return None

    def _visit(
        self,
        components: tuple[Component, ...],
        path: StructurePath,
        above: tuple[Placement, ...],
        selected: tuple[tuple[Placement, str], ...],
    ) -> None:
        """Index a component list, then descend into its units and selector ports.

        Add the level's non-instrument devices and selectors to every chain
        before recording instruments. Record devices first, then visit units
        and selectors in their authored order.
        """
        here = above + self._scan(components, path)

        for node in components:
            if isinstance(node, Device):
                placement = node.placement(path)
                self._records[placement] = node

                if node.instrument:
                    self._chains[placement] = here + (placement,)
                    self._ports[placement] = selected

        for node in components:
            match node:
                case Unit():
                    self._visit(node.components, path + (node.unit,), here, selected)
                case Selector():
                    placement = Placement(node.selector, path)
                    self._records[placement] = node

                    for port in node.ports:
                        self._visit(
                            port.components,
                            path + (port.name,),
                            here,
                            selected + ((placement, port.name),),
                        )

    def _scan(
        self, components: tuple[Component, ...], path: StructurePath
    ) -> tuple[Placement, ...]:
        """Validate this level and return its shared placements in authored order.

        Non-instrument devices and selectors are shared by the level's
        instruments and descendants. Each instrument ends its own chain.

        Raises:
            ValueError: A device is placed twice, or two children contribute
                the same path segment.
        """
        shared: list[Placement] = []
        segments: list[str] = []

        for node in components:
            match node:
                case Device():
                    placement = node.placement(path)
                    self._claim(node.device, placement)

                    if node.instrument:
                        segments.append(node.device)
                    else:
                        shared.append(placement)
                case Unit():
                    segments.append(node.unit)
                case Selector():
                    placement = Placement(node.selector, path)
                    self._claim(node.selector, placement)
                    shared.append(placement)
                    segments.extend(port.name for port in node.ports)

        # Selectors add no path segment, so their ports share a namespace with
        # sibling units, instruments and other selectors' ports.
        dupes = sorted({s for s in segments if segments.count(s) > 1})

        if dupes:
            where = format_path(path, "<root>")
            raise ValueError(f"positions named twice under '{where}': {', '.join(dupes)}")

        return tuple(shared)

    def _claim(self, device: DeviceKey, placement: Placement) -> None:
        """Reserve a device key for one placement.

        Raises:
            ValueError: The device key has already been claimed.
        """
        held = self._placed.get(device)

        if held is not None:
            raise ValueError(
                f"device '{device}' is placed twice, at "
                f"'{format_path(held.path, '<root>')}' and "
                f"'{format_path(placement.path, '<root>')}'"
            )

        self._placed[device] = placement

    def _owned(self, placement: Placement, instrument: Placement) -> bool:
        """Test whether a chain placement is local and has only this reader."""
        local = placement.path in (instrument.path[:-1], instrument.path)

        return local and self._readers[placement] == (instrument,)
