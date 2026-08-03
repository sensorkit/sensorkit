# SPDX-License-Identifier: Apache-2.0
"""Derived views over a `structure.SensorModel`.

`structure.py` holds the structure and nothing else. Everything here is
deterministically derived from it and carries no policy: no phase ordering, no
scheduling preferences, no device clients. These classes expose structural *facts*;
the layers above interpret them.

Two views, because there are two axes along which to ask about an observatory, and
a question runs along one of them:

* `Topology` — **instrument-major.** Which devices an instrument owns, which it
  shares and with whom, which instruments a selector makes mutually exclusive.
* `DeviceIndex` — **device-major.** Which devices claim a trait, which traits a ref
  claims, which claims lie on the chain above an instrument.

Two axes, not two audiences — most consumers read both, and that is not a smell:

* `lifecycle.compile_table` is purely device-major; it is the one that reads a
  single view.
* `collect.compile_collect` reads `Topology` for the structure it compiles and
  `DeviceIndex` for the traits it stamps on each op.
* `capability.RequestResolver` reads both because its question spans the axes:
  *which* device serves an abstract dimension is device-major, *whether that device
  is private or shared* is instrument-major, and neither axis answers it alone.

What would be a smell is a view answering along the axis it is not indexed on.
Ownership is `Topology`'s: `InstrumentView.private` and `.shared` partition an
instrument's optical chain, and callers test membership there rather than
re-deriving it from claim positions.

They are deliberately separate classes rather than one fat object, and neither
holds the other: each is one walk of the tree, built from a `SensorModel`, and a
caller takes the views it can name a use for. Assembling them for a deployment is
`deployment.SensorPlan`'s job.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from sensorkit.common.keyword import KeywordDict
from sensorkit.workflow.structure import (
    ClaimKind,
    DeviceNode,
    DeviceRef,
    InstrumentAssembly,
    InstrumentPath,
    Part,
    SelectorAssembly,
    SensorModel,
    Trait,
    on_chain,
)


@dataclass(frozen=True)
class InstrumentView:
    """Everything orchestration needs to know about one instrument.

    `private` and `shared` partition the instrument's optical chain: together they
    are every device on the root-to-leaf path, and no ref is in both.
    """

    path: InstrumentPath
    assembly: InstrumentAssembly
    private: frozenset[DeviceRef]               # reached by this instrument alone
    shared: tuple[DeviceRef, ...]               # reached by another too, root -> leaf
    # (selector, port) chain, root -> leaf. "Port" is this view's word for the thing
    # the structure calls a part: a named position under a selector, and hence a
    # value to command on it.
    selector_ports: tuple[tuple[DeviceRef, str], ...]


class Topology:
    """Instrument-major view: what each instrument owns, shares, and excludes."""

    def __init__(self, sensor: SensorModel):
        self.sensor = sensor
        self.instruments: dict[InstrumentPath, InstrumentView] = {}

        # Every position each ref is claimed at. Ownership is a fact about the
        # *device*, so settling it needs all of a ref's claims, not just the one
        # that put it on this chain: a ref claimed both above and here (a shared
        # wheel doubling as this camera's shutter), or on a sibling leaf under some
        # other trait, is reached by another instrument and so is shared.
        # Commanding a device moves the device, not the capability that named it.
        # Construction-time only — a standing ref -> positions index would be
        # `DeviceIndex`'s axis, not this one.
        positions: dict[DeviceRef, set[InstrumentPath]] = {}
        for n in sensor.iter_devices():
            positions.setdefault(n.ref, set()).add(n.path)

        root = tuple(a.ref for a in sensor.attachments)
        for part in sensor.parts:
            self._add(part, (), root, (), positions)

    def _add(self, node: Part, path: InstrumentPath,
             above: tuple[DeviceRef, ...],
             sel_chain: tuple[tuple[DeviceRef, str], ...],
             positions: Mapping[DeviceRef, set[InstrumentPath]]) -> None:
        path = path + (node.name,)

        if isinstance(node, InstrumentAssembly):
            # A multi-trait ref appears once; order stays root -> leaf, so an own
            # device that turns out to be shared lands last.
            chain = tuple(dict.fromkeys(
                above + (node.instrument,)
                + tuple(a.ref for a in node.attachments)))
            private = frozenset(r for r in chain if positions[r] == {path})
            self.instruments[path] = InstrumentView(
                path=path,
                assembly=node,
                private=private,
                shared=tuple(r for r in chain if r not in private),
                selector_ports=sel_chain,
            )
            return

        # An assembly contributes its devices to everything below it, and a
        # selector contributes one more plus the position each part is reached at.
        # Narrowed once into a local rather than branched on twice; the field
        # itself is never optional.
        sel = node.selector if isinstance(node, SelectorAssembly) else None
        above = above + ((sel,) if sel else ()) + tuple(
            a.ref for a in node.attachments)

        for part in node.parts:
            self._add(part, path, above,
                      sel_chain + (((sel, part.name),) if sel else ()),
                      positions)

    def collect_targets(self) -> Iterator[InstrumentView]:
        return (v for v in self.instruments.values()
                if v.assembly.role.is_collect_target)

    def services(self) -> Iterator[InstrumentView]:
        return (v for v in self.instruments.values()
                if not v.assembly.role.is_collect_target)

    def mutually_exclusive(self, a: InstrumentPath,
                           b: InstrumentPath) -> DeviceRef | None:
        """The selector forcing instruments a and b onto different ports, if any —
        i.e. the structural reason they can never be in the light path
        simultaneously."""
        ports_b = dict(self.instruments[b].selector_ports)

        for sel, port in self.instruments[a].selector_ports:
            if sel in ports_b and ports_b[sel] != port:
                return sel

        return None


class DeviceIndex:
    """The flat walk of claims, indexed the ways its consumers ask for it: by trait,
    by structural kind, by ref, and by the root-to-leaf chain ending at an
    instrument — the last of those trait-indexed (`claims_on_chain`) or across both
    namespaces (`chain`), since routing a command and stamping a header ask about
    devices rather than about capabilities.

    One walk, done once, and every device-major question is answered from it — so a
    compiler names the axis it reads instead of building an index of its own.
    Naming an axis is not the same as one view per caller: `capability` reads this
    one *and* `Topology`.

    Trait-indexed and kind-indexed lookups are separate because the two are
    separate namespaces: a trait is a label a deployment chose, a kind is what the
    structure says a node is. Only `refs` spans both — a device is a device however
    it got here, which is what makes a per-device op like connect reach instruments
    and selectors too.
    """

    def __init__(self, sensor: SensorModel):
        self.sensor = sensor
        self.nodes: tuple[DeviceNode, ...] = tuple(sensor.iter_devices())

        by_trait: dict[Trait, list[DeviceNode]] = {}
        by_kind: dict[ClaimKind, list[DeviceNode]] = {}
        traits: dict[DeviceRef, set[Trait]] = {}
        first: dict[DeviceRef, DeviceNode] = {}
        static: dict[DeviceRef, KeywordDict] = {}

        for n in self.nodes:
            by_kind.setdefault(n.kind, []).append(n)
            if n.trait is not None:
                by_trait.setdefault(n.trait, []).append(n)
                traits.setdefault(n.ref, set()).add(n.trait)
            if n.keywords:
                # A ref claimed twice may be described at either claim; both are
                # about the one device, so they merge in walk order.
                static.setdefault(n.ref, KeywordDict()).update(n.keywords)
            first.setdefault(n.ref, n)          # root-first walk order

        self.by_trait: Mapping[Trait, tuple[DeviceNode, ...]] = {
            t: tuple(ns) for t, ns in by_trait.items()}
        self.by_kind: Mapping[ClaimKind, tuple[DeviceNode, ...]] = {
            k: tuple(ns) for k, ns in by_kind.items()}
        self._traits = {r: tuple(sorted(ts)) for r, ts in traits.items()}
        self._static = static
        # Insertion-ordered, and that order is the walk's: a per-device op like
        # connect visits the sensor root-first.
        self.refs: Mapping[DeviceRef, DeviceNode] = first
        self._chains: dict[InstrumentPath, Mapping[Trait, tuple[DeviceRef, ...]]] = {}
        self._depths: dict[InstrumentPath, Mapping[DeviceRef, int]] = {}

    def claiming(self, trait: Trait) -> tuple[DeviceNode, ...]:
        """Every claim of this trait, anywhere in the structure."""
        return self.by_trait.get(trait, ())

    def of_kind(self, kind: ClaimKind) -> tuple[DeviceNode, ...]:
        """Every structural claim of this kind, anywhere.

        The instrument and selector counterpart to `claiming`, kept apart from it
        because kinds and traits are different namespaces.
        """
        return self.by_kind.get(kind, ())

    def traits_of(self, ref: DeviceRef) -> tuple[Trait, ...]:
        """Every trait this ref claims, sorted.

        Empty for an unknown ref, and also for a ref whose only claim is structural
        — use `ref in index` to tell those cases apart.
        """
        return self._traits.get(ref, ())

    def __contains__(self, ref: object) -> bool:
        return ref in self.refs

    def path_of(self, ref: DeviceRef) -> InstrumentPath:
        """Where this ref first appears. A multi-trait ref has several positions;
        this is the root-most one, used for display."""
        return self.refs[ref].path

    def claims_on_chain(
            self, path: InstrumentPath) -> Mapping[Trait, tuple[DeviceRef, ...]]:
        """trait -> refs claimed anywhere on the root-to-leaf chain ending at
        `path`.

        Trait-indexed, so structural claims do not appear; `Topology` is the view
        that answers about those.
        """
        cached = self._chains.get(path)

        if cached is None:
            chain: dict[Trait, list[DeviceRef]] = {}
            for n in self.nodes:
                if n.trait is not None and on_chain(n.path, path):
                    chain.setdefault(n.trait, []).append(n.ref)
            cached = {t: tuple(rs) for t, rs in chain.items()}
            self._chains[path] = cached

        return cached

    def chain(self, path: InstrumentPath) -> Mapping[DeviceRef, int]:
        """Every device on the root-to-leaf chain ending at `path`, in walk order,
        against the depth of its deepest claim on that chain.

        The counterpart of `claims_on_chain` across both namespaces: an instrument
        and a selector are devices like any other, so the instrument itself is here
        and the trait-indexed answer cannot say so.

        The two things a chain is read for, in one walk. *Order* is root-first, and
        so is the order anything merged along a chain merges in — a device's
        keywords, a camera's frame metadata. *Depth* is what "the deepest claim
        wins" reads: a private wheel outranks the selector wheel above it, and a
        ref claimed at two positions is addressed at the lower one, because
        commanding a device moves the device rather than the capability that named
        it.
        """
        cached = self._depths.get(path)

        if cached is None:
            depths: dict[DeviceRef, int] = {}
            for n in self.nodes:
                if on_chain(n.path, path):
                    depths[n.ref] = max(depths.get(n.ref, 0), len(n.path))
            cached = self._depths[path] = depths

        return cached

    def keywords_of(self, ref: DeviceRef) -> KeywordDict:
        """What the structure says about this device, merged over its claims.

        Static config, and so empty for most refs: it is what a site knows and a
        driver does not publish.
        """
        return self._static.get(ref) or KeywordDict()

    def root_claims(self) -> Mapping[Trait, tuple[DeviceRef, ...]]:
        """Sensor-wide devices: the claims at the root of the tree."""
        return self.claims_on_chain(())
