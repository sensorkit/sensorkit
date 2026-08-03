# SPDX-License-Identifier: Apache-2.0
"""Collect orchestration over the sensor structural model.

This is the layer above `lifecycle`'s peer: the collect authoring surface, its
validation, and how it compiles. Dispatch is `ops.py`'s and is shared with
lifecycle — a collect's two verbs, `apply` and `expose`, reach the deployment
through the same `OpHook` that homes a focuser.

A collect is an *individually schedulable unit*: one logical target, executed as an
ordered sequence of steps. Everything above it — what to observe when,
calibration/focus/science sequencing, retries, ordering between collects — belongs
to the external scheduler (calibration frames and focus sweeps are ordinary
collects with `target_type` / settings chosen appropriately). Everything below it —
how to overlap the work without violating the optics — is compiled here. The
executor is strictly serial: one collect at a time; concurrency lives inside the
compiled graph.

```text
Collect   = [Step, ...]           ordered; barriers are derived
Step      = settings {ref: value} shared state commanded for the
                                  step; typically the mount target
          + plans {instrument: FramePlan}
FramePlan = n_frames x exposure_s + private-device settings
```

Examples of the shape: a dither is one step per position; a focus sweep is one step
per focus value (same target throughout); the rate-sidereal satellite collect is
two steps — N-1 frames rate tracking, then one sidereal frame for the streak. The
mount is not special anywhere below the authoring surface: "a sequence of targets"
is just each step's settings commanding the mount ref.

**Barriers are per-device, derived, not asserted:**

* `apply(device, value)` waits — soft edge — for the latest frame block of every
  camera whose optical path contains the device: "a device may change once nobody
  is exposing through it". The mount is on every camera's path, so a target change
  waits for everyone; the global barrier falls out as exactly the case where it is
  physically real.
* A frame block hard-depends on the *governing* apply of every device on its path
  commanded so far in the collect — a failed filter move skips exactly the frames
  it invalidates. Re-commands of an unchanged value are elided at compile time, so
  the governing apply (and its failure) persists across steps.
* Same device serializes with itself; same camera serializes with itself (soft — a
  camera may try again in a later step).

**Validation separates malformed input from scheduling.** A step whose plans
disagree on shared state, or that commands a derived selector position, is not a
scheduling problem to solve but bad input to reject. Every check is step-local —
nothing crosses a step boundary, because a boundary is precisely the license for a
device to take a new value — so `validate_step` is the unit and `validate_collect`
is the loop over it. Producers that build a collect one step at a time
(`capability`) check as they go and never hand back something that only fails here.

A plan's frames compile to one node each, chained on the one camera. The barriers
reason about the *block* — an apply waits for a camera's latest frame, whichever
that is — while a node is one action, so the report names which frames happened.

Timing: a step's frames start when their dependencies resolve.
`Step.align="midpoint"` adds a sync node plus a start offset on each block's first
frame, so the blocks' midpoints coincide (block = `n_frames x exposure_s`; readout
is unmodeled here, and per-frame alignment would need it — out of scope).

Failure: `Collect.on_failure` takes dag's vocabulary directly and applies to every
node — `skip` is the one a sequence usually wants, so a failed filter move costs
exactly the frames it invalidates rather than the rest of the night. Outcomes are
`dag.RunReport`s; nothing here raises past validation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from itertools import combinations
from typing import Literal

from sensorkit.workflow.abort import AbortSignal
from sensorkit.workflow.dag import (
    DagRunner,
    Graph,
    GraphBuilder,
    Node,
    OnFailure,
    RunReport,
)
from sensorkit.workflow.ops import Op, OpContext, OpHook, RunContext
from sensorkit.workflow.structure import DeviceRef, InstrumentPath
from sensorkit.workflow.views import DeviceIndex, Topology

type Setting = object
"""Opaque commanded state: filter name, slew target, tracking-rate spec, structured
command object. Must support `==`; nothing here looks inside."""


@dataclass(frozen=True)
class FramePlan:
    """One camera's work within a step.

    `settings` may name only devices private to the instrument — anything shared
    belongs on the `Step`, where agreement across plans is structural rather than
    coincidental.
    """

    exposure_s: float
    n_frames: int = 1
    target_type: str = "science"     # what the frames are; open vocabulary
    settings: Mapping[DeviceRef, Setting] = field(default_factory=dict)

    @property
    def duration_s(self) -> float:
        # Readout/overhead deliberately unmodeled at this layer.
        return self.n_frames * self.exposure_s


@dataclass(frozen=True)
class Step:
    """One configuration epoch: shared settings (typically the mount target) plus
    the frames each participating camera takes under them.

    Selector positions are derived from the participants and must not be commanded
    explicitly.
    """

    plans: Mapping[InstrumentPath, FramePlan]
    settings: Mapping[DeviceRef, Setting] = field(default_factory=dict)
    align: Literal["start", "midpoint"] = "start"


@dataclass(frozen=True)
class Collect:
    steps: tuple[Step, ...]
    name: str = ""
    on_failure: OnFailure = "stop"        # dag's vocabulary, every node


def validate_collect(topo: Topology, collect: Collect) -> None:
    for k, step in enumerate(collect.steps):
        validate_step(topo, step, f"{collect.name or 'collect'}, step {k}")


def validate_step(topo: Topology, step: Step, where: str) -> None:
    """One step's share of the checks — which is all of them.

    Nothing here crosses a step boundary, because a step boundary is precisely the
    license for a device to take a new value.

    Split out from `validate_collect` so a producer of steps can hold the same
    contract as a producer of collects. `capability` resolves one step at a time and
    checks each as it goes, rather than handing back a `Collect` that only fails
    later at compile.
    """
    if not step.plans:
        raise ValueError(f"{where}: step has no frame plans")

    views = {}
    for path, plan in step.plans.items():
        view = topo.instruments.get(path)
        if view is None:
            raise ValueError(f"{where}: unknown instrument {'/'.join(path)}")
        if not view.assembly.role.is_collect_target:
            raise ValueError(
                f"{where}: {'/'.join(path)} has role "
                f"'{view.assembly.role.value}', not a collect target")
        stray = set(plan.settings) - view.private
        if stray:
            raise ValueError(
                f"{where}: {'/'.join(path)}: plan settings must be "
                f"private devices; {sorted(stray)} are not (shared "
                f"settings belong on the step)")
        views[path] = view

    for a, b in combinations(views, 2):
        sel = topo.mutually_exclusive(a, b)
        if sel:
            raise ValueError(
                f"{where}: {'/'.join(a)} and {'/'.join(b)} are on "
                f"different ports of selector {sel}")

    selectors = {sel for v in views.values() for sel, _ in v.selector_ports}
    bad = set(step.settings) & selectors
    if bad:
        raise ValueError(
            f"{where}: selector positions are derived from the "
            f"participants; do not command {sorted(bad)}")

    shared = set().union(*(set(v.shared) for v in views.values()))
    stray = set(step.settings) - shared
    if stray:
        raise ValueError(
            f"{where}: step settings for devices not shared by any "
            f"participant: {sorted(stray)}")


# The two verbs a collect dispatches. Fixed here because they are the whole
# vocabulary of the layer; what a deployment *does* for them is its business, and a
# dispatcher keyed on (trait, op) can give `apply` a different meaning per
# capability without renaming anything.
OP_APPLY = "apply"          # params: {"value": Setting}
OP_EXPOSE = "expose"        # params: {"exposure_s", "target_type",
                            #          "frame", "n_frames"}


@dataclass(frozen=True)
class SyncPoint:
    """Node payload: no work — a common time reference for the aligned frames that
    depend on it.

    The one payload that is not an `ops.Op`, because it is not an action on a
    device: it exists so several frames can share a predecessor and take their start
    offsets from it. `CollectRunner` absorbs it, so a hook never sees one.
    """


type Governing = dict[DeviceRef, tuple[int, Setting]]
type BlockDeps = dict[InstrumentPath, tuple[set[int], set[int]]]


def compile_collect(topo: Topology, devices: DeviceIndex,
                    collect: Collect) -> Graph:
    """Expand a collect into the dag IR under the per-device barrier rules.

    Pure data in, pure data out — dry-runnable, no device access; acyclic by
    construction (edges only point backwards).
    """
    validate_collect(topo, collect)

    g = GraphBuilder()

    # camera -> devices on its optical path; device -> cameras that expose through
    # it (the readers of the optics)
    path_devs: dict[InstrumentPath, frozenset[DeviceRef]] = {
        p: v.private | frozenset(v.shared) for p, v in topo.instruments.items()}
    readers: dict[DeviceRef, list[InstrumentPath]] = {}
    for p, devs in path_devs.items():
        for ref in devs:
            readers.setdefault(ref, []).append(p)

    governing: Governing = {}                    # ref -> (apply node, value)
    last_frame: dict[InstrumentPath, int] = {}   # camera -> its latest frame

    for k, step in enumerate(collect.steps):
        group = f"step {k}"

        _emit_applies(g, devices, group, _step_commands(topo, step),
                      governing, readers, last_frame, collect.on_failure)

        block_deps = _block_deps(step, path_devs, governing, last_frame)
        sync, offsets = _align(g, step, block_deps, group, collect.on_failure)

        for path, plan in step.plans.items():
            hard, soft = block_deps[path]
            if sync is not None:
                soft = soft | {sync}
            last_frame[path] = _emit_frames(
                g, topo, devices, path, plan, group, hard, soft,
                collect.on_failure, offsets[path])

    return g.build()


def _step_commands(topo: Topology, step: Step) -> list[tuple[DeviceRef, Setting]]:
    """Everything commanded this step: derived selector positions, step-shared
    settings, per-camera private settings."""
    cmds: list[tuple[DeviceRef, Setting]] = []
    seen: set[DeviceRef] = set()

    for path in step.plans:
        for sel, port in topo.instruments[path].selector_ports:
            if sel not in seen:
                seen.add(sel)
                cmds.append((sel, port))

    cmds.extend(step.settings.items())
    for plan in step.plans.values():
        cmds.extend(plan.settings.items())

    return cmds


def _emit_applies(g: GraphBuilder, devices: DeviceIndex, group: str,
                  cmds: list[tuple[DeviceRef, Setting]], governing: Governing,
                  readers: Mapping[DeviceRef, list[InstrumentPath]],
                  last_frame: Mapping[InstrumentPath, int],
                  on_failure: OnFailure) -> None:
    """Add this step's apply nodes, updating the governing apply per device."""
    for ref, val in cmds:
        gov = governing.get(ref)
        if gov is not None and gov[1] == val:
            continue                    # unchanged: elided, gov persists

        deps = {last_frame[c] for c in readers.get(ref, ())
                if c in last_frame}     # drain exposing readers
        if gov is not None:
            deps.add(gov[0])            # device serializes with itself

        # Addressed by ref: a step names devices, not capabilities, so the hook
        # resolves which capability this is (`match="device"` tells it consulting
        # `traits` is meaningful).
        nid = g.add(f"{ref} := {val!r}", group,
                    Op(ref=ref, op=OP_APPLY, params={"value": val},
                       traits=devices.traits_of(ref),
                       path=devices.path_of(ref), match="device"),
                    soft=deps, on_failure=on_failure)
        governing[ref] = (nid, val)


def _block_deps(step: Step, path_devs: Mapping[InstrumentPath, frozenset[DeviceRef]],
                governing: Governing,
                last_frame: Mapping[InstrumentPath, int]) -> BlockDeps:
    """Per participant, the (hard, soft) dependencies its frame block takes: the
    governing apply of every device on its path, and its own previous block."""
    return {
        path: ({governing[ref][0] for ref in path_devs[path]
                if ref in governing},
               {last_frame[path]} if path in last_frame else set())
        for path in step.plans}


def _align(g: GraphBuilder, step: Step, block_deps: BlockDeps, group: str,
           on_failure: OnFailure) -> tuple[int | None, dict[InstrumentPath, float]]:
    """The sync node and per-block start offsets that make midpoints coincide."""
    if step.align != "midpoint" or len(step.plans) <= 1:
        return None, dict.fromkeys(step.plans, 0.0)

    union = set().union(*(h | s for h, s in block_deps.values()))
    sync = g.add("align", group, SyncPoint(), soft=union, on_failure=on_failure)
    dmax = max(pl.duration_s for pl in step.plans.values())

    return sync, {p: (dmax - pl.duration_s) / 2
                  for p, pl in step.plans.items()}


def _emit_frames(g: GraphBuilder, topo: Topology, devices: DeviceIndex,
                 path: InstrumentPath, plan: FramePlan, group: str,
                 hard: set[int], soft: set[int], on_failure: OnFailure,
                 delay_s: float) -> int:
    """Add one plan's frames and return the id of its last.

    One node per frame: a node is one action, so the report says which frames
    happened and which did not. Every frame carries the block's hard deps — a failed
    filter move invalidates all of them, not just the first — while the soft chain
    keeps them in order on the one camera.
    """
    inst = topo.instruments[path].assembly.instrument
    prev_frame: int | None = None

    for i in range(plan.n_frames):
        label = f"expose {inst} {plan.exposure_s:g}s {plan.target_type}"
        if plan.n_frames > 1:
            label += f" [{i + 1}/{plan.n_frames}]"
        prev_frame = g.add(
            label, group,
            Op(ref=inst, op=OP_EXPOSE,
               params={"exposure_s": plan.exposure_s,
                       "target_type": plan.target_type,
                       "frame": i, "n_frames": plan.n_frames},
               traits=devices.traits_of(inst),
               path=path, match="instrument"),
            hard=hard,
            soft=soft if prev_frame is None else {prev_frame},
            on_failure=on_failure,
            delay_s=delay_s if prev_frame is None else 0.0)

    return prev_frame


class CollectRunner:
    """Collect frontend to `dag.DagRunner`: hands `ops.Op` payloads to the op hook —
    the *same* hook lifecycle uses.

    `run()` = compile + execute; one collect at a time, because the derived edges
    are the only concurrency control there is and they are sound only within one
    graph.

    Holds nothing but the hook, so one runner serves any number of sensors and is
    tied to no topology. Nothing here raises: outcomes are the `RunReport`.
    """

    def __init__(self, perform: OpHook):
        self.perform = perform

    async def run(self, topo: Topology, devices: DeviceIndex,
                  collect: Collect, *,
                  abort: AbortSignal | None = None) -> RunReport:
        graph = compile_collect(topo, devices, collect)
        return await self.execute(graph, name=collect.name or "collect",
                                  abort=abort)

    async def execute(self, graph: Graph, *, name: str = "",
                      abort: AbortSignal | None = None) -> RunReport:
        run = RunContext(name=name, graph=graph, abort=abort)

        async def dispatch(node: Node) -> object:
            if isinstance(node.payload, SyncPoint):
                return None             # a time reference, not an action
            return await self.perform(OpContext(node.payload, node, run))

        return await DagRunner(dispatch).execute(graph, name=name, abort=abort)
