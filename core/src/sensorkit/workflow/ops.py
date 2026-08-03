# SPDX-License-Identifier: Apache-2.0
"""The dispatch boundary: one payload type, one context, one hook — shared by every
frontend.

`lifecycle` and `collect` compile different things (ordered phases of device ops;
ordered steps of settings and exposures) into different edge patterns, but what
they *dispatch* is the same shape: **a device, a verb, and whatever that verb
needs**. Homing a focuser, moving a filter wheel and taking an exposure differ in
their parameters, not in their kind.

So there is one `OpHook` and not two. A deployment writes one dispatcher, registers
its drivers once, and both runners use it — the filter wheel that homes during init
is the same object, reached the same way, that applies a filter during a collect.

`Op` carries what a hook cannot reconstruct from the graph alone:

* `ref` / `op` — the device and the verb.
* `params` — the verb's arguments. Empty for a nullary op like `home`;
  `{"value": ...}` for an apply; the exposure's terms for a frame. A plain mapping
  rather than a typed object, because this crosses into user code and the graph is
  meant to stay serializable.
* `trait` / `traits` / `match` — *which capability* is being addressed, and how the
  authoring surface picked the device. Only the compiler knows the last, only the
  sensor knows the second, and resolution needs both. `trait` is set only for a
  trait match; for every other match `match` itself is what a dispatcher keys on —
  a `device:` entry and an apply mean "this device, whatever it is", so walking
  `traits` is the meaningful move; `instrument` and `selector` name a structural
  kind; an `all` op like connect has no capability to speak of.
* `path` — where the device sits in the structure.

Carried on the payload, so a graph stays self-contained and a runner needs no
sensor of its own to dispatch it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal

from sensorkit.workflow.abort import AbortSignal
from sensorkit.workflow.dag import Graph, Node
from sensorkit.workflow.structure import DeviceRef, InstrumentPath, Trait

type Match = Literal["trait", "device", "instrument", "selector", "all"]
"""How the authoring surface picked this device:

* `trait` — it claimed a capability the author named
* `device` — it was named directly (or a collect commanded it)
* `instrument` — it is the instrument terminating an optical path
* `selector` — it is the device routing a selector's ports
* `all` — it is a device, and that was the whole criterion

The last three are structural: the author named a kind, not a label, so `Op.trait`
is `None` and there is nothing in the deployment's trait vocabulary standing for
them."""

STRUCTURAL_MATCHES = frozenset({"instrument", "selector"})
"""The kinds a dispatcher can key on that are not trait claims."""


@dataclass(frozen=True)
class Op:
    """Node payload: one action on one device."""

    ref: DeviceRef
    op: str
    params: Mapping[str, object] = field(default_factory=dict)
    trait: Trait | None = None       # set only when match == "trait"
    traits: tuple[Trait, ...] = ()   # every trait this ref claims, sorted
    path: InstrumentPath = ()
    match: Match = "device"

    def describe(self) -> str:
        what = self.trait or (
            self.match if self.match in STRUCTURAL_MATCHES else "device")
        return f"{self.ref} ({what} @ {'/'.join(self.path) or '<root>'})"


@dataclass
class RunContext:
    """Per-run state, shared by every op of one run."""

    name: str                        # the table's or collect's name
    graph: Graph
    abort: AbortSignal | None        # consult, don't poll
    state: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class OpContext:
    """The hook's entire argument."""

    op: Op
    node: Node                       # .group is the phase or step
    run: RunContext


type OpHook = Callable[[OpContext], Awaitable[object]]
"""The integrator's half of the API: perform one action, return whatever it
measured.

Raising means the step failed; the workflow's `on_failure` decides what that costs.
Cancellation means the run is going away — `ctx.run.abort` says whether that is a
domain abort — and must not be swallowed."""
