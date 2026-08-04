# SPDX-License-Identifier: Apache-2.0
"""The op hook: performing one workflow op against SensorKit devices.

A compiled graph says *what* to do to *which* device; this is the half that does
it. Both compilers reach it through the one `workflow.OpHook`, so the filter wheel
that homes during init is the same object, reached the same way, that applies a
filter during a collect.

**An op is a command.** `DeviceCommand.registry` is a name -> model registry, and
the archetypes a device claims are defined as the commands it supports, so a phase
table's op names are command ids and need no vocabulary of their own: `Init`
resolves through the registry to `std.traits.Init`, `OpenEnclosure` to
`std.enclosure.OpenEnclosure`. A collect's two verbs are the exception, and only
because they carry more than a name — `apply` already holds the command it means,
and `expose` builds one around the frame's metadata.

**Resolution is a ladder keyed on `Op.match`**, most specific first, so a
deployment overrides one device's quirk without displacing anything else:

| rung | when |
|---|---|
| `(ref, op)` | always — a named device beats every capability it claims |
| `(trait, op)` | `match="trait"`: the table named the capability, so use it |
| `(trait, op)` for each claimed trait | `match` in `device`/`all` |
| `(kind, op)` | `match` in `instrument`/`selector`: no trait to walk |
| `(None, op)` | the default, and the reason most tables register nothing |

Timeouts take the same ladder, because a timeout is a property of hardware rather
than of a workflow: it belongs to *this dome*, not to the table that happens to
open it. So deadlines arrive as a table keyed for that ladder, assembled by the
caller from whatever its site states, rather than as table content.

**Cancellation is load-bearing.** Dropping an `ExtendedCall` does not reach the
device, so an op cancelled mid-move leaves hardware moving. Every cancelled op
therefore shield-sends the strongest halt its device claims before letting the
cancellation through.

**An op no device can perform is neither a success nor a failure.** Nothing was
attempted, so recording it as failed would make `RunReport.failures` mean less than
it did when a handler tested for a rejection by hand — and marking the op
`optional` instead would tolerate genuine failures too. `compile_supported` answers
for those ops at compile time, so they appear in a dry run as what they are.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping, Sequence
from typing import Literal

from loguru import logger

import sensorkit.api as sk
from sensorkit.core.device import Abort, DeviceClient, DeviceCommand
from sensorkit.sensor.compat import add_compat_context
from sensorkit.std.instrument import AcquireData, CameraCapture
from sensorkit.std.traits import Stop
from sensorkit.workflow import (
    OP_APPLY,
    OP_EXPOSE,
    STRUCTURAL_MATCHES,
    CapabilityIndex,
    DeviceIndex,
    DeviceRef,
    Graph,
    Op,
    OpContext,
    Override,
    PhaseTable,
    SensorPlan,
    capabilities_of,
    compile_table,
)

type DeviceContexts = Callable[[], Mapping[DeviceRef, sk.Context]]
"""What each device has to say about itself, as of now.

Sampled once per frame rather than passed once per collect, because a header
records the state a frame was taken in and pointing moves between frames. The merge
into a single context is the dispatcher's, not the caller's: it follows each
instrument's own optical path, root first, so that a frame's header carries the
devices that actually shaped it and nothing else.
"""

type FrameKeywords = Callable[[OpContext, int], Iterable[object]]
"""What a collect has to say about one frame, given the exposure's op and the
frame's number within the run.

The seam a task translation fills: the graph knows which exposure a node belongs
to, and the translation knows what that exposure was asked for."""

type Handler = Callable[[OpContext], Awaitable[object]]
"""Perform one op. Registered against a rung of the resolution ladder."""

type HandlerKey = tuple[str | None, str]
"""A rung: a device ref, a trait, a structural kind or None, against an op name."""

type OpOutcome = Literal["begin", "ok", "failed", "cancelled"]

type Listener = Callable[[OpContext, OpOutcome, object], None]
"""Watch ops as they resolve. The third argument is whatever the op returned on
`ok` and the exception on `failed`; nothing on the other two.

Called from the dispatch path, so a listener must not raise and must not block."""

FRAME_NUMBERS = "frame_numbers"
"""Run-scoped key under which each instrument's frame counter is kept.

Per run rather than per dispatcher, and per instrument rather than per graph, so
that a collect split into several steps still numbers its frames the way a header
reads them: monotonically, from the start of the collect."""

HALT_COMMANDS: tuple[type[DeviceCommand], ...] = (Abort, Stop)
"""What a cancelled op sends its device, strongest first."""


def unresolved(capabilities: CapabilityIndex,
               graph: Graph) -> tuple[tuple[DeviceRef, str], ...]:
    """The (device, op) pairs in a lifecycle graph the device cannot perform.

    Every op in a phase table is a registered command id, so what a device publishes
    it supports answers the question before the run asks it. Collect graphs are not
    asked: their verbs are `apply` and `expose`, and the resolver has already routed
    them to devices that claim the commands.
    """
    return tuple(dict.fromkeys(
        (op.ref, op.op) for n in graph.nodes
        if isinstance(op := n.payload, Op)
        and op.op not in capabilities_of(capabilities, op.ref).commands))


def unsupported(capabilities: CapabilityIndex,
                graph: Graph) -> tuple[Override, ...]:
    """Rules answering for the ops `unresolved` names."""
    return tuple(
        Override(device=ref, ops=(op,), outcome="skipped",
                 reason=f"{ref} does not support {op}")
        for ref, op in unresolved(capabilities, graph))


def compile_supported(devices: DeviceIndex, table: PhaseTable,
                      capabilities: CapabilityIndex,
                      overrides: Sequence[Override] = ()) -> Graph:
    """Compile a phase table against what its devices report they can do.

    Two passes, because which devices an entry selects is what compilation decides:
    the first says which ops the table would dispatch, and the second answers for
    the ones that would be refused.

    Capability rules go ahead of the caller's, which is the one place this layer
    takes precedence over an operator: what a device cannot do is a fact, and a rule
    recording `ok` over it would claim work that never happened.
    """
    first = compile_table(devices, table, overrides)
    return compile_table(devices, table,
                         (*unsupported(capabilities, first), *overrides))


def log_ops(ctx: OpContext, outcome: OpOutcome, detail: object) -> None:
    """The listener every dispatcher carries: one line per op transition.

    What the hand-written sensor got from its `logger.info` calls, minus having to
    remember to write them.
    """
    where = f"[{ctx.run.name}] {ctx.node.group}: {ctx.node.label.strip()}"

    match outcome:
        case "begin":
            logger.info(where)
        case "ok":
            logger.debug(f"{where}: done")
        case "failed":
            logger.warning(f"{where}: failed ({detail})")
        case "cancelled":
            logger.warning(f"{where}: cancelled")


def no_keywords(ctx: OpContext, number: int) -> Iterable[object]:
    """What a collect with nothing to say about its frames contributes to them."""
    return ()


class Dispatcher:
    """Perform workflow ops against a sensor's devices.

    An instance is the `workflow.OpHook` both runners take, and holds nothing that
    belongs to one run: frame numbering lives in the `RunContext`, so the same
    dispatcher serves any number of lifecycle runs and collects.

    The collect-facing arguments are what a frame's header is assembled from, and a
    lifecycle-only dispatcher needs none of them. `deadlines` is keyed for the
    resolution ladder; an op no key answers for runs to completion.
    """

    def __init__(self, plan: SensorPlan,
                 clients: Mapping[DeviceRef, DeviceClient],
                 capabilities: CapabilityIndex,
                 deadlines: Mapping[HandlerKey, float] | None = None, *,
                 contexts: DeviceContexts | None = None,
                 base: sk.Context | None = None,
                 frame_keywords: FrameKeywords = no_keywords,
                 listeners: Sequence[Listener] = ()):
        self.plan = plan
        self.clients = clients
        self.capabilities = capabilities
        self.deadlines = deadlines or {}
        self.contexts = contexts
        self.base = base
        self.frame_keywords = frame_keywords
        self.listeners = (log_ops, *listeners)
        self.handlers: dict[HandlerKey, Handler] = {
            (None, OP_APPLY): self.apply,
            ("instrument", OP_EXPOSE): self.expose,
        }

    async def __call__(self, ctx: OpContext) -> object:
        self._notify(ctx, "begin", None)

        try:
            value = await self._perform(ctx)
        except asyncio.CancelledError:
            # The run is going away and the device does not know that, so tell it
            # before letting the cancellation through.
            await self.halt(ctx.op.ref)
            self._notify(ctx, "cancelled", None)
            raise
        except Exception as e:
            self._notify(ctx, "failed", e)
            raise

        self._notify(ctx, "ok", value)
        return value

    def resolve(self, op: Op) -> Handler:
        """The handler for an op: the most specific rung registered, else the
        command the op names."""
        for key in self.rungs(op):
            if (handler := self.handlers.get(key)) is not None:
                return handler

        return self.command

    def rungs(self, op: Op) -> Iterator[HandlerKey]:
        """The keys an op resolves through, most specific first.

        Branching on `match` rather than on whether a trait is set is what keeps the
        structural rungs reachable: an instrument claims traits too, and walking them
        would answer for `expose` with a filter changer's handler.
        """
        yield (op.ref, op.op)

        match op.match:
            case "trait":
                yield (op.trait, op.op)
            case "device" | "all":
                yield from ((trait, op.op) for trait in op.traits)
            case kind if kind in STRUCTURAL_MATCHES:
                yield (kind, op.op)

        yield (None, op.op)

    def deadline(self, op: Op) -> float | None:
        """How long this op has, or None to run to completion."""
        return next((d for key in self.rungs(op)
                     if (d := self.deadlines.get(key)) is not None), None)

    def command_for(self, op: Op) -> DeviceCommand:
        """The command an op names.

        The op vocabulary is the command registry, so a table naming a command a
        module registered reaches it with nothing authored in between.

        Raises:
            LookupError: The op names no command, or an ambiguous one.
        """
        registry = DeviceCommand.registry
        namespaces = registry.get_namespaces(op.op)

        match namespaces:
            case ():
                raise LookupError(f"'{op.op}' is not a registered device command")
            case (only,) if only is not None:
                model = registry.get_type(op.op, only)
            case _ if None in namespaces:
                # The standard vocabulary wins the name it declared.
                model = registry.get_type(op.op)
            case _:
                raise LookupError(
                    f"'{op.op}' is registered by several modules {namespaces}; "
                    f"a table cannot say which")

        return model.model_validate(dict(op.params))

    async def command(self, ctx: OpContext) -> object:
        """The default rung: send the command the op names."""
        return await self.send(ctx.op.ref, self.command_for(ctx.op))

    async def apply(self, ctx: OpContext) -> object:
        """A collect's `apply`: the setting is already the command to send."""
        command = ctx.op.params["value"]

        if not isinstance(command, DeviceCommand):
            raise TypeError(
                f"{ctx.op.ref}: apply carries {type(command).__name__}, "
                f"not a DeviceCommand")

        return await self.send(ctx.op.ref, command)

    async def expose(self, ctx: OpContext) -> object:
        """A collect's `expose`: one frame, with the header it was taken under."""
        op = ctx.op
        number = self.frame_number(ctx)
        context = self.frame_context(ctx, number)
        commands = capabilities_of(self.capabilities, op.ref).commands

        # A camera exposes for a time; anything else acquires for as long as it
        # takes, which is the whole of the difference at this layer.
        command: DeviceCommand = (
            CameraCapture(integration_time=float(op.params["exposure_s"]),
                          context=context)
            if CameraCapture.model_tag() in commands
            else AcquireData(action="acquire", context=context))

        return await self.send(op.ref, command)

    def frame_context(self, ctx: OpContext, number: int) -> sk.Context:
        """The header one frame is taken under.

        Built per node and never persisted, so two instruments exposing at once
        cannot hand each other their snapshots. The devices are those on this
        instrument's own optical path, merged root-first so the deepest publisher
        wins — the same merge, in the same order, that a capability predicate reads,
        so a frame records the values it was selected and configured on.
        """
        context = sk.Context(self.base)
        available = self.contexts() if self.contexts is not None else {}

        for ref in self.plan.devices.chain(ctx.op.path):
            if (reported := available.get(ref)) is not None:
                context.update(reported)

        context.set(*self.frame_keywords(ctx, number))
        add_compat_context(context)

        return context

    def frame_number(self, ctx: OpContext) -> int:
        """This instrument's next frame number within the run.

        `FramePlan` numbers frames within a block, so a collect split into steps
        would restart at zero and a header would say so.
        """
        counters: dict[DeviceRef, int] = ctx.run.state.setdefault(FRAME_NUMBERS, {})
        number = counters.get(ctx.op.ref, 0)
        counters[ctx.op.ref] = number + 1

        return number

    async def send(self, ref: DeviceRef, command: DeviceCommand) -> object:
        client = self.clients.get(ref)

        if client is None:
            raise LookupError(f"no client for device '{ref}'")

        return await client.command(command)

    async def halt(self, ref: DeviceRef) -> None:
        """Stop whatever a cancelled op left moving.

        Best effort by construction: this runs while the run is being torn down, so
        a device that refuses, or a second cancellation arriving mid-send, must not
        displace the cancellation being handled.
        """
        commands = capabilities_of(self.capabilities, ref).commands
        command = next((c() for c in HALT_COMMANDS
                        if c.model_tag() in commands), None)
        client = self.clients.get(ref)

        if command is None or client is None:
            return

        # Shielded because the caller is already cancelled: an unshielded send would
        # be cancelled before it left.
        with contextlib.suppress(BaseException):
            await asyncio.shield(client.command(command))

    def _notify(self, ctx: OpContext, outcome: OpOutcome, detail: object) -> None:
        for listener in self.listeners:
            listener(ctx, outcome, detail)

    async def _perform(self, ctx: OpContext) -> object:
        handler = self.resolve(ctx.op)
        deadline = self.deadline(ctx.op)

        if deadline is None:
            return await handler(ctx)

        async with asyncio.timeout(deadline):
            return await handler(ctx)
