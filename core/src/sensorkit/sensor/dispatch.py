# SPDX-License-Identifier: Apache-2.0
"""Send planned commands, sample acquisition headers and interrupt dropped
calls.

Operations already contain resolved timeouts and command parameters. For an
acquisition, dispatch samples a fresh header from base context, device contexts
along the chain, then planned keywords. It adds the header to a copy of the
acquisition command.

Record attempts immediately before sending; failures before this point do not
arm cleanup. A cancelled or timed-out call triggers one built-in Abort attempt
when supported. Record its result separately from the operation's outcome. An
acknowledgement confirms a response, not that the hardware stopped.

The dispatcher owns and drains both call tasks before releasing its caller.
Abort waits up to five seconds for a response, then drains local teardown;
caller cancellation does not interrupt that attempt. Operation events go to
direct logging and an optional observer with bounded subscriber queues.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from loguru import logger

import sensorkit.api as sk
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.dag import Node
from sensorkit.core.device import Abort, DeviceClient, DeviceCommand
from sensorkit.data.context import merge_contexts
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.topology import DeviceKey
from sensorkit.sensor.workflow import Operation

type DeviceContexts = Callable[[], Mapping[DeviceKey, sk.Context]]
"""Callable supplying current device contexts, sampled for each acquisition."""

type OperationOutcome = Literal["begin", "ok", "failed", "cancelled"]

type InterruptionOutcome = Literal["acknowledged", "unsupported", "failed", "timed_out"]

ABORT_TIMEOUT_S = 5.0
"""Abort response timeout in seconds, excluding local call teardown."""


type AttemptRecorder = Callable[[Operation], None]
"""Callback recording an operation immediately before its device call
starts.

`ExecutionState.attempted.add` implements it for cleanup arming by identity.
"""

type InterruptionRecorder = Callable[[Interruption], None]
"""Callback storing one Abort outcome before dispatch returns or raises.

Caller-owned storage retains the outcome when cancellation propagates.
"""


@dataclass(frozen=True)
class OperationEvent:
    """A begin or terminal event for an attempted operation.

    Each attempted operation emits begin followed by ok, failed or cancelled.
    `detail` is the return value for ok, the exception for failed, or `None`.
    """

    operation: Operation
    node: Node
    outcome: OperationOutcome
    detail: object = None


@dataclass(frozen=True)
class Interruption:
    """Result of the built-in Abort attempt for a dropped call.

    Acknowledged means the device answered, not that motion stopped.
    Unsupported means nothing was sent. `error` holds the Abort exception
    when available.
    """

    outcome: InterruptionOutcome
    error: BaseException | None = None


class Dispatcher:
    """Send operations using one run's header sources.

    The bound sensor, clients and observer may be shared across runs; construct
    a dispatcher per run to keep base and sampled contexts separate.
    """

    def __init__(
        self,
        sensor: BoundSensor,
        clients: Mapping[DeviceKey, DeviceClient],
        *,
        contexts: DeviceContexts | None = None,
        base: sk.Context | None = None,
        events: AsyncObserver[OperationEvent] | None = None,
    ):
        self.sensor = sensor
        self.clients = clients
        self.contexts = contexts
        self.base = base
        self.events = events

    async def perform(
        self,
        operation: Operation,
        node: Node,
        attempt: AttemptRecorder,
        interrupted: InterruptionRecorder,
    ) -> object:
        """Send an operation under its resolved timeout and drain its call
        task.

        Record the attempt before sending. A dropped call triggers Abort; its
        outcome goes to `interrupted` without replacing the operation's
        failure. Held caller cancellation takes precedence over a timeout after
        local teardown completes.

        Args:
            operation: Planned command, target and timeout.
            node: Graph node included in operation events.
            attempt: Callback recording that dispatch is starting.
            interrupted: Callback storing the Abort outcome, including when
                the device does not support Abort.

        Returns:
            The device command's result.

        Raises:
            TimeoutError: The command exceeds its timeout or the transport
                times out.
            LookupError: No client exists for the target; nothing is sent.
            asyncio.CancelledError: Cancellation propagates after local tasks
                drain.
            Exception: A command or header-building error propagates.
        """
        device = operation.target.device
        client = self.clients.get(device)

        if client is None:
            raise LookupError(f"no client for device '{device}'")

        command = self.outgoing(operation)

        async def send() -> object:
            return await client.command(command)

        attempt(operation)
        self.notify(OperationEvent(operation, node, "begin"))
        sending = asyncio.create_task(send())
        expired, cancelled = await _owned(sending, operation.timeout_s, interruptible=True)

        # Handle Abort outside the command wait to avoid recursive interruption.
        if _dropped(sending):
            try:
                await self.abort(device, interrupted)
            except asyncio.CancelledError as e:
                cancelled = cancelled or e

        return self._concluded(operation, node, sending, expired, cancelled)

    def outgoing(self, operation: Operation) -> DeviceCommand:
        """Return the command to send, adding a fresh header for an
        acquisition.

        Acquisition commands are deep-copied; ordinary commands are returned
        unchanged.
        """
        if operation.acquisition is None:
            return operation.command

        # Give each acquisition its own context without modifying the planned command.
        return operation.command.model_copy(deep=True, update={"context": self.header(operation)})

    def header(self, operation: Operation) -> sk.Context:
        """Build a fresh acquisition context with sources in precedence
        order.

        Merge base context, device contexts from root to instrument, then
        planned acquisition keywords. Deeper publishers override earlier ones;
        planned keywords take final precedence.

        Raises:
            KeyError: The target is not an instrument in this topology.
        """
        # Frame numbering is already included in planned acquisition keywords.
        reported = self.contexts() if self.contexts is not None else {}
        chain = self.sensor.topology.chain(operation.target)
        planned = operation.acquisition.keywords if operation.acquisition is not None else None

        return merge_contexts(self.base, *(reported.get(p.device) for p in chain), planned)

    async def abort(self, device: DeviceKey, record: InterruptionRecorder) -> Interruption:
        """Attempt built-in Abort once, recording the outcome before
        returning or raising.

        Unsupported devices receive nothing. Failures become interruption
        results; no fallback command or recursive Abort is sent. Wait up to
        `ABORT_TIMEOUT_S` for a response, then drain local teardown. Hold
        caller cancellation until the attempt ends.

        Args:
            device: Target of the interrupted call.
            record: Callback storing the interruption result.

        Returns:
            The interruption result also passed to `record`.

        Raises:
            asyncio.CancelledError: The caller was cancelled during the
                attempt.
        """
        if Abort.model_tag() not in self.sensor.supported_commands(device):
            logger.warning(f"{device}: interrupted, and it has no Abort")
            record(interruption := Interruption("unsupported"))
            return interruption

        async def abort() -> object:
            return await self.clients[device].command(Abort())

        # Send directly so Abort failure cannot trigger another Abort.
        attempt = asyncio.create_task(abort())
        expired, cancelled = await _owned(attempt, ABORT_TIMEOUT_S, interruptible=False)

        if expired:
            interruption = Interruption("timed_out")
            logger.warning(f"{device}: Abort unanswered after {ABORT_TIMEOUT_S}s")
        elif attempt.cancelled():
            # An unexpired attempt was cancelled externally, such as by loop teardown.
            interruption = Interruption("failed")
            logger.warning(f"{device}: Abort cancelled")
        elif (error := attempt.exception()) is not None:
            interruption = Interruption("failed", error)
            logger.warning(f"{device}: Abort failed ({error})")
        else:
            interruption = Interruption("acknowledged")
            logger.info(f"{device}: Abort acknowledged")

        # Preserve held cancellation even if recording fails.
        try:
            record(interruption)
        finally:
            if cancelled is not None:
                raise cancelled

        return interruption

    def notify(self, event: OperationEvent) -> None:
        """Log an operation event and notify observers without waiting on
        subscribers.

        Observer failures are logged rather than propagated into dispatch.
        """
        where = f"{event.operation.id}: {event.node.label.strip()}"

        match event.outcome:
            case "begin":
                logger.info(where)
            case "ok":
                logger.debug(f"{where}: done")
            case "failed":
                logger.warning(f"{where}: failed ({event.detail})")
            case "cancelled":
                logger.warning(f"{where}: cancelled")

        if self.events is None:
            return

        try:
            self.events.notify(event)
        except Exception as e:
            logger.warning(f"{where}: an operation subscriber failed ({e!r})")

    def _concluded(
        self,
        operation: Operation,
        node: Node,
        sending: asyncio.Task,
        expired: bool,
        cancelled: asyncio.CancelledError | None,
    ) -> object:
        """Emit the call's terminal event, then return its result or
        propagate failure.

        A held caller cancellation takes precedence even if the call ended
        otherwise.
        """
        match sending.cancelled(), expired:
            case True, True if cancelled is None:
                error: BaseException | None = TimeoutError(
                    f"'{operation.id}' did not finish within {operation.timeout_s}s"
                )
            case True, _:
                error = cancelled or asyncio.CancelledError()
            case _:
                error = sending.exception()

        match error:
            case None:
                self.notify(OperationEvent(operation, node, "ok", sending.result()))
            case asyncio.CancelledError():
                self.notify(OperationEvent(operation, node, "cancelled"))
            case _:
                self.notify(OperationEvent(operation, node, "failed", error))

        if cancelled is not None:
            raise cancelled

        if error is not None:
            raise error

        return sending.result()


async def _owned(
    task: asyncio.Task, seconds: float | None, *, interruptible: bool
) -> tuple[bool, asyncio.CancelledError | None]:
    """Wait for an owned task, cancelling it once when the initial wait
    ends.

    For interruptible tasks, caller cancellation ends the initial wait.
    Otherwise wait until completion or timeout. Drain afterward without a
    separate timeout, retaining the first caller cancellation and its
    cancellation count.

    Returns:
        Whether the task expired and the first caller cancellation, if any.
    """
    expired, cancelled = await _until_stopped(task, seconds, interruptible)
    task.cancel()

    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as e:
            cancelled = cancelled or e

    return expired, cancelled


async def _until_stopped(
    task: asyncio.Task, seconds: float | None, interruptible: bool
) -> tuple[bool, asyncio.CancelledError | None]:
    """Wait for completion, timeout or caller cancellation when
    interruptible.

    Returns:
        Whether the task expired and the first caller cancellation, if any.
    """
    loop = asyncio.get_running_loop()
    expiry = None if seconds is None else loop.time() + seconds
    cancelled: asyncio.CancelledError | None = None

    while not task.done():
        remaining = None if expiry is None else expiry - loop.time()

        try:
            await asyncio.wait({task}, timeout=remaining)
        except asyncio.CancelledError as e:
            cancelled = cancelled or e

            if interruptible:
                break

            continue

        if not task.done():
            return True, cancelled

    return False, cancelled


def _dropped(sending: asyncio.Task) -> bool:
    """Test whether the local call was cancelled or ended with a timeout.

    A transport timeout can leave work running on the device.
    """
    return sending.cancelled() or isinstance(sending.exception(), TimeoutError)
