# SPDX-License-Identifier: Apache-2.0
"""Reconcile controller demand with reports from one monitor session."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from loguru import logger

from sensorkit.auto.procedures import (
    ControllerFailure,
    ControllerIndeterminate,
    ControllerOffline,
    ControllerUnrecoverable,
    DemandState,
    LifecycleProcedure,
    LifecycleStep,
    RecoverProcedure,
)
from sensorkit.common.aio import cleanup_future, defer_cancellation
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerMonitor,
    ControllerOperatingState,
    ControllerStateUnknown,
    InternalControllerState,
)
from sensorkit.core.task import TaskExecution

if TYPE_CHECKING:
    import uuid

    from sensorkit.auto.lifecycle import ControllerLifecycle


@dataclass(slots=True)
class RecoveryState:
    """Track controller failures and schedule retries, recovery, and emergency shutdown.

    Two failures trigger recovery unless the controller stayed settled for the stable period
    between them. Exhausting recovery attempts triggers an emergency shutdown. Each procedure
    counts at most one failure, so one incident seen by several observers counts once.
    """

    retry_delay: float = 5.0
    """Seconds before retrying after a failure."""
    recovery_attempts: int = 6
    """Recovery attempts before an emergency shutdown."""
    attempt_delay: float = 20.0
    """Seconds between failed recovery attempts."""
    cycle_delay: float = 120.0
    """Seconds after a failed emergency shutdown before recovery resumes."""
    stable_period: float = 900.0
    """Seconds a confirmed state must hold before a new failure starts a fresh count."""
    failures: int = field(default=0, init=False)
    """Failures counted toward recovery. Successful recovery resets this to one."""
    attempts: int = field(default=0, init=False)
    """Failed recovery attempts in the current recovery cycle."""
    retry_at: float | None = field(default=None, init=False)
    """Earliest event loop time for the next retry or recovery attempt."""
    failure: BaseException | None = field(default=None, init=False)
    """First controller failure counted for the current procedure, if any."""
    settled_at: float | None = field(default=None, init=False)
    """Event loop time when the current settled interval began."""

    @property
    def recovery_due(self) -> bool:
        """Whether the next command is a recovery attempt."""
        return self.failures >= 2

    @property
    def emergency_due(self) -> bool:
        """Whether the next recovery attempt is an emergency shutdown."""
        return self.attempts >= self.recovery_attempts

    def waiting(self, now: float) -> float | None:
        """Return the pending retry deadline in event loop time, or None if no delay remains."""
        if self.retry_at is not None and now < self.retry_at:
            return self.retry_at

        return None

    def begin_procedure(self):
        """Clear the current procedure's failure without clearing accumulated counts."""
        self.failure = None

    def record_initial_error(self):
        """Count a controller error found at startup once, without delaying the first command."""
        if self.failures == 0:
            self.failures = 1
            self.settled_at = None

    def record_failure(self, now: float, error: BaseException):
        """Record and count the first controller failure for the current procedure."""
        if self.failure is not None:
            return

        self.failure = error
        self.set_settled(False, now)
        self.failures += 1

        # Only the first failure schedules a retry delay.
        if not self.recovery_due:
            self.retry_at = now + self.retry_delay

    def set_settled(self, settled: bool, now: float):
        """Track continuous settled time and reset failure counts when earned."""
        if settled:
            if self.settled_at is None:
                self.settled_at = now
            return

        if self.settled_at is not None:
            if now - self.settled_at >= self.stable_period:
                self.failures = 0
            self.settled_at = None

    def delay_retry(self, now: float):
        """Delay the next command without counting a controller failure."""
        self.retry_at = now + self.retry_delay

    def record_recovery(self, now: float):
        """Record a successful recovery.

        Another failure before the stable period ends triggers recovery again.
        """
        self.failures = 1
        self.attempts = 0
        self.retry_at = now + self.retry_delay

    def record_failed_attempt(self, now: float):
        """Count a failed recovery attempt."""
        self.attempts += 1

        # Exhausted attempts fall back to emergency shutdown without waiting.
        if not self.emergency_due:
            self.retry_at = now + self.attempt_delay

    def record_failed_emergency(self, now: float):
        """Resume recovery attempts after a delay."""
        self.attempts = 0
        self.retry_at = now + self.cycle_delay

    def clear(self):
        """Reset escalation counters and timing after emergency shutdown."""
        self.failures = 0
        self.attempts = 0
        self.retry_at = None
        self.settled_at = None


class ControllerReactor:
    """Run lifecycle procedures one at a time using a controller monitor.

    Each procedure finishes cleanup before its replacement starts. Controller reports and
    delays for external changes reset when the reactor restarts. Recovery state persists
    in the lifecycle.
    """

    def __init__(self, lifecycle: ControllerLifecycle, monitor: ControllerMonitor):
        """Create a reactor for a started monitor.

        Args:
            lifecycle: The lifecycle supplying demand, enablement, and recovery state.
            monitor: A controller monitor whose start method has completed.
        """
        self.lifecycle = lifecycle
        self.monitor = monitor
        self.controller = monitor.controller
        self.recovery_state = lifecycle.recovery_state
        self.step = LifecycleStep.WAIT
        self.achieved_once = False
        """Whether a procedure commanded the demanded state during this reactor's lifetime."""
        self._wake = asyncio.Event()
        self._closed = False
        self._fresh: ControllerOperatingState | None = None
        self._procedure: LifecycleProcedure | None = None
        self._achieved: tuple[InternalControllerState, KeywordDict] | None = None
        self._hold_state: (
            tuple[tuple[InternalControllerState | None, KeywordDict, str | None], float] | None
        ) = None
        self._settle_wait: tuple[uuid.UUID, float] | None = None

    def wake(self):
        """Evaluate the lifecycle's inputs again."""
        self._wake.set()

    async def run(self):
        """Reconcile demand while the lifecycle is enabled.

        On exit, abort the current procedure and wait for its cleanup.

        Raises:
            ControllerStateUnknown: If observation failed.
            ControllerOffline: If the controller lost its lease.
            ControllerIndeterminate: If a procedure left work that may still be running.
            ControllerUnrecoverable: If an emergency shutdown succeeded.
        """
        if self.monitor.operating_state.current == InternalControllerState.ERROR:
            self.recovery_state.record_initial_error()

        try:
            with self.monitor.state_changes(maxsize=1, overflow="drop_oldest") as changes:
                # Read current demand and reports on every wakeup.
                while True:
                    self._wake.clear()
                    deadline = self._evaluate()

                    # Evaluation can conclude the last procedure of a disabled lifecycle.
                    if not self.lifecycle.enabled and self._procedure is None:
                        return

                    await self._wait(changes, deadline)
        finally:
            await self._release()

    async def _wait(
        self,
        changes: asyncio.Queue[ControllerOperatingState],
        deadline: float | None,
    ):
        """Wait for a wakeup, controller report, procedure completion, or policy deadline."""
        loop = asyncio.get_running_loop()

        # Reports only wake evaluation, which reads the latest state.
        waiters = [asyncio.ensure_future(self._wake.wait()), asyncio.ensure_future(changes.get())]

        # Cancel only local waiters. The procedure manages its own cleanup.
        borrowed = [] if self._procedure is None else [self._procedure.future()]
        timeout = None if deadline is None else max(0.0, deadline - loop.time())

        try:
            await asyncio.wait(
                [*waiters, *borrowed], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for waiter in waiters:
                waiter.cancel()

            await asyncio.wait(waiters)

            # Retrieve waiter errors; the next evaluation detects a failed monitor.
            for waiter in waiters:
                cleanup_future(waiter)

    def may_dispatch(self, procedure: LifecycleProcedure) -> bool:
        """Return whether a procedure may start another operation.

        Observation must be healthy and the lifecycle enabled. Recovery remains permitted
        across demand changes. Other procedures must match the latest demand, including one the
        reactor has not evaluated yet.
        """
        if self._closed or not self.lifecycle.enabled:
            return False

        # Observation can fail before the reactor evaluates that failure.
        try:
            self.observed()
        except ControllerStateUnknown:
            return False

        if isinstance(procedure, RecoverProcedure):
            return True

        return procedure.demand == self.lifecycle.demand_state

    def observed(self) -> ControllerOperatingState:
        """Return the latest operating state while the controller monitor is healthy.

        A state read after a lifecycle task can be newer than the monitor's report.

        Raises:
            ControllerStateUnknown: If observation failed or stopped.
        """
        monitored = self.monitor.operating_state
        fresh = self._fresh

        # Reports from one controller are ordered by their UUIDv7 event IDs.
        if fresh is not None and fresh.event_id.int > monitored.event_id.int:
            return fresh.model_copy(deep=True)

        self._fresh = None
        return monitored

    def observe(self, operating: ControllerOperatingState):
        """Retain a directly read operating state until the monitor catches up."""
        fresh = self._fresh

        if fresh is None or operating.event_id.int > fresh.event_id.int:
            self._fresh = operating.model_copy(deep=True)

    def running_execution(self) -> TaskExecution | None:
        """Return the execution the controller last reported running, if any.

        Raises:
            ControllerStateUnknown: If observation failed or stopped.
        """
        state = self.monitor.state.execution_state
        return state.execution if state.executing else None

    def record_achieved_state(
        self,
        state: InternalControllerState,
        context: KeywordDict,
        *,
        commanded: bool,
    ):
        """Record the controller state and lifecycle context confirmed by a procedure.

        This ends any delay for external changes. Only a state the procedure commanded clears
        the recovery state, since a matching report does not show that a failure was resolved.
        """
        self._achieved = (state, copy.deepcopy(context))
        self._clear_hold()

        if commanded:
            self.achieved_once = True
            self.recovery_state.clear()

    def _program_interruptible(self) -> bool:
        """Return whether a program's interrupt policy may abort the current procedure.

        The controller must report OPERATE with no transition or lifecycle task call pending.
        """
        if self._procedure is not None and self._procedure.pending():
            return False

        actual = self.observed()
        return actual.current == InternalControllerState.OPERATE and actual.target is None

    def _evaluate(self) -> float | None:
        """Reconcile demand and reported state, returning the next event loop deadline, if any."""
        demand = self.lifecycle.demand_state

        if self._procedure is not None and self._procedure.done():
            procedure = self._procedure
            self._procedure = None
            self._conclude(procedure)

        actual = self.observed()
        procedure = self._procedure

        if procedure is not None:
            match self._interrupt_kind(procedure, demand, actual):
                case "abort":
                    procedure.request_abort()
                case "stop":
                    procedure.request_stop()

            return None

        # A withdrawn demand ends its hold.
        if demand.state is None:
            self._clear_hold()

        if not self.lifecycle.enabled or demand.state is None:
            return None

        return self._dispatch(demand, actual)

    def _interrupt_kind(
        self,
        procedure: LifecycleProcedure,
        demand: DemandState,
        actual: ControllerOperatingState,
    ) -> Literal["stop", "abort"] | None:
        """Return how the running procedure must end, if it must.

        Unexpected state or lifecycle context changes start a hold unless the controller reports
        ERROR or UNKNOWN.
        """
        if not self.lifecycle.enabled or demand.state is None:
            return "abort"

        # Recovery may be needed to reach any demand, including shutdown.
        if isinstance(procedure, RecoverProcedure):
            return None

        if procedure.demand != demand:
            if demand.interrupt or (demand.program_interrupt and self._program_interruptible()):
                return "abort"

            return "stop"

        # Let the procedure finish its lifecycle calls before comparing reported state.
        achieved = self._achieved

        if not procedure.achieved or procedure.pending() or achieved is None:
            return None

        if actual.matches(*achieved):
            return None

        match actual.current:
            case InternalControllerState.ERROR | InternalControllerState.UNKNOWN:
                return "abort"
            case _:
                # The hold applies even if demand changes while tasking stops.
                self._hold(procedure.demand)
                return "stop"

    def _dispatch(self, demand: DemandState, actual: ControllerOperatingState) -> float | None:
        """Start recovery or the demanded procedure, or return the deadline delaying it."""
        now = asyncio.get_running_loop().time()
        self._classify(demand, actual, now)
        recovery_state = self.recovery_state

        # Recovery skips delays for external changes and handles running tasks in its procedure.
        if recovery_state.recovery_due:
            if (until := recovery_state.waiting(now)) is not None:
                return until

            logger.error(f"{self.controller.entity} failed repeatedly, starting recovery")
            self._start(RecoverProcedure(self, demand, emergency=recovery_state.emergency_due))
            return None

        # Shutdown bypasses retry delays and holds.
        if demand.state != InternalControllerState.SHUTDOWN:
            if (until := recovery_state.waiting(now)) is not None:
                return until

            if (until := self._held(demand, actual, now)) is not None:
                return until

        if (until := self._settling(demand, actual, now)) is not None:
            return until

        self._start(self.lifecycle.procedures[demand.state](self, demand))
        return None

    def _settling(
        self,
        demand: DemandState,
        actual: ControllerOperatingState,
        now: float,
    ) -> float | None:
        """Return when to stop waiting for a running lifecycle task that matches the demand.

        Procedures abort any other running work. The wait ends at the execution's expiry or
        after the lifecycle's settle timeout, whichever comes first.
        """
        execution = self.running_execution()

        if (
            execution is None
            or actual.current != InternalControllerState.TRANSITION
            or actual.target != demand.state
            or actual.context != demand.lifecycle_context()
        ):
            return None

        task_id = execution.task_id

        if self._settle_wait is None or self._settle_wait[0] != task_id:
            self._settle_wait = (task_id, now)

        until = self._settle_wait[1] + self.lifecycle.settle_timeout

        if execution.expiry_time is not None:
            remaining = (execution.expiry_time - datetime.now(UTC)).total_seconds()
            until = min(until, now + remaining)

        return until if now < until else None

    def _classify(self, demand: DemandState, actual: ControllerOperatingState, now: float):
        """Update failure counts or start a hold when confirmed state or context changes."""
        achieved = self._achieved

        if achieved is None or actual.current == InternalControllerState.TRANSITION:
            return

        if actual.matches(*achieved):
            return

        # Procedures clear the confirmed state and context before commanding.
        # A change while they remain recorded is external.
        self._achieved = None

        match actual.current:
            case InternalControllerState.ERROR | InternalControllerState.UNKNOWN:
                logger.error(f"{self.controller.entity} lost {achieved[0]} ({actual.current})")
                self.recovery_state.record_failure(now, ControllerFailure(f"{self.controller.entity} lost {achieved[0]} ({actual.current})"))
            case _:
                logger.warning(
                    f"{self.controller.entity} changed to {actual.current} out of band, holding"
                )
                self._hold(demand)

    def _hold(self, demand: DemandState):
        """Delay restoring demand after an external state or context change.

        Reuse the deadline while the demanded state, lifecycle context, and program are unchanged.
        """
        program = None if demand.program is None else str(demand.program.entity)
        key = demand.state, demand.lifecycle_context(), program

        if self._hold_state is None or key != self._hold_state[0]:
            now = asyncio.get_running_loop().time()
            self._hold_state = (key, now + self.lifecycle.external_change_delay)

    def _held(
        self,
        demand: DemandState,
        actual: ControllerOperatingState,
        now: float,
    ) -> float | None:
        """Return the deadline delaying demand after an external change, if still needed."""
        if self._hold_state is None:
            return None

        # Resume reconciliation once demand is satisfied or the delay expires.
        if now >= self._hold_state[1] or actual.matches(
            demand.state, demand.lifecycle_context()
        ):
            self._clear_hold()
            return None

        # A different state, lifecycle context, or program starts a new delay.
        self._hold(demand)
        return self._hold_state[1]

    def _clear_hold(self):
        self._hold_state = None

    def _conclude(self, procedure: LifecycleProcedure):
        """Update failure counts and retry timing from a finished procedure.

        Raises:
            ControllerIndeterminate: If the procedure left work that may still be running.
            ControllerOffline: If the procedure found the controller offline.
            ControllerUnrecoverable: If an emergency shutdown succeeded.
        """
        error = procedure.error()
        now = asyncio.get_running_loop().time()
        recovery_state = self.recovery_state

        if procedure.unresolved:
            raise ControllerIndeterminate(
                "Procedure cleanup could not confirm that controller work stopped"
            ) from error

        match procedure, error:
            case _, ControllerOffline():
                raise error
            case RecoverProcedure(), ControllerUnrecoverable():
                # Emergency shutdown requires operator intervention.
                logger.critical(
                    f"{self.controller.entity} requires operator intervention after emergency shutdown"
                )
                recovery_state.clear()
                raise error
            case RecoverProcedure(recovered=True), _:
                recovery_state.record_recovery(now)
            case RecoverProcedure(), None:
                # An interrupted attempt changes nothing.
                pass
            case RecoverProcedure(emergency=True), _:
                logger.opt(exception=error).error(f"Emergency shutdown failed: {error}")
                recovery_state.record_failed_emergency(now)
            case RecoverProcedure(), _:
                logger.opt(exception=error).error(f"Recovery attempt failed: {error}")
                recovery_state.record_failed_attempt(now)
            case _, None:
                pass
            case _, ControllerFailure():
                logger.error(f"Controller lifecycle error: {error}")
                recovery_state.record_failure(now, error)
                self._achieved = None
            case _:
                logger.opt(exception=error).error(f"Lifecycle procedure error: {error}")
                recovery_state.delay_retry(now)

    def _start(self, procedure: LifecycleProcedure):
        # A procedure's own commands can change the reported state and lifecycle context.
        self._achieved = None
        self._procedure = procedure
        self.recovery_state.begin_procedure()
        self.recovery_state.retry_at = None
        procedure.start()

    async def _release(self):
        """Prevent new operations, abort the current procedure, and wait for cleanup.

        Cancellation stops local cleanup without sending further controller or program requests.
        """
        self._closed = True
        procedure = self._procedure
        self._procedure = None

        if procedure is None:
            return

        try:
            await procedure.abort()
        except asyncio.CancelledError:
            procedure.cancel()
            await defer_cancellation(procedure.join())
            raise

        if procedure.error() is not None or procedure.unresolved:
            logger.warning(f"{type(procedure).__name__} ended during reactor release")
