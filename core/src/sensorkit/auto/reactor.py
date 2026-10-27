# SPDX-License-Identifier: Apache-2.0
"""Reconcile controller demand while controller state monitoring is running."""

from __future__ import annotations

import asyncio
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
from sensorkit.core.controller import ControllerOperatingState, InternalControllerState

if TYPE_CHECKING:
    import uuid

    from sensorkit.auto.lifecycle import ControllerLifecycle
    from sensorkit.auto.supervisor import ControllerStateSupervisor


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
    """Run lifecycle procedures one at a time while a state supervisor monitors the controller.

    Each procedure finishes cleanup before its replacement starts. A controller failure aborts
    the current procedure, and demand is reconciled again after its cleanup. Recovery state
    persists in the lifecycle.
    """

    def __init__(self, lifecycle: ControllerLifecycle, supervisor: ControllerStateSupervisor):
        """Create a reactor for a started supervisor.

        Args:
            lifecycle: The lifecycle supplying demand, enablement, and recovery state.
            supervisor: A started supervisor for the controller.
        """
        self.lifecycle = lifecycle
        self.supervisor = supervisor
        self.controller = supervisor.controller
        self.recovery_state = lifecycle.recovery_state
        self.achieved_once = False
        """Whether a procedure commanded the demanded state during this reactor's lifetime."""
        self._wake = asyncio.Event()
        self._procedure: LifecycleProcedure | None = None
        self._settle_wait: tuple[uuid.UUID, float] | None = None

    @property
    def step(self) -> LifecycleStep:
        """The current procedure's step, or WAIT without a procedure."""
        return LifecycleStep.WAIT if self._procedure is None else self._procedure.step

    def wake(self):
        """Evaluate the lifecycle's inputs again."""
        self._wake.set()

    async def run(self):
        """Reconcile demand while the lifecycle is enabled.

        On exit, abort the current procedure and wait for its cleanup.

        Raises:
            ControllerStateUnknown: If controller state monitoring failed.
            ControllerOffline: If the controller lost its lease.
            ControllerIndeterminate: If a procedure left work that may still be running.
            ControllerUnrecoverable: If an emergency shutdown succeeded.
        """
        if self.supervisor.state.current == InternalControllerState.ERROR:
            self.recovery_state.record_initial_error()

        try:
            with self.supervisor.state_changes() as changes:
                # Read current demand and observed controller state on every wakeup.
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
        """Wait for demand, controller state, failure, procedure completion, or a deadline."""
        loop = asyncio.get_running_loop()

        # State changes wake evaluation, which reads the latest observed state.
        waiters = [asyncio.ensure_future(self._wake.wait()), asyncio.ensure_future(changes.get())]

        # A failure that is already set was evaluated before this wait.
        if not self.supervisor.failed.is_set():
            waiters.append(asyncio.ensure_future(self.supervisor.failed.wait()))

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

            # Retrieve waiter errors; the next evaluation detects ended supervision.
            for waiter in waiters:
                cleanup_future(waiter)

    @staticmethod
    def _program_interruptible(
        procedure: LifecycleProcedure,
        actual: ControllerOperatingState,
    ) -> bool:
        """Return whether a program's interrupt policy may abort the current procedure.

        The controller must report OPERATE with no transition or lifecycle task call pending.
        """
        return (
            not procedure.pending()
            and actual.current == InternalControllerState.OPERATE
            and actual.target is None
        )

    def _evaluate(self) -> float | None:
        """Reconcile demand and reported state, returning the next event loop deadline, if any."""
        demand = self.lifecycle.demand_state

        if self._procedure is not None and self._procedure.done():
            procedure = self._procedure
            self._procedure = None
            self._conclude(procedure)

        actual = self.supervisor.state
        procedure = self._procedure

        if procedure is not None:
            match self._interrupt_kind(procedure, demand, actual):
                case "abort":
                    procedure.request_abort()
                case "stop":
                    procedure.request_stop()

            return None

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

        A controller failure aborts it. A confirmed lifecycle context that the controller no
        longer reports stops it, so the demand is applied again.
        """
        if not self.lifecycle.enabled or demand.state is None:
            return "abort"

        if self.recovery_state.failure is not None:
            return "abort"

        # Recovery may be needed to reach any demand, including shutdown.
        if isinstance(procedure, RecoverProcedure):
            return None

        if procedure.demand != demand:
            if demand.interrupt or (
                demand.program_interrupt and self._program_interruptible(procedure, actual)
            ):
                return "abort"

            return "stop"

        # Let the procedure finish its lifecycle calls before comparing reported state.
        if not procedure.achieved or procedure.pending():
            return None

        if actual.matches(demand.state, demand.lifecycle_context()):
            return None

        return "stop"

    def _dispatch(self, demand: DemandState, actual: ControllerOperatingState) -> float | None:
        """Start recovery or the demanded procedure, or return the deadline delaying it."""
        now = asyncio.get_running_loop().time()
        recovery_state = self.recovery_state

        # Recovery handles running tasks in its procedure.
        if recovery_state.recovery_due:
            if (until := recovery_state.waiting(now)) is not None:
                return until

            logger.error(f"{self.controller.entity} failed repeatedly, starting recovery")
            self._start(
                RecoverProcedure(self.supervisor, demand, emergency=recovery_state.emergency_due)
            )
            return None

        # Shutdown bypasses retry delays.
        if demand.state != InternalControllerState.SHUTDOWN:
            if (until := recovery_state.waiting(now)) is not None:
                return until

        if (until := self._settling(demand, actual, now)) is not None:
            return until

        self._start(self.lifecycle.procedures[demand.state](self.supervisor, demand))
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
        execution = self.supervisor.running_execution()

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
        self.achieved_once |= procedure.commanded

        if procedure.unresolved:
            raise ControllerIndeterminate(
                "Procedure cleanup could not confirm that controller work stopped"
            ) from recovery_state.failure or error

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
            case _ if recovery_state.failure is not None:
                # This controller failure was already counted and its retry scheduled.
                logger.error(f"Controller lifecycle error: {recovery_state.failure} ({error})")
            case _, ControllerFailure():
                logger.error(f"Controller lifecycle error: {error}")
                recovery_state.record_failure(now, error)
            case _:
                logger.opt(exception=error).error(f"Lifecycle procedure error: {error}")
                recovery_state.delay_retry(now)

    def _start(self, procedure: LifecycleProcedure):
        self._procedure = procedure
        self.recovery_state.begin_procedure()
        self.recovery_state.retry_at = None
        procedure.start()

    async def _release(self):
        """Abort the current procedure and wait for cleanup.

        Cancellation stops local cleanup without sending further controller or program requests.
        """
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
        finally:
            self.achieved_once |= procedure.commanded

        if procedure.error() is not None or procedure.unresolved:
            logger.warning(f"{type(procedure).__name__} ended during reactor release")
