# SPDX-License-Identifier: Apache-2.0
"""Reconcile controller demand while controller state monitoring is running."""

from __future__ import annotations

import asyncio
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


class ControllerReactor:
    """Run lifecycle procedures one at a time while a state supervisor monitors the controller.

    Each procedure finishes cleanup before its replacement starts. A controller failure aborts
    the current procedure, and demand is reconciled again after its cleanup. Failure counts
    and retry timing survive reactor restarts.
    """

    def __init__(self, lifecycle: ControllerLifecycle, supervisor: ControllerStateSupervisor):
        """Create a reactor for a started supervisor.

        Args:
            lifecycle: The lifecycle supplying demand, enablement, and failure state.
            supervisor: A started supervisor for the controller.
        """
        self.lifecycle = lifecycle
        self.supervisor = supervisor
        self.controller = supervisor.controller
        self.failure_state = lifecycle.failure_state
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
            self.failure_state.record_initial_error()

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

        if self.failure_state.error is not None:
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
        failure_state = self.failure_state

        # Recovery handles running tasks in its procedure.
        if failure_state.recovery_due:
            if (until := failure_state.waiting(now)) is not None:
                return until

            logger.error(f"{self.controller.entity} failed repeatedly, starting recovery")
            self._start(
                RecoverProcedure(self.supervisor, demand, emergency=failure_state.emergency_due)
            )
            return None

        # Shutdown bypasses retry delays.
        if demand.state != InternalControllerState.SHUTDOWN:
            if (until := failure_state.waiting(now)) is not None:
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
        failure_state = self.failure_state
        self.achieved_once |= procedure.commanded

        if procedure.unresolved:
            raise ControllerIndeterminate(
                "Procedure cleanup could not confirm that controller work stopped"
            ) from failure_state.error or error

        match procedure, error:
            case _, ControllerOffline():
                raise error
            case RecoverProcedure(), ControllerUnrecoverable():
                # Emergency shutdown requires operator intervention.
                logger.critical(
                    f"{self.controller.entity} requires operator intervention after emergency shutdown"
                )
                failure_state.clear()
                raise error
            case RecoverProcedure(recovered=True), _:
                failure_state.record_recovery(now)
            case RecoverProcedure(), None:
                # An interrupted attempt changes nothing.
                pass
            case RecoverProcedure(emergency=True), _:
                logger.opt(exception=error).error(f"Emergency shutdown failed: {error}")
                failure_state.record_failed_emergency(now)
            case RecoverProcedure(), _:
                logger.opt(exception=error).error(f"Recovery attempt failed: {error}")
                failure_state.record_failed_attempt(now)
            case _, None:
                pass
            case _ if failure_state.error is not None:
                # This controller failure was already counted and its retry scheduled.
                logger.error(f"Controller lifecycle error: {failure_state.error} ({error})")
            case _, ControllerFailure():
                logger.error(f"Controller lifecycle error: {error}")
                failure_state.record_failure(now, error)
            case _:
                logger.opt(exception=error).error(f"Lifecycle procedure error: {error}")
                failure_state.delay_retry(now)

    def _start(self, procedure: LifecycleProcedure):
        self._procedure = procedure
        self.failure_state.begin_procedure()
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
