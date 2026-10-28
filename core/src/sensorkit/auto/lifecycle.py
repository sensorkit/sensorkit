# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from typing import ClassVar, Literal, overload

from loguru import logger

from sensorkit.auto.failure import FailureState
from sensorkit.auto.procedures import (
    ControllerUnrecoverable,
    DemandState,
    LifecycleProcedure,
    LifecycleStep,
    OperateProcedure,
    ShutdownProcedure,
    StandbyProcedure,
)
from sensorkit.auto.reactor import ControllerReactor
from sensorkit.auto.supervisor import ControllerStateSupervisor
from sensorkit.common.aio import defer_cancellation
from sensorkit.core.controller import ControllerClient, ControllerMonitor, InternalControllerState
from sensorkit.core.program import ProgramClient
from sensorkit.core.task import TaskContextMap


class ControllerLifecycle:
    """Maintain the demanded controller state and restart failed reactors.

    Each session starts a controller monitor and a state supervisor, then runs a reactor while
    the lifecycle is enabled. Observation failures and offline controllers trigger a restart
    after an increasing delay. Demand, enablement, and failure state persist across restarts.
    """

    procedures: ClassVar[dict[InternalControllerState, type[LifecycleProcedure]]] = {
        InternalControllerState.OPERATE: OperateProcedure,
        InternalControllerState.STANDBY: StandbyProcedure,
        InternalControllerState.SHUTDOWN: ShutdownProcedure,
    }
    """Procedures for each supported demand state."""

    settle_timeout: ClassVar[float] = 300.0
    """Maximum time, in seconds, to wait for an existing lifecycle task that matches demand."""

    restart_delay: ClassVar[float] = 1.0
    """Seconds before the first reactor restart."""

    restart_delay_max: ClassVar[float] = 60.0
    """Maximum restart delay in seconds. A reactor running this long resets the delay."""

    def __init__(
        self,
        *,
        transition_grace: float = 10.0,
    ):
        """Create a lifecycle that does nothing until started and enabled.

        Args:
            transition_grace: Maximum seconds of disagreement with the expected state during
                a commanded lifecycle transition. Time in TRANSITION does not count toward
                this limit.

        Raises:
            ValueError: If `transition_grace` is not positive.
        """
        if not transition_grace > 0:
            raise ValueError(f"transition_grace must be positive, got {transition_grace}")

        self.transition_grace = transition_grace
        self.failure_state = FailureState()
        self.last_error: BaseException | None = None
        """The most recent reactor failure, retained across restarts."""
        self._demand = DemandState(state=None)
        self._enabled = asyncio.Event()
        self._reactor: ControllerReactor | None = None

    def start(
        self,
        controller: ControllerClient,
        *,
        task_group=asyncio,
    ):
        """Bind to *controller* and launch the lifecycle loop in *task_group*."""
        self.controller = controller
        self._main_task = task_group.create_task(self._supervise())

    async def stop(self):
        """Cancel the lifecycle loop and wait for it to finish.

        Canceling the caller cancels the loop again and stops procedure cleanup without sending
        further controller or program requests.
        """
        self._main_task.cancel()

        try:
            # The loop's own cancellation is expected and does not propagate.
            await asyncio.wait((self._main_task,))
        except asyncio.CancelledError:
            self._main_task.cancel()
            raise

        if not self._main_task.cancelled():
            self._main_task.result()

    def enable(self):
        """Allow the lifecycle to issue demand commands to the controller."""
        self._enabled.set()

    def disable(self):
        """Withdraw demand and request interruption of the current procedure."""
        self._enabled.clear()
        self._demand = DemandState(state=None)
        self._wake()

    @property
    def enabled(self):
        """True if the lifecycle is currently allowed to command the controller."""
        return self._enabled.is_set()

    @property
    def demand_state(self):
        """The latest demand, available to the reactor and procedures as soon as it is set."""
        return self._demand

    @property
    def step(self) -> LifecycleStep:
        """The current lifecycle step, or WAIT when no reactor is running."""
        return LifecycleStep.WAIT if self._reactor is None else self._reactor.step

    @overload
    def set_demand_state(
        self,
        demand: Literal[InternalControllerState.OPERATE],
        *,
        contexts: TaskContextMap = ...,
        program: ProgramClient,
        interrupt: bool = ...,
        program_interrupt: bool = ...,
    ): ...

    @overload
    def set_demand_state(
        self,
        demand: Literal[InternalControllerState.STANDBY, InternalControllerState.SHUTDOWN],
        *,
        contexts: TaskContextMap = ...,
        interrupt: bool = ...,
    ): ...

    def set_demand_state(self, demand: InternalControllerState, **kwargs):
        """Set demand and return whether its state, contexts, or program changed."""
        if demand not in self.procedures:
            raise ValueError(f"Cannot demand controller state {demand}")

        # Snapshot contexts so caller mutations cannot change the demand.
        if (contexts := kwargs.get("contexts")) is not None:
            kwargs["contexts"] = contexts.model_copy(deep=True)

        state = DemandState(demand, **kwargs)
        changed = state != self._demand
        self._demand = state
        self._wake()

        return changed

    def _wake(self):
        if self._reactor is not None:
            self._reactor.wake()

    async def _supervise(self):
        loop = asyncio.get_running_loop()
        delay = self.restart_delay

        while True:
            await self._enabled.wait()
            started = loop.time()
            error, achieved = await self._session()

            # Commanding the demanded state or outlasting the longest delay resets the backoff.
            if error is None or achieved or loop.time() - started >= self.restart_delay_max:
                delay = self.restart_delay

            if error is None:
                continue

            self.last_error = error

            logger.opt(exception=error).error(
                f"{self.controller.entity} lifecycle restarting in {delay} s: {error}"
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.restart_delay_max)

    async def _session(self) -> tuple[Exception | None, bool]:
        """Run a reactor with its monitor and state supervisor.

        Returns:
            The reactor's failure, if any, and whether it ever commanded the demanded state.
        """
        monitor = ControllerMonitor(self.controller)
        supervisor = None
        reactor = None

        try:
            await monitor.start()

            supervisor = ControllerStateSupervisor(
                monitor,
                self.failure_state,
                transition_grace=self.transition_grace,
            )
            await supervisor.start()

            reactor = ControllerReactor(self, supervisor)
            self._reactor = reactor
            await reactor.run()
        except ControllerUnrecoverable as e:
            # Emergency shutdown requires operator intervention.
            self.last_error = e
            self.disable()
        except Exception as e:
            return e, reactor is not None and reactor.achieved_once
        finally:
            self._reactor = None

            # Keep state monitoring cleanup failures from propagating to the caller's task group.
            try:
                await self._release(monitor, supervisor)
            except Exception as e:
                logger.opt(exception=e).error(
                    f"{self.controller.entity} state monitoring cleanup failed"
                )

        return None, reactor is not None and reactor.achieved_once

    @staticmethod
    async def _release(monitor: ControllerMonitor, supervisor: ControllerStateSupervisor | None):
        """Stop the supervisor, then the monitor, completing both despite cancellation."""
        try:
            if supervisor is not None:
                await defer_cancellation(supervisor.stop())
        finally:
            await defer_cancellation(monitor.stop())
