# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum, auto
from typing import Any, Literal, final, overload, override

from loguru import logger

from sensorkit.common.aio import AsyncValueLatch
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import ControllerClient, ControllerState, InternalControllerState
from sensorkit.core.program import ProgramClient
from sensorkit.core.task import (
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
    Task,
    TaskContextMap,
)


class LifecycleStep(StrEnum):
    """Steps in the Controller lifecycle logic."""
    INIT = auto()
    TASKING = auto()
    STANDBY = auto()
    SHUTDOWN = auto()
    RECOVER = auto()
    WAIT = auto()


@dataclass(slots=True, frozen=True)
class DemandState:
    """The demanded InternalState and associated context."""
    state: InternalControllerState | None
    contexts: TaskContextMap | None = None
    program: ProgramClient | None = None

    # Other fields must have `compare=False` to ensure correct demand state difference checks.
    interrupt: bool = field(default=False, compare=False)


# TODO: Move to sensorkit.core.controller
class ControllerStateMonitor:
    """Monitors a Controller's error state."""

    def __init__(self, controller: ControllerClient):
        self.controller = controller

    async def start(self):
        """Begin monitoring the controller state and wait until the initial state is known."""
        ready = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._monitor(ready))
        await ready

    async def _monitor(self, initial_update: asyncio.Future):
        try:
            # TODO: For now just query the state once, since we're only using it for initial state.
            state = await self.controller.kv_get_model(ControllerState)
            self.current_state = state.operating_state.current
            initial_update.set_result(True)
        except asyncio.CancelledError:
            initial_update.cancel()
            raise
        except BaseException as e:
            initial_update.set_exception(e)
            raise

    def in_error(self):
        """Return True if the controller is currently in an error state (stub)."""
        # TODO
        return False

    async def wait(self):
        """Wait until a passive error is detected (stub, waits forever)."""
        # TODO
        await asyncio.sleep(float("inf"))


class DemandProcError(Exception):
    """Raised by a DemandProc to signal a lifecycle error with a categorized origin."""

    type ErrorKind = Literal["program", "controller", "internal"]

    def __init__(self, *args, kind: ErrorKind = "internal", **kwargs):
        super().__init__(*args, **kwargs)
        self.kind = kind


class DemandProc(ABC):
    """Wraps an asyncio.Task and implements a component of the Controller lifecycle logic."""

    def __init__(
        self,
        lifecycle: ControllerLifecycle,
        demand: DemandState,
    ):
        self.lifecycle = lifecycle
        self.demand = demand
        self._interrupt_count = 0
        self._interrupt_level = 0
        self._cleanup_tasks: set[uuid.UUID] = set()
        self._cleanup_futures: set[asyncio.Future] = set()

    @final
    def start(self):
        """Create and schedule the underlying asyncio task for this demand procedure."""
        self._aio_task = asyncio.create_task(self._demand_proc())

    async def _demand_proc(self):
        logger.debug(f"{self.demand.state} procedure starting")
        prereq_coro = asyncio.sleep(0)
        logic_coro = self.state_logic()

        while True:
            try:
                # Await prerequisite actions. Initially this is nothing, but after an interrupt
                # this is operations to effect the stop or abort.
                await prereq_coro

                # Run the user logic. Initially this is the main state driving logic, but after
                # an interrupt this is stop or abort logic.
                await logic_coro
            except asyncio.CancelledError:
                if next_stage := self._demand_proc_cancelled():
                    # Managed cancellation. Loop to the next stage.
                    prereq_coro, logic_coro = next_stage
                else:
                    logger.debug(f"{self.demand.state} procedure exiting due to hard cancel")
                    raise
            except BaseException as e:
                logger.debug(f"{self.demand.state} procedure exiting due to error ({e})")
                raise
            else:
                logger.warning(f"{self.demand.state} procedure exiting without raising")
                break

    def _demand_proc_cancelled(self) -> tuple[asyncio.Future, asyncio.Future] | None:
        if self._aio_task.cancelling() > self._interrupt_count:
            # This means the task was externally cancelled coincident with a stop or abort!
            # Fall through to a hard cancel.
            self._interrupt_level = 3

        match self._interrupt_level:
            case 1:
                logger.debug(f"{self.demand.state} is stopping gracefully")

                while self._aio_task.cancelling() > 0:
                    self._aio_task.uncancel()

                # Gracefully stop awaitables and SK tasks.
                prereq_coro = asyncio.gather(
                    *(future for future in self._cleanup_futures if not future.done()),
                    *(
                        self.lifecycle.controller.wait_for_task(task_id)
                        for task_id in self._cleanup_tasks
                    ),
                )

                # Run the user stop logic at the next iteration.
                logic_coro = self.stop_logic()
            case 2:
                logger.debug(f"{self.demand.state} procedure is aborting immediately")

                while self._aio_task.cancelling() > 0:
                    self._aio_task.uncancel()

                # Forcefully stop awaitables and SK tasks.
                for fut in self._cleanup_futures:
                    fut.cancel("Demand procedure abort")

                prereq_coro = asyncio.gather(
                    *(
                        self.lifecycle.controller.abort_task(task_id)
                        for task_id in self._cleanup_tasks
                    )
                )

                # Run the user abort logic at the next iteration.
                logic_coro = self.abort_logic()
            case _:
                return None

        self._interrupt_count = 0
        self._interrupt_level = 0
        self._cleanup_tasks.clear()
        self._cleanup_futures.clear()

        return prereq_coro, logic_coro

    async def shield_with_cleanup[T](self, fut: asyncio.Future[T]):
        """Awaits the input Future while shielding it from cancellation and handling stop/abort."""
        self._cleanup_futures.add(fut)
        result = await asyncio.shield(fut)
        self._cleanup_futures.discard(fut)
        return result

    def run_with_cleanup(self, task: Task, *, context: KeywordDict | None = None):
        """Dispatch *task* (interrupting any current task) and await its completion under cleanup.

        The execution future is registered for graceful/forced cleanup *synchronously*, before any
        cancellable await, so the dispatch and completion happen inside the shielded region: a
        demand change that cancels the procedure cannot slip in between dispatch and registration.
        The controller-minted task id is registered for forced abort as soon as it is known.

        Args:
            task: The lifecycle task to execute.
            context: Optional keyword context to attach to the execution.

        Returns:
            An awaitable yielding the final task execution result.
        """
        async def _execute():
            execution = await self.lifecycle.controller.start_task(
                task, context=context, interrupt=True
            )
            self._cleanup_tasks.add(execution.task_id)

            try:
                return await execution
            finally:
                self._cleanup_tasks.discard(execution.task_id)

        return self.shield_with_cleanup(asyncio.ensure_future(_execute()))

    @abstractmethod
    async def state_logic(self):
        """Run logic for this demand procedure."""

    async def stop_logic(self):
        """Graceful stop logic for this demand procedure."""

    async def abort_logic(self):
        """Immediate abort logic for this demand procedure."""

    @final
    async def interrupt_no_raise(self, msg: Any | None = None, *, abort: bool = False):
        """Interrupt the current demand procedure and return any exceptions that occurred."""
        try:
            if abort:
                await self.abort()
            else:
                await self.stop()
        except Exception as e:
            return e

        return None

    @final
    async def stop(self, msg: Any | None = None):
        """End the task gracefully and wait for completion."""
        if self._interrupt_level > 1:
            return

        self._interrupt_level = 1
        self._interrupt_count += 1
        self._aio_task.cancel(msg)

        # Wait until the task ends.
        if not self._aio_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await self._aio_task

    @final
    async def abort(self, msg: Any | None = None):
        """End the task as soon as possible and wait for completion."""
        if self._interrupt_level > 2:
            return

        self._interrupt_level = 2
        self._interrupt_count += 1
        self._aio_task.cancel(msg)

        # Wait until the task ends.
        if not self._aio_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await self._aio_task

    @final
    def cancel(self, msg: Any | None = None):
        """Hard-cancel the demand procedure without waiting for cleanup."""
        self._interrupt_level = 3
        self._aio_task.cancel(msg)

    @final
    def done(self):
        """Return True if the underlying asyncio task has finished."""
        return self._aio_task.done()

    @final
    def error(self):
        """Return the exception raised by the task, or None if still running or successful."""
        return self._aio_task.exception() if self._aio_task.done() else None

    @final
    def future(self) -> asyncio.Future[None]:
        """Return the underlying asyncio.Task for use as an awaitable or in `asyncio.wait`."""
        return self._aio_task


class ControllerLifecycle:
    """Implements full lifecycle orchestration of a Controller."""

    def __init__(self):
        self.step = LifecycleStep.WAIT
        # TODO: Belief state needs to be a triple like DemandState in order to properly no-op
        #       init and standby transitions.
        self.belief_state: InternalControllerState | None = None
        self._demand = AsyncValueLatch(DemandState(state=None))
        self._can_operate = asyncio.Event()

        # Define the mapping of demand states to demand procedure implementations.
        self._demand_procs = {
            InternalControllerState.OPERATE: self.OperateProc,
            InternalControllerState.STANDBY: self.StandbyProc,
            InternalControllerState.SHUTDOWN: self.ShutdownProc,
        }

    def start(
        self,
        controller: ControllerClient,
        *,
        task_group=asyncio,
    ):
        """Bind to *controller* and launch the lifecycle loop in *task_group*."""
        self.controller = controller

        # Create a monitor to observe the actual Controller state.
        self._monitor = ControllerStateMonitor(self.controller)

        # Start the main task, which will block until enable is called.
        self._main_task = task_group.create_task(self._lifecycle())

    async def stop(self):
        """Cancel the lifecycle loop and wait for it to finish."""
        self._main_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await self._main_task

    def enable(self):
        """Allow the lifecycle to issue demand commands to the controller."""
        self._can_operate.set()

    def disable(self):
        """Prevent any further demand commands and interrupt the current demand procedure."""
        self._can_operate.clear()

        # Interrupt the current demand procedure.
        self._demand.update(DemandState(state=None, interrupt=True))

    @property
    def enabled(self):
        """True if the lifecycle is currently allowed to command the controller."""
        return self._can_operate.is_set()

    @property
    def demand_state(self):
        """The currently applied DemandState."""
        return self._demand.value

    @overload
    def set_demand_state(self, state: DemandState): ...

    @overload
    def set_demand_state(
        self,
        demand: Literal[InternalControllerState.OPERATE],
        *,
        contexts: TaskContextMap = ...,
        program: ProgramClient,
        interrupt: bool = ...,
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
        """Set the demanded controller state, returning True if the value changed."""
        if demand not in self._demand_procs:
            raise ValueError(f"Cannot demand controller state {demand}")

        return self._demand.update(
            DemandState(demand, **kwargs),
            only_if_different=True
        )

    async def _lifecycle(self):
        # Start the controller monitor and get an initial belief state.
        await self._monitor.start()
        self.belief_state = self._monitor.current_state
        logger.debug(f"got initial state from controller: {self.belief_state}")

        # Loop forever, pausing when the Controller is marked inoperable. Note that cancellation
        # here is OK because we are guaranteed to not have a demand procedure running.
        while await self._can_operate.wait():
            # Get the Controller demand state. If there was a pending change to the demand state,
            # it is applied here.
            demand = self._demand.apply()

            if demand.state is not None:
                # Delegate to a demand procedure that corresponds to the demand state.
                demand_proc_type = self._demand_procs[demand.state]
                demand_proc = demand_proc_type(self, demand)
                demand_proc.start()
            else:
                # If there is no demanded Controller state, we just fall through to wait for a
                # demand state change or a passive error to occur.
                demand_proc = None

            try:
                # Wait for our procedure to complete or an error to occur.
                error_kind = await self._lifecycle_reactor(demand_proc)
            except asyncio.CancelledError:
                logger.debug(f"{self.controller.entity} lifecycle loop cancelled")

                if demand_proc:
                    # We were externally cancelled with our demand procedure potentially still
                    # running. Await successful cancellation of the task and re-raise.
                    with contextlib.suppress(Exception):
                        await demand_proc.abort()

                raise

            # Sleep to avoid thrashing, but only when there was an error.
            if error_kind is not None:
                await asyncio.sleep(5.0)

    async def _lifecycle_reactor(self, demand_proc: DemandProc | None):
        # Build a list of futures that capture when:
        #   a) the demand procedure, if there is one, ends
        #     - it should have raised; this is considered an active error
        #     - if it did not raise, this is a bug
        #   b) a demand state change is detected
        #   c) the Controller explicitly reports an error
        futures: list[asyncio.Future[None]] = [
            asyncio.create_task(self._demand.wait_until_pending()),
            asyncio.create_task(self._monitor.wait()),
            *([demand_proc.future()] if demand_proc else []),
        ]

        try:
            # Wait for any of the above conditions to occur.
            await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)

            # Determine whether we are in an error state.
            active_error: BaseException | None = None

            if demand_proc:
                if demand_proc.done():
                    # The demand procedure has ended. This can only happen through cancellation
                    # or by an error occurring, so by contract it must have raised.
                    active_error = demand_proc.error()
                elif self._demand.pending_change():
                    # The demand procedure is still executing and our demand has changed. Stop
                    # it according to the interrupt policy of the incoming demand.
                    active_error = await demand_proc.interrupt_no_raise(
                        abort=self._demand.pending_value.interrupt
                    )
                else:
                    # The demand procedure is still executing and a passive error was detected.
                    # In this case we always send a hard abort.
                    assert self._monitor.in_error()
                    active_error = await demand_proc.interrupt_no_raise(abort=True)

            error_kind = (
                "controller"
                if self._monitor.in_error()
                else active_error.kind
                if isinstance(active_error, DemandProcError)
                else "internal"
                if active_error
                else None
            )

            match error_kind:
                case "controller":
                    # Run the recovery loop if the controller is in an error state.
                    logger.error(f"Controller lifecycle error: {active_error or '<passive>'}")

                    try:
                        await self._error_recovery()
                    except Exception:
                        # If we were unable to recover from the error state, the controller is
                        # inoperable!
                        # FIXME: This will be replaced by blacklisting/backoff, and throwing in
                        #        the towel will be left as a higher-level decision.
                        logger.critical("Controller is inoperable!")
                        self._can_operate.clear()
                case "program":
                    logger.opt(exception=active_error).error(f"Program lifecycle error: {active_error}")
                    # TODO: Handle retry counting and blacklisting via ProgramStateManager.

            return error_kind
        finally:
            # Clean up tasks.
            for future in futures:
                future.cancel()

    async def _error_recovery(self):
        self.belief_state = InternalControllerState.ERROR
        retries_until_shutdown = 6
        time_between_retries = 20.0
        time_between_cycles = 120.0
        recover_timeout = 120.0
        shutdown_timeout = 240.0

        while True:
            self.step = LifecycleStep.RECOVER

            # Attempt recovery several times.
            for attempt in range(1, retries_until_shutdown + 1):
                logger.info("Attempting recovery...")

                try:
                    async with asyncio.timeout(recover_timeout):
                        await self.controller.execute_task(
                            RecoverTask(),
                            interrupt=True,
                        )

                    logger.info("Recovery succeeded.")
                    return
                except Exception as e:
                    logger.error(f"Recovery attempt failed with {type(e).__name__}: {e}")

                    if attempt < retries_until_shutdown:
                        await asyncio.sleep(time_between_retries)

            # We can't seem to recover, so we are getting desperate. Just try to shut down.
            logger.warning("Failed to recover. Attempting shutdown.")
            self.step = LifecycleStep.SHUTDOWN

            try:
                async with asyncio.timeout(shutdown_timeout):
                    await self.controller.execute_task(
                        ShutdownTask(),
                        interrupt=True,
                    )
            except Exception as e:
                logger.error(f"Emergency shutdown failed with {type(e).__name__}: {e}")
            else:
                # The shutdown task succeeded, in spite of the initial error state and recovery
                # troubles. Let's play it safe and raise an error, which will cause the
                # lifecycle logic to mark this Controller as inoperable pending external
                # intervention.
                logger.info("Emergency shutdown succeeded.")
                self.belief_state = InternalControllerState.SHUTDOWN
                raise RuntimeError("Emergency shutdown")

            logger.warning(
                "Emergency shutdown failed. "
                f"Resuming recovery attempts in {time_between_cycles} sec."
            )
            self.step = LifecycleStep.WAIT
            await asyncio.sleep(time_between_cycles)

    class OperateProc(DemandProc):
        """Drives the Controller to the OPERATE state."""

        @override
        async def state_logic(self):
            controller = str(self.lifecycle.controller.entity)

            # Begin INIT step.
            self.lifecycle.step = LifecycleStep.INIT

            logger.info(f"Preparing to operate ({controller})")
            task = InitTask()
            context = self.demand.contexts.get("init") if self.demand.contexts else None

            self.lifecycle.belief_state = InternalControllerState.UNKNOWN  # FIXME: clear to avoid shutdown no-op
            await self.run_with_cleanup(task, context=context)

            # Init was successful.
            self.lifecycle.belief_state = InternalControllerState.OPERATE

            # If there is no program specified, just wait.
            if self.demand.program is None:
                self.lifecycle.step = LifecycleStep.WAIT
                logger.info(f"Waiting for a program for tasking ({controller})")
                await asyncio.sleep(float("inf"))

            while True:
                # Begin TASKING step.
                logger.info(f"Activating {self.demand.program.entity} program ({controller})")
                self.lifecycle.step = LifecycleStep.TASKING

                try:
                    async with asyncio.timeout(5.0):
                        await self.demand.program.start_tasking(self.demand.contexts)
                except Exception as e:
                    raise DemandProcError("Failed to start tasking", kind="program") from e

                logger.info(f"Controller is operating ({controller})")

                # Wait until tasking stops out of band of our own stop logic. This will typically
                # happen due to an error, but it is also possible that there was a third-party
                # request for the Program to stop normally or the Program service restarted.
                stop_event = await self.demand.program.wait_until_tasking_stops()

                if stop_event is None:
                    raise DemandProcError("Failed to start tasking", kind="program")

                match stop_event.origin:
                    case "error":
                        logger.error(f"Error reported ({controller})")
                        raise DemandProcError("Program reported error", kind="controller")
                    case "init":
                        # It looks like the Program service was restarted.
                        # TODO: Make sure this was a fast enough restart. Currently, there isn't
                        #       enough information published to determine this.
                        logger.warning(f"Active program restarted ({controller})")

                        # Pause briefly and try again.
                        self.lifecycle.step = LifecycleStep.WAIT
                        await asyncio.sleep(3)
                    case "request":
                        logger.info(f"Tasking stopped by request ({controller})")
                        raise DemandProcError("Program was stopped", kind="program")

        @override
        async def stop_logic(self):
            if self.demand.program:
                try:
                    await self.demand.program.stop_tasking()
                except BaseException as e:
                    logger.error(f"Program stop logic failed with {type(e).__name__}: {e}")
                    raise

        @override
        async def abort_logic(self):
            if self.demand.program:
                await self.demand.program.abort_tasking()

    class StandbyProc(DemandProc):
        """Drives the Controller to the STANDBY state."""

        @override
        async def state_logic(self):
            controller = str(self.lifecycle.controller.entity)

            # Begin STANDBY step.
            self.lifecycle.step = LifecycleStep.STANDBY

            logger.info(f"Entering standby mode ({controller})")
            task = StandbyTask()
            context = self.demand.contexts.get("standby") if self.demand.contexts else None

            self.lifecycle.belief_state = InternalControllerState.UNKNOWN  # FIXME: clear to avoid shutdown no-op
            await self.run_with_cleanup(task, context=context)

            # Standby was successful.
            self.lifecycle.belief_state = InternalControllerState.STANDBY
            self.lifecycle.step = LifecycleStep.WAIT

            logger.info(f"Controller is idle in standby ({controller})")
            await asyncio.sleep(float("inf"))

    class ShutdownProc(DemandProc):
        """Drives the Controller to the SHUTDOWN state."""

        @override
        async def state_logic(self):
            controller = str(self.lifecycle.controller.entity)

            # Begin SHUTDOWN step.
            self.lifecycle.step = LifecycleStep.SHUTDOWN

            # FIXME: Need a passive monitor to check whether belief state equals actual (reported
            #        by Controller) state, and consider it a local error (trigger recovery logic)
            #        if not. That will force a re-command and hopefully re-synchronization of the
            #        belief state. Then, the no-op below should be safe.
            if self.lifecycle.belief_state != InternalControllerState.SHUTDOWN:
                logger.info(f"Commencing shutdown ({controller})")
                task = ShutdownTask()
                context = (
                    self.demand.contexts.get("shutdown") if self.demand.contexts else None
                )

                await self.run_with_cleanup(task, context=context)

            # Shutdown was successful.
            self.lifecycle.belief_state = InternalControllerState.SHUTDOWN
            self.lifecycle.step = LifecycleStep.WAIT

            logger.info(f"Controller is shut down ({controller})")
            await asyncio.sleep(float("inf"))
