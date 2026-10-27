# SPDX-License-Identifier: Apache-2.0
"""Lifecycle procedures that track controller tasks and manage program tasking."""

from __future__ import annotations

import asyncio
import contextlib
import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar, Literal, final, override

from loguru import logger

from sensorkit.backend.base import Entity, KeyNotFound
from sensorkit.backend.request import CallRejected
from sensorkit.common.aio import cleanup_future, defer_cancellation, interruptible_wait
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerState,
    ControllerStateUnknown,
    InternalControllerState,
)
from sensorkit.core.entity import EntityClient
from sensorkit.core.executor import TaskCall, TaskFailed, TaskOutcomeUnknown
from sensorkit.core.program import (
    ProgramActivation,
    ProgramActiveState,
    ProgramClient,
    ProgramState,
    TaskingError,
    TaskingLoopState,
    TaskingStartupFailed,
)
from sensorkit.core.task import (
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
    Task,
    TaskContextMap,
)

if TYPE_CHECKING:
    from sensorkit.auto.supervisor import ControllerStateSupervisor

LIFECYCLE_TASK_TYPES: dict[InternalControllerState, str] = {
    InternalControllerState.OPERATE: "init",
    InternalControllerState.STANDBY: "standby",
    InternalControllerState.SHUTDOWN: "shutdown",
}
"""Lifecycle task types for the demandable controller states."""


class LifecycleStep(StrEnum):
    """The lifecycle operation currently in progress."""

    INIT = auto()
    TASKING = auto()
    STANDBY = auto()
    SHUTDOWN = auto()
    RECOVER = auto()
    WAIT = auto()


@dataclass(slots=True, frozen=True)
class DemandState:
    """Demanded controller state, task contexts, and program."""

    state: InternalControllerState | None
    contexts: TaskContextMap | None = None
    program: ProgramClient | None = None

    # Interruption flags wake the reactor but do not affect demand equality.
    interrupt: bool = field(default=False, compare=False)
    """Abort the current procedure when this demand replaces it."""
    program_interrupt: bool = field(default=False, compare=False)
    """Allow aborting program tasking at OPERATE with no transition or lifecycle call pending."""

    def lifecycle_context(self, state: InternalControllerState | None = None) -> KeywordDict:
        """Return the task context for a lifecycle state, defaulting to the demanded state.

        Return an empty context if no contexts or state are set.
        """
        state = self.state if state is None else state

        if self.contexts is None or state is None:
            return KeywordDict()

        return copy.deepcopy(self.contexts.get(LIFECYCLE_TASK_TYPES[state]))


class ProcedureError(Exception):
    """A procedure failed and requires a delayed retry."""


class ControllerFailure(ProcedureError):
    """A procedure failure that contributes to the controller's recovery threshold."""


class ControllerIndeterminate(Exception):
    """Controller task execution or program tasking could not be confirmed stopped."""


class ControllerOffline(Exception):
    """The controller does not hold its lease."""


class ControllerUnrecoverable(Exception):
    """Automatic recovery was exhausted and emergency shutdown succeeded.

    Operator intervention is required before lifecycle control resumes.
    """


class LifecycleProcedure(ABC):
    """Apply demand and track dispatched tasks through cleanup.

    A stop request or state logic failure triggers graceful cleanup. An abort request or failed
    graceful cleanup triggers abort cleanup. Dispatched tasks and registered futures remain
    tracked when the logic awaiting them is interrupted.

    Returning from state logic sends no cleanup requests. Canceling the procedure stops local
    cleanup without sending further controller or program requests. Work that cannot be
    confirmed stopped is marked unresolved, causing the reactor to restart.
    """

    graceful_budget: ClassVar[float] = 150.0
    """Time limit in seconds for each graceful cleanup stage before escalating to abort."""

    abort_budget: ClassVar[float] = 15.0
    """Time limit in seconds for each abort cleanup stage."""

    expects_target: ClassVar[bool] = True
    """Whether lifecycle task dispatch establishes a TRANSITION expectation."""

    def __init__(self, supervisor: ControllerStateSupervisor, demand: DemandState):
        self.supervisor = supervisor
        self.demand = demand
        self.step = LifecycleStep.WAIT
        """The lifecycle operation in progress."""
        self.interrupt: Literal["stop", "abort"] | None = None
        """The most severe interruption requested."""
        self.achieved = False
        """Whether this procedure confirmed the required controller state and lifecycle context."""
        self.commanded = False
        """Whether this procedure's own lifecycle task established the confirmed state."""
        self.unresolved = False
        """Whether tracked tasks, cleanup futures, or program tasking may still be active."""
        self.cleanup_errors: list[BaseException] = []
        self._calls: list[TaskCall] = []
        self._cleanup_futures: set[asyncio.Future] = set()
        self._logic: asyncio.Task[None] | None = None
        self._stopping: asyncio.Task[bool] | None = None
        self._task: asyncio.Task[None] | None = None

    @final
    def start(self):
        """Schedule the procedure."""
        if not self.expects_target:
            self.supervisor.invalidate()

        self._task = asyncio.create_task(self._run())

    async def _run(self):
        logger.debug(f"{type(self).__name__} starting")

        controller = self.supervisor.controller

        try:
            # Check the controller's lease before starting procedure logic.
            if self.interrupt is None and not await controller.is_online():
                raise ControllerOffline(f"{controller.entity} is offline")

            # A procedure interrupted before its first step has nothing to run.
            if self.interrupt is None:
                self._logic = asyncio.create_task(self.state_logic())
                await self._finish(self._logic)

                # Failed logic escalates to a graceful stop.
                if not self._logic.cancelled() and self._logic.exception() is not None:
                    self.request_stop()

            if self.interrupt == "stop":
                self._stopping = asyncio.create_task(self._stop())
                await self._finish(self._stopping)

                # Work that did not end gracefully is aborted.
                if self.interrupt != "abort" and not self._stopping.result():
                    self.request_abort()

            if self.interrupt == "abort":
                await self._abort()
        finally:
            self.unresolved |= any(not future.done() for future in self._owned()) or any(
                isinstance(call.error(), TaskOutcomeUnknown) for call in self._calls
            )

            for error in self.cleanup_errors:
                logger.opt(exception=error).warning(
                    f"{type(self).__name__} cleanup failed: {error}"
                )

        if (error := self._failure()) is not None:
            raise error

    @staticmethod
    async def _finish(stage: asyncio.Task):
        """Wait for a stage, canceling and joining it if this wait is canceled."""
        try:
            await asyncio.wait((stage,))
        except asyncio.CancelledError:
            stage.cancel()
            await defer_cancellation(asyncio.wait((stage,)))
            raise

    async def _stop(self) -> bool:
        """Wait for tracked tasks and futures to finish, then run the stop logic.

        Returns:
            Whether tracked work and stop logic finished within their time limits.
        """
        if await interruptible_wait(self._owned(), timeout=self.graceful_budget):
            return False

        await self._read_ended()

        try:
            async with asyncio.timeout(self.graceful_budget):
                await self.stop_logic()
        except Exception as e:
            self.cleanup_errors.append(e)
            return False

        return True

    async def _abort(self):
        """Abort tracked tasks, cancel registered futures, then run the abort logic."""
        for call in self._calls:
            call.abort()

        for future in self._cleanup_futures:
            future.cancel("Procedure abort")

        if await interruptible_wait(self._owned(), timeout=self.abort_budget):
            logger.error(f"{type(self).__name__} owned work did not end after abort")

        await self._read_ended()

        try:
            async with asyncio.timeout(self.abort_budget):
                await self.abort_logic()
        except Exception as e:
            self.cleanup_errors.append(e)

    def _owned(self) -> list[asyncio.Future]:
        """Return completion futures for tracked task calls and registered cleanup work."""
        return [*(call.future() for call in self._calls), *self._cleanup_futures]

    async def _read_ended(self):
        """Refresh controller state after an interrupted task call finishes.

        The task may have changed state before the monitor received the update.
        """
        if any(self._ended(call) for call in self._calls):
            with contextlib.suppress(ControllerFailure, ControllerStateUnknown):
                await self.supervisor.refresh()

    def _failure(self) -> BaseException | None:
        """Return the primary failure of a finished procedure."""
        logic = self._logic

        if logic is not None and not logic.cancelled() and (error := logic.exception()):
            return error

        # A request can fail after interruption ended the logic that sent it.
        for call in self._calls:
            match cause := call.error():
                case CallRejected():
                    error = ProcedureError(f"{call.task.task_type} task rejected")
                case TaskFailed(aborted=False):
                    error = ControllerFailure(f"{call.task.task_type} task failed")
                case _:
                    continue

            error.__cause__ = cause
            return error

        return next(iter(self.cleanup_errors), None)

    def request_stop(self):
        """Request a graceful end, letting sent requests and tasking finish."""
        if self.interrupt is None:
            self.interrupt = "stop"

            if self._logic is not None:
                self._logic.cancel()

    def request_abort(self):
        """Request abort cleanup, interrupting state logic or graceful cleanup."""
        if self.interrupt != "abort":
            self.interrupt = "abort"

            for stage in (self._logic, self._stopping):
                if stage is not None:
                    stage.cancel()

    def may_dispatch(self) -> bool:
        """Return whether the procedure may start another operation.

        Interruption or unhealthy state supervision prevents further operations.
        """
        return self.interrupt is None and self.supervisor.healthy()

    def _check_dispatch(self):
        """Raise cancellation if the procedure may not start another operation."""
        if not self.may_dispatch():
            raise asyncio.CancelledError()

    async def _prepare(self, client: EntityClient):
        """Subscribe to client events and check that another operation is still permitted.

        Callers must send their request before the next suspension to preserve this check.
        """
        await client.subscribe_events()
        self._check_dispatch()

    def pending(self) -> bool:
        """Return True while a tracked lifecycle task call is awaiting a result."""
        return any(not call.future().done() for call in self._calls)

    @staticmethod
    def _ended(call: TaskCall) -> bool:
        """Return whether a call's execution is known to have ended."""
        return call.future().done() and isinstance(call.error(), TaskFailed | None)

    async def run_task(
        self,
        task: Task,
        *,
        context: KeywordDict | None = None,
        timeout: float | None = None,
    ) -> ControllerState:
        """Execute and track a lifecycle task, interrupting any controller task in progress.

        Outside recovery, the supervisor expects a transition from dispatch until it verifies
        the result. Any other outcome leaves the expected state unknown.

        Args:
            task: The lifecycle task to execute.
            context: Optional keyword context to attach to the execution.
            timeout: Seconds before the execution is aborted, or None to wait for its result.

        Returns:
            The controller state read after the task succeeded.

        Raises:
            ProcedureError: If the request is rejected.
            ControllerFailure: If the task fails, times out, or the controller state cannot be
                read afterward.
            ControllerIndeterminate: If the execution may still be running.
        """
        controller = self.supervisor.controller
        await self._prepare(controller)

        call = TaskCall(controller, task, context=context, interrupt=True)
        self._calls.append(call)

        if self.expects_target:
            self.supervisor.expect_transition()

        # Dispatch before yielding after the interruption check.
        # Keep waiting for acceptance if procedure logic is interrupted.
        sent = asyncio.eager_task_factory(asyncio.get_running_loop(), call.send())
        sent.add_done_callback(cleanup_future)

        try:
            # Read dispatch failures from the call's shared result below.
            with contextlib.suppress(CallRejected, TaskOutcomeUnknown):
                await asyncio.shield(sent)

            if await interruptible_wait((call.future(),), timeout=timeout):
                call.abort()
                await interruptible_wait((call.future(),), timeout=self.abort_budget)

            error = call.error()

            # Remove resolved calls before raising their result to avoid duplicate cleanup errors.
            if call.future().done() and not isinstance(error, TaskOutcomeUnknown):
                self._calls.remove(call)

            match error:
                case None if not call.future().done():
                    raise ControllerIndeterminate(
                        f"{task.task_type} execution may still be running"
                    )
                case None if self.expects_target:
                    return await self.confirm_state(task.target_state(), context, commanded=True)
                case None if (state := task.target_state()) is not None:
                    return await self.supervisor.verify(state, context)
                case None:
                    return await self.supervisor.refresh()
                case CallRejected():
                    raise ProcedureError(f"{task.task_type} task rejected") from error
                case TaskFailed(aborted=True):
                    raise ControllerFailure(f"{task.task_type} task aborted") from error
                case TaskFailed():
                    raise ControllerFailure(f"{task.task_type} task failed") from error
                case _:
                    raise ControllerIndeterminate(
                        f"{task.task_type} execution may still be running"
                    ) from error
        except BaseException:
            self.supervisor.invalidate()
            raise

    async def run_unless_achieved(self, task: Task):
        """Run a lifecycle task unless the controller is known to be settled at its target state.

        Both the state and demanded lifecycle context must match, with no transition pending.
        Matching controller state alone is not enough after a failure or an unconfirmed task.

        Raises:
            ProcedureError: If the task request is rejected.
            ControllerFailure: If the task fails or the resulting state or context differs.
            ControllerIndeterminate: If the task cannot be confirmed stopped.
        """
        state = task.target_state()
        context = self.demand.lifecycle_context()
        self._check_dispatch()

        # Controllers should also skip work that is already done, but the agent does not rely on it.
        if self.supervisor.settled(state, context):
            await self.confirm_state(state, context, commanded=False)
            return

        await self.run_task(task, context=context)

    async def confirm_state(
        self,
        state: InternalControllerState,
        context: KeywordDict | None = None,
        *,
        commanded: bool,
    ) -> ControllerState:
        """Verify and record the required controller state and lifecycle context.

        Args:
            state: The required state.
            context: The required lifecycle context, or None for an empty context.
            commanded: Whether this procedure's own lifecycle task established the state.
        """
        snapshot = await self.supervisor.confirm(state, context)
        self.achieved = True
        self.commanded |= commanded
        return snapshot

    @abstractmethod
    async def state_logic(self):
        """Run logic for this procedure."""

    async def stop_logic(self):
        """Perform graceful cleanup specific to this procedure.

        Runs after tracked lifecycle tasks and registered futures finish. Overrides should stop
        any program tasking they started or adopted. Failure, timeout, or an abort request
        escalates to abort logic.
        """

    async def abort_logic(self):
        """Perform abort cleanup specific to this procedure.

        Runs after aborting tracked lifecycle tasks and canceling registered futures.
        Overrides should abort any program tasking they started or adopted.
        """

    async def shield_with_cleanup[T](self, fut: asyncio.Future[T]) -> T:
        """Await a future while shielding it from interruption.

        Graceful cleanup waits for the future, and abort cancels it.
        """
        self._cleanup_futures.add(fut)
        result = await asyncio.shield(fut)
        self._cleanup_futures.discard(fut)
        return result

    @final
    async def stop(self):
        """Request a graceful end and wait until cleanup has finished."""
        self.request_stop()
        await self.join()

    @final
    async def abort(self):
        """Request an immediate end and wait until cleanup has finished."""
        self.request_abort()
        await self.join()

    @final
    def cancel(self):
        """Cancel procedure logic and cleanup without sending further requests.

        Work that has not ended remains unresolved.
        """
        self.future().cancel()

    @final
    async def join(self):
        """Wait for the procedure and cleanup without propagating their exceptions."""
        await asyncio.wait((self.future(),))

    @final
    def done(self):
        """Return True if the procedure and its cleanup have finished."""
        return self.future().done()

    @final
    def error(self) -> BaseException | None:
        """Return the procedure's failure, or None if running, successful, or cancelled."""
        task = self.future()

        if not task.done() or task.cancelled():
            return None

        return task.exception()

    @final
    def future(self) -> asyncio.Task[None]:
        """Return the procedure task for use as an awaitable or in `asyncio.wait`."""
        assert self._task is not None, "procedure not started"
        return self._task


class OperateProcedure(LifecycleProcedure):
    """Achieve OPERATE and supervise program tasking.

    Cleanup targets the session started or acknowledged by this procedure.
    Returning from state logic leaves tasking for the next procedure to adopt or abort.

    Each procedure owns one tasking session. The session's first reported fault remains the
    procedure's failure through cleanup.
    """

    activation_timeout: ClassVar[float] = 5.0
    """Seconds to wait for program tasking to start."""

    def __init__(self, supervisor: ControllerStateSupervisor, demand: DemandState):
        super().__init__(supervisor, demand)
        self._activation: ProgramActivation | None = None
        self._ends_tasking = False

    @override
    def _failure(self) -> BaseException | None:
        if (fault := self._tasking_fault()) is not None:
            return self._fault_error(fault)

        return super()._failure()

    @override
    async def state_logic(self):
        controller = str(self.supervisor.controller.entity)
        program = self.demand.program

        self.step = LifecycleStep.INIT
        await self._reconcile_existing_tasking()

        # Matching tasking has already been acknowledged by activation.
        adopted = self._activation is not None

        logger.info(f"Preparing to operate ({controller})")
        await self.run_unless_achieved(InitTask())

        # Maintain OPERATE while waiting for a program demand.
        if program is None:
            self.step = LifecycleStep.WAIT
            logger.info(f"Waiting for a program for tasking ({controller})")
            await asyncio.sleep(float("inf"))

        self.step = LifecycleStep.TASKING

        if adopted:
            logger.info(f"Resuming {program.entity} tasking ({controller})")
        else:
            logger.info(f"Activating {program.entity} program ({controller})")
            await self.activate_tasking()

        logger.info(f"Controller is operating ({controller})")

        # Follow session faults, external stop requests, and program restarts.
        stop_event = await self._tasking_ended()

        match stop_event.origin:
            case "error":
                # Unclassified legacy errors count toward controller recovery.
                logger.error(f"Error reported ({controller})")
                raise ControllerFailure("Program reported error")
            case "init":
                # TODO: Bound tasking retries after program restarts.
                logger.warning(f"Active program restarted ({controller})")

                # Pause briefly, then return for replacement.
                self.step = LifecycleStep.WAIT
                await asyncio.sleep(3)
            case "request":
                logger.info(f"Tasking stopped by request ({controller})")
                raise ProcedureError("Program was stopped")

    @override
    async def stop_logic(self):
        await self._end_tasking(abort=False)

    @override
    async def abort_logic(self):
        try:
            await self._end_tasking(abort=True)
        finally:
            # A failed abort leaves no confirmation that program tasking stopped.
            self.unresolved |= self._ends_tasking

    async def _end_tasking(self, *, abort: bool):
        """Wait for startup, then stop or abort the accepted session."""
        activation = self._activation

        if activation is None:
            return

        try:
            await activation
        except Exception as e:
            self._classify_activation_failure(e)

        try:
            if not self._ends_tasking:
                return

            # Cleanup requires the identity returned by acceptance.
            if activation.monitor is None:
                self.unresolved = True
                return

            program = self.demand.program
            end = program.abort_tasking if abort else program.stop_tasking
            await end(activation.monitor.session_id)
            self._ends_tasking = False
        finally:
            # Read any session fault preceding the cleanup reply.
            if activation.monitor is not None:
                activation.monitor.read_events()

    @staticmethod
    def _fault_error(state: TaskingLoopState) -> ProcedureError:
        """Convert a session fault to a procedure failure."""
        operation = state.stage if state.task_type is None else f"{state.task_type} {state.stage}"
        message = f"Tasking failed during {operation} ({state.error})"

        match state.fault:
            case "controller":
                return ControllerFailure(message)
            case _:
                return ProcedureError(message)

    def _tasking_fault(self) -> TaskingLoopState | None:
        """Return the owned session's first reported fault, if any."""
        if self._activation is None or (monitor := self._activation.monitor) is None:
            return None

        return monitor.fault

    async def _tasking_ended(self) -> ProgramActiveState:
        """Wait for the owned session to end or report a fault.

        Raises:
            ProcedureError: If the session reported a fault.
        """
        monitor = self._activation.monitor
        await monitor.wait()

        if (fault := monitor.fault) is not None:
            raise self._fault_error(fault)

        return monitor.ending

    async def _reconcile_existing_tasking(self):
        """Adopt the demanded program's active tasking, or abort it.

        Adopt tasking only if its controller and task contexts match demand and the controller
        reports the required state and lifecycle context. Otherwise, abort the program's tasking.

        Raises:
            ProcedureError: If existing tasking could not be acknowledged or ended.
        """
        program = self.demand.program

        if program is None:
            return

        try:
            tasking = await program.kv_get_model(ProgramState)
        except KeyNotFound:
            return

        if not tasking.active_state.active:
            return

        enabled_on = tasking.enable_state.controller
        session = tasking.tasking_state.loop_state

        if (
            not tasking.active_state.stopping
            and (session is None or session.fault is None)
            and enabled_on is not None
            and Entity.at(enabled_on) == self.supervisor.controller.entity
            and tasking.active_state.contexts == (self.demand.contexts or TaskContextMap())
            and self.supervisor.settled(self.demand.state, self.demand.lifecycle_context())
        ):
            await self.activate_tasking(adopt=True)
            return

        await self._prepare(program)

        # Reconcile current tasking before taking responsibility for a session.
        try:
            async with asyncio.timeout(self.abort_budget):
                await program.abort_tasking()
        except Exception as e:
            raise ProcedureError(f"Could not abort {program.entity} tasking") from e

    async def activate_tasking(self, *, adopt: bool = False):
        """Activate tasking and retain its identity for cleanup.

        New activation clears running controller work first. Adoption preserves the matching
        session's execution. Cleanup waits for activation and targets its returned session ID.
        Uncertain acceptance or completion leaves tasking unresolved.

        Args:
            adopt: Whether existing tasking already matches demand.

        Raises:
            ControllerFailure: If a running execution could not be aborted.
            ProcedureError: If tasking did not start in time.
        """
        program = self.demand.program
        controller = self.supervisor.controller

        if self._activation is not None:
            raise RuntimeError("Tasking was already activated")

        if not adopt and (execution := self.supervisor.running_execution()) is not None:
            await self._prepare(controller)

            try:
                async with asyncio.timeout(self.abort_budget):
                    await controller.abort_task(execution.task_id)
            except CallRejected:
                # The execution already ended. A later request is rejected if other work started.
                pass
            except Exception as e:
                raise ControllerFailure(f"Could not abort {execution.task.task_type} task") from e

        activation = program.start_tasking(self.demand.contexts)

        try:
            # Keep a sent start request alive for cleanup if activation times out.
            async with asyncio.timeout(self.activation_timeout) as deadline:
                await activation.prepare()

                # Prevent dispatch if procedure cancellation is already pending.
                if self.future().cancelling():
                    raise asyncio.CancelledError()

                # Recheck demand after preparation and send before yielding.
                self._check_dispatch()
                self._activation = activation
                self._ends_tasking = True
                self.future().add_done_callback(lambda _: activation.close())
                await activation.send()
        except Exception as e:
            if deadline.expired():
                raise ProcedureError("Tasking did not start in time") from e

            cause = e if self._activation is None else self._classify_activation_failure(e)
            raise ProcedureError("Failed to start tasking") from cause
        finally:
            if self._activation is None:
                activation.close()

    def _classify_activation_failure(self, error: Exception) -> Exception:
        """Classify an activation failure and update cleanup requirements."""
        program = self.demand.program.entity

        match error:
            case CallRejected():
                # A rejected start needs no session cleanup.
                self._ends_tasking = False
                return error
            case TaskingStartupFailed():
                # A failure response confirms that the start handler finished.
                return error
            case TaskingError():
                message = f"{program} did not confirm tasking"
            case _:
                message = f"{program} may start tasking later"

        self.unresolved = True
        indeterminate = ControllerIndeterminate(message)
        indeterminate.__cause__ = error
        return indeterminate


class StandbyProcedure(LifecycleProcedure):
    """Achieve STANDBY and wait for replacement."""

    @override
    async def state_logic(self):
        controller = str(self.supervisor.controller.entity)

        self.step = LifecycleStep.STANDBY

        logger.info(f"Entering standby mode ({controller})")
        await self.run_unless_achieved(StandbyTask())

        self.step = LifecycleStep.WAIT

        logger.info(f"Controller is idle in standby ({controller})")
        await asyncio.sleep(float("inf"))


class ShutdownProcedure(LifecycleProcedure):
    """Achieve SHUTDOWN and wait for replacement."""

    @override
    async def state_logic(self):
        controller = str(self.supervisor.controller.entity)

        self.step = LifecycleStep.SHUTDOWN

        logger.info(f"Commencing shutdown ({controller})")
        await self.run_unless_achieved(ShutdownTask())

        self.step = LifecycleStep.WAIT

        logger.info(f"Controller is shut down ({controller})")
        await asyncio.sleep(float("inf"))


class RecoverProcedure(LifecycleProcedure):
    """Run one recovery attempt, or an emergency shutdown once attempts are exhausted.

    Recovery can start from and leave any reported state, so it leaves the expected state
    unknown. The reactor schedules attempts and evaluates demand again after each one.
    """

    expects_target = False

    recover_timeout: ClassVar[float] = 120.0
    """Seconds before a recovery attempt is aborted."""

    shutdown_timeout: ClassVar[float] = 240.0
    """Seconds before an emergency shutdown is aborted."""

    def __init__(
        self,
        supervisor: ControllerStateSupervisor,
        demand: DemandState,
        *,
        emergency: bool,
    ):
        super().__init__(supervisor, demand)
        self.emergency = emergency
        """Whether this attempt is an emergency shutdown."""
        self.recovered = False
        """Whether the recovery task succeeded."""

    @override
    async def state_logic(self):
        controller = str(self.supervisor.controller.entity)

        if self.emergency:
            self.step = LifecycleStep.SHUTDOWN
            task = ShutdownTask()
            context = self.demand.lifecycle_context(task.target_state())

            logger.warning(f"Failed to recover, attempting shutdown ({controller})")
            await self.run_task(task, context=context, timeout=self.shutdown_timeout)

            # Emergency shutdown leaves control disabled pending operator intervention.
            logger.info(f"Emergency shutdown succeeded ({controller})")
            raise ControllerUnrecoverable("Emergency shutdown")

        self.step = LifecycleStep.RECOVER

        logger.info(f"Attempting recovery ({controller})")
        await self.run_task(RecoverTask(), timeout=self.recover_timeout)

        logger.info(f"Recovery succeeded ({controller})")
        self.recovered = True
