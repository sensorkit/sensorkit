# SPDX-License-Identifier: Apache-2.0
"""Tests for lifecycle reconciliation of demand with reported controller state."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
import pytest_asyncio

from sensorkit.auto.lifecycle import ControllerLifecycle, LifecycleStep
from sensorkit.auto.procedures import ControllerIndeterminate, ControllerOffline
from sensorkit.auto.reactor import RecoveryState
from sensorkit.backend.request import CallError
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.client import SensorKit
from sensorkit.core.controller import (
    ControllerState,
    ControllerStateUnknown,
    InternalControllerState,
)
from sensorkit.core.program import (
    ProgramActiveState,
    ProgramEnableState,
    ProgramState,
    ProgramTaskingState,
    set_active_state_request,
)
from sensorkit.core.task import (
    CollectTask,
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
    TaskContextMap,
    TaskExecution,
)

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY
SHUTDOWN = InternalControllerState.SHUTDOWN
ERROR = InternalControllerState.ERROR
UNKNOWN = InternalControllerState.UNKNOWN
TRANSITION = InternalControllerState.TRANSITION


class QuickRestart(ControllerLifecycle):
    """Restart reactors immediately in tests."""

    restart_delay = 0.05


def quick_recovery_state() -> RecoveryState:
    """Return a recovery state that retries without the production delays."""
    return RecoveryState(retry_delay=0.05, attempt_delay=0.05, cycle_delay=0.05)


async def operating(kit) -> ControllerState:
    return await kit.controller("ctrl1").kv_get_model(ControllerState)


async def reports(kit, state: InternalControllerState) -> bool:
    """Return whether ctrl1 reports the requested state with no transition pending."""
    operating_state = (await operating(kit)).operating_state
    return operating_state.current == state and operating_state.target is None


async def tasking_inactive(kit, name: str) -> bool:
    return not (await kit.program(name).kv_get_model(ProgramState)).active_state.active


@contextlib.asynccontextmanager
async def started(lifecycle: ControllerLifecycle, kit, *gates: asyncio.Event):
    """Start a lifecycle, then release handler gates and join the lifecycle on exit."""
    lifecycle.start(kit.controller("ctrl1"))

    try:
        yield lifecycle
    finally:
        for gate in gates:
            gate.set()

        async with asyncio.timeout(10.0):
            await lifecycle.stop()


@pytest_asyncio.fixture
async def controller(kit, service_context):
    impl = await service_context.register_controller("ctrl1")
    await kit.controller("ctrl1").enable()
    return impl


@pytest_asyncio.fixture
async def inits(controller):
    """Count successful initialization tasks."""
    count = [0]

    @controller.task_handler(InitTask)
    async def init(task):
        count[0] += 1

    return count


async def register_program(kit, service_context, name: str, ran: list[str] | None = None):
    """Register a program that yields one collect per tasking session."""
    program = await service_context.register_program(name)

    @program.task_factory
    async def factory():
        if ran is not None:
            ran.append(name)

        yield CollectTask()

    await kit.program(name).enable("ctrl1")
    return kit.program(name)


async def register_idle_program(kit, service_context, name: str):
    """Register a program that never yields a task."""
    program = await service_context.register_program(name)

    @program.task_factory
    async def factory():
        return
        yield

    await kit.program(name).enable("ctrl1")
    return kit.program(name)


# Recovery state


def test_repeated_failure_calls_for_recovery():
    """One failure delays a retry, and a second calls for recovery without delay."""
    recovery_state = RecoveryState(retry_delay=5.0)
    recovery_state.record_failure(100.0)

    assert recovery_state.waiting(101.0) == 105.0
    assert not recovery_state.recovery_due

    recovery_state.record_failure(106.0)

    assert recovery_state.recovery_due
    assert recovery_state.waiting(106.0) is None


def test_exhausted_attempts_call_for_emergency_shutdown():
    """Failed recovery attempts delay retries until the limit triggers emergency shutdown."""
    recovery_state = RecoveryState(recovery_attempts=2, attempt_delay=20.0, cycle_delay=120.0)
    recovery_state.record_failed_attempt(0.0)

    assert recovery_state.waiting(1.0) == 20.0
    assert not recovery_state.emergency_due

    recovery_state.record_failed_attempt(20.0)

    assert recovery_state.emergency_due
    assert recovery_state.waiting(20.0) is None

    recovery_state.record_failed_emergency(30.0)

    assert not recovery_state.emergency_due
    assert recovery_state.waiting(31.0) == 150.0


def test_initial_error_counts_only_without_prior_failures():
    """An error found at startup adds no failure to one already counted."""
    recovery_state = RecoveryState()
    recovery_state.record_initial_error()
    recovery_state.record_initial_error()

    assert recovery_state.failures == 1
    assert recovery_state.waiting(0.0) is None


# Replacement


@pytest.mark.asyncio
async def test_interrupting_demand_escalates_graceful_stop(
    kit, service_context, controller, inits, until
):
    """An interrupting demand aborts tasking that a graceful replacement is waiting on."""
    collect_started = asyncio.Event()
    collect_aborted = asyncio.Event()
    collect_release = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        collect_started.set()

        try:
            await collect_release.wait()
        except asyncio.CancelledError:
            collect_aborted.set()
            raise

    @controller.task_handler(ShutdownTask)
    async def shutdown(task):
        shutdown_ran.set()

    program = await register_program(kit, service_context, "prog1")

    async with started(ControllerLifecycle(), kit, collect_release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        async with asyncio.timeout(5.0):
            await collect_started.wait()

        # A graceful replacement waits for the running collect.
        lifecycle.set_demand_state(STANDBY)
        await asyncio.sleep(0.2)

        assert not collect_aborted.is_set()

        lifecycle.set_demand_state(SHUTDOWN, interrupt=True)

        async with asyncio.timeout(5.0):
            await collect_aborted.wait()
            await shutdown_ran.wait()

        await until(lambda: reports(kit, SHUTDOWN))

    # The superseded standby demand was never commanded.
    assert inits[0] == 1


@pytest.mark.asyncio
async def test_latest_demand_wins_after_cleanup(kit, service_context, controller, inits, until):
    """Demand changes during cleanup coalesce into the latest demand."""
    first_started = asyncio.Event()
    first_release = asyncio.Event()
    standby_ran = asyncio.Event()
    collects = 0
    ran: list[str] = []

    @controller.task_handler(CollectTask)
    async def collect(task):
        nonlocal collects
        collects += 1

        if collects == 1:
            first_started.set()
            await first_release.wait()

    @controller.task_handler(StandbyTask)
    async def standby(task):
        standby_ran.set()

    prog_a = await register_program(kit, service_context, "prog_a", ran)
    prog_b = await register_program(kit, service_context, "prog_b", ran)
    prog_c = await register_program(kit, service_context, "prog_c", ran)

    async with started(ControllerLifecycle(), kit, first_release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=prog_a)

        async with asyncio.timeout(5.0):
            await first_started.wait()

        lifecycle.set_demand_state(OPERATE, program=prog_b)
        await asyncio.sleep(0)
        lifecycle.set_demand_state(STANDBY)
        await asyncio.sleep(0)
        lifecycle.set_demand_state(OPERATE, program=prog_c)

        first_release.set()
        await until(lambda: "prog_c" in ran)

    assert "prog_b" not in ran
    assert not standby_ran.is_set()

    # The controller stayed at OPERATE, so the latest demand needed no second init.
    assert inits[0] == 1


@pytest.mark.asyncio
async def test_same_demand_with_interrupt_escalates_cleanup(kit, controller):
    """Repeating a demand with an interrupt aborts the cleanup that demand is waiting on."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        entered.set()

        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @controller.task_handler(ShutdownTask)
    async def shutdown(task):
        shutdown_ran.set()

    async with started(ControllerLifecycle(), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        async with asyncio.timeout(5.0):
            await entered.wait()

        # Graceful replacement waits for the running init.
        assert lifecycle.set_demand_state(SHUTDOWN)
        await asyncio.sleep(0.2)

        assert not cancelled.is_set()

        # The same demand, now interrupting, is not a demand change.
        assert not lifecycle.set_demand_state(SHUTDOWN, interrupt=True)

        async with asyncio.timeout(5.0):
            await cancelled.wait()
            await shutdown_ran.wait()


# Resuming and taking control


@pytest.mark.asyncio
async def test_settled_controller_needs_no_command(kit, controller, inits, until):
    """A controller already reporting the demanded state and context needs no lifecycle task."""
    await controller.set_internal_state(OPERATE)

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        await until(lambda: lifecycle.step == LifecycleStep.WAIT)
        await asyncio.sleep(0.2)

    assert inits[0] == 0


@pytest.mark.asyncio
async def test_failures_under_reported_state_escalate(
    kit, service_context, controller, inits, until
):
    """A lifecycle task skipped for a reported state does not reset the failure count."""
    recoveries = 0

    @controller.task_handler(CollectTask)
    async def collect(task):
        if not recoveries:
            raise RuntimeError("devices not connected")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        nonlocal recoveries
        recoveries += 1

    program = await register_program(kit, service_context, "prog1")
    await controller.set_internal_state(OPERATE)
    lifecycle = ControllerLifecycle(recovery_state=quick_recovery_state())

    async with started(lifecycle, kit):
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        # Recovery leaves the state unknown, so init runs before tasking resumes.
        await until(lambda: recoveries == 1)
        await until(lambda: inits[0] == 1 and lifecycle.step == LifecycleStep.TASKING)


@pytest.mark.asyncio
async def test_matching_transition_is_awaited(kit, controller, until):
    """A running lifecycle task matching demand is allowed to finish without repeating it."""
    entered = asyncio.Event()
    release = asyncio.Event()
    inits = 0

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1
        entered.set()
        await release.wait()

    external = asyncio.ensure_future(kit.controller("ctrl1").execute_task(InitTask()))

    try:
        async with asyncio.timeout(5.0):
            await entered.wait()

        async with started(ControllerLifecycle(), kit, release) as lifecycle:
            lifecycle.enable()
            lifecycle.set_demand_state(OPERATE, program=None)
            await asyncio.sleep(0.2)

            assert not external.done()

            release.set()
            await until(lambda: reports(kit, OPERATE))
            await asyncio.sleep(0.2)

        await external
    finally:
        release.set()

        with contextlib.suppress(Exception):
            await external

    assert inits == 1


@pytest.mark.asyncio
async def test_matching_transition_wait_is_bounded(kit, controller, until):
    """A matching lifecycle task that outlasts the settle timeout is aborted and replaced."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    inits = 0

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        # Only the first init hangs.
        if inits == 1:
            entered.set()

            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    class QuickSettle(ControllerLifecycle):
        settle_timeout = 0.3

    external = asyncio.ensure_future(kit.controller("ctrl1").execute_task(InitTask()))

    try:
        async with asyncio.timeout(5.0):
            await entered.wait()

        async with started(QuickSettle(), kit) as lifecycle:
            lifecycle.enable()
            lifecycle.set_demand_state(OPERATE, program=None)

            async with asyncio.timeout(5.0):
                await cancelled.wait()

            await until(lambda: inits == 2)
            await until(lambda: reports(kit, OPERATE))
    finally:
        with contextlib.suppress(Exception):
            await external


@pytest.mark.asyncio
async def test_other_transition_is_interrupted(kit, controller, inits, until):
    """The demanded lifecycle task interrupts a running lifecycle task toward another state."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    @controller.task_handler(StandbyTask)
    async def standby(task):
        entered.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    external = asyncio.ensure_future(kit.controller("ctrl1").execute_task(StandbyTask()))

    async with asyncio.timeout(5.0):
        await entered.wait()

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        async with asyncio.timeout(5.0):
            await cancelled.wait()

        await until(lambda: inits[0] == 1)
        await until(lambda: reports(kit, OPERATE))

    with pytest.raises(CallError):
        await external


@pytest.mark.asyncio
async def test_task_without_tasking_is_interrupted(kit, controller, until):
    """The demanded lifecycle task interrupts a controller task started outside program tasking."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    standby_ran = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @controller.task_handler(StandbyTask)
    async def standby(task):
        standby_ran.set()

    external = asyncio.ensure_future(kit.controller("ctrl1").execute_task(CollectTask()))

    async with asyncio.timeout(5.0):
        await entered.wait()

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(STANDBY)

        async with asyncio.timeout(5.0):
            await cancelled.wait()
            await standby_ran.wait()

    with pytest.raises(CallError):
        await external


@pytest.mark.asyncio
async def test_activation_aborts_running_task(kit, service_context, controller, inits, until):
    """A running task that would reject program tasking is aborted when init is not needed."""
    collects = 0
    external_started = asyncio.Event()
    external_aborted = asyncio.Event()
    tasking_started = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        nonlocal collects
        collects += 1

        if collects == 1:
            external_started.set()
        else:
            tasking_started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if collects == 1:
                external_aborted.set()

            raise

    await controller.set_internal_state(OPERATE)
    program = await register_program(kit, service_context, "prog1")
    external = asyncio.ensure_future(kit.controller("ctrl1").execute_task(CollectTask()))

    async with asyncio.timeout(5.0):
        await external_started.wait()

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        async with asyncio.timeout(5.0):
            await external_aborted.wait()
            await tasking_started.wait()

    with pytest.raises(CallError):
        await external

    assert inits[0] == 0


@pytest.mark.asyncio
async def test_settled_demand_leaves_running_task(kit, controller, until):
    """A running task is left alone when the controller already reports the demanded state."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()

        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    await controller.set_internal_state(STANDBY)
    external = asyncio.ensure_future(kit.controller("ctrl1").execute_task(CollectTask()))

    async with asyncio.timeout(5.0):
        await entered.wait()

    async with started(ControllerLifecycle(), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(STANDBY)

        await until(lambda: lifecycle.step == LifecycleStep.WAIT)
        await asyncio.sleep(0.2)

        assert not cancelled.is_set()

    await external


@pytest.mark.asyncio
async def test_matching_tasking_is_adopted(kit, service_context, controller, inits, until):
    """A procedure adopts matching program tasking and aborts it during cleanup."""
    collect_started = asyncio.Event()
    collect_aborted = asyncio.Event()
    ran: list[str] = []

    @controller.task_handler(CollectTask)
    async def collect(task):
        collect_started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            collect_aborted.set()
            raise

    program = await register_program(kit, service_context, "prog1", ran)
    await controller.set_internal_state(OPERATE)
    tasking = await program.start_tasking()
    tasking.close()

    async with asyncio.timeout(5.0):
        await collect_started.wait()

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        await until(lambda: lifecycle.step == LifecycleStep.TASKING)
        await asyncio.sleep(0.2)
        state = await program.kv_get_model(ProgramState)

        assert not collect_aborted.is_set()
        assert ran == ["prog1"]
        assert state.tasking_state.loop_state.session_id == tasking.session_id

        # Ending the adopted tasking aborts its running collect.
        lifecycle.set_demand_state(STANDBY, interrupt=True)

        async with asyncio.timeout(5.0):
            await collect_aborted.wait()

        await until(lambda: tasking_inactive(kit, "prog1"))

    assert inits[0] == 0


@pytest.mark.asyncio
async def test_other_tasking_is_aborted(kit, service_context, controller, inits, until):
    """Program tasking is aborted and restarted if the controller state does not match demand."""
    collects = 0
    first_aborted = asyncio.Event()
    second_started = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        nonlocal collects
        collects += 1

        if collects > 1:
            second_started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if collects == 1:
                first_aborted.set()

            raise

    program = await register_program(kit, service_context, "prog1")

    # Establish OPERATE with a lifecycle context that differs from demand.
    other = KeywordDict()
    other.set_value("dome", "closed")
    await controller.set_internal_state(OPERATE, context=other)
    (await program.start_tasking()).close()
    await until(lambda: collects == 1)

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        async with asyncio.timeout(5.0):
            await first_aborted.wait()
            await second_started.wait()

        await until(lambda: lifecycle.step == LifecycleStep.TASKING)

    assert inits[0] == 1


# Postconditions, retry, and recovery


@pytest.mark.asyncio
async def test_staged_context_is_independent_of_caller_changes(kit, controller, until):
    """Mutating caller contexts cannot alter stored demand or hide a later demand change."""
    offsets = [[1.0, 2.0]]
    contexts = TaskContextMap(by_type={"init": KeywordDict({"offsets": offsets})})
    reported: list[float] = []

    async def init(execution: TaskExecution):
        reported.append(execution.context["offsets"][0][0])

    controller.add_task_handler(InitTask, execution_callback=init)

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.set_demand_state(OPERATE, contexts=contexts, program=None)
        offsets[0][0] = 3.0
        lifecycle.enable()
        await until(lambda: len(reported) == 1)

        assert reported == [1.0]
        assert lifecycle.set_demand_state(OPERATE, contexts=contexts, program=None)

        await until(lambda: len(reported) == 2)

    assert reported == [1.0, 3.0]


@pytest.mark.asyncio
async def test_failed_postcondition_does_not_start_tasking(
    kit, service_context, controller, inits, until
):
    """A successful init whose reported context differs cannot start tasking."""
    ran: list[str] = []
    program = await register_program(kit, service_context, "prog1", ran)

    # A tuple is reported back as a list, so the controller reports another context.
    context = KeywordDict()
    context.set_value("dome", ("open",))
    contexts = TaskContextMap(by_type={"init": context})

    async with started(ControllerLifecycle(recovery_state=quick_recovery_state()), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, contexts=contexts, program=program)

        # Each mismatch is a controller failure, so the second one recovers.
        await until(lambda: lifecycle.step == LifecycleStep.RECOVER)

        state = await program.kv_get_model(ProgramState)
        reported = (await operating(kit)).operating_state

    assert inits[0] == 2
    assert ran == []
    assert not state.active_state.active
    assert reported.current in (OPERATE, UNKNOWN)


@pytest.mark.asyncio
async def test_one_retry_precedes_recovery(kit, controller, until):
    """Repeated reports of one failure do not increase the failure count during the retry delay."""
    inits = 0
    recovered = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        if inits == 1:
            raise RuntimeError("init failed")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        recovered.set()

    recovery_state = RecoveryState(retry_delay=0.5)

    async with started(ControllerLifecycle(recovery_state=recovery_state), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        await until(lambda: inits == 1)
        await until(lambda: reports(kit, ERROR))

        # Repeated reports of the failure while the retry waits.
        await controller.set_internal_state(UNKNOWN)
        await controller.set_internal_state(ERROR)

        await until(lambda: inits == 2)
        await until(lambda: reports(kit, OPERATE))

    assert not recovered.is_set()


@pytest.mark.asyncio
async def test_recovery_accepts_any_reported_state(kit, controller, until):
    """A successful recovery may report a state other than the demanded state."""
    inits = 0
    recoveries = 0

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        if not recoveries:
            raise RuntimeError("init failed")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        nonlocal recoveries
        recoveries += 1

        # Recovery from ERROR into a state nobody demanded.
        await controller.set_internal_state(STANDBY)

    async with started(ControllerLifecycle(recovery_state=quick_recovery_state()), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        await until(lambda: inits == 3)
        await until(lambda: reports(kit, OPERATE))

    assert recoveries == 1


@pytest.mark.asyncio
async def test_recovery_succeeds_after_failed_attempt(kit, controller, until):
    """A failed recovery attempt is followed by another, and success restores demand."""
    inits = 0
    attempts = 0

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        if attempts < 2:
            raise RuntimeError("init failed")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            raise RuntimeError("recovery failed")

    async with started(ControllerLifecycle(recovery_state=quick_recovery_state()), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        await until(lambda: inits == 3)
        await until(lambda: reports(kit, OPERATE))
        await asyncio.sleep(0.2)

    assert attempts == 2
    assert inits == 3


@pytest.mark.asyncio
async def test_exhausted_recovery_shuts_down_and_disables(kit, controller, until):
    """Recovery attempts that keep failing end in an emergency shutdown that disables control.

    The shutdown carries the demand's shutdown context.
    """
    attempts = 0
    shutdowns: list[bool] = []
    contexts = TaskContextMap(by_type={"shutdown": KeywordDict({"park": True})})

    @controller.task_handler(InitTask)
    async def init(task):
        raise RuntimeError("init failed")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("recovery failed")

    async def shutdown(execution: TaskExecution):
        shutdowns.append(execution.context["park"])

    controller.add_task_handler(ShutdownTask, execution_callback=shutdown)
    recovery_state = RecoveryState(retry_delay=0.05, recovery_attempts=2, attempt_delay=0.05)

    async with started(ControllerLifecycle(recovery_state=recovery_state), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, contexts=contexts, program=None)

        await until(lambda: not lifecycle.enabled)

    assert attempts == 2
    assert shutdowns == [True]


@pytest.mark.asyncio
async def test_unconfirmed_emergency_shutdown_resumes_recovery(kit, controller, until):
    """Emergency shutdown with a mismatched reported context leaves control enabled."""
    shutdowns = 0

    # A tuple is reported back as a list, so the controller reports another context.
    context = KeywordDict()
    context.set_value("dome", ("closed",))
    contexts = TaskContextMap(by_type={"shutdown": context})

    @controller.task_handler(InitTask)
    async def init(task):
        raise RuntimeError("init failed")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        raise RuntimeError("recovery failed")

    @controller.task_handler(ShutdownTask)
    async def shutdown(task):
        nonlocal shutdowns
        shutdowns += 1

    recovery_state = RecoveryState(
        retry_delay=0.05, recovery_attempts=1, attempt_delay=0.05, cycle_delay=0.05
    )

    async with started(ControllerLifecycle(recovery_state=recovery_state), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, contexts=contexts, program=None)

        await until(lambda: shutdowns == 2)

        assert lifecycle.enabled


@pytest.mark.asyncio
async def test_shutdown_after_recovery_reconnects(kit, controller, until):
    """Shutdown that needs recovery to reconnect completes after recovery."""
    connected = False
    shutdowns = 0

    @controller.task_handler(ShutdownTask)
    async def shutdown(task):
        nonlocal shutdowns
        shutdowns += 1

        if not connected:
            raise RuntimeError("device disconnected")

    @controller.task_handler(RecoverTask)
    async def recover(task):
        nonlocal connected
        connected = True

    async with started(ControllerLifecycle(recovery_state=quick_recovery_state()), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(SHUTDOWN)

        await until(lambda: connected)
        await until(lambda: reports(kit, SHUTDOWN))

    assert shutdowns == 3


# Out-of-band changes


@pytest.mark.asyncio
async def test_hold_keeps_expiry_for_same_key(kit, controller, inits, until):
    """Further external changes do not extend the delay while demand stays unchanged."""
    loop = asyncio.get_running_loop()

    async with started(ControllerLifecycle(external_change_delay=1.0), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)
        await until(lambda: inits[0] == 1 and lifecycle.step == LifecycleStep.WAIT)
        await until(lambda: reports(kit, OPERATE))

        held_at = loop.time()
        await controller.set_internal_state(STANDBY)

        # Another external context change wakes the reactor without changing demand.
        await asyncio.sleep(0.5)
        other = KeywordDict()
        other.set_value("dome", "closed")
        await controller.set_internal_state(STANDBY, context=other)

        await until(lambda: inits[0] == 2)
        elapsed = loop.time() - held_at

    assert 0.9 <= elapsed < 1.4


@pytest.mark.asyncio
async def test_hold_restarts_for_changed_key(kit, service_context, controller, inits, until):
    """Changing the demanded program restarts the delay after an external change."""
    loop = asyncio.get_running_loop()
    prog_a = await register_idle_program(kit, service_context, "prog_a")
    prog_b = await register_idle_program(kit, service_context, "prog_b")

    async with started(ControllerLifecycle(external_change_delay=1.0), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=prog_a)
        await until(lambda: lifecycle.step == LifecycleStep.TASKING)

        held_at = loop.time()
        await controller.set_internal_state(STANDBY)

        await asyncio.sleep(0.5)
        lifecycle.set_demand_state(OPERATE, program=prog_b)

        # The delay for the original demand would have expired by now.
        await asyncio.sleep(0.7)
        assert inits[0] == 1

        await until(lambda: inits[0] == 2)
        elapsed = loop.time() - held_at

    assert elapsed >= 1.4


@pytest.mark.asyncio
async def test_hold_survives_demand_change_during_cleanup(
    kit, service_context, controller, inits, until
):
    """Changing context during cleanup preserves the need to delay restoring demand."""
    loop = asyncio.get_running_loop()
    collect_started = asyncio.Event()
    collect_release = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        collect_started.set()
        await collect_release.wait()

    program = await register_program(kit, service_context, "prog1")
    context = KeywordDict()
    context.set_value("dome", "closed")
    contexts = TaskContextMap(by_type={"init": context})

    async with started(
        ControllerLifecycle(external_change_delay=1.0), kit, collect_release
    ) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        async with asyncio.timeout(5.0):
            await collect_started.wait()

        # Tasking stops gracefully after the external change, waiting for the collect.
        await controller.set_internal_state(STANDBY)
        await asyncio.sleep(0.1)

        # Demand changes its lifecycle context while cleanup runs.
        lifecycle.set_demand_state(OPERATE, contexts=contexts, program=program)
        await asyncio.sleep(0.1)
        released_at = loop.time()
        collect_release.set()

        await asyncio.sleep(0.5)
        assert inits[0] == 1

        await until(lambda: inits[0] == 2)
        elapsed = loop.time() - released_at

    assert elapsed >= 0.9


@pytest.mark.asyncio
async def test_expired_hold_does_not_restart_after_interrupted_init(
    kit, service_context, controller, until
):
    """A program switch after hold expiry restores demand without another external change."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()
    inits = 0

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        if inits == 2:
            entered.set()

            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    program = await register_idle_program(kit, service_context, "prog1")

    async with started(ControllerLifecycle(external_change_delay=1.0), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)
        await until(lambda: inits == 1 and lifecycle.step == LifecycleStep.WAIT)
        await until(lambda: reports(kit, OPERATE))
        await controller.set_internal_state(STANDBY)

        async with asyncio.timeout(5.0):
            await entered.wait()

        lifecycle.set_demand_state(OPERATE, program=program, interrupt=True)

        async with asyncio.timeout(5.0):
            await cancelled.wait()

        await until(lambda: inits == 3, timeout=0.7)
        await until(lambda: lifecycle.step == LifecycleStep.TASKING)


@pytest.mark.asyncio
async def test_shutdown_bypasses_hold(kit, controller, inits, until):
    """Shutdown does not wait for an out-of-band hold."""
    shutdown_ran = asyncio.Event()

    @controller.task_handler(ShutdownTask)
    async def shutdown(task):
        shutdown_ran.set()

    async with started(ControllerLifecycle(external_change_delay=30.0), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)
        await until(lambda: inits[0] == 1 and lifecycle.step == LifecycleStep.WAIT)
        await until(lambda: reports(kit, OPERATE))

        await controller.set_internal_state(STANDBY)
        await until(lambda: reports(kit, STANDBY))
        await asyncio.sleep(0.1)

        lifecycle.set_demand_state(SHUTDOWN)

        async with asyncio.timeout(5.0):
            await shutdown_ran.wait()

    assert inits[0] == 1


# Observation and restarts


@pytest.mark.asyncio
async def test_observation_failure_restarts_reactor(kit, service_context, controller, until):
    """Failed observation aborts owned work, and a fresh reactor resumes once reports return."""
    collect_started = asyncio.Event()
    collect_aborted = asyncio.Event()
    standby_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        pass

    @controller.task_handler(CollectTask)
    async def collect(task):
        collect_started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            collect_aborted.set()
            raise

    @controller.task_handler(StandbyTask)
    async def standby(task):
        standby_ran.set()

    program = await register_program(kit, service_context, "prog1")

    async with started(QuickRestart(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)

        async with asyncio.timeout(5.0):
            await collect_started.wait()

        await kit.backend.key_value(controller.entity).update("ControllerState", b"{}")

        async with asyncio.timeout(5.0):
            await collect_aborted.wait()

        await until(lambda: isinstance(lifecycle.last_error, ControllerStateUnknown))

        # The aborted collect republished a valid state, so a fresh reactor can act on demand.
        lifecycle.set_demand_state(STANDBY)

        async with asyncio.timeout(5.0):
            await standby_ran.wait()

    # Losing observation is not a controller failure.
    assert lifecycle.recovery_state.failures == 0


@pytest.mark.asyncio
async def test_unconfirmed_tasking_stop_restarts_reactor(kit, controller, entity_impl, until):
    """An unconfirmed program stop restarts the reactor, which then applies the latest demand."""
    activated = asyncio.Event()
    standby_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        pass

    @controller.task_handler(StandbyTask)
    async def standby(task):
        standby_ran.set()

    # The program starts tasking but can never confirm that its tasking stopped.
    async def set_active_state(msg, call):
        if msg.action != "start":
            raise RuntimeError("Tasking state unknown")

        await entity_impl.kv_put_model(
            ProgramState(
                enable_state=ProgramEnableState(enabled=True, controller="ctrl1"),
                active_state=ProgramActiveState(active=True, origin="request"),
                tasking_state=ProgramTaskingState(),
            )
        )
        call.accept()
        await call.succeed(result=None)
        activated.set()

    await entity_impl.handle_request(set_active_state_request, set_active_state)

    async with started(QuickRestart(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=kit.program(entity_impl.entity))

        async with asyncio.timeout(5.0):
            await activated.wait()

        lifecycle.set_demand_state(STANDBY)

        async with asyncio.timeout(5.0):
            await standby_ran.wait()

    assert isinstance(lifecycle.last_error, ControllerIndeterminate)


@pytest.mark.asyncio
async def test_offline_controller_restarts_reactor(kit, backend, until):
    """Finding the controller offline before starting a procedure ends the reactor."""
    controllers = await SensorKit(backend).register_service("controller-svc", "0.1.0")

    try:
        await controllers.register_controller("ctrl1")
        await kit.controller("ctrl1").enable()
    finally:
        await controllers.shutdown()

    async with started(QuickRestart(), kit) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(STANDBY)

        await until(lambda: isinstance(lifecycle.last_error, ControllerOffline))


@pytest.mark.asyncio
async def test_reactor_runs_only_while_enabled(kit, service_context, controller, inits, until):
    """A disabled lifecycle neither observes nor commands, and disabling ends the reactor."""
    program = await register_idle_program(kit, service_context, "prog1")

    async with started(ControllerLifecycle(), kit) as lifecycle:
        lifecycle.set_demand_state(OPERATE, program=program)
        await asyncio.sleep(0.2)

        assert inits[0] == 0

        lifecycle.enable()
        await until(lambda: lifecycle.step == LifecycleStep.TASKING)

        # The reactor ends once its procedure is cleaned up, without any further input.
        lifecycle.disable()
        await until(lambda: lifecycle.step == LifecycleStep.WAIT)
        await until(lambda: tasking_inactive(kit, "prog1"))

        await controller.set_internal_state(UNKNOWN)
        lifecycle.set_demand_state(OPERATE, program=None)
        await asyncio.sleep(0.2)

    assert inits[0] == 1


@pytest.mark.asyncio
async def test_repeated_stop_aborts_pending_init(kit, controller):
    """Concurrent lifecycle stops wait for the same cleanup and abort the pending init task."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        entered.set()

        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with started(ControllerLifecycle(), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        async with asyncio.timeout(5.0):
            await entered.wait()
            await asyncio.gather(lifecycle.stop(), lifecycle.stop())

    assert cancelled.is_set()
    assert not controller.task_running()


@pytest_asyncio.fixture
async def lingering_init(controller):
    """Register an init handler that records cancellation and keeps running until released."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        entered.set()

        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    return entered, cancelled, release


@pytest.mark.asyncio
async def test_second_stop_releases_unfinished_abort(kit, controller, lingering_init):
    """A second stop cancels cleanup without waiting for the controller task to end."""
    entered, cancelled, release = lingering_init

    async with started(ControllerLifecycle(), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        async with asyncio.timeout(5.0):
            await entered.wait()
            first = asyncio.create_task(lifecycle.stop())
            await cancelled.wait()

        second = asyncio.create_task(lifecycle.stop())

        # The wait is shorter than the abort budget.
        _, pending = await asyncio.wait((first, second), timeout=5.0)

        assert not pending
        assert controller.task_running()


@pytest.mark.asyncio
async def test_cancelled_stop_propagates(kit, controller, lingering_init):
    """Canceling the stop caller ends lifecycle cleanup while the controller task keeps running."""
    entered, cancelled, release = lingering_init

    async with started(ControllerLifecycle(), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)

        async with asyncio.timeout(5.0):
            await entered.wait()
            stopping = asyncio.create_task(lifecycle.stop())
            await cancelled.wait()

        stopping.cancel()

        # The wait is shorter than the abort budget.
        _, pending = await asyncio.wait((stopping,), timeout=5.0)

        assert not pending
        assert stopping.cancelled()
        assert controller.task_running()


# Program interrupt policy


@pytest.mark.asyncio
async def test_program_interrupt_waits_for_pending_init(kit, service_context, controller, until):
    """A program's interrupt policy cannot abort a pending lifecycle task."""
    entered = asyncio.Event()
    release = asyncio.Event()
    cancelled = asyncio.Event()
    inits = 0
    ran: list[str] = []

    @controller.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        if inits == 1:
            entered.set()

            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    @controller.task_handler(CollectTask)
    async def collect(task):
        pass

    prog_a = await register_program(kit, service_context, "prog_a", ran)
    prog_b = await register_program(kit, service_context, "prog_b", ran)

    async with started(ControllerLifecycle(), kit, release) as lifecycle:
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=prog_a)

        async with asyncio.timeout(5.0):
            await entered.wait()

        lifecycle.set_demand_state(OPERATE, program=prog_b, program_interrupt=True)
        await asyncio.sleep(0.2)

        assert not cancelled.is_set()

        release.set()
        await until(lambda: "prog_b" in ran)

    assert not cancelled.is_set()
    assert ran[0] == "prog_b"
