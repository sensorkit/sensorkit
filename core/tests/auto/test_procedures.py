# SPDX-License-Identifier: Apache-2.0
"""Tests for lifecycle procedure interruption, task tracking, and cleanup."""

import asyncio
import contextlib

import pytest
import pytest_asyncio
import uuid_utils.compat as uuid

from sensorkit.auto.failure import FailureState
from sensorkit.auto.lifecycle import ControllerLifecycle
from sensorkit.auto.procedures import (
    ControllerFailure,
    DemandState,
    LifecycleProcedure,
    LifecycleStep,
    OperateProcedure,
    ProcedureError,
    RecoverProcedure,
)
from sensorkit.auto.reactor import ControllerReactor
from sensorkit.auto.supervisor import ControllerStateSupervisor
from sensorkit.backend.request import declare_request
from sensorkit.core.controller import ControllerMonitor, InternalControllerState
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.program import (
    ProgramActiveStateRequest,
    ProgramActiveStateResponse,
    ProgramClient,
    ProgramState,
    set_active_state_request,
)
from sensorkit.core.task import CollectTask, InitTask

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY
UNKNOWN = InternalControllerState.UNKNOWN


class GatedProcedure(LifecycleProcedure):
    """Run procedure logic with a controlled outcome."""

    def __init__(self, supervisor: ControllerStateSupervisor):
        super().__init__(supervisor, DemandState(OPERATE))
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.outcome: Exception | None = None

    async def state_logic(self):
        self.started.set()
        await self.release.wait()

        if self.outcome is not None:
            raise self.outcome


class HookedProcedure(GatedProcedure):
    """Record the cleanup logic that runs, holding stop logic until released."""

    def __init__(self, supervisor: ControllerStateSupervisor):
        super().__init__(supervisor)
        self.ran: list[str] = []
        self.stopping = asyncio.Event()
        self.stop_release = asyncio.Event()
        self.stop_outcome: Exception | None = None

    async def stop_logic(self):
        self.ran.append("stop")
        self.stopping.set()
        await self.stop_release.wait()

        if self.stop_outcome is not None:
            raise self.stop_outcome

    async def abort_logic(self):
        self.ran.append("abort")


class ShieldingProcedure(LifecycleProcedure):
    """Await work shielded from interruption."""

    def __init__(self, supervisor: ControllerStateSupervisor, work: asyncio.Future[None]):
        super().__init__(supervisor, DemandState(OPERATE))
        self.started = asyncio.Event()
        self.work = work

    async def state_logic(self):
        self.started.set()
        await self.shield_with_cleanup(self.work)


class GatedAdmission(ControllerImpl):
    """Delay handling execute requests until the test releases them."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.received = asyncio.Event()
        self.admit = asyncio.Event()

    async def _execute_request(self, msg, call):
        self.received.set()
        await self.admit.wait()
        await super()._execute_request(msg, call)


class InitProcedure(LifecycleProcedure):
    """Execute one initialization task."""

    async def state_logic(self):
        await self.run_task(InitTask())


class CommandProcedure(LifecycleProcedure):
    """Initialize unless the controller is known to be settled at OPERATE."""

    async def state_logic(self):
        await self.run_unless_achieved(InitTask())


class PausedRecovery(RecoverProcedure):
    """Hold recovery logic before it dispatches a task."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def state_logic(self):
        self.entered.set()
        await self.release.wait()
        await super().state_logic()


class TaskingProcedure(OperateProcedure):
    """Activate tasking, then fail if told to."""

    def __init__(self, supervisor: ControllerStateSupervisor, program: ProgramClient):
        super().__init__(supervisor, DemandState(OPERATE, program=program))
        self.release = asyncio.Event()
        self.outcome: Exception | None = None

    async def state_logic(self):
        self.step = LifecycleStep.TASKING
        await self.activate_tasking()
        await self.release.wait()

        if self.outcome is not None:
            raise self.outcome


class GatedProgramClient(ProgramClient):
    """Hold the external state read during activation preparation."""

    def __init__(self, *args):
        super().__init__(*args)
        self.reading = asyncio.Event()
        self.release_read = asyncio.Event()
        self.finished_read = asyncio.Event()
        self.read_error: Exception | None = None

    async def kv_get_model(self, model):
        self.reading.set()

        try:
            await self.release_read.wait()

            if self.read_error is not None:
                raise self.read_error

            return await super().kv_get_model(model)
        finally:
            self.finished_read.set()


async def finish(procedure: LifecycleProcedure):
    async with asyncio.timeout(5.0):
        await procedure.join()


@contextlib.asynccontextmanager
async def running(procedure: LifecycleProcedure):
    """Start a procedure, then abort and join it on exit."""
    procedure.start()

    try:
        yield procedure
    finally:
        procedure.request_abort()
        await finish(procedure)
        procedure.error()


async def program_inactive(kit, name: str) -> bool:
    state = await kit.program(name).kv_get_model(ProgramState)
    return not state.active_state.active


@pytest_asyncio.fixture
async def observing(kit):
    """Return a factory for started state supervisors of a controller.

    No reactor runs, so only the procedures under test send requests.
    """
    started: list[tuple[ControllerMonitor, ControllerStateSupervisor]] = []

    async def observe(controller: str) -> ControllerStateSupervisor:
        monitor = ControllerMonitor(kit.controller(controller))
        await monitor.start()
        supervisor = ControllerStateSupervisor(monitor, FailureState(), transition_grace=10.0)
        await supervisor.start()
        started.append((monitor, supervisor))
        return supervisor

    yield observe

    for monitor, supervisor in started:
        await supervisor.stop()
        await monitor.stop()


@pytest_asyncio.fixture
async def proc(controller_impl, observing):
    supervisor = await observing(controller_impl.entity)

    async with running(GatedProcedure(supervisor)) as procedure:
        async with asyncio.timeout(5.0):
            await procedure.started.wait()

        yield procedure


@pytest_asyncio.fixture
async def gated_init(controller_impl):
    """Register an init handler that waits for a gate, recording entry and cancellation."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def init(task):
        entered.set()

        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    try:
        yield entered, cancelled, release
    finally:
        release.set()


@pytest_asyncio.fixture
async def tasking(kit, service_context):
    """Register a program whose collect task waits for release or cancellation."""
    controller = await service_context.register_controller("ctrl1")
    program = await service_context.register_program("prog1")
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        started.set()

        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @program.task_factory
    async def factory():
        yield CollectTask()

    await kit.controller("ctrl1").enable()
    await controller.set_internal_state(OPERATE)
    await kit.program("prog1").enable("ctrl1")

    try:
        yield started, cancelled, release
    finally:
        release.set()


# Procedure lifetime


@pytest.mark.asyncio
async def test_error_while_running(proc):
    """A running procedure has no terminal error."""
    assert not proc.done()
    assert proc.error() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [None, ProcedureError("tasking failed"), RuntimeError("logic failed")],
    ids=["success", "program_error", "ordinary_error"],
)
async def test_error_after_completion(proc, outcome):
    """Completion preserves the original exception and its category."""
    proc.outcome = outcome
    proc.release.set()
    await finish(proc)

    assert proc.done()
    assert proc.error() is outcome


@pytest.mark.asyncio
async def test_requested_abort_is_not_an_error(proc):
    """A requested end leaves no error, and repeated requests join the same cleanup."""
    proc.request_stop()
    proc.request_abort()
    proc.request_stop()

    await asyncio.gather(proc.abort(), proc.stop(), finish(proc))

    assert proc.done()
    assert proc.interrupt == "abort"
    assert proc.error() is None


@pytest.mark.asyncio
async def test_interruption_before_first_step(controller_impl, observing):
    """A procedure interrupted before its first step never runs its logic."""
    supervisor = await observing(controller_impl.entity)

    async with running(GatedProcedure(supervisor)) as procedure:
        procedure.request_abort()
        await finish(procedure)

    assert not procedure.started.is_set()
    assert procedure.error() is None


# Cleanup stages


@pytest_asyncio.fixture
async def hooked(controller_impl, observing):
    supervisor = await observing(controller_impl.entity)

    async with running(HookedProcedure(supervisor)) as procedure:
        async with asyncio.timeout(5.0):
            await procedure.started.wait()

        yield procedure


@pytest.mark.asyncio
async def test_stop_runs_stop_logic(hooked):
    """A graceful stop runs only the stop logic."""
    hooked.stop_release.set()
    hooked.request_stop()
    await finish(hooked)

    assert hooked.ran == ["stop"]
    assert hooked.error() is None


@pytest.mark.asyncio
async def test_abort_runs_abort_logic(hooked):
    """An abort skips the stop logic."""
    hooked.request_abort()
    await finish(hooked)

    assert hooked.ran == ["abort"]
    assert hooked.error() is None


@pytest.mark.asyncio
async def test_abort_escalates_stop_logic(hooked):
    """An abort requested during the stop logic ends it and runs the abort logic."""
    hooked.request_stop()

    async with asyncio.timeout(5.0):
        await hooked.stopping.wait()

    hooked.request_abort()
    await finish(hooked)

    assert hooked.ran == ["stop", "abort"]
    assert hooked.error() is None


@pytest.mark.asyncio
async def test_failed_stop_logic_escalates(hooked):
    """Failed stop logic triggers abort logic and fails the procedure."""
    hooked.stop_outcome = RuntimeError("stop failed")
    hooked.stop_release.set()
    hooked.request_stop()
    await finish(hooked)

    assert hooked.ran == ["stop", "abort"]
    assert hooked.error() is hooked.stop_outcome


@pytest.mark.asyncio
async def test_completed_logic_runs_no_cleanup_logic(hooked):
    """Logic that ends on its own runs neither stop nor abort logic."""
    hooked.release.set()
    await finish(hooked)

    assert hooked.ran == []
    assert hooked.error() is None


@pytest.mark.asyncio
async def test_failed_logic_runs_stop_logic(hooked):
    """Failed state logic escalates to the stop logic."""
    hooked.outcome = RuntimeError("logic failed")
    hooked.stop_release.set()
    hooked.release.set()
    await finish(hooked)

    assert hooked.ran == ["stop"]
    assert hooked.error() is hooked.outcome


@pytest.mark.asyncio
async def test_stop_waits_for_shielded_work(controller_impl, observing):
    """A graceful stop waits for shielded work without canceling it."""
    supervisor = await observing(controller_impl.entity)
    work = asyncio.get_running_loop().create_future()

    async with running(ShieldingProcedure(supervisor, work)) as procedure:
        async with asyncio.timeout(5.0):
            await procedure.started.wait()

        procedure.request_stop()
        await asyncio.sleep(0.1)

        assert not procedure.done()
        assert not work.done()

        work.set_result(None)
        await finish(procedure)

    assert procedure.error() is None
    assert procedure.unresolved is False


@pytest.mark.asyncio
async def test_abort_cancels_shielded_work(controller_impl, observing):
    """An abort cancels shielded work."""
    supervisor = await observing(controller_impl.entity)
    work = asyncio.get_running_loop().create_future()

    async with running(ShieldingProcedure(supervisor, work)) as procedure:
        async with asyncio.timeout(5.0):
            await procedure.started.wait()

        procedure.request_abort()
        await finish(procedure)

    assert work.cancelled()
    assert procedure.unresolved is False


# Procedure ownership


@pytest.mark.asyncio
async def test_call_survives_interrupted_waiter(controller_impl, gated_init, observing):
    """Graceful cleanup waits for a task after its procedure logic is interrupted."""
    entered, _, release = gated_init
    supervisor = await observing(controller_impl.entity)

    async with running(InitProcedure(supervisor, DemandState(OPERATE))) as procedure:
        async with asyncio.timeout(5.0):
            await entered.wait()

        procedure.request_stop()
        await asyncio.sleep(0.1)

        # Graceful cleanup waits for the sent init.
        assert not procedure.done()

        release.set()
        await finish(procedure)

    # An interrupted procedure does not confirm the state it was sent to reach.
    assert procedure.error() is None
    assert not procedure.achieved


@pytest.mark.asyncio
async def test_failure_during_cleanup_is_reported(controller_impl, observing):
    """A sent request that fails after interruption is still the procedure's failure."""
    entered = asyncio.Event()
    release = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def init(task):
        entered.set()
        await release.wait()
        raise RuntimeError("init failed")

    supervisor = await observing(controller_impl.entity)

    try:
        async with running(InitProcedure(supervisor, DemandState(OPERATE))) as procedure:
            async with asyncio.timeout(5.0):
                await entered.wait()

            procedure.request_stop()
            release.set()
            await finish(procedure)
    finally:
        release.set()

    assert isinstance(procedure.error(), ControllerFailure)


@pytest.mark.asyncio
async def test_interruption_during_send_keeps_the_reply(service_context, observing):
    """A stop before task acceptance still waits for the accepted task to finish."""
    controller = await service_context.register_impl(
        GatedAdmission.for_service_context(service_context, "gated")
    )

    @controller.task_handler(InitTask)
    async def init(task):
        pass

    supervisor = await observing("gated")
    procedure = InitProcedure(supervisor, DemandState(OPERATE))

    async with running(procedure):
        async with asyncio.timeout(5.0):
            await controller.received.wait()

        procedure.request_stop()
        controller.admit.set()
        await finish(procedure)

    assert procedure.error() is None
    assert procedure.unresolved is False
    assert not procedure.achieved


# Expected state


@pytest_asyncio.fixture
async def init_count(controller_impl):
    """Count initialization tasks."""
    count = [0]

    @controller_impl.task_handler(InitTask)
    async def init(task):
        count[0] += 1

    return count


@pytest.mark.asyncio
async def test_command_confirms_expected_state(controller_impl, init_count, observing):
    """A verified lifecycle task sets the expected state to its target."""
    supervisor = await observing(controller_impl.entity)

    async with running(CommandProcedure(supervisor, DemandState(OPERATE))) as procedure:
        await finish(procedure)

    assert procedure.commanded
    assert supervisor.expected == OPERATE
    assert init_count[0] == 1


@pytest.mark.asyncio
async def test_unknown_expectation_requires_command(
    controller_impl, init_count, observing, until
):
    """A matching report does not skip the lifecycle task while the expected state is unknown."""
    await controller_impl.set_internal_state(OPERATE)
    supervisor = await observing(controller_impl.entity)
    await until(lambda: supervisor.expected == OPERATE)
    supervisor.invalidate()

    async with running(CommandProcedure(supervisor, DemandState(OPERATE))) as procedure:
        await finish(procedure)

    assert procedure.commanded
    assert init_count[0] == 1


@pytest.mark.asyncio
async def test_interrupted_command_leaves_expectation_unknown(
    controller_impl, gated_init, observing
):
    """A lifecycle task that finishes after interruption does not confirm its target."""
    entered, _, release = gated_init
    supervisor = await observing(controller_impl.entity)

    async with running(CommandProcedure(supervisor, DemandState(OPERATE))) as procedure:
        async with asyncio.timeout(5.0):
            await entered.wait()

        assert supervisor.expected == InternalControllerState.TRANSITION

        # Graceful cleanup lets the init succeed.
        procedure.request_stop()
        release.set()
        await finish(procedure)

    assert procedure.error() is None
    assert not procedure.achieved
    assert supervisor.expected == UNKNOWN


@pytest.mark.asyncio
@pytest.mark.parametrize("emergency", [False, True])
async def test_recovery_is_exempt_before_dispatch(kit, controller_impl, emergency):
    """Recovery permits state changes from synchronous startup through task preparation."""
    await controller_impl.set_internal_state(OPERATE)
    lifecycle = ControllerLifecycle()
    async with contextlib.AsyncExitStack() as cleanup:
        monitor = ControllerMonitor(kit.controller(controller_impl.entity))
        await monitor.start()
        cleanup.push_async_callback(monitor.stop)
        supervisor = ControllerStateSupervisor(
            monitor, lifecycle.failure_state, transition_grace=10.0
        )
        await supervisor.start()
        cleanup.push_async_callback(supervisor.stop)

        failure_state = lifecycle.failure_state
        now = asyncio.get_running_loop().time()
        failure_state.record_failure(now, ControllerFailure("first"))
        failure_state.begin_procedure()
        failure_state.record_failure(now, ControllerFailure("second"))
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=None)
        reactor = ControllerReactor(lifecycle, supervisor)
        procedure = PausedRecovery(supervisor, lifecycle.demand_state, emergency=emergency)

        try:
            reactor._start(procedure)

            # The exemption is active before the procedure's task gets its first turn.
            assert supervisor.expected == UNKNOWN

            async with asyncio.timeout(5.0):
                await procedure.entered.wait()

                with supervisor.state_changes() as changes:
                    await controller_impl.set_internal_state(STANDBY)

                    while (await changes.get()).current != STANDBY:
                        pass

            reactor._evaluate()

            assert procedure.interrupt is None
            assert failure_state.failures == 2
            assert failure_state.failure is None
        finally:
            procedure.release.set()
            await reactor._release()


# Program tasking


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rejected", "remote", "unexpected"])
async def test_activation_failure_preserves_ownership_uncertainty(
    kit, service_context, entity_impl, observing, failure
):
    """A failed activation reply cannot establish that tasking stayed inactive."""
    if failure == "unexpected":
        request = declare_request("set_active_state", message=ProgramActiveStateRequest)

        async def activate(msg):
            return None
    else:
        request = set_active_state_request

        async def activate(msg, call):
            if msg.action != "start":
                call.accept()
                await call.succeed(result=None)
            elif failure == "rejected":
                call.reject(reason="Unavailable")
            else:
                raise RuntimeError("Activation failed")

    await entity_impl.handle_request(request, activate)
    await service_context.register_controller("ctrl1")
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, kit.program(entity_impl.entity))) as procedure:
        await finish(procedure)

    assert procedure.error() is not None
    assert procedure.unresolved is (failure != "rejected")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["failed", "interrupted"])
async def test_cleanup_targets_the_accepted_session(
    kit, service_context, entity_impl, observing, failure
):
    """Cleanup targets the accepted session when startup fails."""
    session_id = uuid.uuid7()
    requests: list[tuple[str, uuid.UUID | None]] = []
    accepted = asyncio.Event()
    release = asyncio.Event()

    async def activate(msg, call):
        requests.append((msg.action, msg.session_id))
        call.accept(response=ProgramActiveStateResponse(session_id=session_id))

        if msg.action == "start":
            accepted.set()
            await release.wait()

            if failure == "failed":
                await call.fail("Start failed")
                return

        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, activate)
    await service_context.register_controller("ctrl1")
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, kit.program(entity_impl.entity))) as procedure:
        async with asyncio.timeout(5.0):
            await accepted.wait()

        # Interrupt after acceptance while startup is pending.
        if failure == "interrupted":
            procedure.request_abort()

        release.set()
        await finish(procedure)

    cleanup = "stop" if failure == "failed" else "abort"

    assert requests == [("start", None), (cleanup, session_id)]
    assert (procedure.error() is None) is (failure == "interrupted")
    assert procedure.unresolved is False


@pytest.mark.asyncio
async def test_failed_tasking_abort_is_unresolved(kit, service_context, entity_impl, observing):
    """Tasking whose abort fails may still be running."""
    activated = asyncio.Event()

    async def activate(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=uuid.uuid7()))

        if msg.action == "abort":
            raise RuntimeError("Abort failed")

        await call.succeed(result=None)
        activated.set()

    await entity_impl.handle_request(set_active_state_request, activate)
    await service_context.register_controller("ctrl1")
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, kit.program(entity_impl.entity))) as procedure:
        async with asyncio.timeout(5.0):
            await activated.wait()

        procedure.request_abort()
        await finish(procedure)

    assert procedure.unresolved is True


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["activating", "tasking"])
async def test_interrupted_activation_stops_tasking(kit, tasking, when, observing):
    """Abort cleanup waits for a pending start request before aborting program tasking."""
    started, cancelled, _ = tasking
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, kit.program("prog1"))) as procedure:
        async with asyncio.timeout(5.0):
            if when == "activating":
                # The logic is waiting on activation, which has not run yet.
                while procedure.step != LifecycleStep.TASKING:
                    await asyncio.sleep(0)
            else:
                await started.wait()

        procedure.request_abort()
        await finish(procedure)

    assert procedure.error() is None
    assert procedure.unresolved is False
    assert await program_inactive(kit, "prog1")

    if started.is_set():
        assert cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["stop", "abort", "cancel"])
async def test_preparation_invalidation_prevents_activation(kit, tasking, observing, change):
    """Invalidation during a state read prevents dispatch without unresolved work."""
    started, _, _ = tasking
    client = GatedProgramClient(kit, kit.program("prog1").entity)
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, client)) as procedure:
        async with asyncio.timeout(5.0):
            await client.reading.wait()

            match change:
                case "stop":
                    procedure.request_stop()
                case "abort":
                    procedure.request_abort()
                case "cancel":
                    procedure.cancel()

            client.release_read.set()
            await client.finished_read.wait()
            await finish(procedure)

    assert procedure.error() is None
    assert not procedure.unresolved
    assert not started.is_set()
    assert await program_inactive(kit, "prog1")


@pytest.mark.asyncio
async def test_preparation_failure_creates_no_cleanup_obligation(kit, tasking, observing):
    """A failed initial read cannot leave tasking that was never requested unresolved."""
    started, _, _ = tasking
    client = GatedProgramClient(kit, kit.program("prog1").entity)
    client.read_error = RuntimeError("Program state unavailable")
    client.release_read.set()
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, client)) as procedure:
        await finish(procedure)

    assert isinstance(procedure.error(), ProcedureError)
    assert procedure.error().__cause__ is client.read_error
    assert not procedure.unresolved
    assert not started.is_set()
    assert await program_inactive(kit, "prog1")


@pytest.mark.asyncio
async def test_failed_procedure_stops_tasking(kit, tasking, observing):
    """Failed logic escalates to a graceful stop of the tasking it activated."""
    started, _, release = tasking
    supervisor = await observing("ctrl1")
    procedure = TaskingProcedure(supervisor, kit.program("prog1"))
    procedure.outcome = RuntimeError("logic failed")

    async with running(procedure):
        async with asyncio.timeout(5.0):
            await started.wait()

        # Graceful cleanup waits for the running collect.
        release.set()
        procedure.release.set()
        await finish(procedure)

    assert procedure.error() is procedure.outcome
    assert await program_inactive(kit, "prog1")


@pytest.mark.asyncio
async def test_completed_procedure_leaves_tasking(kit, tasking, observing):
    """Logic that returns leaves its tasking for the next procedure."""
    started, cancelled, _ = tasking
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, kit.program("prog1"))) as procedure:
        async with asyncio.timeout(5.0):
            await started.wait()

        procedure.release.set()
        await finish(procedure)

    try:
        assert procedure.error() is None
        assert procedure.unresolved is False
        assert not await program_inactive(kit, "prog1")
        assert not cancelled.is_set()
    finally:
        await kit.program("prog1").abort_tasking()


@pytest.mark.asyncio
async def test_cancelled_procedure_leaves_tasking(kit, tasking, observing):
    """Canceling a procedure leaves its program tasking running for the next procedure."""
    started, cancelled, _ = tasking
    supervisor = await observing("ctrl1")

    async with running(TaskingProcedure(supervisor, kit.program("prog1"))) as procedure:
        async with asyncio.timeout(5.0):
            await started.wait()

        procedure.cancel()
        await finish(procedure)

    try:
        assert procedure.unresolved is False
        assert not await program_inactive(kit, "prog1")
        assert not cancelled.is_set()
    finally:
        await kit.program("prog1").abort_tasking()
