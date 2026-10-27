# SPDX-License-Identifier: Apache-2.0
"""Tests for lifecycle procedure interruption, task tracking, and cleanup."""

import asyncio
import contextlib

import pytest
import pytest_asyncio

from sensorkit.auto.lifecycle import ControllerLifecycle
from sensorkit.auto.procedures import (
    ControllerFailure,
    LifecycleProcedure,
    LifecycleStep,
    OperateProcedure,
    ProcedureError,
)
from sensorkit.auto.reactor import ControllerReactor
from sensorkit.backend.request import declare_request
from sensorkit.core.controller import ControllerMonitor, InternalControllerState
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.program import (
    ProgramActiveStateRequest,
    ProgramClient,
    ProgramState,
    set_active_state_request,
)
from sensorkit.core.task import CollectTask, InitTask

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY


class GatedProcedure(LifecycleProcedure):
    """Run procedure logic with a controlled outcome."""

    def __init__(self, reactor: ControllerReactor):
        super().__init__(reactor, reactor.lifecycle.demand_state)
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

    def __init__(self, reactor: ControllerReactor):
        super().__init__(reactor)
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

    def __init__(self, reactor: ControllerReactor, work: asyncio.Future[None]):
        super().__init__(reactor, reactor.lifecycle.demand_state)
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


class TaskingProcedure(OperateProcedure):
    """Activate tasking, then fail if told to."""

    def __init__(self, reactor: ControllerReactor):
        super().__init__(reactor, reactor.lifecycle.demand_state)
        self.release = asyncio.Event()
        self.outcome: Exception | None = None

    async def state_logic(self):
        self.reactor.step = LifecycleStep.TASKING
        await self.activate_tasking()
        await self.release.wait()

        if self.outcome is not None:
            raise self.outcome


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
    """Return a factory for reactors monitoring a controller under an OPERATE demand.

    Reactor loops remain stopped so only the procedures under test send requests.
    """
    monitors: list[ControllerMonitor] = []

    async def observe(controller: str, program: ProgramClient | None = None) -> ControllerReactor:
        lifecycle = ControllerLifecycle()
        lifecycle.enable()
        lifecycle.set_demand_state(OPERATE, program=program)
        monitor = ControllerMonitor(kit.controller(controller))
        await monitor.start()
        monitors.append(monitor)
        return ControllerReactor(lifecycle, monitor)

    yield observe

    for monitor in monitors:
        await monitor.stop()


@pytest_asyncio.fixture
async def proc(controller_impl, observing):
    reactor = await observing(controller_impl.entity)

    async with running(GatedProcedure(reactor)) as procedure:
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
    reactor = await observing(controller_impl.entity)

    async with running(GatedProcedure(reactor)) as procedure:
        procedure.request_abort()
        await finish(procedure)

    assert not procedure.started.is_set()
    assert procedure.error() is None


# Cleanup stages


@pytest_asyncio.fixture
async def hooked(controller_impl, observing):
    reactor = await observing(controller_impl.entity)

    async with running(HookedProcedure(reactor)) as procedure:
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
    reactor = await observing(controller_impl.entity)
    work = asyncio.get_running_loop().create_future()

    async with running(ShieldingProcedure(reactor, work)) as procedure:
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
    reactor = await observing(controller_impl.entity)
    work = asyncio.get_running_loop().create_future()

    async with running(ShieldingProcedure(reactor, work)) as procedure:
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
    reactor = await observing(controller_impl.entity)

    async with running(InitProcedure(reactor, reactor.lifecycle.demand_state)) as procedure:
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

    reactor = await observing(controller_impl.entity)

    try:
        async with running(InitProcedure(reactor, reactor.lifecycle.demand_state)) as procedure:
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

    reactor = await observing("gated")
    procedure = InitProcedure(reactor, reactor.lifecycle.demand_state)

    async with running(procedure):
        async with asyncio.timeout(5.0):
            await controller.received.wait()

        procedure.request_stop()
        controller.admit.set()
        await finish(procedure)

    assert procedure.error() is None
    assert procedure.unresolved is False
    assert not procedure.achieved


@pytest.mark.asyncio
async def test_superseding_demand_blocks_next_operation(controller_impl, gated_init, observing):
    """A newer demand blocks a procedure's next request before the procedure is interrupted."""
    entered, _, _ = gated_init
    reactor = await observing(controller_impl.entity)
    procedure = InitProcedure(reactor, reactor.lifecycle.demand_state)

    async with running(procedure):
        reactor.lifecycle.set_demand_state(STANDBY)
        await finish(procedure)

    assert procedure.error() is None
    assert not entered.is_set()


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
    reactor = await observing("ctrl1", kit.program(entity_impl.entity))

    async with running(TaskingProcedure(reactor)) as procedure:
        await finish(procedure)

    assert procedure.error() is not None
    assert procedure.unresolved is (failure != "rejected")


@pytest.mark.asyncio
async def test_failed_tasking_abort_is_unresolved(kit, service_context, entity_impl, observing):
    """Tasking whose abort fails may still be running."""
    activated = asyncio.Event()

    async def activate(msg, call):
        call.accept()

        if msg.action == "abort":
            raise RuntimeError("Abort failed")

        await call.succeed(result=None)
        activated.set()

    await entity_impl.handle_request(set_active_state_request, activate)
    await service_context.register_controller("ctrl1")
    reactor = await observing("ctrl1", kit.program(entity_impl.entity))

    async with running(TaskingProcedure(reactor)) as procedure:
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
    reactor = await observing("ctrl1", kit.program("prog1"))

    async with running(TaskingProcedure(reactor)) as procedure:
        async with asyncio.timeout(5.0):
            if when == "activating":
                # The logic is waiting on activation, which has not run yet.
                while reactor.step != LifecycleStep.TASKING:
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
async def test_superseded_activation_sends_nothing(kit, tasking, observing):
    """A newer demand blocks tasking activation before the procedure is interrupted."""
    started, _, _ = tasking
    reactor = await observing("ctrl1", kit.program("prog1"))
    procedure = TaskingProcedure(reactor)

    async with running(procedure):
        async with asyncio.timeout(5.0):
            while reactor.step != LifecycleStep.TASKING:
                await asyncio.sleep(0)

        reactor.lifecycle.set_demand_state(STANDBY)
        await finish(procedure)

    assert procedure.error() is None
    assert not started.is_set()
    assert await program_inactive(kit, "prog1")


@pytest.mark.asyncio
async def test_failed_procedure_stops_tasking(kit, tasking, observing):
    """Failed logic escalates to a graceful stop of the tasking it activated."""
    started, _, release = tasking
    reactor = await observing("ctrl1", kit.program("prog1"))
    procedure = TaskingProcedure(reactor)
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
    reactor = await observing("ctrl1", kit.program("prog1"))

    async with running(TaskingProcedure(reactor)) as procedure:
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
    reactor = await observing("ctrl1", kit.program("prog1"))

    async with running(TaskingProcedure(reactor)) as procedure:
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
