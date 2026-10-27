# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
import uuid_utils.compat as uuid

from sensorkit.backend.event import Event
from sensorkit.backend.lease import Lease
from sensorkit.backend.request import CallError, CallRejected, declare_request
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    AbortResponseMessage,
    ControllerEnableState,
    ControllerOperatingState,
    ControllerState,
    ExecuteRequestMessage,
    ExecuteResponseMessage,
    InternalControllerState,
    TaskExecutionResult,
    TaskExecutionState,
    TaskFinishInfo,
    abort_task_request,
    execute_task_request,
)
from sensorkit.core.executor import TaskFailed, TaskOutcomeUnknown
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.core.program import (
    ProgramActiveState,
    ProgramActiveStateRequest,
    ProgramActiveStateResponse,
    ProgramEnableState,
    ProgramOffering,
    ProgramState,
    ProgramTaskingMonitor,
    ProgramTaskingState,
    TaskingError,
    TaskingLoopState,
    TaskingStartupFailed,
    set_active_state_request,
)
from sensorkit.core.task import CollectTask, TaskExecution

OPERATE = InternalControllerState.OPERATE


class RecoverableController(ControllerImpl):
    """Controller whose abort endpoint can temporarily fail."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.allow_abort = asyncio.Event()
        self.allow_abort.set()
        self.abort_failed = asyncio.Event()
        self.abort_requests: list[uuid.UUID | None] = []

    async def _abort_request(self, requested, call):
        self.abort_requests.append(requested.task_id)

        if not self.allow_abort.is_set():
            self.abort_failed.set()
            raise RuntimeError("Controller connection lost")

        await super()._abort_request(requested, call)


class GatedTaskingProgram(ProgramImpl):
    """Hold session event publication at the external stream interface."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.reporting = asyncio.Event()
        self.release_report = asyncio.Event()

    async def emit_event(self, event):
        if isinstance(event, ProgramTaskingState) and event.loop_state is not None:
            self.reporting.set()
            await self.release_report.wait()

        await super().emit_event(event)


async def tasking_ended(client) -> bool:
    return not (await client.kv_get_model(ProgramState)).active_state.active


async def tasking_stopping(client) -> bool:
    return (await client.kv_get_model(ProgramState)).active_state.stopping


async def report_operate(endpoint: EntityImpl):
    """Publish OPERATE for an external controller endpoint."""
    await endpoint.kv_put_model(
        ControllerState(
            enable_state=ControllerEnableState(enabled=True),
            operating_state=ControllerOperatingState(current=OPERATE, context=KeywordDict()),
            execution_state=TaskExecutionState(execution=None),
        )
    )


@contextlib.asynccontextmanager
async def tasking_reports(client):
    """Collect each program tasking event."""
    reports: list[ProgramTaskingState] = []
    stream = await client.monitor_event(ProgramTaskingState)

    async def record():
        async for event in stream:
            reports.append(event)

    recorder = asyncio.create_task(record())

    try:
        yield reports
    finally:
        recorder.cancel()
        await asyncio.wait((recorder,))
        await stream.aclose()


def faults(reports: list[ProgramTaskingState]) -> list[TaskingLoopState]:
    return [r.loop_state for r in reports if r.loop_state is not None and r.loop_state.fault]


def session_ended(monitor: ProgramTaskingMonitor) -> bool:
    monitor.read_events()
    return monitor.ending is not None


async def ended(reports: list[ProgramTaskingState], until):
    """Return the reports through the first session ending."""
    await until(lambda: any(r.loop_state is None for r in reports))
    return reports[: next(i for i, r in enumerate(reports) if r.loop_state is None) + 1]


@pytest_asyncio.fixture
async def controller_endpoint(kit, service_context):
    endpoint = await service_context.register_impl(
        EntityImpl.for_service_context(service_context, "endpoint"), acquire_lease=False
    )
    kv = kit.backend.key_value(endpoint.entity)

    async with await Lease.acquire(kv, "EntityLease", 60, None) as lease:
        await report_operate(endpoint)
        yield endpoint, lease


@pytest_asyncio.fixture
async def leased_controller(kit, service_context):
    controller = await service_context.register_impl(
        RecoverableController.for_service_context(service_context, "controller"),
        acquire_lease=False,
    )
    kv = kit.backend.key_value(controller.entity)

    async with await Lease.acquire(kv, "EntityLease", 60, None) as lease:
        await controller.set_internal_state(OPERATE)
        yield controller, lease


@pytest.mark.asyncio
async def test_program_offering(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        program = await sc.register_program("myprogram")

    ready = asyncio.Event()
    now = datetime.now().replace(microsecond=0)
    offer = (now, now + timedelta(hours=1))
    program_client = kit.program("myprogram")

    async def monitor_offering():
        stream = await program_client.monitor(ProgramOffering)
        ready.set()

        async for _, data in stream:
            offered = data.offer_windows
            assert len(offered) == 1
            assert offered[0].begin == offer[0]
            assert offered[0].end == offer[1]
            break

    task = asyncio.create_task(monitor_offering())

    async with asyncio.timeout(2.0):
        await program_client.enable("any")
        await ready.wait()

        program.clear_offers()
        program.add_offer(*offer)
        await program.publish_offers()
        await task


@pytest.mark.asyncio
async def test_program_tasking(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")
        program = await sc.register_program("myprogram")
        await controller.set_internal_state(OPERATE)

    task_obj = CollectTask(task_id=uuid.uuid1(), controller_id="mycontroller")
    task_ran = False
    enabled = asyncio.Event()
    disabled = asyncio.Event()
    done = asyncio.Event()

    @program.on_enable
    async def program_enable():
        assert ProgramImpl.current.get() is program
        enabled.set()

    @program.on_disable
    async def program_disable():
        assert ProgramImpl.current.get() is program
        disabled.set()

    @program.task_factory
    async def program_tasker():
        assert ProgramImpl.current.get() is program

        if not done.is_set():
            assert not task_ran
            await (yield task_obj)
            assert task_ran
            done.set()

    @controller.task_handler(CollectTask)
    async def run_task(task: CollectTask):
        assert ControllerImpl.current.get() is controller
        assert task == task_obj
        nonlocal task_ran
        task_ran = True

    ready = asyncio.Event()
    activated = asyncio.Event()
    stopping = asyncio.Event()
    deactivated = asyncio.Event()
    program_client = kit.program("myprogram")

    async def state_monitor():
        events = await program_client.tasking_change_events()
        ready.set()

        async for event in events:
            if event.active:
                if event.stopping:
                    assert not stopping.is_set()
                    stopping.set()
                else:
                    assert not activated.is_set()
                    activated.set()
            else:
                assert not deactivated.is_set()
                deactivated.set()

    _t = asyncio.create_task(state_monitor())

    async with asyncio.timeout(2.0):
        await kit.controller("mycontroller").enable()
        await ready.wait()
        await program_client.enable("mycontroller")
        await enabled.wait()
        (await program_client.start_tasking()).close()
        await activated.wait()
        await done.wait()
        await program_client.stop_tasking()
        await stopping.wait()
        await deactivated.wait()
        await program_client.disable()
        await disabled.wait()


@pytest.mark.asyncio
async def test_session_reports_progress_in_order(kit, service_context, until):
    """Session progress and execution outcomes precede the terminal fault and ending."""
    controller = await service_context.register_controller("mycontroller")
    program = await service_context.register_program("myprogram")
    await controller.set_internal_state(OPERATE)
    collects = 0

    @controller.task_handler(CollectTask)
    async def collect(task):
        nonlocal collects
        collects += 1

        if collects == 2:
            raise RuntimeError("Camera fault")

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await client.enable("mycontroller")

    async with tasking_reports(client) as reports, asyncio.timeout(5.0):
        (await client.start_tasking()).close()
        session = await ended(reports, until)

    live = session[:-1]
    healthy = [r.loop_state.stage for r in live if not r.loop_state.fault]
    stages = [stage for i, stage in enumerate(healthy) if i == 0 or healthy[i - 1] != stage]
    executions = [r.execution_state for r in live if r.execution_state is not None]
    first, second = dict.fromkeys(state.execution.task_id for state in executions)
    end = session[-1].execution_state

    assert len({r.loop_state.session_id for r in live}) == 1
    assert live[0].loop_state.stage == "factory"
    assert stages == ["factory", "dispatch", "post_task"] * 2
    assert any(
        s.execution.task_id == first and s.finished and not s.finished.error for s in executions
    )
    assert {(f.fault, f.stage, f.task_type) for f in faults(live)} == {
        ("controller", "execution", "collect")
    }
    assert end.execution.task_id == second
    assert end.finished.error


@pytest.mark.asyncio
async def test_execution_reports_follow_the_owned_execution(
    kit, service_context, controller_endpoint, until
):
    """Early execution reports retain their order and exclude unrelated work."""
    endpoint, _ = controller_endpoint
    other = TaskExecution(task=CollectTask(), task_id=uuid.uuid7(), controller_id="other")
    owned: list[TaskExecutionState] = []

    async def execute(msg, call):
        execution = TaskExecution(
            task=msg.task, task_id=uuid.uuid7(), controller_id=str(endpoint.entity)
        )
        owned.extend(
            (
                TaskExecutionState(executing=True, execution=execution),
                TaskExecutionState(finished=TaskFinishInfo(), execution=execution),
            )
        )
        now = datetime.now(UTC)

        # Queue both execution reports before replying to dispatch.
        await endpoint.emit_event(owned[0])
        await endpoint.emit_event(TaskExecutionState(executing=True, execution=other))
        await endpoint.emit_event(owned[1])
        call.accept(response=ExecuteResponseMessage(execution=execution))
        await call.succeed(
            result=TaskExecutionResult(task_id=execution.task_id, start_time=now, end_time=now)
        )

    await endpoint.handle_request(execute_task_request, execute)
    program = await service_context.register_program("fast-program")

    @program.task_factory
    async def factory():
        if not owned:
            yield CollectTask()

    client = kit.program(program.entity)
    await client.enable(str(endpoint.entity))

    async with tasking_reports(client) as reports, asyncio.timeout(5.0):
        (await client.start_tasking()).close()
        await until(lambda: any(r.execution_state and r.execution_state.finished for r in reports))
        await client.stop_tasking()
        session = await ended(reports, until)

    executions = dict.fromkeys(r.execution_state.event_id for r in session if r.execution_state)

    assert list(executions) == [state.event_id for state in owned]
    assert not faults(session)


@pytest.mark.asyncio
async def test_dispatch_rejection_reports_task_type(
    kit, service_context, leased_controller, until
):
    """Dispatch rejection retains the task type in the terminal controller fault."""
    controller, _ = leased_controller
    program = await service_context.register_program("rejected-program")

    @program.task_factory
    async def factory():
        yield CollectTask()

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    async with tasking_reports(client) as reports, asyncio.timeout(5.0):
        (await client.start_tasking()).close()
        session = await ended(reports, until)

    assert {(f.fault, f.stage, f.task_type) for f in faults(session)} == {
        ("controller", "dispatch", "collect")
    }
    assert session[-1].execution_state is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("factory", ("program", "factory", None)),
        ("pending", ("program", "post_task", "collect")),
        ("handled", ("controller", "execution", "collect")),
        ("caught", ("controller", "execution", "collect")),
        ("succeeded", ("program", "post_task", "collect")),
    ],
)
async def test_fault_origin_survives_cleanup(
    kit, service_context, leased_controller, until, case, expected
):
    """Cleanup preserves fault origin and the failed operation."""
    controller, _ = leased_controller
    program = await service_context.register_program("origin-program")
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    dispatched = 0

    @controller.task_handler(CollectTask)
    async def collect(task):
        match case:
            case "pending":
                entered.set()

                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
            case "handled" | "caught":
                raise RuntimeError("Camera fault")

    @program.task_factory
    async def factory():
        nonlocal dispatched

        if case == "factory":
            raise RuntimeError("Planner failed")

        dispatched += 1
        execution = yield CollectTask()

        match case:
            case "pending":
                await entered.wait()
                raise RuntimeError("Planner failed")
            case "handled":
                try:
                    await execution
                except TaskFailed as e:
                    raise RuntimeError("Handling failed") from e
            case "caught":
                with contextlib.suppress(TaskFailed):
                    await execution
            case "succeeded":
                await execution
                raise RuntimeError("Handling failed")

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    async with tasking_reports(client) as reports, asyncio.timeout(5.0):
        (await client.start_tasking()).close()
        session = await ended(reports, until)
        await until(lambda: tasking_ended(client))
        state = await client.kv_get_model(ProgramState)

    end = session[-1].execution_state

    assert {(f.fault, f.stage, f.task_type) for f in faults(session)} == {expected}
    assert state.active_state.origin == "error"
    assert dispatched == (0 if case == "factory" else 1)

    match case:
        case "factory":
            assert end is None
        case "pending":
            assert cancelled.is_set()
            assert end.finished.aborted
        case "succeeded":
            assert not end.finished.error


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["stop", "abort"])
async def test_requested_stop_is_not_a_fault(
    kit, service_context, leased_controller, until, action
):
    """Requested stop and abort complete without a session fault."""
    controller, _ = leased_controller
    program = await service_context.register_program("stopped-program")
    entered = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()
        await release.wait()

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    try:
        async with tasking_reports(client) as reports, asyncio.timeout(5.0):
            (await client.start_tasking()).close()
            await entered.wait()

            if action == "abort":
                await client.abort_tasking()
            else:
                stop = asyncio.create_task(client.stop_tasking())
                await until(lambda: tasking_stopping(client))
                release.set()
                await stop

            session = await ended(reports, until)
            state = await client.kv_get_model(ProgramState)
    finally:
        release.set()

    assert not faults(session)
    assert state.active_state.origin == "request"
    assert session[-1].execution_state.finished.aborted is (action == "abort")


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["start", "execution"])
async def test_tasking_requires_operate(kit, service_context, leased_controller, until, when):
    """The program enforces OPERATE independently of the agent."""
    controller, _ = leased_controller
    program = await service_context.register_program("operate-program")
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    if when == "start":
        await controller.set_internal_state(InternalControllerState.STANDBY)

    async with tasking_reports(client) as reports, asyncio.timeout(5.0):
        (await client.start_tasking()).close()

        if when == "execution":
            await entered.wait()
            await until(
                lambda: any(r.loop_state and r.loop_state.stage == "post_task" for r in reports)
            )
            await controller.set_internal_state(InternalControllerState.STANDBY)

        session = await ended(reports, until)

    expected = ("factory", None) if when == "start" else ("post_task", "collect")

    assert {(f.fault, f.stage, f.task_type) for f in faults(session)} == {
        ("controller", *expected)
    }
    assert cancelled.is_set() is (when == "execution")


@pytest.mark.asyncio
async def test_program_reuses_task_instance(kit, service_context):
    controller = await service_context.register_controller("mycontroller")
    program = await service_context.register_program("myprogram")
    await controller.set_internal_state(OPERATE)

    task_obj = CollectTask()
    executed: list[uuid.UUID] = []
    done = asyncio.Event()

    @program.task_factory
    async def program_tasker():
        if not done.is_set():
            await (yield task_obj)

            if len(executed) == 2:
                done.set()

    async def run_task(execution: TaskExecution[CollectTask]):
        assert execution.task == task_obj
        executed.append(execution.task_id)

    controller.add_task_handler(CollectTask, execution_callback=run_task)

    program_client = kit.program("myprogram")

    async with asyncio.timeout(2.0):
        await kit.controller("mycontroller").enable()
        await program_client.enable("mycontroller")
        (await program_client.start_tasking()).close()
        await done.wait()
        await program_client.stop_tasking()

    assert len(set(executed)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
async def test_program_abort_waits_for_controller_cleanup(kit, service_context, accepted):
    """Aborting tasking returns only after the controller has ended the running task."""
    controller = await service_context.register_controller("mycontroller")
    program = await service_context.register_program("myprogram")
    await controller.set_internal_state(OPERATE)
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    @program.task_factory
    async def factory():
        yield CollectTask()

    program_client = kit.program("myprogram")
    await kit.controller("mycontroller").enable()
    await program_client.enable("mycontroller")
    abort = None

    try:
        async with asyncio.timeout(5.0):
            (await program_client.start_tasking()).close()
            await entered.wait()

            if accepted:
                state = await program_client.kv_get_model(ProgramState)

                while state.tasking_state.execution_state is None:
                    await asyncio.sleep(0.01)
                    state = await program_client.kv_get_model(ProgramState)

            abort = asyncio.create_task(program_client.abort_tasking())
            await cleaning.wait()

        done, _ = await asyncio.wait((abort,), timeout=0.2)

        assert not done
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            if abort is not None:
                await abort
            else:
                await program_client.abort_tasking()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rejected", "remote", "unexpected"])
async def test_dispatch_failure_ends_tasking(
    kit, service_context, controller_endpoint, until, failure
):
    """A failed dispatch ends tasking, and another tasking session can follow."""
    entity_impl, _ = controller_endpoint
    requests = 0
    finished = asyncio.Event()

    if failure == "unexpected":
        request = declare_request(
            "execute_task", message=ExecuteRequestMessage, response=ExecuteResponseMessage
        )

        async def execute(msg):
            nonlocal requests
            requests += 1
            return ExecuteResponseMessage()
    else:
        request = execute_task_request

        async def execute(msg, call):
            nonlocal requests
            requests += 1

            if failure == "rejected":
                call.reject(reason="Unavailable")
            else:
                raise RuntimeError("Admission failed")

    await entity_impl.handle_request(request, execute)
    program = await service_context.register_program("dispatch-program")

    @program.task_factory
    async def factory():
        try:
            yield CollectTask()
        finally:
            finished.set()

    client = kit.program("dispatch-program")
    await client.enable(str(entity_impl.entity))

    async with asyncio.timeout(5.0):
        (await client.start_tasking()).close()
        await finished.wait()

        assert requests == 1

        if failure == "rejected":
            await client.stop_tasking()
            (await client.start_tasking()).close()

            while (await client.kv_get_model(ProgramState)).active_state.active:
                await asyncio.sleep(0)

            assert requests == 2
        else:
            # Without acceptance, tasking ends in error rather than waiting on the controller.
            await until(lambda: tasking_ended(client))
            state = await client.kv_get_model(ProgramState)
            assert state.active_state.origin == "error"

            (await client.start_tasking()).close()
            await until(lambda: requests == 2)


@pytest.mark.asyncio
async def test_abort_before_first_execution_event(kit, service_context, controller_endpoint):
    """An abort before the first execution event waits for the delayed end event."""
    endpoint, _ = controller_endpoint
    execution = TaskExecution(
        task=CollectTask(), task_id=uuid.uuid7(), controller_id=str(endpoint.entity)
    )
    admitted = asyncio.Event()
    release = asyncio.Event()
    errors: list[BaseException] = []

    async def execute(msg, call):
        call.accept(response=ExecuteResponseMessage(execution=execution))
        admitted.set()
        await release.wait()
        await call.fail("Aborted")

    async def abort_task(msg, call):
        assert msg.task_id == execution.task_id
        call.accept(response=AbortResponseMessage(aborting=True, task_id=msg.task_id))
        await call.succeed(result=None)

    await endpoint.handle_request(execute_task_request, execute)
    await endpoint.handle_request(abort_task_request, abort_task)
    program = await service_context.register_program("admission-abort-program")

    @program.task_factory
    async def factory():
        try:
            yield CollectTask()
        except BaseException as error:
            errors.append(error)

    client = kit.program(program.entity)
    await client.enable(str(endpoint.entity))
    abort = None

    try:
        async with asyncio.timeout(5.0):
            (await client.start_tasking()).close()
            loop = program._task_loop
            await admitted.wait()
            abort = asyncio.create_task(client.abort_tasking())

            # The abort waits for the execution's end event.
            done, _ = await asyncio.wait((abort,), timeout=0.1)
            assert not done

            release.set()
            await abort

            assert len(errors) == 1
            assert isinstance(errors[0], asyncio.CancelledError)
            assert loop.finalized
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            if abort is not None:
                await abort

            await client.abort_tasking()


@pytest.mark.asyncio
async def test_abort_interrupts_dispatch_error_cleanup(kit, service_context, controller_endpoint):
    """A user abort interrupts factory cleanup after an unresolved dispatch."""
    endpoint, _ = controller_endpoint
    execution = TaskExecution(
        task=CollectTask(), task_id=uuid.uuid7(), controller_id=str(endpoint.entity)
    )
    cleaning = asyncio.Event()
    interrupted = asyncio.Event()
    release = asyncio.Event()
    request = declare_request(
        "execute_task", message=ExecuteRequestMessage, response=ExecuteResponseMessage
    )

    async def execute(msg):
        return ExecuteResponseMessage(execution=execution)

    await endpoint.handle_request(request, execute)
    program = await service_context.register_program("error-cleanup-program")

    @program.task_factory
    async def factory():
        try:
            yield CollectTask()
        except TaskOutcomeUnknown:
            cleaning.set()

            try:
                await release.wait()
            except asyncio.CancelledError:
                interrupted.set()
                raise

    client = kit.program(program.entity)
    await client.enable(str(endpoint.entity))

    try:
        async with asyncio.timeout(5.0):
            (await client.start_tasking()).close()
            await cleaning.wait()
            loop = program._task_loop

            # The dispatch was never confirmed, so the abort cannot report the work ended.
            with pytest.raises(CallError):
                await client.abort_tasking()

            assert interrupted.is_set()
            assert loop.finalized
            assert loop.unresolved
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            await client.abort_tasking()


@pytest.mark.asyncio
async def test_unanswered_dispatch_ends_tasking(kit, service_context, leased_controller, until):
    """A dispatch reply timeout ends tasking and allows another tasking session to start."""
    controller_impl, _ = leased_controller
    program = await service_context.register_program("cleanup-program")
    release = asyncio.Event()

    @controller_impl.task_handler(CollectTask)
    async def collect(task):
        await release.wait()

    @program.task_factory
    async def factory():
        yield CollectTask()

    client = kit.program("cleanup-program")
    controller = kit.controller(controller_impl.entity)
    await client.enable(str(controller_impl.entity))

    try:
        async with asyncio.timeout(30.0):
            # Delay admission until the dispatch reply times out.
            async with controller_impl._state.updating(controller_impl):
                (await client.start_tasking()).close()
                loop = program._task_loop
                await until(lambda: tasking_ended(client), timeout=25.0)
                state = await client.kv_get_model(ProgramState)

                assert state.active_state.origin == "error"
                assert loop.finalized
                assert loop.unresolved
    finally:
        release.set()

        with contextlib.suppress(CallError):
            async with asyncio.timeout(5.0):
                await controller.abort_task()


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", ["abort", "completion", "successor"])
async def test_cleanup_retries_and_allows_restart(
    kit, service_context, leased_controller, confirmation
):
    """Loop finalization waits for the task's end event, with or without an accepted abort."""
    controller, _ = leased_controller
    program = await service_context.register_program("retry-program")
    entered = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()
        await release.wait()

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await kit.controller(controller.entity).enable()
    await client.enable(str(controller.entity))
    graceful = abort = retry = successor = None

    try:
        async with asyncio.timeout(5.0):
            (await client.start_tasking()).close()
            await entered.wait()
            loop = program._task_loop
            assert not loop.finalized
            controller.allow_abort.clear()
            graceful = asyncio.create_task(client.stop_tasking())

            while not (await client.kv_get_model(ProgramState)).active_state.stopping:
                await asyncio.sleep(0)

            with pytest.raises(CallError):
                await client.start_tasking()

            abort = asyncio.create_task(client.abort_tasking())
            await controller.abort_failed.wait()

            assert program._task_loop is loop
            assert not loop.finalized

            with pytest.raises(CallError):
                await client.start_tasking()

            match confirmation:
                case "abort":
                    # Retry the abort request that the controller previously failed to handle.
                    controller.allow_abort.set()
                    retry = asyncio.create_task(client.abort_tasking())
                case "completion":
                    release.set()
                case "successor":
                    entered.clear()
                    successor = kit.controller(controller.entity).execute_task(
                        CollectTask(), interrupt=True
                    )
                    await successor.invoke()
                    await entered.wait()

            await asyncio.gather(*(stop for stop in (graceful, abort, retry) if stop is not None))
            assert loop.finalized
            assert program._task_loop is None

            if successor is not None:
                assert not successor.done()
                release.set()
                await successor

            entered.clear()
            release.clear()
            (await client.start_tasking()).close()
            await entered.wait()
            assert program._task_loop is not loop
    finally:
        controller.allow_abort.set()
        release.set()

        async with asyncio.timeout(5.0):
            await client.abort_tasking()

            if graceful is not None:
                await graceful

            if abort is not None:
                await abort

            if retry is not None:
                await retry

            if successor is not None:
                with contextlib.suppress(CallError):
                    await successor


@pytest.mark.asyncio
async def test_controller_lease_absence_preserves_error_origin(
    kit, service_context, leased_controller
):
    """A factory error remains the tasking stop reason when controller cleanup fails."""
    controller, lease = leased_controller
    program = await service_context.register_program("error-origin-program")
    fail = asyncio.Event()

    @program.task_factory
    async def factory():
        await fail.wait()
        raise RuntimeError("Task factory failed")

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))
    (await client.start_tasking()).close()
    loop = program._task_loop
    finalizer = program._task_finalizer

    try:
        async with asyncio.timeout(5.0):
            await lease.expire()
            fail.set()
            await asyncio.shield(finalizer)

            state = await client.kv_get_model(ProgramState)
            assert not state.active_state.active
            assert state.active_state.origin == "error"
            assert loop.finalized
            assert not loop.stop_requested
    finally:
        fail.set()

        async with asyncio.timeout(5.0):
            await client.abort_tasking()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["factory", "execution"])
async def test_abort_does_not_repeat_for_released_work(
    kit, service_context, leased_controller, phase
):
    """Producer teardown does not repeatedly abort work that already ended."""
    controller, _ = leased_controller
    program = await service_context.register_program("released-work-program")
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    emitted = False

    @controller.task_handler(CollectTask)
    async def collect(task):
        if phase == "execution":
            await asyncio.Event().wait()

    @program.task_factory
    async def factory():
        nonlocal emitted

        if phase == "factory" and emitted:
            entered.set()

            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()

            return

        emitted = True
        execution = yield CollectTask()

        if phase == "execution":
            entered.set()

            try:
                await execution
            finally:
                cleaning.set()
                await release.wait()
        else:
            await execution

    client = kit.program(program.entity)
    await kit.controller(controller.entity).enable()
    await client.enable(str(controller.entity))
    abort = None

    try:
        async with asyncio.timeout(5.0):
            (await client.start_tasking()).close()
            await entered.wait()
            loop = program._task_loop
            abort = asyncio.create_task(client.abort_tasking())
            await cleaning.wait()
            done, _ = await asyncio.wait((abort,), timeout=0.1)

            assert not done
            assert len(controller.abort_requests) == (1 if phase == "execution" else 0)

            release.set()
            await abort
            assert loop.finalized
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            if abort is not None:
                await abort

            await client.abort_tasking()


@pytest.mark.asyncio
async def test_target_change_waits_for_cleanup(kit, service_context, leased_controller):
    """Changing controllers waits for the current controller task's cleanup to finish."""
    original, _ = leased_controller
    replacement = await service_context.register_controller("replacement")
    await replacement.set_internal_state(OPERATE)
    program = await service_context.register_program("switch-program")
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    switched = asyncio.Event()

    @original.task_handler(CollectTask)
    async def old_collect(task):
        entered.set()

        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    @replacement.task_handler(CollectTask)
    async def new_collect(task):
        switched.set()

    @program.task_factory
    async def factory():
        if not switched.is_set():
            yield CollectTask()

    client = kit.program(program.entity)
    await kit.controller(original.entity).enable()
    await kit.controller(replacement.entity).enable()
    await client.enable(str(original.entity))
    switch = None

    try:
        async with asyncio.timeout(5.0):
            (await client.start_tasking()).close()
            await entered.wait()
            loop = program._task_loop
            switch = asyncio.create_task(client.enable(str(replacement.entity)))
            await cleaning.wait()

            assert not loop.finalized
            assert not switch.done()
            assert not switched.is_set()
            assert (await client.kv_get_model(ProgramState)).enable_state.controller == str(
                original.entity
            )

            with pytest.raises(CallError):
                await client.start_tasking()

            release.set()
            await switch
            assert loop.finalized
            (await client.start_tasking()).close()
            await switched.wait()
            await client.stop_tasking()
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            if switch is not None:
                await switch

            await client.abort_tasking()


@pytest.mark.asyncio
@pytest.mark.parametrize("previous", ["idle", "tasking"])
async def test_delayed_stop_cannot_stop_replacement_loop(
    kit, service_context, leased_controller, previous
):
    """A delayed stop cannot affect a tasking loop started after the request."""
    controller, _ = leased_controller
    program = await service_context.register_program("delayed-stop-program")
    entered = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()
        await asyncio.Event().wait()

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await kit.controller(controller.entity).enable()
    await client.enable(str(controller.entity))
    stop = None

    try:
        async with asyncio.timeout(5.0):
            if previous == "tasking":
                (await client.start_tasking()).close()
                await entered.wait()

            stop = program._stop_loop(program._task_loop, abort=True)
            await client.abort_tasking()
            entered.clear()
            (await client.start_tasking()).close()
            await entered.wait()
            replacement = program._task_loop

            await stop
            stop = None

            assert program._task_loop is replacement
            assert replacement.running
            state = await client.kv_get_model(ProgramState)
            assert state.active_state.active and not state.active_state.stopping
    finally:
        if stop is not None:
            stop.close()

        async with asyncio.timeout(5.0):
            await client.abort_tasking()


@pytest.mark.asyncio
async def test_enable_hook_gates_start_without_blocking_abort(kit, service_context):
    """An enable hook blocks new tasking while abort requests remain available."""
    controller = await service_context.register_controller("hook-controller")
    await controller.set_internal_state(OPERATE)
    program = await service_context.register_program("hook-program")
    entered = asyncio.Event()
    release = asyncio.Event()
    task_entered = asyncio.Event()

    @program.on_enable
    async def enable_hook():
        entered.set()
        await release.wait()

    @controller.task_handler(CollectTask)
    async def collect(task):
        task_entered.set()
        await asyncio.Event().wait()

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await kit.controller(controller.entity).enable()
    enable = asyncio.create_task(client.enable(str(controller.entity)))

    try:
        async with asyncio.timeout(5.0):
            await entered.wait()

            with pytest.raises(CallError):
                await client.start_tasking()

            await client.abort_tasking()
            assert not enable.done()
            assert not task_entered.is_set()

            release.set()
            await enable
            (await client.start_tasking()).close()
            await task_entered.wait()
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            await enable
            await client.abort_tasking()


# Session monitoring


@pytest.mark.asyncio
async def test_monitor_follows_the_selected_session(
    kit, service_context, leased_controller, until
):
    """Monitor outcomes belong to the server-selected session."""
    controller, _ = leased_controller
    program = await service_context.register_program("monitored-program")

    @program.task_factory
    async def factory():
        raise RuntimeError("Planner failed")

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    async with tasking_reports(client) as reports, asyncio.timeout(5.0):
        with await client.start_tasking() as monitor:
            await monitor.wait()
            await until(lambda: session_ended(monitor))

        session = await ended(reports, until)

    assert {r.loop_state.session_id for r in session if r.loop_state} == {monitor.session_id}
    assert (monitor.fault.fault, monitor.fault.stage) == ("program", "factory")
    assert monitor.ending.origin == "error"


@pytest.mark.asyncio
@pytest.mark.parametrize("abort_before_report", [False, True])
async def test_start_waits_for_session_publication(
    kit, service_context, leased_controller, abort_before_report, until
):
    """Startup waits for a session report or fails if the producer ends first."""
    controller, _ = leased_controller
    program = await service_context.register_impl(
        GatedTaskingProgram.for_service_context(service_context, "publication-program")
    )

    @program.task_factory
    def factory():
        return None

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))
    await client.subscribe_events()
    call = client.call(set_active_state_request, ProgramActiveStateRequest(action="start"))
    abort = None
    sender = asyncio.create_task(call.invoke())

    try:
        async with asyncio.timeout(5.0):
            await program.reporting.wait()
            await until(lambda: call.response is not None)
            response = call.response

            if abort_before_report:
                abort = asyncio.create_task(client.abort_tasking(response.session_id))

                with pytest.raises(CallError):
                    await call
            else:
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(sender), timeout=0.1)

                program.release_report.set()
                await sender
                await call

            program.release_report.set()

            if abort is not None:
                await abort
            else:
                await client.stop_tasking(response.session_id)
    finally:
        program.release_report.set()

        if abort is not None:
            await asyncio.wait((abort,))

        await asyncio.wait((sender,))


@pytest.mark.asyncio
async def test_idempotent_start_acknowledges_the_session(kit, service_context, leased_controller):
    """Idempotent activation preserves the session and its running execution."""
    controller, _ = leased_controller
    program = await service_context.register_program("idempotent-program")
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    try:
        async with asyncio.timeout(5.0):
            with await client.start_tasking() as first:
                await entered.wait()

                with await client.start_tasking() as second:
                    state = await client.kv_get_model(ProgramState)

                    assert second.session_id == first.session_id
                    assert state.tasking_state.loop_state.session_id == first.session_id
                    assert not state.active_state.stopping
                    assert not cancelled.is_set()
    finally:
        async with asyncio.timeout(5.0):
            await client.abort_tasking()


@pytest.mark.asyncio
async def test_monitor_binds_once_without_yielding_snapshot(kit, entity_impl):
    client = kit.program(entity_impl.entity)
    await client.subscribe_events()
    selected = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    initial = ProgramState(
        enable_state=ProgramEnableState(enabled=True, controller="ctrl1"),
        active_state=ProgramActiveState(active=True, origin="request"),
        tasking_state=ProgramTaskingState(loop_state=selected),
    )

    with ProgramTaskingMonitor(client) as monitor:
        with pytest.raises(RuntimeError, match="not bound"):
            monitor.read_events()
        with pytest.raises(RuntimeError, match="not bound"):
            await anext(monitor.events())
        with pytest.raises(RuntimeError, match="not bound"):
            _ = monitor.session_id

        monitor.bind(selected.session_id, initial)

        assert monitor.session_id == selected.session_id
        assert monitor.loop_state == selected
        assert monitor.read_events() == []

        with pytest.raises(RuntimeError, match="already bound"):
            monitor.bind(uuid.uuid7())

        monitor.close()
        with pytest.raises(asyncio.QueueShutDown):
            await anext(monitor.events())


@pytest.mark.asyncio
async def test_monitor_iteration_retains_buffered_cleanup_events(kit, entity_impl):
    selected = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    first = ProgramTaskingState(loop_state=selected)
    fault = ProgramTaskingState(
        loop_state=selected.model_copy(update={"fault": "program", "error": "fault"})
    )
    clear = ProgramTaskingState()

    async def start(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=selected.session_id))
        await entity_impl.emit_event(ProgramActiveState(active=True, origin="request"))
        for event in (first, fault, clear):
            await entity_impl.emit_event(event)
        await entity_impl.emit_event(ProgramActiveState(active=False, origin="error"))
        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, start)

    async with asyncio.timeout(5.0):
        with await kit.program(entity_impl.entity).start_tasking() as monitor:
            assert [state async for state in monitor.events()] == [first, fault]
            assert monitor.fault == fault.loop_state
            assert monitor.ending is None

            assert monitor.read_events() == [clear]
            assert monitor.ending.origin == "error"
            assert monitor.read_events() == []
            assert [state async for state in monitor.events()] == []


@pytest.mark.asyncio
async def test_monitor_iteration_keeps_session_end_order(kit, entity_impl):
    selected = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    replacement = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    first = ProgramTaskingState(loop_state=selected)
    clear = ProgramTaskingState()

    async def start(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=selected.session_id))
        for event in (
            first,
            clear,
            ProgramActiveState(active=False, origin="request"),
            ProgramTaskingState(loop_state=replacement),
        ):
            await entity_impl.emit_event(event)
        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, start)

    async with asyncio.timeout(5.0):
        with await kit.program(entity_impl.entity).start_tasking() as monitor:
            assert [state async for state in monitor.events()] == [first, clear]
            assert monitor.ending.origin == "request"
            assert monitor.loop_state == selected
            assert monitor.fault is None
            assert monitor.read_events() == []


@pytest.mark.asyncio
async def test_monitor_ignores_other_sessions(kit, entity_impl, until):
    """Unrelated session history does not change the selected outcome."""
    earlier = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    competing = TaskingLoopState(
        session_id=uuid.uuid7(), stage="execution", fault="controller", error="late"
    )
    selected = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    fault = selected.model_copy(update={"fault": "program", "error": "fault"})

    async def set_active_state(msg, call):
        # Clear the earlier session before selecting new tasking.
        await entity_impl.emit_event(ProgramTaskingState())
        await entity_impl.emit_event(ProgramActiveState(active=False, origin="request"))

        # Retain a competing session's complete history in the same stream.
        await entity_impl.emit_event(ProgramActiveState(active=True, origin="request"))
        await entity_impl.emit_event(ProgramTaskingState(loop_state=competing))
        await entity_impl.emit_event(ProgramTaskingState())
        await entity_impl.emit_event(ProgramActiveState(active=False, origin="error"))

        await entity_impl.emit_event(ProgramActiveState(active=True, origin="request"))
        await entity_impl.emit_event(ProgramTaskingState(loop_state=selected))
        call.accept(response=ProgramActiveStateResponse(session_id=selected.session_id))
        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, set_active_state)
    await entity_impl.kv_put_model(
        ProgramState(
            enable_state=ProgramEnableState(enabled=True, controller="ctrl1"),
            active_state=ProgramActiveState(active=True, origin="request"),
            tasking_state=ProgramTaskingState(loop_state=earlier),
        )
    )
    client = kit.program(entity_impl.entity)

    async with asyncio.timeout(5.0):
        with await client.start_tasking() as monitor:
            # Completion places all preceding reports in the monitor's queue.
            monitor.read_events()

            assert monitor.loop_state == selected
            assert monitor.ending is None

            await entity_impl.emit_event(ProgramTaskingState(loop_state=fault))
            await entity_impl.emit_event(ProgramTaskingState())
            await entity_impl.emit_event(ProgramActiveState(active=False, origin="error"))
            await monitor.wait()
            await until(lambda: session_ended(monitor))

    assert monitor.fault == fault
    assert monitor.ending.origin == "error"


@pytest.mark.asyncio
async def test_monitor_ignores_untagged_history_before_its_snapshot(kit, entity_impl):
    """An older untagged ending cannot terminate a session identified by the snapshot."""
    previous_clear = ProgramTaskingState()
    previous_end = ProgramActiveState(active=False, origin="request")
    selected = TaskingLoopState(session_id=uuid.uuid7(), stage="factory")
    initial = ProgramState(
        enable_state=ProgramEnableState(enabled=True, controller="ctrl1"),
        active_state=ProgramActiveState(active=True, origin="request"),
        tasking_state=ProgramTaskingState(loop_state=selected),
    )
    await entity_impl.kv_put_model(initial)

    async def start(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=selected.session_id))
        await entity_impl.emit_event(previous_clear)
        await entity_impl.emit_event(previous_end)
        await entity_impl.emit_event(initial.active_state)
        await entity_impl.emit_event(initial.tasking_state)
        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, start)

    with await kit.program(entity_impl.entity).start_tasking() as monitor:
        monitor.read_events()

        assert monitor.loop_state == selected
        assert monitor.ending is None


@pytest.mark.asyncio
async def test_monitor_retains_an_ending_before_the_start_result(kit, entity_impl):
    """Buffered faults and endings remain visible after startup completes."""
    selected = TaskingLoopState(
        session_id=uuid.uuid7(), stage="factory", fault="controller", error="fault"
    )

    async def set_active_state(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=selected.session_id))
        await entity_impl.emit_event(ProgramActiveState(active=True, origin="request"))
        await entity_impl.emit_event(ProgramTaskingState(loop_state=selected))
        await entity_impl.emit_event(ProgramTaskingState())
        await entity_impl.emit_event(ProgramActiveState(active=False, origin="error"))
        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, set_active_state)
    client = kit.program(entity_impl.entity)

    async with asyncio.timeout(5.0):
        with await client.start_tasking() as monitor:
            monitor.read_events()

    assert monitor.fault == selected
    assert monitor.ending.origin == "error"


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["stop", "abort"])
async def test_targeted_request_cannot_end_a_replacement(
    kit, service_context, leased_controller, until, action
):
    """A request targeting an ended session succeeds without affecting its replacement."""
    controller, _ = leased_controller
    program = await service_context.register_program("targeted-program")
    entered = asyncio.Event()

    @controller.task_handler(CollectTask)
    async def collect(task):
        entered.set()
        await asyncio.Event().wait()

    @program.task_factory
    async def factory():
        await (yield CollectTask())

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))
    end = client.abort_tasking if action == "abort" else client.stop_tasking

    try:
        async with asyncio.timeout(5.0):
            with await client.start_tasking() as original:
                await entered.wait()
                await client.abort_tasking(original.session_id)
                await until(lambda: session_ended(original))

            entered.clear()

            with await client.start_tasking() as replacement:
                await entered.wait()

                # The request arrives after the original session ended.
                await end(original.session_id)
                state = await client.kv_get_model(ProgramState)
                replacement.read_events()

                assert replacement.session_id != original.session_id
                assert state.tasking_state.loop_state.session_id == replacement.session_id
                assert state.active_state.active and not state.active_state.stopping
                assert replacement.ending is None

                await client.abort_tasking(replacement.session_id)
                await until(lambda: session_ended(replacement))
    finally:
        async with asyncio.timeout(5.0):
            await client.abort_tasking()

    assert original.ending.origin == "request"
    assert replacement.ending.origin == "request"


@pytest.mark.asyncio
async def test_canceled_wait_retains_session_events(
    kit, service_context, leased_controller, until
):
    """Canceling a wait keeps queued events, and closing the monitor ends observation."""
    controller, _ = leased_controller
    program = await service_context.register_program("waiting-program")
    fail = asyncio.Event()

    @program.task_factory
    async def factory():
        await fail.wait()
        raise RuntimeError("Planner failed")

    client = kit.program(program.entity)
    await client.enable(str(controller.entity))

    async with asyncio.timeout(5.0):
        with await client.start_tasking() as monitor:
            waiter = asyncio.create_task(monitor.wait())
            await asyncio.sleep(0.1)
            waiter.cancel()
            await asyncio.wait((waiter,))

            fail.set()
            await until(lambda: tasking_ended(client))
            await until(lambda: session_ended(monitor))

        assert monitor.fault.fault == "program"

        fail.clear()
        closed = await client.start_tasking()
        closed.close()
        fail.set()
        await until(lambda: tasking_ended(client))
        closed.read_events()

        assert closed.fault is None
        assert closed.ending is None

        with pytest.raises(asyncio.QueueShutDown):
            await closed.wait()


@pytest.mark.asyncio
async def test_activation_joins_one_start_request(kit, entity_impl, until):
    """Repeated sends share one request even if a waiter is canceled."""
    session_id = uuid.uuid7()
    requests: list[str] = []
    release = asyncio.Event()

    async def start(msg, call):
        requests.append(msg.action)
        call.accept(response=ProgramActiveStateResponse(session_id=session_id))
        await call.progress(5.0)
        await release.wait()
        await call.succeed(result=None)

    await entity_impl.handle_request(set_active_state_request, start)
    activation = kit.program(entity_impl.entity).start_tasking()

    try:
        async with asyncio.timeout(5.0):
            await activation.prepare()

            assert activation.monitor is None

            # Cancel the first waiter after acceptance while startup is pending.
            waiter = asyncio.create_task(activation.send())
            await until(lambda: activation.monitor is not None)
            waiter.cancel()
            await asyncio.wait((waiter,))
            release.set()

            monitor = await activation
            joined = await activation.send()
    finally:
        activation.close()

    assert waiter.cancelled()
    assert requests == ["start"]
    assert monitor is joined is activation.monitor
    assert monitor.session_id == session_id


@pytest.mark.asyncio
async def test_activation_retains_monitor_after_start_failure(kit, entity_impl):
    """A failed startup retains the accepted session's monitor."""
    session_id = uuid.uuid7()

    async def start(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=session_id))
        await call.fail("Start failed")

    await entity_impl.handle_request(set_active_state_request, start)
    activation = kit.program(entity_impl.entity).start_tasking()

    try:
        async with asyncio.timeout(5.0):
            with pytest.raises(TaskingStartupFailed):
                await activation

            with pytest.raises(TaskingStartupFailed):
                await activation.send()
    finally:
        activation.close()

    assert activation.monitor.session_id == session_id


@pytest.mark.asyncio
async def test_activation_retains_monitor_when_startup_is_unconfirmed(kit, entity_impl):
    """Missing progress leaves startup uncertain while retaining the accepted session."""
    session_id = uuid.uuid7()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def start(msg, call):
        call.accept(response=ProgramActiveStateResponse(session_id=session_id))

        try:
            await call.progress(0.05)
            await release.wait()
            await call.succeed(result=None)
        finally:
            finished.set()

    await entity_impl.handle_request(set_active_state_request, start)
    activation = kit.program(entity_impl.entity).start_tasking()

    try:
        async with asyncio.timeout(5.0):
            with pytest.raises(TaskingError) as failed:
                await activation

        assert not isinstance(failed.value, TaskingStartupFailed)
        assert isinstance(failed.value.__cause__, TimeoutError)
        assert failed.value.session_id == session_id
        assert activation.monitor.session_id == session_id
    finally:
        release.set()

        async with asyncio.timeout(5.0):
            await finished.wait()

        activation.close()


@pytest.mark.asyncio
async def test_rejected_activation_has_no_session(kit, entity_impl):
    """Rejection leaves no session monitor."""

    async def start(msg, call):
        call.reject(reason="Unavailable")

    await entity_impl.handle_request(set_active_state_request, start)
    activation = kit.program(entity_impl.entity).start_tasking()

    async with asyncio.timeout(5.0):
        with pytest.raises(CallRejected):
            await activation

    assert activation.monitor is None


# Legacy tasking reports used bare IDs and full executions.
def test_legacy_tasking_events_decode_without_a_session():
    """Historical payloads retain event and execution IDs without inventing a session."""
    execution = TaskExecution(
        task=CollectTask(), task_id=uuid.uuid7(), controller_id="mycontroller"
    )
    full = {
        "event_model": ProgramTaskingState.model_tag(),
        "event_id": str(uuid.uuid7()),
        "executing_task": json.loads(execution.model_dump_json()),
    }
    bare = {
        "event_model": ProgramTaskingState.model_tag(),
        "event_id": str(uuid.uuid7()),
        "executing_task_id": str(execution.task_id),
    }
    snapshot = {
        "enable_state": {"enabled": True, "controller": "mycontroller"},
        "active_state": {"active": True, "origin": "request"},
        "tasking_state": full,
    }

    running = Event.model_validate_json(json.dumps(full))
    idle = Event.model_validate_json(json.dumps(bare))
    stored = ProgramState.model_validate_json(json.dumps(snapshot)).tasking_state

    for event, payload in ((running, full), (idle, bare), (stored, full)):
        assert isinstance(event, ProgramTaskingState)
        assert str(event.event_id) == payload["event_id"]
        assert event.loop_state is None

    assert running.execution_state.execution == execution
    assert stored.execution_state.execution == execution
    assert idle.execution_state is None


def test_legacy_tasking_status_migrates_to_tasking_state():
    """Legacy task IDs provide no execution or session state."""
    legacy = {
        "enable_state": {"enabled": True, "controller": "mycontroller"},
        "active_state": {"active": False, "origin": "init"},
        "tasking_status": {"executing_task_id": str(uuid.uuid4())},
    }

    state = ProgramState.model_validate(legacy)

    assert isinstance(state.tasking_state, ProgramTaskingState)
    assert state.tasking_state.loop_state is None
    assert state.tasking_state.execution_state is None


def test_legacy_executing_task_migrates_to_execution_state():
    """Legacy executions retain running state without inventing a session."""
    execution = TaskExecution(
        task=CollectTask(), task_id=uuid.uuid7(), controller_id="mycontroller"
    )
    legacy = {"executing_task": json.loads(execution.model_dump_json())}

    state = ProgramTaskingState.model_validate_json(json.dumps(legacy))

    assert state.loop_state is None
    assert state.execution_state.executing
    assert state.execution_state.execution == execution


def test_legacy_program_state_loads_from_kv_json():
    """A full legacy ProgramState snapshot deserialises from JSON without error."""
    legacy = {
        "enable_state": {"enabled": True, "controller": "mycontroller"},
        "active_state": {"active": False, "origin": "init"},
        "tasking_status": {"executing_task_id": None},
    }

    state = ProgramState.model_validate_json(json.dumps(legacy))

    assert state.tasking_state.execution_state is None


def test_program_state_missing_tasking_key_defaults_to_idle():
    """A snapshot predating the tasking field entirely is treated as idle rather than failing."""
    legacy = {
        "enable_state": {"enabled": False},
        "active_state": {"active": False, "origin": "init"},
    }

    state = ProgramState.model_validate(legacy)

    assert state.tasking_state.loop_state is None
    assert state.tasking_state.execution_state is None


def test_current_program_state_roundtrips_unchanged():
    """Current-shape state passes through the migration validators as a no-op."""
    loop_state = TaskingLoopState(session_id=uuid.uuid7(), stage="dispatch", task_type="collect")
    state = ProgramState.model_validate(
        {
            "enable_state": {"enabled": True, "controller": "mycontroller"},
            "active_state": {"active": True, "origin": "request"},
            "tasking_state": {"loop_state": loop_state.model_dump(mode="json")},
        }
    )

    reloaded = ProgramState.model_validate_json(state.model_dump_json())

    assert reloaded.tasking_state == state.tasking_state
    assert reloaded.tasking_state.loop_state == loop_state
