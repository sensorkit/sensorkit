# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, override

import pytest
import pytest_asyncio
import uuid_utils.compat as uuid
from pydantic import BaseModel

from sensorkit.backend.base import Backend, BackendError, Subject
from sensorkit.backend.event import Event
from sensorkit.backend.fake import FakeBackendImpl
from sensorkit.backend.request import CallContext, CallError, CallRejected
from sensorkit.common.keyword import KeywordDict, declare_keyword
from sensorkit.core.client import SensorKit
from sensorkit.core.controller import (
    ControllerClient,
    ControllerDevice,
    ControllerEnableState,
    ControllerOperatingState,
    ControllerState,
    ExecuteRequestMessage,
    InternalControllerState,
    TaskExecutionState,
    TaskFinishInfo,
)
from sensorkit.core.device import DeviceClient
from sensorkit.core.impl.controller import ControllerImpl, TaskRun, TaskRunPhase
from sensorkit.core.task import (
    CollectTask,
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
    TaskExecution,
)

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY
SHUTDOWN = InternalControllerState.SHUTDOWN
ERROR = InternalControllerState.ERROR
UNKNOWN = InternalControllerState.UNKNOWN
TRANSITION = InternalControllerState.TRANSITION


@dataclass
class PublishFault:
    """A rule that blocks or fails one matching stream publication."""

    match: Callable[[dict[str, Any]], bool]
    fail: bool = False
    reached: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)


class GatedBackendImpl(FakeBackendImpl):
    """Fake backend with gates and failures for stream publication."""

    def __init__(self):
        super().__init__()
        self.faults: list[PublishFault] = []

    def arm(
        self, match: Callable[[dict[str, Any]], bool], *, fail=False, hold=False
    ) -> PublishFault:
        """Block or fail the next matching publication.

        A failure is immediate unless held, in which case it waits for release.
        """
        fault = PublishFault(match, fail)

        if fail and not hold:
            fault.release.set()

        self.faults.append(fault)
        return fault

    def take_fault(self, payload: bytes) -> PublishFault | None:
        """Consume the first fault matching the publication payload."""
        try:
            data = json.loads(payload)
        except ValueError:
            return None

        if not isinstance(data, dict):
            return None

        for fault in self.faults:
            if fault.match(data):
                self.faults.remove(fault)
                return fault

        return None

    @override
    async def stream_publish(self, target: Subject, payload: bytes):
        if fault := self.take_fault(payload):
            fault.reached.set()
            await fault.release.wait()

            if fault.fail:
                raise BackendError("injected publication failure")

        await super().stream_publish(target, payload)


@pytest_asyncio.fixture
async def gated_backend():
    return GatedBackendImpl()


@pytest_asyncio.fixture
async def gated_kit(gated_backend):
    return SensorKit(Backend(impl=gated_backend))


def starting_execution(data: dict[str, Any]) -> bool:
    return data.get("event_model") == "TaskExecutionState" and data.get("executing") is True


def finishing_execution(data: dict[str, Any]) -> bool:
    return data.get("event_model") == "TaskExecutionState" and data.get("finished") is not None


def entering_transition(data: dict[str, Any]) -> bool:
    return (
        data.get("event_model") == "ControllerOperatingState" and data["current"] == "transition"
    )


def succeeding_call(data: dict[str, Any]) -> bool:
    return data.get("call_state") == "success"


def reporting_progress(data: dict[str, Any]) -> bool:
    return data.get("call_state") == "running"


def reporting(state: InternalControllerState) -> Callable[[dict[str, Any]], bool]:
    def match(data: dict[str, Any]) -> bool:
        return data.get("event_model") == "ControllerOperatingState" and data["current"] == state

    return match


@contextlib.asynccontextmanager
async def watch_state(client: ControllerClient):
    """Open a controller snapshot subscription and consume its initial value."""
    async with contextlib.aclosing(client.kv_monitor_model(ControllerState)) as states:
        await anext(states)
        yield states


async def collect_until(
    states: AsyncIterator[ControllerState],
    predicate: Callable[[ControllerState], bool],
) -> list[ControllerState]:
    """Collect snapshots through the first match."""
    seen = []

    async for state in states:
        seen.append(state)

        if predicate(state):
            return seen

    raise AssertionError("state stream ended")


def execution_id(state: ControllerState) -> uuid.UUID | None:
    execution = state.execution_state.execution
    return execution.task_id if execution else None


def is_finished(state: ControllerState, task_id: uuid.UUID) -> bool:
    return execution_id(state) == task_id and state.execution_state.finished is not None


def operating_history(
    states: list[ControllerState],
    *task_ids: uuid.UUID,
) -> list[tuple[uuid.UUID | None, InternalControllerState]]:
    """Return execution IDs and operating states for the selected tasks."""
    return [
        (execution_id(state), state.operating_state.current)
        for state in states
        if execution_id(state) in task_ids
    ]


def transition_history(
    states: list[ControllerState],
    *task_ids: uuid.UUID,
) -> list[tuple[uuid.UUID | None, InternalControllerState, InternalControllerState | None]]:
    """Return execution IDs, operating states, and targets for the selected tasks."""
    return [
        (execution_id(state), state.operating_state.current, state.operating_state.target)
        for state in states
        if execution_id(state) in task_ids
    ]


@pytest.mark.asyncio
async def test_controller(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    disabled = asyncio.Event()
    enabled = asyncio.Event()
    done = asyncio.Event()

    @controller.on_disable
    async def disable():
        disabled.set()

    @controller.on_enable
    async def on_enable():
        enabled.set()

    @controller.task_handler(InitTask)
    async def run_task(task: InitTask):
        assert ControllerImpl.current.get() is controller
        assert isinstance(task, InitTask)
        done.set()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.disable()
        await disabled.wait()
        await cli.enable()
        await enabled.wait()
        await cli.execute_task(InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"))
        await done.wait()


@pytest.mark.asyncio
async def test_controller_derived_task(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    class MyCollectTask(CollectTask):
        task_type: Literal["my_collect"] = "my_collect"
        my_field: int

    remaining = {MyCollectTask, CollectTask}

    @controller.task_handler(MyCollectTask)
    async def run_custom_collect(task: MyCollectTask):
        assert type(task) is MyCollectTask
        remaining.discard(MyCollectTask)

    @controller.task_handler(CollectTask)
    async def run_generic_collect(task: CollectTask):
        assert type(task) is CollectTask
        remaining.discard(CollectTask)

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.enable()
        await cli.execute_task(CollectTask(task_id=uuid.uuid1(), controller_id="mycontroller"))
        await cli.execute_task(
            MyCollectTask(task_id=uuid.uuid1(), controller_id="mycontroller", my_field=42)
        )
        assert not remaining


@pytest.mark.asyncio
async def test_controller_interrupt(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_task(_: InitTask):
        started.set()
        await can_finish.wait()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        exec_call = cli.execute_task(InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"))
        await exec_call.invoke()
        await started.wait()
        started.clear()

        with pytest.raises(CallError):
            await cli.execute_task(
                InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"),
                interrupt=False,
            )

        assert not started.is_set()

        interrupting_call = cli.execute_task(
            InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"),
            interrupt=True,
        )
        await interrupting_call.invoke()
        await started.wait()

        with pytest.raises(CallError):
            await exec_call

        can_finish.set()
        await interrupting_call


@pytest.mark.asyncio
async def test_controller_abort(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    ready = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_task(_: InitTask):
        ready.set()
        await asyncio.sleep(5)

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        exec_call = cli.execute_task(InitTask())
        # The controller mints the task_id; learn it from the initial response.
        exec_response = await exec_call.invoke()
        task_id = exec_response.execution.task_id
        await ready.wait()

        abort_call = cli.abort_task()
        abort_response = await abort_call.invoke()

        assert abort_response.aborting
        assert abort_response.task_id == task_id

        await abort_call.wait()
        assert abort_call.result() is None

        with pytest.raises(CallError):
            await exec_call.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("succeed", InternalControllerState.SHUTDOWN),
        ("fail", InternalControllerState.ERROR),
        ("abort", InternalControllerState.UNKNOWN),
    ],
)
async def test_lifecycle_task_reports_arrival_only_on_success(kit, outcome, expected):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(ShutdownTask)
    async def run_shutdown(_: ShutdownTask):
        started.set()

        match outcome:
            case "fail":
                raise RuntimeError("enclosure fault")
            case "abort":
                await asyncio.sleep(5)

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.execute_task(InitTask())

        exec_call = cli.execute_task(ShutdownTask())
        task_id = (await exec_call.invoke()).execution.task_id
        await started.wait()

        match outcome:
            case "succeed":
                await exec_call.wait()
            case "fail":
                with pytest.raises(CallError):
                    await exec_call.wait()
            case "abort":
                await cli.abort_task()

                with pytest.raises(CallError):
                    await exec_call.wait()

        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == expected
    assert state.operating_state.previous == TRANSITION
    assert state.operating_state.target is None
    assert is_finished(state, task_id)
    assert state.execution_state.finished.aborted == (outcome == "abort")
    assert bool(state.execution_state.finished.error) == (outcome == "fail")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_type", "target"),
    [(InitTask, OPERATE), (StandbyTask, STANDBY), (ShutdownTask, SHUTDOWN)],
    ids=("init", "standby", "shutdown"),
)
async def test_lifecycle_task_reports_transition(kit, task_type, target):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(task_type)
    async def run_task(_):
        started.set()
        await can_finish.wait()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        exec_call = cli.execute_task(task_type())
        task_id = (await exec_call.invoke()).execution.task_id
        await started.wait()

        running = await cli.kv_get_model(ControllerState)
        can_finish.set()
        await exec_call
        settled = await cli.kv_get_model(ControllerState)

    assert running.operating_state.current == TRANSITION
    assert running.operating_state.target == target
    assert running.execution_state.executing
    assert execution_id(running) == task_id

    assert settled.operating_state.current == target
    assert settled.operating_state.previous == TRANSITION
    assert settled.operating_state.target is None
    assert is_finished(settled, task_id)


@pytest.mark.asyncio
async def test_fast_lifecycle_task_reports_transition(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0), watch_state(cli) as states:
        exec_call = cli.execute_task(InitTask())
        task_id = (await exec_call.invoke()).execution.task_id
        await exec_call
        seen = await collect_until(states, lambda state: is_finished(state, task_id))

    assert operating_history(seen, task_id) == [(task_id, TRANSITION), (task_id, OPERATE)]


@pytest.mark.asyncio
async def test_interrupting_lifecycle_task_takes_over_transition(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await asyncio.Event().wait()

    @controller.task_handler(ShutdownTask)
    async def run_shutdown(_: ShutdownTask):
        pass

    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0), watch_state(cli) as states:
        first = cli.execute_task(InitTask())
        first_id = (await first.invoke()).execution.task_id
        await started.wait()

        second = cli.execute_task(ShutdownTask(), interrupt=True)
        second_id = (await second.invoke()).execution.task_id
        await second

        with pytest.raises(CallError):
            await first.wait()

        seen = await collect_until(states, lambda state: is_finished(state, second_id))

    # The interrupted task finishes without settling the transition it no longer owns.
    assert transition_history(seen, first_id, second_id) == [
        (first_id, TRANSITION, OPERATE),
        (first_id, TRANSITION, SHUTDOWN),
        (first_id, TRANSITION, SHUTDOWN),
        (second_id, TRANSITION, SHUTDOWN),
        (second_id, SHUTDOWN, None),
    ]

    interrupted = next(state for state in seen if is_finished(state, first_id))
    assert interrupted.execution_state.finished.aborted


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["succeed", "fail", "abort"])
async def test_non_lifecycle_task_preserves_reported_state(kit, outcome):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(CollectTask)
    async def run_collect(_: CollectTask):
        started.set()

        match outcome:
            case "succeed":
                await can_finish.wait()
            case "fail":
                await can_finish.wait()
                raise RuntimeError("camera fault")
            case "abort":
                await asyncio.Event().wait()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.execute_task(InitTask())

        exec_call = cli.execute_task(CollectTask())
        task_id = (await exec_call.invoke()).execution.task_id
        await started.wait()

        await controller.report_state(ERROR)
        can_finish.set()

        match outcome:
            case "succeed":
                await exec_call.wait()
            case "fail":
                with pytest.raises(CallError):
                    await exec_call.wait()
            case "abort":
                await cli.abort_task()

                with pytest.raises(CallError):
                    await exec_call.wait()

        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == ERROR
    assert state.operating_state.previous == OPERATE
    assert state.operating_state.target is None
    assert is_finished(state, task_id)
    assert state.execution_state.finished.aborted == (outcome == "abort")


@pytest.mark.asyncio
async def test_reporting_state_during_lifecycle_task_keeps_transition(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await can_finish.wait()

    with pytest.raises(ValueError):
        await controller.report_state(TRANSITION)

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        exec_call = cli.execute_task(InitTask())
        await exec_call.invoke()
        await started.wait()

        await controller.report_state(OPERATE, KeywordDict())
        running = await cli.kv_get_model(ControllerState)
        can_finish.set()
        await exec_call
        settled = await cli.kv_get_model(ControllerState)

        await controller.report_state(STANDBY, KeywordDict())
        reported = await cli.kv_get_model(ControllerState)

    assert running.operating_state.current == TRANSITION
    assert running.operating_state.target == OPERATE
    assert settled.operating_state.current == OPERATE
    assert reported.operating_state.current == STANDBY
    assert reported.operating_state.previous == OPERATE


def seeded_state(
    operating: ControllerOperatingState,
    execution_state: TaskExecutionState,
) -> ControllerState:
    return ControllerState(
        enable_state=ControllerEnableState(enabled=True),
        operating_state=operating,
        execution_state=execution_state,
    )


def seeded_execution() -> TaskExecution:
    return TaskExecution(task=ShutdownTask(), task_id=uuid.uuid7(), controller_id="mycontroller")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operating", "executing", "aborting", "finished"),
    [
        (ControllerOperatingState(current=TRANSITION, target=SHUTDOWN), True, False, None),
        (ControllerOperatingState(current=TRANSITION, target=SHUTDOWN), True, True, None),
        (
            ControllerOperatingState(current=OPERATE, target=SHUTDOWN, context=KeywordDict()),
            True,
            False,
            None,
        ),
        (
            ControllerOperatingState(current=TRANSITION, target=SHUTDOWN),
            False,
            False,
            TaskFinishInfo(error="lost"),
        ),
    ],
    ids=("executing", "aborting", "legacy_target", "finished"),
)
async def test_startup_recovers_interrupted_transition(
    kit, operating, executing, aborting, finished
):
    execution = seeded_execution()
    cli = kit.controller("mycontroller")
    observed: list[ControllerState] = []

    await cli.kv_put_model(
        seeded_state(
            operating,
            TaskExecutionState(
                executing=executing,
                aborting=aborting,
                finished=finished,
                execution=execution,
            ),
        )
    )

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        impl = ControllerImpl.for_service_context(sc, "mycontroller")

        @impl.on_attach
        async def observe():
            observed.append(await cli.kv_get_model(ControllerState))

        await sc.register_impl(impl)

    [state] = observed
    assert state.operating_state.current == UNKNOWN
    assert state.operating_state.previous == operating.current
    assert state.operating_state.target is None
    assert is_finished(state, execution.task_id)

    if finished is None:
        assert state.execution_state.finished.aborted
    else:
        assert state.execution_state.finished == finished


@pytest.mark.asyncio
@pytest.mark.parametrize("settled", [OPERATE, STANDBY, SHUTDOWN, ERROR])
async def test_startup_reports_settled_state_unknown(kit, settled):
    cli = kit.controller("mycontroller")
    seeded = seeded_state(
        ControllerOperatingState(current=settled, previous=TRANSITION, context=KeywordDict()),
        TaskExecutionState(finished=TaskFinishInfo(), execution=seeded_execution()),
    )

    await cli.kv_put_model(seeded)

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        await sc.register_controller("mycontroller")
        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == UNKNOWN
    assert state.operating_state.previous == settled
    assert state.operating_state.target is None
    assert state.execution_state.model_dump() == seeded.execution_state.model_dump()


@pytest.mark.asyncio
async def test_startup_aborts_interrupted_task(kit):
    cli = kit.controller("mycontroller")
    execution = TaskExecution(
        task=CollectTask(), task_id=uuid.uuid7(), controller_id="mycontroller"
    )

    await cli.kv_put_model(
        seeded_state(
            ControllerOperatingState(current=OPERATE, context=KeywordDict()),
            TaskExecutionState(executing=True, execution=execution),
        )
    )

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        await sc.register_controller("mycontroller")
        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == UNKNOWN
    assert is_finished(state, execution.task_id)
    assert state.execution_state.finished.aborted


@pytest.mark.asyncio
async def test_attach_hook_reports_determined_state(kit):
    cli = kit.controller("mycontroller")

    await cli.kv_put_model(
        seeded_state(
            ControllerOperatingState(current=SHUTDOWN, context=KeywordDict()),
            TaskExecutionState(execution=None),
        )
    )

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        impl = ControllerImpl.for_service_context(sc, "mycontroller")

        @impl.on_attach
        async def determine():
            await impl.report_state(OPERATE, KeywordDict())

        await sc.register_impl(impl)
        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == OPERATE
    assert state.operating_state.previous == UNKNOWN


@pytest.mark.asyncio
async def test_transition_start_follows_held_report(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await can_finish.wait()

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")

        # An earlier report holds the update lock while it publishes.
        fault = gated_backend.arm(reporting(STANDBY))
        earlier = asyncio.create_task(controller.report_state(STANDBY, KeywordDict()))
        await fault.reached.wait()

        # Admission and the lifecycle start wait behind the held report.
        exec_call = cli.execute_task(InitTask())
        accepting = asyncio.create_task(exec_call.invoke())
        fault.release.set()
        await earlier
        await accepting
        await started.wait()

        # A state reported beneath the running lifecycle task leaves its transition published.
        await controller.report_state(OPERATE, KeywordDict())
        running = await cli.kv_get_model(ControllerState)
        can_finish.set()
        await exec_call

    assert running.operating_state.current == TRANSITION
    assert running.operating_state.previous == STANDBY
    assert running.operating_state.target == OPERATE


@pytest.mark.asyncio
async def test_abort_after_settlement_is_rejected(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")

        # Holding the success response keeps the settled execution's request in flight.
        fault = gated_backend.arm(succeeding_call)
        exec_call = cli.execute_task(InitTask())
        task_id = (await exec_call.invoke()).execution.task_id
        await fault.reached.wait()

        with pytest.raises(CallError):
            await cli.abort_task().invoke()

        fault.release.set()
        await exec_call
        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == OPERATE
    assert is_finished(state, task_id)
    assert not state.execution_state.aborting
    assert not state.execution_state.finished.aborted


@pytest.mark.asyncio
async def test_interrupt_during_settlement_completes_it(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await asyncio.Event().wait()

    @controller.task_handler(ShutdownTask)
    async def run_shutdown(_: ShutdownTask):
        pass

    cli = gated_kit.controller("mycontroller")

    async with asyncio.timeout(1.0), watch_state(cli) as states:
        first = cli.execute_task(InitTask())
        first_id = (await first.invoke()).execution.task_id
        await started.wait()

        fault = gated_backend.arm(finishing_execution)
        abort_call = cli.abort_task()
        await abort_call.invoke()
        await fault.reached.wait()

        # The interrupt arrives while the first execution publishes its final state.
        second = cli.execute_task(ShutdownTask(), interrupt=True)
        accepting = asyncio.create_task(second.invoke())
        fault.release.set()
        second_id = (await accepting).execution.task_id
        await second
        await abort_call.wait()

        with pytest.raises(CallError):
            await first.wait()

        seen = await collect_until(states, lambda state: is_finished(state, second_id))

    # Admission waits for the first execution to settle its own transition.
    assert transition_history(seen, first_id, second_id) == [
        (first_id, TRANSITION, OPERATE),
        (first_id, TRANSITION, OPERATE),
        (first_id, UNKNOWN, None),
        (first_id, TRANSITION, SHUTDOWN),
        (second_id, TRANSITION, SHUTDOWN),
        (second_id, SHUTDOWN, None),
    ]

    interrupted = next(state for state in seen if is_finished(state, first_id))
    assert interrupted.execution_state.finished.aborted


@pytest.mark.asyncio
async def test_failed_start_publication_skips_handler(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        ran.set()

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")
        await cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))

        gated_backend.arm(starting_execution, fail=True)
        exec_call = cli.execute_task(StandbyTask(), context=KeywordDict({"mode": "standby"}))
        task_id = (await exec_call.invoke()).execution.task_id

        with pytest.raises(CallError):
            await exec_call.wait()

        state = await cli.kv_get_model(ControllerState)

    assert not ran.is_set()
    assert state.operating_state.current == ERROR
    assert state.operating_state.target is None
    assert state.operating_state.context == {"mode": "standby"}
    assert is_finished(state, task_id)
    assert state.execution_state.finished.error


@pytest.mark.asyncio
async def test_failed_transition_publication_fails_request(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    ran = asyncio.Event()

    @controller.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        ran.set()

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")
        gated_backend.arm(entering_transition, fail=True)

        with pytest.raises(CallRejected):
            await cli.execute_task(StandbyTask()).invoke()

        # No admitted task owns the unpublished transition, so a reported state replaces it.
        await controller.report_state(STANDBY, KeywordDict())
        reported = await cli.kv_get_model(ControllerState)

    assert not ran.is_set()
    assert reported.operating_state.current == STANDBY
    assert reported.operating_state.target is None
    assert reported.execution_state.execution is None


@pytest.mark.asyncio
async def test_failed_recovery_start_skips_handler(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(RecoverTask)
    async def run_recover(_: RecoverTask):
        ran.set()

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")
        await cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))
        emitted: list[Event] = []

        async with contextlib.aclosing(await cli.monitor_all_events()) as events:
            gated_backend.arm(starting_execution, fail=True)
            exec_call = cli.execute_task(RecoverTask())
            task_id = (await exec_call.invoke()).execution.task_id

            with pytest.raises(CallError):
                await exec_call.wait()

            state = await cli.kv_get_model(ControllerState)

            async for event in events:
                emitted.append(event)

                if isinstance(event, ControllerOperatingState) and event.current == ERROR:
                    break

    # Settlement emits the failure after the execution finishes and before the snapshot.
    *_, finished, settled = emitted
    assert isinstance(finished, TaskExecutionState)
    assert finished.execution.task_id == task_id
    assert finished.finished is not None
    assert settled.event_id == state.operating_state.event_id
    assert settled == state.operating_state

    # Recovery keeps the recorded lifecycle input.
    assert not ran.is_set()
    assert state.operating_state.current == ERROR
    assert state.operating_state.previous == TRANSITION
    assert state.operating_state.target is None
    assert state.operating_state.context == {"mode": "init"}
    assert is_finished(state, task_id)
    assert state.execution_state.finished.error
    assert not state.execution_state.finished.aborted


@pytest.mark.asyncio
async def test_failed_final_publication_fails_request(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")
        gated_backend.arm(finishing_execution, fail=True)
        exec_call = cli.execute_task(InitTask())

        with pytest.raises(CallError):
            await exec_call

        stale = await cli.kv_get_model(ControllerState)

        # No running task owns the stale transition, so a report replaces it.
        await controller.report_state(STANDBY, KeywordDict())
        reported = await cli.kv_get_model(ControllerState)

    assert stale.operating_state.current == TRANSITION
    assert stale.execution_state.executing
    assert reported.operating_state.current == STANDBY
    assert reported.operating_state.target is None


@pytest.mark.asyncio
async def test_failed_success_publication_fails_request(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")
        gated_backend.arm(succeeding_call, fail=True)

        # The call ends without waiting out its progress deadline.
        with pytest.raises(CallError):
            await cli.execute_task(InitTask())


@pytest.mark.asyncio
async def test_failed_progress_publication_fails_request_after_run_ends(gated_kit, gated_backend):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    stopping = asyncio.Event()
    cleaned = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()

        try:
            await asyncio.Event().wait()
        finally:
            stopping.set()
            await cleaned.wait()

    async with asyncio.timeout(1.0):
        cli = gated_kit.controller("mycontroller")
        fault = gated_backend.arm(reporting_progress, fail=True, hold=True)
        exec_call = asyncio.ensure_future(cli.execute_task(InitTask()).run_to_completion())

        await started.wait()
        fault.release.set()
        await stopping.wait()

        # The request stays open while the cancelled run cleans up.
        done, _ = await asyncio.wait((exec_call,), timeout=0.1)
        assert not done

        cleaned.set()

        with pytest.raises(CallError):
            await exec_call


@pytest.mark.asyncio
async def test_failed_startup_recovery_fails_registration(gated_kit, gated_backend):
    cli = gated_kit.controller("mycontroller")
    seeded = seeded_state(
        ControllerOperatingState(current=TRANSITION, target=SHUTDOWN),
        TaskExecutionState(executing=True, execution=seeded_execution()),
    )

    await cli.kv_put_model(seeded)

    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        gated_backend.arm(finishing_execution, fail=True)

        with pytest.raises(BackendError):
            await sc.register_controller("mycontroller")

        state = await cli.kv_get_model(ControllerState)

    assert state.model_dump() == seeded.model_dump()


@pytest.mark.asyncio
@pytest.mark.parametrize("with_id", [True, False], ids=("with_id", "without_id"))
async def test_wait_for_task(kit, with_id):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_task(_: InitTask):
        started.set()
        await can_finish.wait()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.enable()

        # No task is running yet: waiting for an arbitrary id returns None immediately.
        probe_id = uuid.uuid7() if with_id else None
        event = await cli.wait_for_task(task_id=probe_id)
        assert event is None if with_id else not event.executing

        exec_call = cli.execute_task(InitTask())
        # The controller mints the task_id; learn it from the initial response.
        response = await exec_call.invoke()
        task_id = response.execution.task_id
        await started.wait()

        want_id = task_id if with_id else None
        wait_fut = asyncio.create_task(cli.wait_for_task(task_id=want_id))
        await asyncio.sleep(0)
        assert not wait_fut.done()

        can_finish.set()
        event = await wait_fut
        assert event is not None
        assert event.finished is not None
        assert not event.finished.aborted
        assert not event.executing

        if with_id:
            assert event.execution.task_id == task_id

        await exec_call


@pytest.mark.asyncio
async def test_controller_use_device(kit):
    @declare_keyword
    class TestKeyword(BaseModel):
        value: int

    async with asyncio.timeout(2.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")
        # We need an actual device registered in the backend for subscriptions to work
        device_impl = await sc.register_device("camera")

    # 1. Register a device for use within the controller.
    # Returns a client instance for interacting with the registered device.
    camera_client = controller.use_device("camera")
    assert isinstance(camera_client, DeviceClient)
    assert camera_client.entity.path == ("camera",)

    # 2. Access the registered device via get_device(name)
    # Returns a ControllerDevice instance.
    camera_device = controller.get_device("camera")
    assert isinstance(camera_device, ControllerDevice)
    assert camera_device.client is camera_client

    # 3. KeyError: If attempting to access a device via get_device(name) that hasn't been registered.
    with pytest.raises(KeyError):
        controller.get_device("unknown")

    # 4. all_devices() returns all registered devices.
    devices = list(controller.all_devices())
    assert len(devices) == 1
    assert devices[0] is camera_device

    # 5. Register device and subscribe to specific keyword types.
    # Subscriptions remain inactive until start_device_subscriptions() is called.
    controller.use_device("camera", subscribe=[TestKeyword])

    # Check that it's still the same device
    assert controller.get_device("camera") is camera_device

    # 6. Verify inactivity before start_device_subscriptions()
    await device_impl.publish(TestKeyword(value=10))
    await asyncio.sleep(0.05)
    assert camera_device.get(TestKeyword) is None

    await controller.start_device_subscriptions()

    # 7. Access cached subscription values from the ControllerDevice instance.
    # Publish a value from the device implementation
    await device_impl.publish(TestKeyword(value=42))

    # Wait for the update to propagate through the subscription
    for _ in range(10):
        if camera_device.get(TestKeyword) is not None:
            break
        await asyncio.sleep(0.01)

    pointing = camera_device.get(TestKeyword)
    assert pointing is not None
    assert pointing.value == 42

    # Also test __getitem__
    assert camera_device[TestKeyword].value == 42

    # 8. Test multiple use_device calls accumulate subscriptions
    @declare_keyword
    class AnotherKeyword(BaseModel):
        other_value: int

    controller.use_device("camera", subscribe=[AnotherKeyword])
    await device_impl.publish(AnotherKeyword(other_value=100))

    # Wait for the update to propagate through the subscription
    for _ in range(10):
        if camera_device.get(AnotherKeyword) is not None:
            break
        await asyncio.sleep(0.01)

    assert camera_device.get(AnotherKeyword).other_value == 100
    # Original keyword still works
    assert camera_device.get(TestKeyword).value == 42

    # 9. stop_device_subscriptions()
    await controller.stop_device_subscriptions()


# Dispatch ownership


@dataclass
class HeldCleanup:
    """Handler that waits for release before its cancellation cleanup ends."""

    started: asyncio.Event = field(default_factory=asyncio.Event)
    cleaning: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def handle(self, _):
        self.started.set()

        try:
            await asyncio.Event().wait()
        finally:
            self.cleaning.set()
            await self.release.wait()


def collect_run(controller: ControllerImpl) -> TaskRun:
    return TaskRun.create(
        str(controller.entity),
        CollectTask(),
        context=None,
        expiry_time=None,
    )


async def join_runs(runs: list[TaskRun]):
    """Cancel and join every run a test attached."""
    async with asyncio.timeout(1.0):
        for run in runs:
            run.cancel()
            await run.wait()


@pytest.mark.asyncio
async def test_cancel_before_entry_joins_predecessor(kit, controller_impl):
    held = HeldCleanup()
    second_ran = asyncio.Event()
    third_ran = asyncio.Event()
    runs: list[TaskRun] = []

    async def run_second(_):
        second_ran.set()

    async def run_third(_):
        third_ran.set()

    first = collect_run(controller_impl)
    first.attach(held.handle, controller_impl._run_execution(first, None))
    runs.append(first)

    try:
        async with asyncio.timeout(1.0):
            await held.started.wait()

        second = collect_run(controller_impl)
        second.attach(run_second, controller_impl._run_execution(second, first))
        runs.append(second)

        third = collect_run(controller_impl)
        third.attach(run_third, controller_impl._run_execution(third, second))
        runs.append(third)

        # Cancel before the runner takes its first step.
        second.cancel()
        assert second.phase is TaskRunPhase.RECEIVED

        async with asyncio.timeout(1.0):
            await held.cleaning.wait()

        assert second.phase is TaskRunPhase.INTERRUPTING
        assert not second.done()
        assert not third.done()

        held.release.set()

        async with asyncio.timeout(1.0):
            await third.wait()

        state = await kit.controller(controller_impl.entity).kv_get_model(ControllerState)
    finally:
        held.release.set()
        await join_runs(runs)

    assert first.runner.cancelled()
    assert second.runner.cancelled()
    assert second.start_time is None
    assert not second_ran.is_set()
    assert third_ran.is_set()
    assert third.runner.result().task_id == third.execution.task_id
    assert is_finished(state, third.execution.task_id)


@pytest.mark.asyncio
async def test_repeated_cancellation_requests_once(controller_impl):
    held = HeldCleanup()
    runs: list[TaskRun] = []

    async def run_collect(_):
        pass

    first = collect_run(controller_impl)
    first.attach(held.handle, controller_impl._run_execution(first, None))
    runs.append(first)

    try:
        async with asyncio.timeout(1.0):
            await held.started.wait()

        second = collect_run(controller_impl)
        second.attach(run_collect, controller_impl._run_execution(second, first))
        runs.append(second)

        async with asyncio.timeout(1.0):
            await held.cleaning.wait()

        # The successor already canceled its predecessor.
        first.cancel()
        second.cancel()
        second.cancel()

        assert first.runner.cancelling() == 1
        assert second.runner.cancelling() == 1

        held.release.set()

        async with asyncio.timeout(1.0):
            await asyncio.gather(first.wait(), second.wait())
    finally:
        held.release.set()
        await join_runs(runs)

    assert second.runner.cancelled()
    assert second.start_time is None


@pytest.mark.asyncio
async def test_predecessor_failure_skips_successor(kit, controller_impl):
    started = asyncio.Event()
    ran = asyncio.Event()
    runs: list[TaskRun] = []

    async def fail_on_cancel(_):
        started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("cleanup failed") from None

    async def run_collect(_):
        ran.set()

    first = collect_run(controller_impl)
    first.attach(fail_on_cancel, controller_impl._run_execution(first, None))
    runs.append(first)

    try:
        async with asyncio.timeout(1.0):
            await started.wait()

        second = collect_run(controller_impl)
        second.attach(run_collect, controller_impl._run_execution(second, first))
        runs.append(second)

        async with asyncio.timeout(1.0):
            await second.wait()

        state = await kit.controller(controller_impl.entity).kv_get_model(ControllerState)
    finally:
        await join_runs(runs)

    assert isinstance(first.runner.exception(), RuntimeError)
    assert isinstance(second.runner.exception(), RuntimeError)
    assert second.start_time is None
    assert not ran.is_set()

    # The failed predecessor's settlement remains the published execution.
    assert is_finished(state, first.execution.task_id)
    assert state.execution_state.finished.error


@pytest.mark.asyncio
async def test_disable_during_cleanup_skips_successor_handler(kit, controller_impl):
    held = HeldCleanup()
    successor_ran = asyncio.Event()

    @controller_impl.task_handler(CollectTask)
    async def run_collect(_: CollectTask):
        if held.started.is_set():
            successor_ran.set()
        else:
            await held.handle(None)

    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0):
            first = cli.execute_task(CollectTask())
            await first.invoke()
            await held.started.wait()

            second = cli.execute_task(CollectTask(), interrupt=True)
            second_id = (await second.invoke()).execution.task_id
            await held.cleaning.wait()
            await cli.disable()
            held.release.set()

            with pytest.raises(CallError):
                await first.wait()

            with pytest.raises(CallError):
                await second.wait()

            state = await cli.kv_get_model(ControllerState)
    finally:
        held.release.set()

    assert not successor_ran.is_set()
    assert not state.enable_state.enabled
    assert is_finished(state, second_id)
    assert state.execution_state.finished.error == "Controller is disabled"
    assert not state.execution_state.finished.aborted


@pytest.mark.asyncio
async def test_abort_pending_execution_joins_predecessor(kit, controller_impl):
    held = HeldCleanup()
    shutdown_ran = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller_impl.task_handler(ShutdownTask)
    async def run_shutdown(_: ShutdownTask):
        shutdown_ran.set()

    controller_impl.task_handler(CollectTask)(held.handle)
    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0), watch_state(cli) as states:
            await cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))
            settled = await cli.kv_get_model(ControllerState)

            collect = cli.execute_task(CollectTask())
            collect_id = (await collect.invoke()).execution.task_id
            await held.started.wait()

            shutdown = cli.execute_task(ShutdownTask(), interrupt=True)
            shutdown_id = (await shutdown.invoke()).execution.task_id
            await held.cleaning.wait()

            abort = cli.abort_task(shutdown_id)
            response = await abort.invoke()
            held.release.set()
            await abort
            aborted = await cli.kv_get_model(ControllerState)

            with pytest.raises(CallError):
                await shutdown.wait()

            with pytest.raises(CallError):
                await collect.wait()

            seen = await collect_until(states, lambda state: is_finished(state, collect_id))
            state = await cli.kv_get_model(ControllerState)
    finally:
        held.release.set()

    assert response.aborting
    assert response.task_id == shutdown_id
    assert not shutdown_ran.is_set()

    # The pending shutdown published its transition but no execution of its own.
    assert all(execution_id(snapshot) != shutdown_id for snapshot in seen)
    assert settled.operating_state.current == OPERATE

    # The interrupted collect left the inferred state, settled before the abort succeeded.
    assert aborted.operating_state == state.operating_state
    assert state.operating_state.current == OPERATE
    assert state.operating_state.previous == TRANSITION
    assert state.operating_state.target is None
    assert state.operating_state.context == {"mode": "init"}
    assert is_finished(state, collect_id)
    assert state.execution_state.finished.aborted
    assert not state.execution_state.aborting


@pytest.mark.asyncio
async def test_lifecycle_transition_precedes_interrupted_cleanup(kit, controller_impl):
    held = HeldCleanup()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    controller_impl.task_handler(CollectTask)(held.handle)
    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0):
            await controller_impl.report_state(STANDBY, KeywordDict())

            collect = cli.execute_task(CollectTask())
            collect_id = (await collect.invoke()).execution.task_id
            await held.started.wait()

            init = cli.execute_task(
                InitTask(), context=KeywordDict({"mode": "init"}), interrupt=True
            )
            await init.invoke()
            await held.cleaning.wait()
            stopping = await cli.kv_get_model(ControllerState)

            # Reporting the target beneath the pending transition confirms it at the end.
            await controller_impl.report_state(OPERATE, KeywordDict({"mode": "init"}))
            reported = await cli.kv_get_model(ControllerState)

            held.release.set()
            await init

            with pytest.raises(CallError):
                await collect.wait()

            settled = await cli.kv_get_model(ControllerState)
    finally:
        held.release.set()

    assert stopping.operating_state.current == TRANSITION
    assert stopping.operating_state.previous == STANDBY
    assert stopping.operating_state.target == OPERATE
    assert stopping.operating_state.context == {"mode": "init"}
    assert execution_id(stopping) == collect_id
    assert stopping.execution_state.executing
    assert reported.operating_state.current == TRANSITION
    assert reported.operating_state.target == OPERATE

    assert settled.operating_state.current == OPERATE
    assert settled.operating_state.target is None
    assert settled.operating_state.context == {"mode": "init"}


@pytest.mark.asyncio
async def test_interrupting_requests_join_in_admission_order(kit, controller_impl):
    held = HeldCleanup()
    entered: list[str] = []
    can_finish = asyncio.Event()

    controller_impl.task_handler(CollectTask)(held.handle)

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        entered.append("init")

    @controller_impl.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        entered.append("standby")
        await can_finish.wait()

    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0), watch_state(cli) as states:
            collect = cli.execute_task(CollectTask())
            collect_id = (await collect.invoke()).execution.task_id
            await held.started.wait()

            init = cli.execute_task(InitTask(), interrupt=True)
            init_id = (await init.invoke()).execution.task_id
            await held.cleaning.wait()

            standby = cli.execute_task(StandbyTask(), interrupt=True)
            standby_id = (await standby.invoke()).execution.task_id

            # A pending replacement still occupies the controller.
            with pytest.raises(CallError):
                await cli.execute_task(CollectTask()).invoke()

            assert entered == []
            held.release.set()

            with pytest.raises(CallError):
                await init.wait()

            with pytest.raises(CallError):
                await collect.wait()

            # Superseded requests leave the newest execution current.
            seen = await collect_until(
                states,
                lambda state: (
                    execution_id(state) == standby_id and state.execution_state.executing
                ),
            )
            assert controller_impl.task_running()

            with pytest.raises(CallError):
                await cli.abort_task(init_id).invoke()

            can_finish.set()
            await standby
            seen += await collect_until(states, lambda state: is_finished(state, standby_id))
    finally:
        held.release.set()
        can_finish.set()

    assert entered == ["standby"]
    assert transition_history(seen, collect_id, init_id, standby_id) == [
        (collect_id, UNKNOWN, None),
        (collect_id, TRANSITION, OPERATE),
        (collect_id, TRANSITION, STANDBY),
        (collect_id, TRANSITION, STANDBY),
        (standby_id, TRANSITION, STANDBY),
        (standby_id, STANDBY, None),
    ]


@pytest.mark.asyncio
async def test_rejected_requests_leave_execution_running(kit, controller_impl):
    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller_impl.task_handler(CollectTask)
    async def run_collect(_: CollectTask):
        started.set()
        await can_finish.wait()

    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0):
            collect = cli.execute_task(CollectTask())
            await collect.invoke()
            await started.wait()
            running = await cli.kv_get_model(ControllerState)

            with pytest.raises(CallError):
                await cli.execute_task(RecoverTask(), interrupt=True).invoke()

            await cli.disable()

            with pytest.raises(CallError):
                await cli.execute_task(CollectTask(), interrupt=True).invoke()

            rejected = await cli.kv_get_model(ControllerState)
            can_finish.set()
            await collect
    finally:
        can_finish.set()

    assert rejected.operating_state == running.operating_state
    assert rejected.execution_state == running.execution_state


@pytest.mark.asyncio
async def test_non_lifecycle_interrupt_rejected_between_states(kit, controller_impl):
    started = asyncio.Event()
    can_finish = asyncio.Event()
    collected = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await can_finish.wait()

    @controller_impl.task_handler(CollectTask)
    async def run_collect(_: CollectTask):
        collected.set()

    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0):
            init = cli.execute_task(InitTask())
            await init.invoke()
            await started.wait()
            transition = await cli.kv_get_model(ControllerState)

            with pytest.raises(CallRejected):
                await cli.execute_task(CollectTask(), interrupt=True).invoke()

            rejected = await cli.kv_get_model(ControllerState)
            can_finish.set()
            await init

            # The same task is accepted once the controller settles.
            await cli.execute_task(CollectTask())
    finally:
        can_finish.set()

    assert transition.operating_state.current == TRANSITION
    assert rejected.operating_state == transition.operating_state
    assert rejected.execution_state == transition.execution_state
    assert collected.is_set()


@pytest.mark.asyncio
async def test_abort_rejects_other_or_finished_execution(kit, controller_impl):
    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await can_finish.wait()

    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0):
            init = cli.execute_task(InitTask())
            task_id = (await init.invoke()).execution.task_id
            await started.wait()
            running = await cli.kv_get_model(ControllerState)

            with pytest.raises(CallError):
                await cli.abort_task(uuid.uuid7()).invoke()

            mismatched = await cli.kv_get_model(ControllerState)
            can_finish.set()
            await init
            settled = await cli.kv_get_model(ControllerState)

            with pytest.raises(CallError):
                await cli.abort_task(task_id).invoke()

            finished = await cli.kv_get_model(ControllerState)
    finally:
        can_finish.set()

    assert mismatched.model_dump() == running.model_dump()
    assert finished.model_dump() == settled.model_dump()
    assert settled.operating_state.current == OPERATE


@pytest.mark.asyncio
async def test_lifecycle_interrupt_reports_its_context(kit, controller_impl):
    started = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await asyncio.Event().wait()

    @controller_impl.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        pass

    cli = kit.controller(controller_impl.entity)

    async with asyncio.timeout(2.0), watch_state(cli) as states:
        init = cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))
        init_id = (await init.invoke()).execution.task_id
        await started.wait()

        standby = cli.execute_task(
            StandbyTask(),
            context=KeywordDict({"mode": "standby"}),
            interrupt=True,
        )
        standby_id = (await standby.invoke()).execution.task_id
        await standby

        with pytest.raises(CallError):
            await init.wait()

        seen = await collect_until(states, lambda state: is_finished(state, standby_id))

    history = [
        (execution_id(state), state.operating_state.current, state.operating_state.context)
        for state in seen
        if execution_id(state) in (init_id, standby_id)
    ]

    assert history == [
        (init_id, TRANSITION, {"mode": "init"}),
        (init_id, TRANSITION, {"mode": "standby"}),
        (init_id, TRANSITION, {"mode": "standby"}),
        (standby_id, TRANSITION, {"mode": "standby"}),
        (standby_id, STANDBY, {"mode": "standby"}),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["fail", "cancel", "cancel_twice"])
async def test_unpublished_transition_leaves_predecessor_running(
    gated_kit, gated_backend, outcome
):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        started.set()
        await can_finish.wait()

    @controller.task_handler(ShutdownTask)
    async def run_shutdown(_: ShutdownTask):
        shutdown_ran.set()

    cli = gated_kit.controller("mycontroller")
    request = ExecuteRequestMessage(
        task=ShutdownTask(), context=KeywordDict({"mode": "shutdown"}), interrupt=True
    )

    try:
        async with asyncio.timeout(1.0):
            init = cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))
            await init.invoke()
            await started.wait()
            before = await cli.kv_get_model(ControllerState)

            match outcome:
                case "fail":
                    gated_backend.arm(entering_transition, fail=True)

                    with pytest.raises(CallRejected):
                        await cli.execute_task(request.task, interrupt=True).invoke()
                case _:
                    fault = gated_backend.arm(entering_transition)
                    call = CallContext(uuid.uuid7(), gated_kit.backend.stream(cli.entity))
                    admitting = asyncio.create_task(controller._execute_request(request, call))
                    await fault.reached.wait()

                    # Cancellation waits for the transition publication to finish.
                    for _ in range(1 if outcome == "cancel" else 2):
                        admitting.cancel()
                        await asyncio.sleep(0)

                    fault.release.set()

                    with pytest.raises(asyncio.CancelledError):
                        await admitting

                    assert call.reply.result().state == "rejected"

            restored = await cli.kv_get_model(ControllerState)
            can_finish.set()
            await init
            settled = await cli.kv_get_model(ControllerState)
    finally:
        can_finish.set()

    assert not shutdown_ran.is_set()

    # Only the event identity differs from the predecessor's transition.
    assert restored.operating_state.model_dump(exclude={"event_id"}) == (
        before.operating_state.model_dump(exclude={"event_id"})
    )

    assert settled.operating_state.current == OPERATE
    assert settled.operating_state.context == {"mode": "init"}


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", ["succeed", "fail"])
async def test_lifecycle_task_takes_over_recovery(kit, controller_impl, cleanup):
    started = asyncio.Event()
    release = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller_impl.task_handler(RecoverTask)
    async def run_recover(_: RecoverTask):
        started.set()

        try:
            await asyncio.Event().wait()
        finally:
            await release.wait()

            if cleanup == "fail":
                raise RuntimeError("mount unreachable")

    @controller_impl.task_handler(ShutdownTask)
    async def run_shutdown(_: ShutdownTask):
        pass

    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0), watch_state(cli) as states:
            await cli.execute_task(InitTask())
            recover = cli.execute_task(RecoverTask())
            await recover.invoke()
            await started.wait()

            shutdown = cli.execute_task(ShutdownTask(), interrupt=True)
            await shutdown.invoke()
            release.set()

            with contextlib.suppress(CallError):
                await shutdown

            seen = await collect_until(
                states,
                lambda state: state.operating_state.current in (SHUTDOWN, ERROR),
            )
    finally:
        release.set()

    # Recovery cleanup publishes no state of its own after the shutdown transition.
    history = [(state.operating_state.current, state.operating_state.target) for state in seen]
    handoff = history.index((TRANSITION, SHUTDOWN))
    *pending, final = history[handoff:]

    assert set(pending) == {(TRANSITION, SHUTDOWN)}
    assert final == ((SHUTDOWN, None) if cleanup == "succeed" else (ERROR, None))
    assert (TRANSITION, None) in history[:handoff]


# Observed states and latches


@pytest.mark.asyncio
async def test_observed_state_confirms_lifecycle_target(kit, controller_impl):
    returned = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        returned.set()

    cli = kit.controller(controller_impl.entity)

    async with asyncio.timeout(2.0):
        await controller_impl.report_state(STANDBY, KeywordDict())

        init = cli.execute_task(InitTask())
        await init.invoke()
        await returned.wait()
        waiting = await cli.kv_get_model(ControllerState)

        await controller_impl.report_state(OPERATE, KeywordDict())
        await init
        settled = await cli.kv_get_model(ControllerState)

    assert waiting.operating_state.current == TRANSITION
    assert waiting.execution_state.executing
    assert settled.operating_state.current == OPERATE
    assert settled.operating_state.previous == TRANSITION
    assert not settled.execution_state.finished.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "report, reason",
    [
        ((STANDBY, KeywordDict()), "no longer"),
        ((ERROR,), "before settling"),
        ((UNKNOWN,), "before settling"),
    ],
)
async def test_report_before_settlement_fails_lifecycle_task(
    gated_kit, gated_backend, report, reason
):
    async with asyncio.timeout(1.0):
        sc = await gated_kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    returned = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        returned.set()

    cli = gated_kit.controller("mycontroller")

    async with asyncio.timeout(2.0):
        await controller.report_state(STANDBY, KeywordDict())

        init = cli.execute_task(InitTask())
        init_id = (await init.invoke()).execution.task_id
        await returned.wait()

        # A held context update keeps the update lock while two state reports queue.
        fault = gated_backend.arm(starting_execution)
        held = asyncio.create_task(controller.update_context())
        await fault.reached.wait()

        confirming = asyncio.create_task(controller.report_state(OPERATE, KeywordDict()))
        await asyncio.sleep(0)
        contradicting = asyncio.create_task(controller.report_state(*report))
        await asyncio.sleep(0)

        # The confirmed wait ends, but settlement waits behind the contradicting report.
        fault.release.set()
        await asyncio.gather(held, confirming, contradicting)

        with pytest.raises(CallError):
            await init

        failed = await cli.kv_get_model(ControllerState)

    assert failed.operating_state.current == ERROR
    assert failed.operating_state.target is None
    assert is_finished(failed, init_id)
    assert reason in failed.execution_state.finished.error


@pytest.mark.asyncio
async def test_unconfirmed_lifecycle_target_fails(kit, controller_impl):
    controller_impl.confirmation_timeout = 0.05

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    cli = kit.controller(controller_impl.entity)

    async with asyncio.timeout(2.0):
        await controller_impl.report_state(STANDBY, KeywordDict())

        with pytest.raises(CallError):
            await cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))

        failed = await cli.kv_get_model(ControllerState)

    assert failed.operating_state.current == ERROR
    assert failed.operating_state.target is None
    assert failed.operating_state.context == {"mode": "init"}
    assert "did not report" in failed.execution_state.finished.error


@pytest.mark.asyncio
@pytest.mark.parametrize("latch", [UNKNOWN, ERROR])
async def test_latch_during_confirmation_fails_lifecycle_task(kit, controller_impl, latch):
    returned = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        returned.set()

    cli = kit.controller(controller_impl.entity)

    # The task fails well before the confirmation timeout.
    async with asyncio.timeout(1.0):
        await controller_impl.report_state(STANDBY, KeywordDict())

        init = cli.execute_task(InitTask())
        await init.invoke()
        await returned.wait()
        await controller_impl.report_state(latch)

        with pytest.raises(CallError):
            await init

        failed = await cli.kv_get_model(ControllerState)

    assert failed.operating_state.current == ERROR
    assert f"reported {latch}" in failed.execution_state.finished.error


@pytest.mark.asyncio
@pytest.mark.parametrize("latch", [UNKNOWN, ERROR])
async def test_reporting_unknown_or_error_keeps_inference(kit, controller_impl, latch):
    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    cli = kit.controller(controller_impl.entity)

    async with asyncio.timeout(2.0):
        await controller_impl.report_state(latch)
        latched = await cli.kv_get_model(ControllerState)

        await cli.execute_task(InitTask(), context=KeywordDict({"mode": "init"}))
        settled = await cli.kv_get_model(ControllerState)

    assert latched.operating_state.current == latch
    assert settled.operating_state.current == OPERATE
    assert settled.operating_state.context == {"mode": "init"}


@pytest.mark.asyncio
@pytest.mark.parametrize(("outcome", "latch"), [("fail", ERROR), ("abort", UNKNOWN)])
async def test_latch_outlasts_observed_states_until_success(kit, controller_impl, outcome, latch):
    started = asyncio.Event()

    @controller_impl.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        started.set()

        if outcome == "fail":
            raise RuntimeError("enclosure fault")

        await asyncio.Event().wait()

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    cli = kit.controller(controller_impl.entity)

    async with asyncio.timeout(2.0):
        await controller_impl.report_state(OPERATE, KeywordDict())

        standby = cli.execute_task(StandbyTask())
        await standby.invoke()
        await started.wait()

        if outcome == "abort":
            await cli.abort_task()

        with pytest.raises(CallError):
            await standby.wait()

        # Reporting a known state cannot clear the latch.
        await controller_impl.report_state(OPERATE, KeywordDict())
        latched = await cli.kv_get_model(ControllerState)

        await cli.execute_task(InitTask())
        settled = await cli.kv_get_model(ControllerState)

    assert latched.operating_state.current == latch
    assert settled.operating_state.current == OPERATE
    assert settled.operating_state.previous == TRANSITION


@pytest.mark.asyncio
async def test_pending_lifecycle_abort_keeps_latch(kit, controller_impl):
    held = HeldCleanup()

    @controller_impl.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        raise RuntimeError("enclosure fault")

    @controller_impl.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    controller_impl.task_handler(CollectTask)(held.handle)
    cli = kit.controller(controller_impl.entity)

    try:
        async with asyncio.timeout(2.0):
            await controller_impl.report_state(STANDBY, KeywordDict())

            with pytest.raises(CallError):
                await cli.execute_task(StandbyTask())

            collect = cli.execute_task(CollectTask())
            await collect.invoke()
            await held.started.wait()

            init = cli.execute_task(InitTask(), interrupt=True)
            init_id = (await init.invoke()).execution.task_id
            await held.cleaning.wait()

            # The init never starts, so it cannot clear the latch.
            abort = cli.abort_task(init_id)
            await abort.invoke()
            held.release.set()
            await abort

            with pytest.raises(CallError):
                await init.wait()

            with pytest.raises(CallError):
                await collect.wait()

            state = await cli.kv_get_model(ControllerState)
    finally:
        held.release.set()

    assert state.operating_state.current == ERROR
    assert state.operating_state.target is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "before", "after"),
    [("succeed", OPERATE, STANDBY), ("fail", ERROR, UNKNOWN)],
)
async def test_restart_latches_unsettled_state(kit, outcome, before, after):
    cli = kit.controller("mycontroller")
    reported = KeywordDict({"mode": "standby"})

    async with asyncio.timeout(2.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

        @controller.task_handler(InitTask)
        async def run_init(_: InitTask):
            if outcome == "fail":
                raise RuntimeError("enclosure fault")

        with contextlib.suppress(CallError):
            await cli.execute_task(InitTask())

        stopped = await cli.kv_get_model(ControllerState)
        await sc.shutdown()

        restarted_kit = SensorKit(Backend(impl=kit.backend.impl))
        sc = await restarted_kit.register_service("testservice", "0.1.0")
        impl = ControllerImpl.for_service_context(sc, "mycontroller")

        # A restart is not a successful command, so a reported state cannot clear the latch.
        @impl.on_attach
        async def determine():
            await impl.report_state(STANDBY, reported)

        await sc.register_impl(impl)
        restarted = await cli.kv_get_model(ControllerState)
        await sc.shutdown()

    assert stopped.operating_state.current == before
    assert restarted.operating_state.current == after
    assert restarted.operating_state.context == (
        reported if outcome == "succeed" else KeywordDict()
    )


# Task handler forms


@pytest.mark.asyncio
async def test_task_handlers_receive_declared_input(kit, controller_impl):
    received: dict[str, Any] = {}

    async def run_init(execution: TaskExecution[InitTask]):
        received["init"] = execution

    async def run_standby(task: StandbyTask):
        received["standby"] = task

    @controller_impl.task_handler(CollectTask)
    async def run_collect(task: CollectTask):
        received["collect"] = task

    controller_impl.add_task_handler(InitTask, execution_callback=run_init)
    controller_impl.add_task_handler(StandbyTask, task_callback=run_standby)
    cli = kit.controller(controller_impl.entity)

    async with asyncio.timeout(2.0):
        init = cli.execute_task(InitTask(), context=KeywordDict({"probe": "value"}))
        init_id = (await init.invoke()).execution.task_id
        await init

        await cli.execute_task(StandbyTask())
        await cli.execute_task(CollectTask())

    assert type(received["init"]) is TaskExecution
    assert type(received["init"].task) is InitTask
    assert received["init"].task_id == init_id
    assert received["init"].context["probe"] == "value"

    assert type(received["standby"]) is StandbyTask
    assert type(received["collect"]) is CollectTask


@pytest.mark.asyncio
async def test_add_task_handler_takes_one_callback(controller_impl):
    async def run_init(task: InitTask):
        pass

    async def run_init_execution(execution: TaskExecution[InitTask]):
        pass

    with pytest.raises(TypeError):
        controller_impl.add_task_handler(InitTask)

    with pytest.raises(TypeError):
        controller_impl.add_task_handler(
            InitTask,
            task_callback=run_init,
            execution_callback=run_init_execution,
        )

    controller_impl.add_task_handler(InitTask, task_callback=run_init)

    with pytest.raises(RuntimeError):
        controller_impl.add_task_handler(InitTask, execution_callback=run_init_execution)

    with pytest.raises(RuntimeError):
        controller_impl.task_handler(InitTask)(run_init)


# Legacy state migration: versions before the Task/TaskExecution split stored the executing
# task as a flat ControllerTask on `execution_state.execution`. A `mode="before"` validator on
# TaskExecutionState upgrades that shape so old state from a NATS broker still loads.
_LEGACY_TASK = {
    "task_type": "init",
    "task_id": "0192c3f4-5678-7abc-8def-0123456789ab",
    "controller_id": "legacy-controller",
    "context": None,
    "end_time": "2026-01-01T00:00:00Z",
}
_LEGACY_TASK_ID = uuid.UUID(_LEGACY_TASK["task_id"])
_LEGACY_EXPIRY = datetime(2026, 1, 1, tzinfo=UTC)


def test_legacy_task_execution_state_migrates_flat_task():
    """A legacy flat task on a TaskExecutionState is wrapped into a TaskExecution envelope."""
    state = TaskExecutionState.model_validate(
        {"executing": True, "task": dict(_LEGACY_TASK)}
    )

    assert isinstance(state.execution, TaskExecution)
    assert isinstance(state.execution.task, InitTask)
    assert state.execution.task_id == _LEGACY_TASK_ID
    assert state.execution.controller_id == "legacy-controller"
    # The legacy deadline becomes the envelope expiry; identity is lifted off the task.
    assert state.execution.expiry_time == _LEGACY_EXPIRY


def test_legacy_controller_state_loads_from_kv_json():
    """A full ControllerState snapshot persisted in the old shape deserialises without error."""
    legacy = {
        "enable_state": {"enabled": True},
        "operating_state": {"current": "operate"},
        "execution_state": {"executing": True, "task": dict(_LEGACY_TASK)},
    }

    state = ControllerState.model_validate_json(json.dumps(legacy))

    assert isinstance(state.execution_state.execution, TaskExecution)
    assert isinstance(state.execution_state.execution.task, InitTask)
    assert state.execution_state.execution.task_id == _LEGACY_TASK_ID
    assert state.execution_state.execution.expiry_time == _LEGACY_EXPIRY


def test_legacy_task_execution_event_replays_through_registry():
    """A legacy event on the durable stream is upgraded when narrowed via the Event registry."""
    payload = {
        "event_model": "TaskExecutionState",
        "executing": True,
        "task": dict(_LEGACY_TASK),
    }

    event = Event.model_validate_json(json.dumps(payload))

    assert isinstance(event, TaskExecutionState)
    assert isinstance(event.execution, TaskExecution)
    assert isinstance(event.execution.task, InitTask)
    assert event.execution.task_id == _LEGACY_TASK_ID


def test_current_task_execution_state_roundtrips_unchanged():
    """Current-shape state passes through the migration validator as a no-op."""
    execution = TaskExecution(
        task=InitTask(), task_id=uuid.uuid7(), controller_id="ctl"
    )
    state = TaskExecutionState(executing=True, execution=execution)

    reloaded = TaskExecutionState.model_validate(state.model_dump())

    assert isinstance(reloaded.execution, TaskExecution)
    assert isinstance(reloaded.execution.task, InitTask)
    assert reloaded.execution.task_id == execution.task_id


def test_pre_rename_task_envelope_migrates_to_execution():
    """State persisted after the split but before `task` was renamed loads onto `execution`."""
    execution = TaskExecution(
        task=InitTask(), task_id=uuid.uuid7(), controller_id="ctl"
    )
    legacy = {"executing": True, "task": execution.model_dump()}

    state = TaskExecutionState.model_validate(legacy)

    assert isinstance(state.execution, TaskExecution)
    assert isinstance(state.execution.task, InitTask)
    assert state.execution.task_id == execution.task_id
