# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
import pytest_asyncio
import uuid_utils.compat as uuid
from pydantic import BaseModel, Field

from sensorkit.backend.event import Event
from sensorkit.backend.request import CallError
from sensorkit.common.keyword import KeywordDict, declare_keyword
from sensorkit.core.controller import (
    ControllerEnableState,
    ControllerOperatingState,
    ControllerState,
    InternalControllerState,
    TaskExecutionState,
)
from sensorkit.core.task import (
    CollectTask,
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
    TaskExecution,
    TaskInfo,
)

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY
SHUTDOWN = InternalControllerState.SHUTDOWN
ERROR = InternalControllerState.ERROR
UNKNOWN = InternalControllerState.UNKNOWN
TRANSITION = InternalControllerState.TRANSITION


@declare_keyword
class EnclosureOption(BaseModel):
    """Lifecycle input with a nested value."""

    open: bool
    limits: list[float] = Field(default_factory=list)


def option_context(*, open: bool) -> KeywordDict:
    return KeywordDict(EnclosureOption(open=open, limits=[10.0, 80.0]))


def lifecycle_input(kind: str) -> KeywordDict | None:
    match kind:
        case "omitted":
            return None
        case "empty":
            return KeywordDict()
        case "nested":
            context = option_context(open=False)
            context.set_value("offsets", [[1, 2], [3, 4]])
            return context

    raise ValueError(kind)


@pytest_asyncio.fixture
async def controller_service(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    yield sc

    async with asyncio.timeout(1.0):
        await sc.shutdown()


@pytest_asyncio.fixture
async def controller(controller_service):
    async with asyncio.timeout(1.0):
        return await controller_service.register_controller("mycontroller")


async def operating_state(cli) -> ControllerOperatingState:
    return (await cli.kv_get_model(ControllerState)).operating_state


def seeded_execution() -> TaskExecution:
    return TaskExecution(task=ShutdownTask(), task_id=uuid.uuid7(), controller_id="mycontroller")


# Lifecycle publication


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["omitted", "empty", "nested"])
async def test_lifecycle_success_records_input(kit, controller, kind):
    started = asyncio.Event()
    can_finish = asyncio.Event()

    async def run_init(execution: TaskExecution[InitTask]):
        # Changes to the execution context stay out of the recorded input.
        context = execution.context

        if option := context.get(EnclosureOption):
            option.limits.append(90.0)

        context.set_value("offsets", None)
        await controller.update_context(frame=1)

        started.set()
        await can_finish.wait()

    controller.add_task_handler(InitTask, execution_callback=run_init)

    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0):
        exec_call = cli.execute_task(InitTask(), context=lifecycle_input(kind))
        await exec_call.invoke()
        await started.wait()

        running = await cli.kv_get_model(ControllerState)
        can_finish.set()
        await exec_call
        settled = await operating_state(cli)

    expected = lifecycle_input(kind) or KeywordDict()

    assert running.operating_state.current == TRANSITION
    assert running.operating_state.context == expected
    assert running.execution_state.context["frame"] == 1

    assert settled.current == OPERATE
    assert settled.context == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected", "error"),
    [
        ("fail", ERROR, "enclosure fault"),
        ("fail_silently", ERROR, "RuntimeError"),
        ("abort", UNKNOWN, False),
    ],
)
async def test_lifecycle_failure_retains_attempted_input(
    kit, controller, outcome, expected, error
):
    started = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(StandbyTask)
    async def run_standby(_: StandbyTask):
        started.set()

        match outcome:
            case "fail":
                raise RuntimeError("enclosure fault")
            case "fail_silently":
                raise RuntimeError()
            case "abort":
                await asyncio.Event().wait()

    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0):
        await cli.execute_task(InitTask(), context=option_context(open=True))

        exec_call = cli.execute_task(StandbyTask(), context=option_context(open=False))
        await exec_call.invoke()
        await started.wait()

        if outcome == "abort":
            await cli.abort_task()

        with pytest.raises(CallError):
            await exec_call.wait()

        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == expected
    assert state.operating_state.previous == TRANSITION
    assert state.operating_state.target is None
    assert state.operating_state.context == option_context(open=False)
    assert state.execution_state.finished.error == error
    assert state.execution_state.finished.aborted == (outcome == "abort")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["succeed", "fail", "abort"])
async def test_non_lifecycle_task_preserves_lifecycle_input(kit, controller, outcome):
    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(CollectTask)
    async def run_collect(_: CollectTask):
        await controller.update_context(frame=1)
        started.set()

        match outcome:
            case "succeed":
                await can_finish.wait()
            case "fail":
                await can_finish.wait()
                raise RuntimeError("camera fault")
            case "abort":
                await asyncio.Event().wait()

    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0):
        await cli.execute_task(InitTask(), context=option_context(open=True))

        exec_call = cli.execute_task(CollectTask(), context=KeywordDict({"target": "M31"}))
        await exec_call.invoke()
        await started.wait()

        running = await operating_state(cli)
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

        finished = await operating_state(cli)

    for state in (running, finished):
        assert state.current == OPERATE
        assert state.context == option_context(open=True)


@pytest.mark.asyncio
async def test_reported_target_confirms_task_input(kit, controller):
    async def run_init(execution: TaskExecution[InitTask]):
        # Working context edits stay out of the lifecycle input.
        execution.context.get(EnclosureOption).limits.append(90.0)
        execution.context.set_value("offsets", None)
        await controller.update_context(frame=1)
        await controller.report_state(OPERATE, option_context(open=True))

    controller.add_task_handler(InitTask, execution_callback=run_init)
    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0):
        # Once a known state is reported, the observed state must confirm each lifecycle task.
        await controller.report_state(STANDBY, KeywordDict())
        await cli.execute_task(InitTask(), context=option_context(open=True))
        settled = await operating_state(cli)

    assert settled.current == OPERATE
    assert settled.target is None
    assert settled.context == option_context(open=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["omitted", "empty", "nested"])
async def test_handler_reports_input_after_working_context_changes(kit, controller, kind):
    started = asyncio.Event()
    can_finish = asyncio.Event()
    received: list[TaskExecution] = []

    async def run_init(execution: TaskExecution[InitTask]):
        received.append(execution)

        if option := execution.context.get(EnclosureOption):
            option.limits.append(90.0)

        execution.context.pop("offsets", None)
        await controller.update_context(frame=1)
        await controller.report_state(OPERATE, execution.submitted_context)
        started.set()
        await can_finish.wait()

    controller.add_task_handler(InitTask, execution_callback=run_init)
    cli = kit.controller("mycontroller")
    expected = lifecycle_input(kind) or KeywordDict()

    try:
        async with asyncio.timeout(2.0):
            await controller.report_state(STANDBY, KeywordDict())
            call = cli.execute_task(InitTask(), context=lifecycle_input(kind))
            accepted = (await call.invoke()).execution
            await started.wait()
            running = await cli.kv_get_model(ControllerState)
            can_finish.set()
            await call
            settled = await cli.kv_get_model(ControllerState)
    finally:
        can_finish.set()

    assert received[0].submitted_context == expected
    assert received[0].submitted_context.get(TaskInfo) is None
    assert received[0].context.get(TaskInfo) is not None
    assert accepted.submitted_context == expected
    assert running.execution_state.execution.submitted_context == expected
    assert settled.execution_state.execution.submitted_context == expected
    assert running.execution_state.context["frame"] == 1
    assert settled.operating_state.current == OPERATE
    assert settled.operating_state.context == expected


@pytest.mark.asyncio
async def test_handler_input_edits_do_not_change_confirmation_input(kit, controller):
    async def run_init(execution: TaskExecution[InitTask]):
        execution.submitted_context.get(EnclosureOption).limits.append(90.0)
        await controller.report_state(OPERATE, execution.submitted_context)

    controller.add_task_handler(InitTask, execution_callback=run_init)
    controller.confirmation_timeout = 0.05
    cli = kit.controller("mycontroller")

    async with asyncio.timeout(2.0):
        await controller.report_state(STANDBY, KeywordDict())

        with pytest.raises(CallError):
            await cli.execute_task(InitTask(), context=option_context(open=True))

        failed = await cli.kv_get_model(ControllerState)

    assert failed.operating_state.current == ERROR
    assert failed.operating_state.context == option_context(open=True)
    assert "did not report" in failed.execution_state.finished.error


@pytest.mark.asyncio
async def test_reported_input_must_match_task_input(kit, controller):
    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        await controller.report_state(OPERATE, option_context(open=False))

    controller.confirmation_timeout = 0.05
    cli = kit.controller("mycontroller")

    async with asyncio.timeout(1.0):
        await controller.report_state(STANDBY, KeywordDict())

        with pytest.raises(CallError):
            await cli.execute_task(InitTask(), context=option_context(open=True))

        failed = await cli.kv_get_model(ControllerState)

    assert failed.operating_state.current == ERROR
    assert failed.operating_state.context == option_context(open=True)
    assert "did not report" in failed.execution_state.finished.error


# Recovery


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected", "error"),
    [
        ("succeed", UNKNOWN, False),
        ("report", OPERATE, False),
        ("fail", ERROR, "mount unreachable"),
        ("fail_silently", ERROR, "RuntimeError"),
        ("abort", UNKNOWN, False),
    ],
)
async def test_recovery_has_no_target(kit, controller, outcome, expected, error):
    established = option_context(open=True)
    reported = KeywordDict({"mode": "recovered"})
    cli = kit.controller("mycontroller")
    observed: list[ControllerOperatingState] = []
    started = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    @controller.task_handler(RecoverTask)
    async def run_recover(_: RecoverTask):
        observed.append(await operating_state(cli))
        started.set()

        match outcome:
            case "report":
                await controller.report_state(OPERATE, reported)
            case "fail":
                raise RuntimeError("mount unreachable")
            case "fail_silently":
                raise RuntimeError()
            case "abort":
                await asyncio.Event().wait()

    async with asyncio.timeout(1.0):
        await cli.execute_task(InitTask(), context=established)

        exec_call = cli.execute_task(RecoverTask())
        await exec_call.invoke()
        await started.wait()

        match outcome:
            case "succeed" | "report":
                await exec_call.wait()
            case "abort":
                await cli.abort_task()

                with pytest.raises(CallError):
                    await exec_call.wait()
            case _:
                with pytest.raises(CallError):
                    await exec_call.wait()

        state = await cli.kv_get_model(ControllerState)

    # Recovery does not establish its prior state again.
    [during] = observed
    assert during.current == TRANSITION
    assert during.previous == OPERATE
    assert during.target is None
    assert during.context == established

    assert state.operating_state.current == expected
    assert state.operating_state.target is None
    assert state.operating_state.context == (reported if outcome == "report" else established)
    assert state.execution_state.finished.error == error


# Explicit state reports


@pytest.mark.asyncio
async def test_state_report_context(kit, controller):
    established = option_context(open=True)
    explicit = option_context(open=False)
    cli = kit.controller("mycontroller")
    published: list[tuple[InternalControllerState, KeywordDict]] = []

    @controller.task_handler(InitTask)
    async def run_init(_: InitTask):
        pass

    async def record():
        operating = await operating_state(cli)
        published.append((operating.current, operating.context))

    async with asyncio.timeout(1.0):
        await cli.execute_task(InitTask(), context=established)

        await controller.report_state(UNKNOWN)
        await record()
        await controller.report_state(ERROR)
        await record()

        # The latch hides observed known states until a lifecycle task succeeds.
        await controller.report_state(OPERATE, established)
        await record()
        await cli.execute_task(InitTask(), context=established)
        await record()

        await controller.report_state(STANDBY, explicit)
        await record()
        await controller.report_state(STANDBY, KeywordDict())
        await record()
        await controller.report_state(STANDBY, explicit)

        # The recorded input is independent of the reporter's mapping.
        explicit.get(EnclosureOption).limits.append(90.0)
        await controller.report_state(ERROR)
        await record()

    assert published == [
        (UNKNOWN, established),
        (ERROR, established),
        (ERROR, established),
        (OPERATE, established),
        (STANDBY, option_context(open=False)),
        (STANDBY, KeywordDict()),
        (ERROR, option_context(open=False)),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "context"),
    [(OPERATE, None), (ERROR, KeywordDict())],
    ids=("known_without_input", "latch_with_input"),
)
async def test_report_input_follows_state(controller, state, context):
    with pytest.raises(ValueError):
        await controller.report_state(state, context)


# Restart and persistence


@pytest.mark.asyncio
async def test_startup_retains_interrupted_input(kit, controller_service):
    cli = kit.controller("mycontroller")
    execution = seeded_execution()

    await cli.kv_put_model(
        ControllerState(
            enable_state=ControllerEnableState(enabled=True),
            operating_state=ControllerOperatingState(
                current=TRANSITION,
                target=SHUTDOWN,
                context=option_context(open=False),
            ),
            execution_state=TaskExecutionState(executing=True, execution=execution),
        )
    )

    async with asyncio.timeout(1.0):
        await controller_service.register_controller("mycontroller")
        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == UNKNOWN
    assert state.operating_state.target is None
    assert state.operating_state.context == option_context(open=False)
    assert state.execution_state.finished.aborted


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["empty", "nested"])
async def test_startup_retains_settled_input(kit, controller_service, kind):
    cli = kit.controller("mycontroller")
    seeded = ControllerState(
        enable_state=ControllerEnableState(enabled=True),
        operating_state=ControllerOperatingState(
            current=SHUTDOWN,
            previous=TRANSITION,
            context=lifecycle_input(kind),
        ),
        execution_state=TaskExecutionState(execution=None),
    )

    await cli.kv_put_model(seeded)

    async with asyncio.timeout(1.0):
        await controller_service.register_controller("mycontroller")
        state = await cli.kv_get_model(ControllerState)

    assert state.operating_state.current == UNKNOWN
    assert state.operating_state.previous == SHUTDOWN
    assert state.operating_state.context == lifecycle_input(kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("context", ["omitted", "null"])
@pytest.mark.parametrize(
    ("operating", "executing", "expected", "previous"),
    [
        ({"current": "operate"}, False, UNKNOWN, OPERATE),
        ({"current": "error", "previous": "transition"}, False, UNKNOWN, ERROR),
        ({"current": "operate", "target": "shutdown"}, True, UNKNOWN, OPERATE),
    ],
    ids=("achieved", "error", "interrupted"),
)
async def test_startup_normalizes_legacy_snapshot(
    kit, controller_service, operating, executing, expected, previous, context
):
    cli = kit.controller("mycontroller")
    kv = kit.backend.key_value(cli.entity)
    execution = seeded_execution()
    legacy = {
        "enable_state": {"enabled": True},
        "operating_state": operating if context == "omitted" else {**operating, "context": None},
        "execution_state": {
            "executing": executing,
            "execution": json.loads(execution.model_dump_json()),
        },
    }

    await kv.update(ControllerState.__name__, json.dumps(legacy).encode())
    events = await cli.monitor_event(ControllerOperatingState)

    async with asyncio.timeout(1.0):
        await controller_service.register_controller("mycontroller")
        normalized = await anext(events)
        stored = json.loads((await kv.get(ControllerState.__name__)).value)
        state = await cli.kv_get_model(ControllerState)

    await events.aclose()

    assert normalized.current == expected
    assert normalized.target is None
    assert "context" in normalized.model_fields_set
    assert stored["operating_state"]["current"] == expected
    assert stored["operating_state"]["target"] is None
    assert stored["operating_state"]["context"] == {}
    assert state.operating_state.current == expected
    assert state.operating_state.previous == previous
    assert state.operating_state.target is None
    assert state.operating_state.context == KeywordDict()
    assert state.execution_state.executing is False
    assert (state.execution_state.finished is not None) == executing


@pytest.mark.asyncio
async def test_event_stream_migrates_legacy_achieved_state(kit, controller):
    cli = kit.controller("mycontroller")
    stream = kit.backend.stream(cli.entity)
    events = await cli.monitor_event(ControllerOperatingState)

    async with asyncio.timeout(1.0):
        for fields in ({"current": "operate"}, {"current": "operate", "context": {}}):
            payload = {"event_model": ControllerOperatingState.model_tag(), **fields}
            await stream.publish_event(json.dumps(payload).encode())

        legacy = await anext(events)
        current = await anext(events)

    await events.aclose()

    assert legacy.current == UNKNOWN
    assert legacy.previous == OPERATE
    assert current.current == OPERATE
    assert current.context == KeywordDict()


def decode_event(fields: dict[str, Any]) -> ControllerOperatingState:
    payload = {"event_model": ControllerOperatingState.model_tag(), **fields}
    event = Event.model_validate_json(json.dumps(payload))
    assert isinstance(event, ControllerOperatingState)
    return event


def decode_snapshot(fields: dict[str, Any]) -> ControllerOperatingState:
    snapshot = {
        "enable_state": {"enabled": True},
        "operating_state": fields,
        "execution_state": {"execution": None},
    }

    return ControllerState.model_validate_json(json.dumps(snapshot)).operating_state


def construct(fields: dict[str, Any]) -> ControllerOperatingState:
    return ControllerOperatingState(**fields)


DECODERS = pytest.mark.parametrize(
    "decode", [decode_event, decode_snapshot, construct], ids=("event", "snapshot", "python")
)


@DECODERS
@pytest.mark.parametrize(
    ("fields", "expected", "previous"),
    [
        ({"current": "operate"}, UNKNOWN, OPERATE),
        ({"current": "standby", "context": None}, UNKNOWN, STANDBY),
        ({"current": "shutdown", "previous": "transition"}, UNKNOWN, SHUTDOWN),
        ({"current": "operate", "target": "shutdown"}, UNKNOWN, OPERATE),
        ({"current": "error", "previous": "transition"}, ERROR, TRANSITION),
        ({"current": "unknown"}, UNKNOWN, None),
        ({"current": "transition", "target": "operate"}, TRANSITION, None),
    ],
    ids=("operate", "null", "shutdown", "target", "error", "unknown", "transition"),
)
def test_missing_context_migrates_achieved_state(decode, fields, expected, previous):
    operating = decode(fields)

    assert operating.current == expected
    assert operating.previous == previous
    assert operating.target == fields.get("target")
    assert operating.context == KeywordDict()


@DECODERS
@pytest.mark.parametrize("state", [OPERATE, STANDBY, SHUTDOWN])
def test_explicit_empty_context_keeps_achieved_state(decode, state):
    operating = decode({"current": state.value, "context": {}})

    assert operating.current == state
    assert operating.previous is None
    assert operating.context == KeywordDict()


@pytest.mark.parametrize("kind", ["empty", "nested"])
def test_achieved_context_round_trips(kind):
    original = ControllerOperatingState(current=OPERATE, context=lifecycle_input(kind))
    fields = json.loads(original.model_dump_json())

    for decode in (decode_event, decode_snapshot):
        decoded = decode(fields)

        assert decoded.current == OPERATE
        assert decoded.context == lifecycle_input(kind)
        assert decoded == original
