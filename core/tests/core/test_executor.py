# SPDX-License-Identifier: Apache-2.0
"""Tests for task calls and the executions they follow."""

import asyncio
import contextlib

import pytest
import pytest_asyncio
import uuid_utils.compat as uuid

from sensorkit.backend.base import RemoteRequestError
from sensorkit.backend.request import CallError, CallRejected, declare_request
from sensorkit.core.controller import (
    ControllerClient,
    ControllerState,
    ExecuteRequestMessage,
    ExecuteResponseMessage,
    InternalControllerState,
    abort_task_request,
    execute_task_request,
)
from sensorkit.core.entity import EventsNotSubscribed
from sensorkit.core.executor import TaskCall, TaskFailed, TaskOutcomeUnknown
from sensorkit.core.task import InitTask, TaskExecution


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


async def subscribed(client: ControllerClient) -> ControllerClient:
    async with asyncio.timeout(5.0):
        await client.subscribe_events()

    return client


@contextlib.asynccontextmanager
async def following(call: TaskCall):
    """Abort and join a call on exit."""
    try:
        yield call
    finally:
        call.abort()

        async with asyncio.timeout(5.0):
            await asyncio.wait((call.future(),))


@pytest.mark.asyncio
async def test_call_follows_execution_to_its_result(kit, controller_impl):
    @controller_impl.task_handler(InitTask)
    async def init(task):
        pass

    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask())

    async with following(call), asyncio.timeout(5.0):
        execution = await call.send()
        result = await execution

    assert call.execution is execution
    assert result.task_id == execution.task_id
    assert call.future().result() == result
    assert call.error() is None


@pytest.mark.asyncio
async def test_call_yields_its_execution_states(kit, controller_impl):
    """State events remain in order when execution finishes before acceptance returns."""

    @controller_impl.task_handler(InitTask)
    async def init(task):
        pass

    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask(), observe=True)

    async with following(call), asyncio.timeout(5.0):
        execution = await call.send()
        await call.future()
        states = [state async for state in call.execution_states()]

    assert [state.execution.task_id for state in states] == [execution.task_id] * 2
    assert states[0].executing and states[0].finished is None
    assert states[-1].finished is not None


@pytest.mark.asyncio
async def test_unobserved_call_has_no_execution_states(kit, controller_impl):
    @controller_impl.task_handler(InitTask)
    async def init(task):
        pass

    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask())

    async with following(call), asyncio.timeout(5.0):
        await call.send()
        await call.future()

    with pytest.raises(RuntimeError):
        call.execution_states()


@pytest.mark.asyncio
async def test_call_ignores_other_execution_states(kit, controller_impl):
    """An interrupted call yields only state events for its own execution."""
    entered = asyncio.Event()
    release = asyncio.Event()

    @controller_impl.task_handler(InitTask)
    async def init(task):
        if not entered.is_set():
            entered.set()
            await release.wait()

    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask(), observe=True)

    try:
        async with following(call), asyncio.timeout(5.0):
            execution = await call.send()
            await entered.wait()
            await client.execute_task(InitTask(), interrupt=True)

            with contextlib.suppress(TaskFailed):
                await call.future()

            states = [state async for state in call.execution_states()]
    finally:
        release.set()

    assert {state.execution.task_id for state in states} == {execution.task_id}
    assert states[-1].finished is not None


@pytest.mark.asyncio
async def test_closed_states_keep_following_the_execution(kit, controller_impl, gated_init):
    """Closing state observation leaves the task running and its result available."""
    entered, cancelled, release = gated_init
    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask(), observe=True)

    async with following(call), asyncio.timeout(5.0):
        await call.send()
        await entered.wait()
        await asyncio.sleep(0.05)
        call.execution_states().close()
        states = [state async for state in call.execution_states()]

        release.set()
        await call.future()
        later = [state async for state in call.execution_states()]

    assert not cancelled.is_set()
    assert states and states[-1].executing
    assert not later
    assert call.error() is None


@pytest.mark.asyncio
async def test_send_requires_subscribed_events(kit, controller_impl, gated_init):
    entered, _, _ = gated_init
    call = TaskCall(kit.controller(controller_impl.entity), InitTask())

    with pytest.raises(EventsNotSubscribed):
        await call.send()

    await asyncio.sleep(0.05)
    assert not entered.is_set()


@pytest.mark.asyncio
async def test_cancelled_sender_leaves_call_unresolved(kit, controller_impl, gated_init):
    """Canceling the sender before the reply leaves task acceptance unknown."""
    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask())

    async def send():
        # The cancellation lands at the sender's first suspension, after the request is out.
        asyncio.get_running_loop().call_soon(asyncio.current_task().cancel)
        await call.send()

    sender = asyncio.create_task(send())

    async with asyncio.timeout(5.0):
        await asyncio.wait((sender,))

    assert sender.cancelled()
    assert isinstance(call.error(), TaskOutcomeUnknown)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rejected", "remote", "unexpected"])
async def test_acceptance_failure(kit, entity_impl, failure):
    """Only explicit rejection establishes that the task never ran, and no abort is sent."""
    execution = TaskExecution(
        task=InitTask(), task_id=uuid.uuid7(), controller_id=str(entity_impl.entity)
    )
    aborted = []

    if failure == "unexpected":
        request = declare_request(
            "execute_task", message=ExecuteRequestMessage, response=ExecuteResponseMessage
        )

        async def execute(msg):
            return ExecuteResponseMessage(execution=execution)
    else:
        request = execute_task_request

        async def execute(msg, call):
            if failure == "rejected":
                call.reject(response=ExecuteResponseMessage(execution=execution))
            else:
                raise RuntimeError("Admission failed")

    async def abort(msg, call):
        aborted.append(msg.task_id)
        call.reject()

    await entity_impl.handle_request(request, execute)
    await entity_impl.handle_request(abort_task_request, abort)
    call = TaskCall(await subscribed(kit.controller(entity_impl.entity)), InitTask())

    async with following(call), asyncio.timeout(5.0):
        with contextlib.suppress(CallRejected, TaskOutcomeUnknown):
            await call.send()

    error = call.error()
    assert not aborted

    match failure:
        case "rejected":
            assert isinstance(error, CallRejected)
            assert call.execution.task_id == execution.task_id
        case "remote":
            assert isinstance(error, TaskOutcomeUnknown)
            assert isinstance(error.__cause__, RemoteRequestError)
        case "unexpected":
            assert isinstance(error, TaskOutcomeUnknown)
            assert isinstance(error.__cause__, CallError)
            assert not isinstance(error.__cause__, CallRejected)


@pytest.mark.asyncio
async def test_lost_end_event_is_unresolved(kit, entity_impl):
    """An expired progress deadline without an end event leaves the call unresolved."""
    execution = TaskExecution(
        task=InitTask(), task_id=uuid.uuid7(), controller_id=str(entity_impl.entity)
    )
    release = asyncio.Event()
    aborted = []

    async def execute(msg, call):
        call.accept(response=ExecuteResponseMessage(execution=execution))

        # Let the progress deadline expire without publishing an end event.
        await call.progress(0.1)
        await release.wait()

    async def abort(msg, call):
        aborted.append(msg.task_id)
        call.reject()

    await entity_impl.handle_request(execute_task_request, execute)
    await entity_impl.handle_request(abort_task_request, abort)
    call = TaskCall(await subscribed(kit.controller(entity_impl.entity)), InitTask())

    try:
        async with asyncio.timeout(5.0):
            accepted = await call.send()

            with pytest.raises(TaskOutcomeUnknown):
                await call.future()
    finally:
        release.set()

    assert accepted.task_id == execution.task_id
    assert not aborted


@pytest.mark.asyncio
async def test_abort_requested_during_acceptance(kit, controller_impl, gated_init):
    """An abort requested before acceptance waits for the reply and targets that execution."""
    entered, cancelled, _ = gated_init
    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask())

    async with following(call), asyncio.timeout(5.0):
        sending = asyncio.create_task(call.send())

        # The request is out, but the controller has not replied.
        await asyncio.sleep(0)
        assert call.execution is None
        call.abort()
        await sending

        with pytest.raises(TaskFailed) as raised:
            await call.future()

    assert raised.value.aborted
    assert call.execution is not None
    assert not entered.is_set() or cancelled.is_set()
    assert not controller_impl.task_running()


@pytest.mark.asyncio
async def test_abort_after_acceptance(kit, controller_impl, gated_init):
    """An aborted call finishes only after the controller reports the execution ended."""
    entered, cancelled, _ = gated_init
    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask())

    async with following(call), asyncio.timeout(5.0):
        await call.send()
        await entered.wait()
        call.abort()
        call.abort()

        with pytest.raises(TaskFailed) as raised:
            await call.future()

        state = await client.kv_get_model(ControllerState)

    assert raised.value.aborted
    assert cancelled.is_set()
    assert state.operating_state.current == InternalControllerState.UNKNOWN


@pytest.mark.asyncio
async def test_other_execution_fails_call(kit, controller_impl):
    """A replacement task reaching the same state does not make the interrupted call succeed."""
    entered = asyncio.Event()
    release = asyncio.Event()
    inits = 0

    @controller_impl.task_handler(InitTask)
    async def init(task):
        nonlocal inits
        inits += 1

        # Only the first init waits, until another client interrupts it.
        if inits == 1:
            entered.set()
            await release.wait()

    client = await subscribed(kit.controller(controller_impl.entity))
    call = TaskCall(client, InitTask())

    try:
        async with following(call), asyncio.timeout(5.0):
            await call.send()
            await entered.wait()

            # Another client's init reaches the same target state.
            await client.execute_task(InitTask(), interrupt=True)

            with pytest.raises(TaskFailed) as raised:
                await call.future()
    finally:
        release.set()

    state = await client.kv_get_model(ControllerState)

    assert not raised.value.aborted
    assert state.operating_state.current == InternalControllerState.OPERATE
