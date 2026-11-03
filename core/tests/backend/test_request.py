# SPDX-License-Identifier: Apache-2.0
import asyncio
from typing import override

import pytest
import pytest_asyncio
from pydantic import BaseModel, ValidationError

from sensorkit.backend.base import Backend, RemoteRequestError, Subject
from sensorkit.backend.fake import FakeBackendImpl
from sensorkit.backend.request import (
    CALL_MAX_AGE,
    CallContext,
    CallError,
    CallRejected,
    HandlerError,
    declare_long_request,
    declare_request,
)
from sensorkit.core.client import SensorKit


class Message(BaseModel):
    foo: str


class Response(BaseModel):
    val: int


class Result(BaseModel):
    val: int


class HoldingBackendImpl(FakeBackendImpl):
    """Fake backend that can hold request delivery and event stream startup until released."""

    def __init__(self):
        super().__init__()
        self.requests_released = asyncio.Event()
        self.streams_released = asyncio.Event()

        self.requests_released.set()
        self.streams_released.set()

    @override
    async def request_invoke(self, target: Subject, payload: bytes) -> bytes:
        await self.requests_released.wait()
        return await super().request_invoke(target, payload)

    @override
    async def stream_consume(self, target: Subject, **kwargs):
        await self.streams_released.wait()
        return await super().stream_consume(target, **kwargs)


@pytest_asyncio.fixture
async def holding_backend():
    return HoldingBackendImpl()


@pytest_asyncio.fixture
async def holding_kit(holding_backend):
    return SensorKit(Backend(impl=holding_backend))


@pytest.mark.asyncio
async def test_request(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request(name="foo", message=Message, response=Response)

    async def foo_req(req: Message):
        assert req.foo == "bar"
        return Response(val=42)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(1.0):
        cli = kit.entity(sc.entity)

        call = cli.call(request, Message(foo="bar"))
        response = await call.invoke()
        assert isinstance(response, Response)

        await call.wait()
        result = call.result()
        assert isinstance(result, Response) and result.val == 42


@pytest.mark.asyncio
async def test_request_without_payloads(kit):
    """A request declaring no message and no response completes with None."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request("foo")

    async def foo_req(msg: None) -> None:
        pass

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        assert await kit.entity(sc.entity).call(request) is None


@pytest.mark.asyncio
async def test_request_handler_raising(kit):
    """A simple handler that raises fails the call with its error."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request("foo")

    async def foo_req(msg: None) -> None:
        raise RuntimeError("disk on fire")

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        with pytest.raises(RemoteRequestError, match="RuntimeError: disk on fire"):
            await kit.entity(sc.entity).call(request)


@pytest.mark.asyncio
async def test_request_with_invalid_message(kit):
    """A message the handler cannot validate fails the call."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request("foo", message=Message)
    mismatched = declare_request("foo", message=Response)

    async def foo_req(msg: Message) -> None:
        pass

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        with pytest.raises(RemoteRequestError, match=r"validation error for CallRequest\[Message\]"):
            await kit.entity(sc.entity).call(mismatched, Response(val=1))


@pytest.mark.asyncio
async def test_request_with_invalid_response(kit):
    """A response that does not validate as the declared model settles the call with the error."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request("foo", response=Response)
    mismatched = declare_request("foo", response=Message)

    async def foo_req(msg: None) -> Response:
        return Response(val=1)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        call = kit.entity(sc.entity).call(mismatched)

        with pytest.raises(ValidationError):
            await call.invoke()

        with pytest.raises(ValidationError):
            await call.wait()


@pytest.mark.asyncio
async def test_simple_call_to_a_long_running_handler(kit):
    """A caller expecting a simple reply does not mistake an accepted call for a result."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo")
    mismatched = declare_request("foo")

    async def foo_req(msg: None, call: CallContext[None]):
        call.accept()
        await call.succeed(result=None)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        with pytest.raises(CallError, match="unexpected accepted reply") as raised:
            await kit.entity(sc.entity).call(mismatched)

        assert not isinstance(raised.value, CallRejected)


@pytest.mark.asyncio
async def test_long_request(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", message=Message, response=Response, result=Result)

    async def foo_req(req: Message, call: CallContext[Result, Response]):
        assert req.foo == "bar"
        call.accept(response=Response(val=1))
        await call.succeed(result=Result(val=42))

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(1.0):
        cli = kit.entity(sc.entity)

        call = cli.call(request, Message(foo="bar"))
        response = await call.invoke()
        assert isinstance(response, Response) and response.val == 1

        await call.wait()
        result = call.result()
        assert isinstance(result, Result) and result.val == 42


@pytest.mark.asyncio
async def test_long_request_without_payloads(kit):
    """A long-running request declaring no message, response, or result completes with None."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo")

    async def foo_req(msg: None, call: CallContext[None]):
        call.accept()
        await call.succeed(result=None)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        call = kit.entity(sc.entity).call(request)
        assert await call.invoke() is None
        assert await call is None


@pytest.mark.asyncio
async def test_long_request_rejected(kit):
    """A rejected call raises on invoke, carrying the handler's reason and response."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", message=Message, response=Response, result=Result)

    async def foo_req(req: Message, call: CallContext[Result, Response]):
        if req.foo:
            call.reject(response=Response(val=7), reason=req.foo)
        else:
            call.reject()

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        cli = kit.entity(sc.entity)

        with pytest.raises(CallRejected, match="call rejected"):
            await cli.call(request, Message(foo=""))

        call = cli.call(request, Message(foo="disk on fire"))

        with pytest.raises(CallRejected, match="call rejected: disk on fire"):
            await call.invoke()

        assert call.response == Response(val=7)


@pytest.mark.asyncio
async def test_long_request_rejection_ends_the_call(kit):
    """A handler cannot report on a call it has rejected."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo")
    outcome: asyncio.Future[BaseException | None] = asyncio.get_running_loop().create_future()

    async def foo_req(msg: None, call: CallContext[None]):
        call.reject()

        try:
            await call.fail("too late")
        except HandlerError as e:
            outcome.set_result(e)
        else:
            outcome.set_result(None)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        with pytest.raises(CallError, match="call rejected"):
            await kit.entity(sc.entity).call(request)

        assert isinstance(await outcome, HandlerError)


@pytest.mark.asyncio
async def test_long_request_failed(kit):
    """A handler that fails an accepted call raises the reason to the caller."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo", result=Result)

    async def foo_req(msg: None, call: CallContext[Result]):
        call.accept()
        await call.fail("disk on fire")

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        call = kit.entity(sc.entity).call(request)
        await call.invoke()

        with pytest.raises(CallError, match="disk on fire"):
            await call.wait()


@pytest.mark.asyncio
async def test_long_request_never_answered(kit):
    """A handler that returns without responding reports the error back on the reply."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", response=Response, result=Result)

    async def foo_req(msg: None, call: CallContext[Result, Response]):
        return

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        cli = kit.entity(sc.entity)

        # The caller never learned a call id, so the error cannot arrive as a failure event.
        with pytest.raises(RemoteRequestError, match="did not call"):
            await cli.call(request).run_to_completion()


@pytest.mark.asyncio
async def test_long_request_handler_raising_before_responding(kit):
    """A handler that raises before accepting or rejecting reports its error on the reply."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo")

    async def foo_req(msg: None, call: CallContext[None]):
        raise RuntimeError("disk on fire")

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        cli = kit.entity(sc.entity)

        with pytest.raises(RemoteRequestError, match="disk on fire"):
            await cli.call(request).run_to_completion()


@pytest.mark.asyncio
async def test_long_request_handler_raising_as_it_accepts(kit):
    """A handler that raises right after accepting fails the call it accepted."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", result=Result)

    async def foo_req(msg: None, call: CallContext[Result]):
        call.accept()
        raise RuntimeError("disk on fire")

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        call = kit.entity(sc.entity).call(request)
        await call.invoke()

        with pytest.raises(CallError, match="disk on fire"):
            await call.wait()


@pytest.mark.asyncio
async def test_long_request_abandoned_by_its_handler(kit):
    """A handler that returns after accepting without finalizing fails the call."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", response=Response, result=Result)
    release = asyncio.Event()

    async def foo_req(msg: None, call: CallContext[Result, Response]):
        call.accept(response=Response(val=1))

        # Progress lets the caller finish invoking before the handler walks away.
        await call.progress(5.0)
        await release.wait()

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        cli = kit.entity(sc.entity)
        call = cli.call(request)
        await call.invoke()
        release.set()

        # The handler left no reason behind, so the failure carries none.
        with pytest.raises(CallError, match="response failed: None"):
            await call.wait()


@pytest.mark.asyncio
async def test_long_request_handler_raising_after_accepting(kit):
    """A handler that raises once the call is under way fails it with the reason."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", response=Response, result=Result)
    release = asyncio.Event()

    async def foo_req(msg: None, call: CallContext[Result, Response]):
        call.accept(response=Response(val=1))
        await call.progress(5.0)
        await release.wait()

        raise RuntimeError("disk on fire")

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        cli = kit.entity(sc.entity)
        call = cli.call(request)
        await call.invoke()
        release.set()

        with pytest.raises(CallError, match="disk on fire"):
            await call.wait()


@pytest.mark.asyncio
async def test_long_requests_matched_by_call_id(kit):
    """Concurrent calls to one handler each settle with their own result."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo", message=Message, result=Result)
    releases = {"a": asyncio.Event(), "bb": asyncio.Event()}

    async def foo_req(msg: Message, call: CallContext[Result]):
        call.accept()
        await call.progress(5.0)
        await releases[msg.foo].wait()
        await call.succeed(result=Result(val=len(msg.foo)))

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        cli = kit.entity(sc.entity)
        first = cli.call(request, Message(foo="a"))
        second = cli.call(request, Message(foo="bb"))
        await first.invoke()
        await second.invoke()

        releases["bb"].set()
        assert (await second).val == 2
        assert not first.done()

        releases["a"].set()
        assert (await first).val == 1


@pytest.mark.asyncio
async def test_long_request_sends_before_suspending(kit):
    """A long-running call on subscribed events is sent before its caller first suspends."""
    if isinstance(kit.backend.impl, FakeBackendImpl):
        pytest.skip("the in-memory backend retracts a request when its caller is cancelled")

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", result=Result)
    received = asyncio.Event()

    async def foo_req(msg: None, call: CallContext[Result]):
        received.set()
        call.accept()
        await call.succeed(result=Result(val=1))

    await sc.handle_request(request, foo_req)
    cli = kit.entity(sc.entity)

    async with asyncio.timeout(1.0):
        await cli.subscribe_events()

    async def send():
        # The cancellation lands at the caller's first suspension.
        asyncio.get_running_loop().call_soon(asyncio.current_task().cancel)
        await cli.call(request)

    caller = asyncio.create_task(send())
    await asyncio.wait({caller})
    assert caller.cancelled()

    async with asyncio.timeout(1.0):
        await received.wait()


@pytest.mark.asyncio
async def test_long_request_abandoned_by_its_caller(kit):
    """A caller that stops waiting leaves nothing of the call running on its side."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", result=Result)
    handlers: set[asyncio.Task] = set()

    async def foo_req(msg: None, call: CallContext[Result]):
        handlers.add(asyncio.current_task())
        call.accept()

        # Progress keeps the call alive for longer than this test waits.
        await call.progress(10.0)
        await asyncio.Event().wait()

    await sc.handle_request(request, foo_req)
    cli = kit.entity(sc.entity)

    # The event stream starts on first use and outlives any one call.
    await cli.get_event_mux().wait_ready()
    before = asyncio.all_tasks()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await cli.call(request)

    # Ending the handler sends no further call events, so anything still waiting afterward is
    # waiting on the caller's side.
    for handler in handlers:
        handler.cancel()

    if leftover := asyncio.all_tasks() - before:
        _, pending = await asyncio.wait(leftover, timeout=1.0)
        assert not pending


@pytest.mark.asyncio
async def test_long_request_abandoned_while_its_handler_reports(kit):
    """A caller that stops waiting releases its side while the handler still reports progress."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_long_request(name="foo", result=Result)
    handlers: set[asyncio.Task] = set()
    reported = asyncio.Condition()
    published = 0

    async def foo_req(msg: None, call: CallContext[Result]):
        nonlocal published
        handlers.add(asyncio.current_task())
        call.accept()

        while True:
            await call.progress(10.0)

            async with reported:
                published += 1
                reported.notify_all()

            await asyncio.sleep(0.02)

    await sc.handle_request(request, foo_req)
    cli = kit.entity(sc.entity)

    # The event stream starts on first use and outlives any one call.
    await cli.get_event_mux().wait_ready()
    before = asyncio.all_tasks()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await cli.call(request)

    # The handler goes on reporting after the caller has gone.
    async with reported, asyncio.timeout(1.0):
        abandoned = published
        await reported.wait_for(lambda: published >= abandoned + 3)

    try:
        if leftover := asyncio.all_tasks() - before - handlers:
            _, pending = await asyncio.wait(leftover, timeout=1.0)
            assert not pending
    finally:
        for handler in handlers:
            handler.cancel()

        await asyncio.wait(handlers)


def test_long_request_requires_an_event_stream():
    """A long-running request cannot be served without a stream to publish its events on."""
    request = declare_long_request(name="foo", response=Response)

    async def foo_req(msg: None, call: CallContext[None, Response]):
        call.accept(response=Response(val=1))

    with pytest.raises(HandlerError, match="event stream"):
        request.create_handler(foo_req)


@pytest.mark.asyncio
async def test_long_request_call_requires_events(kit):
    """A long-running request cannot be called without the events that report its result."""
    request = declare_long_request(name="foo", response=Response)

    with pytest.raises(ValueError, match="event multiplexer"):
        request.create_call(kit.backend.request())


# ---- late delivery ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_request_delivered_late_is_rejected(holding_kit, holding_backend):
    """A call that reaches its handler too long after it was sent is rejected without running."""
    async with asyncio.timeout(1.0):
        sc = await holding_kit.register_service("testservice", "0.1.0")

    request = declare_request("foo", message=Message)
    handled = []

    async def foo_req(msg: Message) -> None:
        handled.append(msg)

    await sc.handle_request(request, foo_req)
    holding_backend.requests_released.clear()

    async with asyncio.timeout(5.0):
        cli = holding_kit.entity(sc.entity)
        call = asyncio.create_task(cli.call(request, Message(foo="bar")).run_to_completion())
        await asyncio.sleep(CALL_MAX_AGE + 0.2)
        holding_backend.requests_released.set()

        with pytest.raises(CallRejected, match="too late"):
            await call

    assert not handled


@pytest.mark.asyncio
async def test_long_request_delivered_late_is_rejected(holding_kit, holding_backend):
    """A long-running call that reaches its handler too late is rejected before it starts."""
    async with asyncio.timeout(1.0):
        sc = await holding_kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo", result=Result)
    started = []

    async def foo_req(msg: None, call: CallContext[Result]):
        started.append(call.call_id)
        call.accept()
        await call.succeed(result=Result(val=1))

    await sc.handle_request(request, foo_req)
    holding_backend.requests_released.clear()

    async with asyncio.timeout(5.0):
        cli = holding_kit.entity(sc.entity)
        call = asyncio.create_task(cli.call(request).run_to_completion())
        await asyncio.sleep(CALL_MAX_AGE + 0.2)
        holding_backend.requests_released.set()

        with pytest.raises(CallRejected, match="too late"):
            await call

    assert not started


@pytest.mark.asyncio
async def test_request_delivered_promptly_after_a_delay_is_handled(holding_kit, holding_backend):
    """A call delayed in transit by less than the allowed age still reaches its handler."""
    async with asyncio.timeout(1.0):
        sc = await holding_kit.register_service("testservice", "0.1.0")

    request = declare_request("foo", response=Response)

    async def foo_req(msg: None) -> Response:
        return Response(val=7)

    await sc.handle_request(request, foo_req)
    holding_backend.requests_released.clear()

    async with asyncio.timeout(5.0):
        cli = holding_kit.entity(sc.entity)
        call = asyncio.create_task(cli.call(request).run_to_completion())
        await asyncio.sleep(CALL_MAX_AGE / 4)
        holding_backend.requests_released.set()

        assert (await call).val == 7


@pytest.mark.asyncio
async def test_long_request_age_starts_when_sent(holding_kit, holding_backend):
    """Waiting for the event stream before sending does not count toward a call's age."""
    async with asyncio.timeout(1.0):
        sc = await holding_kit.register_service("testservice", "0.1.0")

    request = declare_long_request("foo", result=Result)

    async def foo_req(msg: None, call: CallContext[Result]):
        call.accept()
        await call.succeed(result=Result(val=3))

    await sc.handle_request(request, foo_req)
    holding_backend.streams_released.clear()

    async with asyncio.timeout(5.0):
        cli = holding_kit.entity(sc.entity)
        call = asyncio.create_task(cli.call(request).run_to_completion())
        await asyncio.sleep(CALL_MAX_AGE + 0.2)
        holding_backend.streams_released.set()

        assert (await call).val == 3


# ---- reply timeout ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unanswered_request_times_out(kit):
    """A call whose reply does not arrive in time raises a TimeoutError."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request("foo", response=Response)
    release = asyncio.Event()

    async def foo_req(msg: None) -> Response:
        await release.wait()
        return Response(val=1)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        with pytest.raises(TimeoutError):
            await kit.entity(sc.entity).call(request).invoke(timeout=1.0)

    release.set()


@pytest.mark.asyncio
async def test_request_runs_on_after_its_caller_stops_waiting(kit):
    """A delivered request runs to completion after its caller times out."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

    request = declare_request("foo", response=Response)
    release = asyncio.Event()
    finished = asyncio.Event()

    async def foo_req(msg: None) -> Response:
        await release.wait()
        finished.set()
        return Response(val=1)

    await sc.handle_request(request, foo_req)

    async with asyncio.timeout(5.0):
        with pytest.raises(TimeoutError):
            await kit.entity(sc.entity).call(request).invoke(timeout=0.2)

        release.set()
        await finished.wait()
