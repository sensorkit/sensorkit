# SPDX-License-Identifier: Apache-2.0
import asyncio

import pytest
from pydantic import BaseModel, ValidationError

from sensorkit.backend.base import RemoteRequestError
from sensorkit.backend.request import (
    CallContext,
    CallError,
    HandlerError,
    declare_long_request,
    declare_request,
)


class Message(BaseModel):
    foo: str


class Response(BaseModel):
    val: int


class Result(BaseModel):
    val: int


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
        with pytest.raises(RemoteRequestError, match="validation error for Message"):
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
        with pytest.raises(CallError, match="unexpected accepted reply"):
            await kit.entity(sc.entity).call(mismatched)


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

        with pytest.raises(CallError, match="call rejected"):
            await cli.call(request, Message(foo=""))

        call = cli.call(request, Message(foo="disk on fire"))

        with pytest.raises(CallError, match="call rejected: disk on fire"):
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


def test_long_request_requires_an_event_stream():
    """A long-running request cannot be served without a stream to publish its events on."""
    request = declare_long_request(name="foo", response=Response)

    async def foo_req(msg: None, call: CallContext[None, Response]):
        call.accept(response=Response(val=1))

    with pytest.raises(HandlerError, match="event stream"):
        request.create_handler(foo_req)
