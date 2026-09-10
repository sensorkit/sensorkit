# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import functools
import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Coroutine, Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Literal, cast, final, override

from loguru import logger
from pydantic import BaseModel

from sensorkit.backend.base import RemoteRequestError, StreamContext
from sensorkit.backend.event import Event, EventMultiplexer
from sensorkit.common.aio import cleanup_future, scoped_waiter

type ReplyState = Literal["accepted", "rejected", "succeeded", "failed"]


class CallReply[R: BaseModel | None = Any](BaseModel):
    """The direct reply to a call.

    Carries the state of the call alongside the handler's response, so the response model holds
    only application data. A simple request succeeds or fails in its reply. A long-running request
    is accepted or rejected in its reply, and reports its outcome on the entity event stream.

    Left unparametrized, the response is kept as plain JSON data.
    """
    call_id: uuid.UUID
    """Identifies the call, and matches the events a long-running call publishes."""
    state: ReplyState
    """Where the call stands once the handler has replied."""
    response: R | None = None
    """The handler's response, where it gave one."""
    reason: str | None = None
    """Why the call was rejected or failed."""
    details: str | None = None
    """Diagnostic detail for a failed call, such as the handler's traceback."""


class Call[R: BaseModel | None, V: BaseModel | None = R](Awaitable[V]):
    """An invocation of a simple request."""

    def __init__(self, coro: Coroutine[Any, Any, bytes], response_type: type[R] | None):
        self._coro = coro
        self._reply_type: type[CallReply[R]] = CallReply[response_type]
        self._future: asyncio.Future[V] = asyncio.get_running_loop().create_future()
        self._got_response = False
        self.response: R | None = None

        self._future.add_done_callback(cleanup_future)

    @final
    async def _invoke(self, timeout: float, expected: ReplyState) -> CallReply[R]:
        # Send request and await the reply.
        async with asyncio.timeout(timeout):
            recv = await self._coro

        reply = self._reply_type.model_validate_json(recv)
        self.response = reply.response
        self._got_response = True

        match reply.state:
            case "failed":
                raise RemoteRequestError(reply.reason, details=reply.details)
            case "rejected":
                raise CallError(f"call rejected: {reply.reason}" if reply.reason else "call rejected")
            case state if state != expected:
                raise CallError(f"unexpected {state} reply")

        return reply

    async def invoke(self, timeout: float = 5.0):
        """Send the request and populate the future with the response."""
        try:
            await self._invoke(timeout, "succeeded")
            self._future.set_result(cast(V, self.response))
            return self.response
        except BaseException as e:
            self._future.set_exception(e)
            raise

    async def wait(self):
        """Wait until the call's future is resolved."""
        await self._future

    def get_future(self) -> asyncio.Future[V]:
        """Return the underlying future resolving to the call's final result."""
        return self._future

    def done(self):
        """Return True if the call has completed (success or error)."""
        return self._future.done()

    def result(self) -> V:
        """Return the call result, raising if the call has not completed or raised."""
        return self._future.result()

    async def run_to_completion(self) -> V:
        """Invoke the call if not already sent, then await and return the final result."""
        if not self._got_response:
            await self.invoke()

        await self._future
        return self._future.result()

    @override
    def __await__(self) -> Generator[Any, None, V]:
        return self.run_to_completion().__await__()


type HandlerFunc[P: BaseModel | None, R: BaseModel | None] = Callable[[P], Coroutine[Any, Any, R]]
"""A simple request handler. A request that declares no message types it as None."""


class CallHandler[P: BaseModel | None]:
    """Adapts a request handler to the backend's bytes in, bytes out contract.

    Every call gets a reply, including one whose handler raised. A request with no message type
    passes None, keeping every handler the same arity.
    """

    def __init__(
        self,
        message: type[P] | None,
        run: Callable[..., Awaitable[CallReply]],
    ):
        self._message = message

        # Simple and long-running requests share this class, differing only in the runner bound.
        self._run = run

    async def __call__(self, payload: bytes) -> bytes:
        call_id = uuid.uuid1()

        try:
            message = self._message.model_validate_json(payload) if self._message else None
            reply = await self._run(call_id, message)
        except Exception as e:
            logger.opt(exception=e).debug(f"Call {call_id} failed in its handler")
            error = RemoteRequestError.from_exception(e)

            reply = CallReply(
                call_id=call_id,
                state="failed",
                reason=str(error),
                details=error.details,
            )

        return reply.model_dump_json().encode()


async def run_call[R: BaseModel | None](
    func: HandlerFunc[Any, R],
    call_id: uuid.UUID,
    message: BaseModel | None,
) -> CallReply[R]:
    """Run a simple handler func and return the reply carrying its response."""
    return CallReply(call_id=call_id, state="succeeded", response=await func(message))


class CallEvent(Event):
    """An event that occurs in the context of a long-running request."""
    call_id: uuid.UUID
    call_state: Literal["running", "success", "failure"] = "running"
    good_until: datetime | None
    payload: Any = None


class LongCall[R: BaseModel | None, V: BaseModel | None](Call[R, V]):
    """A Call extension that supports long-running requests."""

    def __init__(
        self,
        coro: Coroutine[Any, Any, bytes],
        response_type: type[R] | None,
        result_type: type[V] | None,
        event_mux: EventMultiplexer,
    ):
        super().__init__(coro, response_type)
        self._result_type = result_type
        self._event_mux = event_mux

    @override
    async def invoke(self, timeout: float = 10.0):
        # Consume the event stream.
        context = contextlib.ExitStack()
        queue = context.enter_context(self._event_mux.event_queue(CallEvent))

        try:
            await self._event_mux.wait_ready()

            # Execute the initial request-response communication.
            reply = await self._invoke(timeout, "accepted")
        except (asyncio.CancelledError, Exception) as e:
            context.close()
            self._future.set_exception(e)
            raise

        # Start a background task to receive response progress and end events.
        started = asyncio.Event()

        self._task = asyncio.create_task(
            self._await_response_events(
                context,
                queue,
                reply.call_id,
                started,
                timeout,
            )
        )

        def long_call_done(t: asyncio.Task):
            # Note we must check the task exception even if the future is already done to avoid
            # leaks and warnings.
            if t.cancelled():
                self._future.cancel()
            elif err := t.exception():
                if not self._future.done():
                    self._future.set_exception(err)
            elif not self._future.done():
                self._future.set_result(t.result())

        self._task.add_done_callback(long_call_done)

        # Wait until the first event is received or the task ends for some reason, whichever
        # comes first.
        async with scoped_waiter(started.wait()) as wait:
            await asyncio.wait(
                [wait, self._task],
                return_when=asyncio.FIRST_COMPLETED,
            )

        return self.response

    async def _await_response_events(
        self,
        context: contextlib.ExitStack,
        queue: asyncio.Queue[Event],
        response_id: uuid.UUID,
        started: asyncio.Event,
        timeout: float,
    ) -> V:
        with context:
            while True:
                async with asyncio.timeout(timeout):
                    # Consume the next matching ResponseEvent.
                    event = await queue.get()
                    queue.task_done()

                    if not isinstance(event, CallEvent) or event.call_id != response_id:
                        continue

                    match event.call_state:
                        case "success":
                            return cast(
                                V,
                                self._result_type.model_validate(event.payload)
                                if self._result_type is not None
                                else None
                            )
                        case "failure":
                            raise CallError(f"response failed: {event.payload}")

                    # Signal that at least one matching response event has been received.
                    started.set()

                    # Figure the next timeout. This relies on peer clock synchronization.
                    timeout = (event.good_until - datetime.now(UTC)).total_seconds()

        # Unreachable.
        raise RuntimeError


type LongHandlerFunc[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None] = (
    Callable[[P, CallContext[V, R]], Coroutine[Any, Any, None]]
)
"""A long-running request handler."""

type RequestHandlerFunc[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None] = (
    HandlerFunc[P, R] | LongHandlerFunc[P, R, V]
)
"""Any handler shape a request may be declared with."""


class CallContext[V: BaseModel | None, R: BaseModel | None = None]:
    """Context for an incoming long-running request currently being handled."""

    def __init__(self, call_id: uuid.UUID, stream: StreamContext):
        self.call_id = call_id
        self._stream = stream
        self.reply: asyncio.Future[CallReply[R]] = asyncio.get_running_loop().create_future()
        self.finalized = False

    @property
    def responded(self) -> bool:
        """Whether the handler has accepted or rejected the call."""
        return self.reply.done()

    def accept(self, *, response: R | None = None):
        """Accept the call and send the initial response, allowing progress events to follow."""
        self._respond(CallReply(call_id=self.call_id, state="accepted", response=response))

    def reject(self, *, response: R | None = None, reason: str | None = None):
        """Reject the call, ending it without further events.

        Args:
            response: The initial response, for a caller that needs it despite the rejection.
            reason: Why the call was rejected.
        """
        self._respond(
            CallReply(call_id=self.call_id, state="rejected", response=response, reason=reason)
        )
        self.finalized = True

    def _respond(self, reply: CallReply[R]):
        if self.responded:
            raise HandlerResponseError(responded=True)

        self.reply.set_result(reply)

    async def progress(self, ttl: float):
        """Emit a progress event, extending the caller's deadline by ttl seconds."""
        if not self.responded:
            raise HandlerResponseError(responded=False)

        await self._call_event(
            CallEvent(
                call_id=self.call_id,
                call_state="running",
                good_until=datetime.now(UTC) + timedelta(seconds=ttl),
            )
        )

    async def progress_from_task(self, task: asyncio.Task, *, cadence: float, ttl: float):
        """Emit progress events at the given cadence until task completes, then await the task."""
        done = None
        fs = [task]

        while not done:
            await self.progress(ttl)
            done, _ = await asyncio.wait(fs, timeout=cadence)

        # Raise if the task raised.
        await task

    async def succeed(self, *, result: V):
        """Emit a success event carrying the final result, completing the long-running call."""
        if not self.responded:
            raise HandlerResponseError(responded=False)

        await self._call_event(
            CallEvent(
                call_id=self.call_id,
                call_state="success",
                good_until=None,
                payload=result,
            ),
            finalize=True,
        )

    async def fail(self, reason: str | None = None):
        """Emit a failure event, completing the long-running call in a failed state."""
        if not self.responded:
            raise HandlerResponseError(responded=False)

        await self._call_event(
            CallEvent(
                call_id=self.call_id,
                call_state="failure",
                good_until=None,
                payload=reason,
            ),
            finalize=True,
        )

    async def _call_event(self, event: CallEvent, finalize: bool = False):
        if self.finalized:
            raise HandlerError("Call has already been finalized")

        self.finalized = finalize

        await self._stream.publish_event(event.model_dump_json().encode())


_pending_failures: set[asyncio.Task] = set()
"""Failure tasks for abandoned calls, held so the event loop cannot collect them early."""


def _failure_task_done(task: asyncio.Task):
    if not task.cancelled() and (error := task.exception()):
        logger.debug(f"Could not fail abandoned call: {error}")

    _pending_failures.discard(task)


# TODO: Fold finalization into the handler coroutine so this callback and its detached task go
#       away. That also closes the race where a task the handler spawned finalizes between the
#       check here and the failure task running. The handler task still needs an owner after
#       that, so teardown can fail in flight calls rather than drop them, and a handler that is
#       cancelled after accepting stops leaving its caller to time out on the event stream.
def _fail_abandoned_call(context: CallContext[Any, Any], handler_task: asyncio.Task):
    """Fail a call whose handler exited without finalizing it.

    The handler task's exception is always retrieved, even when the call is already settled,
    so that asyncio does not report it as never retrieved.
    """
    if handler_task.cancelled():
        return

    error = handler_task.exception()

    if context.finalized:
        if error is not None:
            logger.debug(f"Exception raised after call was finalized: {error}")

        return

    reason = None if error is None else str(error)
    suffix = "" if reason is None else f" with error {reason}"

    logger.debug(f"Failing unfinalized call handler{suffix}")
    task = asyncio.create_task(context.fail(reason))

    _pending_failures.add(task)
    task.add_done_callback(_failure_task_done)


async def run_long_call[R: BaseModel | None, V: BaseModel | None](
    func: LongHandlerFunc[Any, R, V],
    stream: StreamContext,
    call_id: uuid.UUID,
    message: BaseModel | None,
) -> CallReply[R]:
    """Run a long-running handler func and return the reply for the caller.

    The handler keeps running after this returns, publishing progress and end events to the
    entity event stream until the call is finalized.

    Raises:
        HandlerResponseError: if the handler returned without accepting or rejecting.
    """
    # Create the call context object and run the call handler func.
    context: CallContext[V, R] = CallContext(call_id, stream)
    handler_task = asyncio.create_task(func(message, context))

    # Wait for the handler to reply or exit, whichever comes first.
    await asyncio.wait(
        [
            handler_task,
            context.reply,
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if not context.responded:
        # The handler exited without accepting or rejecting. The caller has no call id to follow
        # events by, so the error goes back on the reply. A cancelled handler has no exception of
        # its own to report.
        if not handler_task.cancelled() and (error := handler_task.exception()):
            raise error

        raise HandlerResponseError(responded=False)

    # Once the handler has replied, anything that ends the call is reported on the event stream,
    # including the handler exiting without finalizing it.
    handler_task.add_done_callback(functools.partial(_fail_abandoned_call, context))
    return context.reply.result()


class CallError(Exception):
    """Raised to a caller when a call is rejected or fails."""


class HandlerError(Exception):
    """Raised when a request handler is declared or used in an invalid state."""


class HandlerResponseError(HandlerError):
    def __init__(self, *, responded: bool):
        self.responded = responded
        problem = "has already called" if responded else "did not call"
        super().__init__(f"Call handler {problem} `accept` or `reject`")


@dataclass(frozen=True, eq=False)
class RequestBase[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None](ABC):
    """A declaration of a callable request.

    Use `declare_request` or `declare_long_request` to build one, rather than constructing it
    directly.
    """
    name: str
    """The name of the request."""
    message: type[P] | None
    """The type expected to be sent in the request call."""
    response: type[R] | None
    """The type expected to be received in the call reply."""
    result: type[V] | None
    """The type a caller receives once the call completes."""

    @abstractmethod
    def create_handler(
        self,
        func: RequestHandlerFunc[P, R, V],
        stream: StreamContext | None = None,
    ) -> CallHandler[P]:
        """Build the backend callback for this request.

        Args:
            func: The handler to run for each incoming call.
            stream: The entity event stream, required for a long-running request.
        """


class Request[P: BaseModel | None, R: BaseModel | None](RequestBase[P, R, R]):
    """A request that its handler answers immediately, with a response that is the whole result."""

    @override
    def create_handler(
        self,
        func: RequestHandlerFunc[P, R, R],
        stream: StreamContext | None = None,
    ) -> CallHandler[P]:
        return CallHandler(self.message, functools.partial(run_call, func))


class LongRequest[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None](
    RequestBase[P, R, V]
):
    """A request that keeps running after its handler replies, reporting its result as an event."""

    @override
    def create_handler(
        self,
        func: RequestHandlerFunc[P, R, V],
        stream: StreamContext | None = None,
    ) -> CallHandler[P]:
        """Build the backend callback for this request.

        Args:
            func: The handler to run for each incoming call.
            stream: The entity event stream the handler reports on.

        Raises:
            HandlerError: if no event stream is given.
        """
        if stream is None:
            raise HandlerError("Long-running request requires an event stream")

        return CallHandler(self.message, functools.partial(run_long_call, func, stream))


def declare_request[R: BaseModel | None = None, P: BaseModel | None = None](
    name: str,
    *,
    message: type[P] | None = None,
    response: type[R] | None = None,
) -> Request[P, R]:
    """Declare a request that its handler answers immediately.

    The response is the whole result, so a caller awaiting the call receives it directly. Omit
    the message for a request that carries no arguments, and the response for one that reports
    nothing back.

    Args:
        name: The request name, unique among the requests served by one entity.
        message: The type sent by the caller.
        response: The type returned by the handler.
    """
    return Request(name, message, response, response)


def declare_long_request[
    V: BaseModel | None = None,
    R: BaseModel | None = None,
    P: BaseModel | None = None,
](
    name: str,
    *,
    message: type[P] | None = None,
    response: type[R] | None = None,
    result: type[V] | None = None,
) -> LongRequest[P, R, V]:
    """Declare a request that keeps running after it replies.

    The handler accepts or rejects the call immediately, then reports progress and its final
    result on the entity event stream. A caller awaiting the call receives the result, while
    the response carries whatever the handler needs to report up front.

    Args:
        name: The request name, unique among the requests served by one entity.
        message: The type sent by the caller.
        response: The type the handler replies with when it accepts or rejects the call.
        result: The type carried by the success event, for a request that reports one.
    """
    return LongRequest(name, message, response, result)
