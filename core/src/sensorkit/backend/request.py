# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Coroutine, Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Literal, cast, final, overload, override

from loguru import logger
from pydantic import BaseModel

from sensorkit.backend.base import StreamContext
from sensorkit.backend.event import Event, EventMultiplexer
from sensorkit.common.aio import cleanup_future, scoped_waiter


class Call[R: BaseModel, V: BaseModel = R](Awaitable[V]):
    """An invocation of a simple request."""

    def __init__(self, coro: Coroutine[Any, Any, bytes], response_type: type[R]):
        self._coro = coro
        self._response_type = response_type
        self._future: asyncio.Future[V] = asyncio.get_running_loop().create_future()
        self._got_response = False
        self.response: R | None = None

        self._future.add_done_callback(cleanup_future)

    @final
    async def _invoke(self, timeout: float) -> R:
        # Send request and await response.
        async with asyncio.timeout(timeout):
            recv = await self._coro

            if self._response_type:
                self.response = self._response_type.model_validate_json(recv)

            self._got_response = True

        return self.response

    async def invoke(self, timeout: float = 5.0):
        """Send the request and populate the future with the parsed response."""
        try:
            response = await self._invoke(timeout)
            self._future.set_result(response)
            return response
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


class CallHandler[P: BaseModel | None, R: BaseModel | None]:
    """Callable that handles incoming simple requests."""

    def __init__(
        self,
        request: Request[P, R, R],
        func: HandlerFunc[P, R],
    ):
        self._request = request
        self._handler_func = func

    def _run_handler(self, data: P):
        return self._handler_func(data)

    async def __call__(self, payload: bytes) -> bytes:
        # Parse the input payload.
        payload_type = self._request.payload
        data: P = (
            payload_type.model_validate_json(payload)
            if self._request.payload is not None
            else None
        )

        # Run the user handler func and get the response object.
        response = await self._run_handler(data)

        # Serialize the response payload.
        response_payload = (
            response.model_dump_json().encode()
            if response is not None
            else b""
        )

        return response_payload


class ExtendedResponse(BaseModel):
    """A long-running request response."""
    call_id: uuid.UUID = None
    call_state: Literal["running", "success", "failure"] = "running"


class CallEvent(Event, ExtendedResponse):
    """An event that occurs in the context of a long-running request."""
    good_until: datetime | None
    payload: Any = None


class ExtendedCall[R: ExtendedResponse, V: BaseModel](Call[R, V]):
    """A Call extension that supports long-running requests."""

    def __init__(
        self,
        coro: Coroutine[Any, Any, bytes],
        response_type: type[R],
        result_type: type[V],
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

        # Execute the initial request-response communication.
        try:
            response = await self._invoke(timeout)
        except (asyncio.CancelledError, Exception) as e:
            self._future.set_exception(e)
            raise

        # Start a background task to receive response progress and end events.
        started = asyncio.Event()

        self._task = asyncio.create_task(
            self._await_response_events(
                context,
                queue,
                response.call_id,
                started,
                timeout,
            )
        )

        def extended_call_done(t: asyncio.Task):
            # Note we must check the task exception even if the future is already done to avoid
            # leaks and warnings.
            if t.cancelled():
                self._future.cancel()
            elif e := t.exception():
                if not self._future.done():
                    self._future.set_exception(t.exception())
            elif not self._future.done():
                self._future.set_result(t.result())

        self._task.add_done_callback(extended_call_done)

        # Wait until the first event is received or the task ends for some reason, whichever
        # comes first.
        async with scoped_waiter(started.wait()) as wait:
            await asyncio.wait(
                [wait, self._task],
                return_when=asyncio.FIRST_COMPLETED,
            )

        return response

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


type ExtendedHandlerFunc[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None] = (
    Callable[
        [P, CallContext[R, V]],
        Coroutine[Any, Any, None]
    ]
)


class CallContext[R: ExtendedResponse | None, V: BaseModel | None]:
    """Context for an incoming extended request currently being handled."""

    def __init__(self, call_id: uuid.UUID, stream: StreamContext):
        self.call_id = call_id
        self._stream = stream
        self.response = asyncio.get_running_loop().create_future()
        self.finalized = False

    def accept(self, *, response: R):
        """Mark the call as accepted and set the initial response, allowing progress events to follow."""
        if self.response.done():
            raise CallHandlerResponseError(responded=True)

        if response is None:
            response = ExtendedResponse()

        response.call_id = self.call_id
        response.call_state = "running"
        self.response.set_result(response)

    def reject(self, *, response: R):
        """Reject the call immediately, returning a failure response without further events."""
        if self.response.done():
            raise CallHandlerResponseError(responded=True)

        if response is None:
            response = ExtendedResponse()

        response.call_id = self.call_id
        response.call_state = "failure"
        self.response.set_result(response)

    async def progress(self, ttl: float, payload: Any = None):
        """Emit a progress event, extending the caller's deadline by ttl seconds."""
        if not self.response.done():
            raise CallHandlerResponseError(responded=False)

        await self._call_event(
            CallEvent(
                call_id=self.call_id,
                call_state="running",
                good_until=datetime.now(UTC) + timedelta(seconds=ttl),
                payload=payload,
            )
        )

    async def progress_from_task(
        self,
        task: asyncio.Task,
        *,
        cadence: float,
        ttl: float,
        payload_func: Callable[[], Any] | None = None,
    ):
        """Emit progress events at the given cadence until task completes, then await the task."""
        done = None
        fs = [task]

        while not done:
            await self.progress(ttl, payload_func() if payload_func else None)
            done, _ = await asyncio.wait(fs, timeout=cadence)

        # Raise if the task raised.
        await task

    async def succeed(self, *, result: V):
        """Emit a success event carrying the final result, completing the extended call."""
        if not self.response.done():
            raise CallHandlerResponseError(responded=False)

        if self.response.result().call_state == "failure":
            raise CallHandlerError("Cannot send success result for rejected call")

        await self._call_event(
            CallEvent(
                call_id=self.call_id,
                call_state="success",
                good_until=None,
                payload=result,
            ),
            finalize=True,
        )

    async def fail(self, payload: Any = None):
        """Emit a failure event, completing the extended call in a failed state."""
        if not self.response.done():
            raise CallHandlerResponseError(responded=False)

        await self._call_event(
            CallEvent(
                call_id=self.call_id,
                call_state="failure",
                good_until=None,
                payload=payload,
            ),
            finalize=True,
        )

    async def _call_event(self, event: CallEvent, finalize: bool = False):
        if self.finalized:
            raise CallHandlerError("Call has already been finalized")

        self.finalized = finalize

        await self._stream.publish_event(event.model_dump_json().encode())


class ExtendedCallHandler[P: BaseModel | None, R: ExtendedResponse, V: BaseModel](
    CallHandler[P, R]
):
    """Callable that handles incoming long-running requests."""

    def __init__(
        self,
        request: Request[P, R, V],
        func: ExtendedHandlerFunc[P, R, V],
        stream: StreamContext,
    ):
        super().__init__(request, func)  # noqa
        self._handler_func = func
        self._stream = stream

    @override
    async def _run_handler(self, data: P):
        # Create the call context object and run the call handler func.
        context = CallContext(uuid.uuid1(), self._stream)
        handler_task = asyncio.create_task(self._handler_func(data, context))

        # Handle the initial response and error paths.
        await asyncio.wait(
            [
                handler_task,
                context.response,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if handler_task.done():
            # The handler func exited already. If it raised, fail.
            if e := handler_task.exception():
                if not context.finalized:
                    await context.fail(str(e))

                logger.warning(f"Exception in call handler: {e}")
                raise e

        if not context.response.done():
            # The handler func did not call `accept` or `reject`.
            await context.fail()
            raise CallHandlerResponseError(responded=False)

        # Get the initial response object.
        response = context.response.result()
        response.call_id = context.call_id

        if response.call_state == "failure":
            # The handler rejected the request. Make sure a fail event is sent.
            if not context.finalized:
                await context.fail("Request rejected")

        # Ensure exceptions raised in the handler are propagated.
        def call_handler_done(_):
            if not context.finalized and not handler_task.cancelled():
                payload = None

                if e := handler_task.exception():
                    payload = str(e)

                logger.debug(f"failing unfinalized call handler {e}")
                _t = asyncio.create_task(context.fail(payload))

        handler_task.add_done_callback(call_handler_done)
        return response


class CallError(Exception):
    """Raised when a call method fails."""


class CallHandlerError(CallError):
    """Raised when a call method is called in an invalid state."""


class CallHandlerResponseError(CallHandlerError):
    def __init__(self, *, responded: bool):
        self.responded = responded
        problem = "has already called" if responded else "did not call"
        super().__init__(f"Call handler {problem} `accept` or `reject`")


@dataclass
class Request[P: BaseModel | None, R: BaseModel | None, V: BaseModel | None]:
    """A declaration of a callable request.

    If the response type is `ExtendedResponse` or a subclass of it, the request is considered
    "extended".
    """
    name: str
    """The name of the request."""
    payload: type[P]
    """The type expected to be sent in the request call."""
    response: type[R]
    """The type expected to be received in the initial call response."""
    result: type[V]
    """The type of the payload of the success ResponseEvent in an extended request.
    Always equal to the response type for simple requests."""

    def is_extended(self):
        """Return True if this is a long-running request using ExtendedResponse."""
        return self.response and issubclass(self.response, ExtendedResponse)

    def create_handler(
        self,
        func: HandlerFunc[P, R] | ExtendedHandlerFunc[P, R, V],
        stream: StreamContext | None = None,
    ) -> CallHandler[P, R]:
        """Construct the appropriate CallHandler or ExtendedCallHandler for this request."""
        if self.is_extended():
            return ExtendedCallHandler(self, func, stream)
        else:
            return CallHandler(self, func)

    @classmethod
    @overload
    def define[R: ExtendedResponse, P: BaseModel | None = None](
        cls,
        name: str,
        *,
        payload: type[P] | None = None,
        response: type[R],
    ) -> Request[P, R, None]:
        """Define a long-running request that uses the entity event stream to track progress."""

    @classmethod
    @overload
    def define[V: BaseModel, R: ExtendedResponse = ExtendedResponse, P: BaseModel | None = None](
        cls,
        name: str,
        *,
        payload: type[P] | None = None,
        response: type[R] = ExtendedResponse,
        result: type[V],
    ) -> Request[P, R, V]:
        """Define a long-running request that uses the entity event stream to track progress."""

    @classmethod
    @overload
    def define[R: BaseModel | None = None, P: BaseModel | None = None](
        cls,
        name: str,
        *,
        payload: type[P] | None = None,
        response: type[R] | None = None,
    ) -> Request[P, R, R]:
        """Define a simple request that requires an immediate response."""

    @classmethod
    def define(
        cls,
        name: str,
        payload = None,
        response = None,
        result = None,
    ):
        if response and issubclass(response, ExtendedResponse):
            # This is an extended request. No checks or defaults are needed in this case.
            return cls(name, payload, response, result)
        elif result:
            # Here a result type is given but no response type. In this case, having a result means
            # this is an extended request, so we default to the bare ExtendedResponse type.
            if response is not None:
                raise TypeError("simple request cannot have a result type")

            return cls(name, payload, ExtendedResponse, result)
        else:
            # Simple requests always treat their response as the result.
            return cls(name, payload, response, result or response)
