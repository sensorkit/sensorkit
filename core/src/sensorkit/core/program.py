# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
from abc import abstractmethod
from collections.abc import AsyncIterator, Generator
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Literal

import uuid_utils.compat as uuid
from intervaltree import Interval
from loguru import logger
from pydantic import BaseModel, Field, model_validator

from sensorkit.backend.base import KeyNotFound
from sensorkit.backend.event import Event
from sensorkit.backend.request import CallError, declare_long_request, declare_request
from sensorkit.common.aio import AsyncObserver, cleanup_future
from sensorkit.common.keyword import declare_keyword
from sensorkit.core.controller import TaskExecutionState
from sensorkit.core.entity import EntityClient, EntityInterface, EntityRef
from sensorkit.core.task import TaskContextMap

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit
    from sensorkit.core.executor import TaskFactoryFunc

type TaskingStage = Literal["factory", "dispatch", "execution", "post_task"]
"""Operations within a tasking session."""

type TaskingFault = Literal["controller", "program"]
"""Fault origins for program tasking."""


class OfferInterval(Interval):
    """An Interval subclass with typed begin/end datetimes used to represent a program offer window."""

    begin: datetime
    end: datetime
    data: Any = None


@declare_keyword
class ProgramOffering(BaseModel):
    """Keyword publishing the set of offer windows during which this program can task a controller."""

    offer_windows: list[OfferInterval]


class ProgramEnableState(Event):
    """Event indicating the Program enable state has changed."""
    enabled: bool = False
    controller: str | None = None


class ProgramActiveState(Event):
    """Event indicating the Program active state has changed."""
    active: bool
    origin: Literal["request", "init", "error"]
    stopping: bool = False
    contexts: TaskContextMap = Field(default_factory=TaskContextMap)


class TaskingLoopState(BaseModel):
    """Current operation and terminal fault for a tasking session.

    A fault retains the failed stage while the session ends.
    """

    session_id: uuid.UUID
    stage: TaskingStage
    task_type: str | None = None
    """Task type supplied by the factory."""
    fault: TaskingFault | None = None
    error: str | None = None


class ProgramTaskingState(Event):
    """Program session progress and controller execution reports."""

    loop_state: TaskingLoopState | None = None
    """Known tasking session, if any."""
    execution_state: TaskExecutionState | None = None
    """Controller report for the accepted execution.

    Cleared before each factory attempt and retained when the session ends.
    """

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_executing_task(cls, data: Any) -> Any:
        """Normalize legacy tasking reports.

        Full executions retain their reported running state. Bare task IDs provide no
        execution state. Legacy reports carry no session identity.
        """
        if not isinstance(data, dict) or not {"executing_task", "executing_task_id"} & data.keys():
            return data

        legacy = data.get("executing_task")
        data = {k: v for k, v in data.items() if k not in ("executing_task", "executing_task_id")}

        if legacy is not None and "execution_state" not in data:
            data["execution_state"] = {"executing": True, "execution": legacy}

        return data


class ProgramState(BaseModel):
    """Internal state snapshot of a Program."""
    enable_state: ProgramEnableState
    active_state: ProgramActiveState
    tasking_state: ProgramTaskingState

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_tasking_state(cls, data: Any) -> Any:
        """Upgrade KV snapshots persisted before `tasking_status` was renamed to `tasking_state`.

        Versions predating the Task/TaskExecution split stored the program's executing task under a
        `tasking_status` key (a `ProgramTaskingStatus` carrying `executing_task_id`); this
        version expects `tasking_state`. Renaming the key here lets `ProgramTaskingState`'s own
        `before` validator finish upgrading the inner shape, so a program starting against a NATS
        broker holding older state no longer raises a `ValidationError`. A snapshot missing both
        keys entirely is treated as idle.
        """
        if not isinstance(data, dict) or "tasking_state" in data:
            return data

        data = dict(data)
        data["tasking_state"] = data.pop("tasking_status", {})
        return data


class ProgramEnableStateRequest(BaseModel):
    """Request that a Program enable or disable task sourcing.

    The `controller` field specifies the name of the target Controller to task. It must be
    specified before activating the Program, and some Program implementations may require that it
    be specified whenever `enable` is True.
    """
    enable: bool
    controller: str | None


set_enable_state_request = declare_request(
    "set_enable_state",
    message=ProgramEnableStateRequest,
)
"""Control the enable state of a Program."""


class ProgramActiveStateRequest(BaseModel):
    """Request that a Program activate or deactivate tasking of the target Controller."""
    action: Literal["start", "stop", "abort"]
    contexts: TaskContextMap = Field(default_factory=TaskContextMap)
    session_id: uuid.UUID | None = None
    """Target session for stop or abort, or None for the current session.

    An ended target succeeds without affecting a later session.
    """

    def active_state(self):
        """Return True if the requested action is 'start', False otherwise."""
        return True if self.action == "start" else False


class ProgramActiveStateResponse(BaseModel):
    """Session selected by a successful start acceptance."""

    session_id: uuid.UUID


set_active_state_request = declare_long_request(
    "set_active_state",
    message=ProgramActiveStateRequest,
    response=ProgramActiveStateResponse,
)
"""Control the active state of a Program."""


class TaskingError(Exception):
    """Program tasking startup failed or could not be confirmed."""

    def __init__(self, *args, session_id: uuid.UUID):
        super().__init__(*args)
        self.session_id = session_id
        """Server-selected session."""


class TaskingStartupFailed(TaskingError):
    """The program explicitly returned a startup failure for an accepted session."""


class ProgramTaskingMonitor:
    """Observe progress and retain the outcome of one program tasking session.

    Ordered program tasking events associate untagged events with their preceding session.
    The selected session's fault and ending remain available after its live report is cleared.

    Construction starts buffering events. Bind to the accepted session before consuming them.
    Use a context manager or close the monitor to release observation.
    """

    def __init__(self, program: ProgramClient):
        """Buffer program events before reading initial state or starting tasking.

        Args:
            program: Program with an active event subscription.
        """
        self._session_id: uuid.UUID | None = None
        self.loop_state: TaskingLoopState | None = None
        """Latest progress with the first session fault retained."""
        self.execution_state: TaskExecutionState | None = None
        """Controller report for the selected session's execution."""
        self.ending: ProgramActiveState | None = None
        """Inactive event ending the selected session."""
        self._subscription = program.get_event_mux().subscribe(
            ProgramTaskingState, ProgramActiveState
        )
        self._current: uuid.UUID | None = None
        self._current_after = 0

    def bind(self, session_id: uuid.UUID, initial: ProgramState | None = None):
        """Select a session and seed observation before processing buffered events.

        Args:
            session_id: Session identified by the program.
            initial: Program state observed before starting tasking.

        Raises:
            RuntimeError: If the monitor is already bound.
        """
        if self._session_id is not None:
            raise RuntimeError("Program tasking monitor is already bound")

        self._session_id = session_id

        if initial is not None:
            self._observe(initial.tasking_state)

    @property
    def session_id(self) -> uuid.UUID:
        """The selected session.

        Raises:
            RuntimeError: If the monitor is not bound.
        """
        if self._session_id is None:
            raise RuntimeError("Program tasking monitor is not bound")

        return self._session_id

    @property
    def fault(self) -> TaskingLoopState | None:
        """Retained terminal fault, if reported."""
        state = self.loop_state
        return state if state is not None and state.fault is not None else None

    def read_events(self) -> list[ProgramTaskingState]:
        """Process buffered events and return the selected session's tasking states.

        Raises:
            RuntimeError: If the monitor is not bound.
        """
        if self._session_id is None:
            raise RuntimeError("Program tasking monitor is not bound")

        states = []

        for event in self._subscription.drain():
            if (state := self._observe(event)) is not None:
                states.append(state)

        return states

    async def events(self) -> AsyncIterator[ProgramTaskingState]:
        """Yield selected-session tasking states until a fault or ending is observed.

        Consume with one iterator or waiter at a time. Cancellation leaves unread events
        available for cleanup. Initial snapshot state is not yielded.

        Raises:
            RuntimeError: If the monitor is not bound.
            QueueShutDown: If observation closes before a session outcome is known.
        """
        if self._session_id is None:
            raise RuntimeError("Program tasking monitor is not bound")

        if self.fault is not None or self.ending is not None:
            return

        async for event in self._subscription:
            if (state := self._observe(event)) is not None:
                yield state

            if self.fault is not None or self.ending is not None:
                return

        raise asyncio.QueueShutDown("Program tasking monitor closed before its outcome")

    async def wait(self):
        """Wait for a session fault or ending.

        Cancellation leaves unread events available for cleanup.

        Raises:
            RuntimeError: If the monitor is not bound.
            QueueShutDown: If the monitor is closed first.
        """
        async for _ in self.events():
            pass

    def close(self):
        """Close observation and wake pending waiters."""
        self._subscription.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _observe(self, event: Event) -> ProgramTaskingState | None:
        """Update the selected session from one ordered event."""
        if self.ending is not None:
            return

        match event:
            case ProgramTaskingState(loop_state=TaskingLoopState() as state):
                self._current = state.session_id
                self._current_after = event.event_id.int

                if state.session_id == self.session_id:
                    self._report(state, event.execution_state)
                    return event
            case ProgramTaskingState() if (
                self._current == self.session_id and event.event_id.int > self._current_after
            ):
                # Keep the final execution report after the live session clears.
                self.execution_state = event.execution_state
                return event
            case ProgramActiveState(active=True, stopping=False) if (
                event.event_id.int > self._current_after
            ):
                # The next report identifies the newly active session.
                self._current = None
                self._current_after = event.event_id.int
            case ProgramActiveState(active=False) if (
                self._current == self.session_id and event.event_id.int > self._current_after
            ):
                self.ending = event

    def _report(self, state: TaskingLoopState, execution_state: TaskExecutionState | None):
        if self.fault is None:
            self.loop_state = state

        self.execution_state = execution_state


class ProgramActivation:
    """Start program tasking or reuse a matching active session.

    Await this object to prepare and send the start request, then receive the session
    monitor after startup is confirmed. Repeated sends and awaits use the same request.

    Call `prepare()` before checking conditions that must hold at dispatch.
    Once prepared, `send()` sends the request before its first suspension.
    """

    def __init__(self, program: ProgramClient, contexts: TaskContextMap):
        """Create an activation without subscribing or sending a request.

        Args:
            program: The program to activate.
            contexts: Task contexts supplied to the program.
        """
        self.program = program
        self.contexts = contexts
        self.monitor: ProgramTaskingMonitor | None = None
        """The accepted session's monitor, retained if startup fails or cannot be confirmed."""
        self._monitor: ProgramTaskingMonitor | None = None
        self._initial: ProgramState | None = None
        self._prepared = False
        self._start: asyncio.Task[ProgramTaskingMonitor] | None = None

    async def prepare(self):
        """Subscribe to program events and read state without sending the start request.

        If preparation fails or is canceled, it releases its subscription and can be retried.

        Raises:
            RuntimeError: If preparation is already in progress.
        """
        if self._prepared:
            return

        if self._monitor is not None:
            raise RuntimeError("Activation is already preparing")

        await self.program.subscribe_events()
        self._monitor = ProgramTaskingMonitor(self.program)

        try:
            # Buffer events received while reading state and starting tasking.
            with contextlib.suppress(KeyNotFound):
                self._initial = await self.program.kv_get_model(ProgramState)
        except BaseException:
            self._monitor.close()
            self._monitor = None
            raise

        self._prepared = True

    async def send(self) -> ProgramTaskingMonitor:
        """Send the start request and return the session monitor after startup is confirmed.

        Preparation runs automatically if needed. Canceling a waiter leaves the request
        running. The caller must close the monitor when observation ends.

        Raises:
            CallRejected: If the program rejects activation.
            TaskingStartupFailed: If the program explicitly returns a startup failure.
            TaskingError: If startup cannot be confirmed after acceptance.
            CallError: If the acceptance response omits the session ID.
        """
        if self._start is None:
            # A prepared request goes out before this call first suspends.
            self._start = asyncio.eager_task_factory(
                asyncio.get_running_loop(), self._send_start_request()
            )
            self._start.add_done_callback(cleanup_future)

        return await asyncio.shield(self._start)

    def __await__(self) -> Generator[Any, None, ProgramTaskingMonitor]:
        return self.send().__await__()

    def close(self):
        """Release the event subscription without stopping program tasking."""
        if self._monitor is not None:
            self._monitor.close()

    async def _send_start_request(self) -> ProgramTaskingMonitor:
        await self.prepare()

        call = self.program.call(
            set_active_state_request,
            ProgramActiveStateRequest(action="start", contexts=self.contexts),
        )

        try:
            response = await call.invoke()

            if response is None:
                raise CallError(f"{self.program.entity} accepted tasking without a session")
        except BaseException:
            self._monitor.close()
            raise

        self._monitor.bind(response.session_id, self._initial)
        self.monitor = self._monitor

        try:
            await call
        except CallError as e:
            raise TaskingStartupFailed(
                f"{self.program.entity} failed to start tasking ({e})",
                session_id=response.session_id,
            ) from e
        except Exception as e:
            raise TaskingError(
                f"{self.program.entity} did not confirm tasking ({e})",
                session_id=response.session_id,
            ) from e

        return self.monitor


class ProgramClient(EntityClient):
    """Object that exposes client-side functionality of a Program."""

    async def enable(self, target_controller: str):
        """Request that the Program enable task sourcing for the target Controller."""
        return await self.call(
            set_enable_state_request,
            ProgramEnableStateRequest(
                enable=True,
                controller=target_controller,
            )
        )

    async def disable(self):
        """Request that the Program disable task sourcing."""
        return await self.call(
            set_enable_state_request,
            ProgramEnableStateRequest(
                enable=False,
                controller=None,
            )
        )

    def start_tasking(self, contexts: TaskContextMap | None = None) -> ProgramActivation:
        """Create an awaitable request to start or reuse program tasking.

        Await it to receive the accepted session's monitor after startup is confirmed.
        A matching active session keeps its identity and current execution.

        Args:
            contexts: Task contexts supplied to the program.
        """
        return ProgramActivation(self, contexts or TaskContextMap())

    async def stop_tasking(self, session_id: uuid.UUID | None = None):
        """Stop producing tasks and wait for the program's current controller task to end.

        Args:
            session_id: The session to stop, or None for the current session.

        Raises:
            CallError: If the program cannot confirm that its controller task ended.
        """
        await self.call(
            set_active_state_request,
            ProgramActiveStateRequest(action="stop", session_id=session_id),
        )

    async def abort_tasking(self, session_id: uuid.UUID | None = None):
        """Stop producing tasks, abort the program's current controller task, and await its end.

        Args:
            session_id: The session to abort, or None for the current session.

        Raises:
            CallError: If the program cannot confirm that its controller task ended.
        """
        await self.call(
            set_active_state_request,
            ProgramActiveStateRequest(action="abort", session_id=session_id),
        )

    async def wait_until_tasking_stops(self):
        """
        Wait until the Program tasking loop stops.

        Returns the corresponding ProgramActiveState event, if any.
        """
        # TODO: This needs to detect if the hosting service goes down and raise if it doesn't
        #       come back and publish new state within a grace period.
        stream = await self.tasking_change_events()
        state = await self.kv_get_model(ProgramState)

        if state.active_state.active:
            async for event in stream:
                if not event.active:
                    return event

        return None

    def tasking_change_events(self):
        """Return an async generator that yields ProgramActiveState events as tasking starts or stops."""
        return self.monitor_event(ProgramActiveState)

    @functools.cache
    def monitor_enable_state(self):
        """Return (creating if needed) a ProgramStateMonitor tracking this program's enable state."""
        monitor = ProgramStateMonitor(self)
        monitor.start()
        return monitor


class ProgramRef(EntityRef[ProgramClient]):
    """A serializable reference to a program client."""

    def _get_client(self, kit: SensorKit) -> ProgramClient:
        return kit.program(self.name)


class ControllerOffers:
    """Monitor published offers for all Programs associated with a Controller."""

    def __init__(self, controller: str, program_discovery: ProgramDiscovery):
        self.controller = controller
        self.program_discovery = program_discovery
        self._monitors: dict[str, asyncio.Task] = {}
        self._offers: dict[str, list[OfferInterval]] = {}

    async def start(self, *, task_group: Any = asyncio):
        """Start the program-offer discovery loop, waiting until the first update is received."""
        self._task_group = task_group

        # Begin discovering controller offers, making sure to wait for the first set of updates.
        ready = asyncio.Event()
        self._discover_task = task_group.create_task(self._discover_programs(ready))
        await ready.wait()

    async def _discover_programs(self, initial_update: asyncio.Event):
        prev = set()

        async for programs in self.program_discovery.controller_programs(self.controller):
            events = []

            for remove in prev.difference(programs):
                self._monitors[remove].cancel()
                del self._monitors[remove]

            for add in programs.difference(prev):
                assert add not in self._monitors
                events.append(asyncio.Event())
                self._monitors[add] = self._task_group.create_task(
                    self._monitor_program(add, events[-1])
                )

            await asyncio.gather(*(event.wait() for event in events), return_exceptions=True)
            initial_update.set()
            prev = programs

    async def _monitor_program(self, program: str, initial_update: asyncio.Event):
        logger.debug(f"starting offer monitor for program {program}")
        self._offers[program] = []

        try:
            # Suppress propagation of cancellation to the parent task group.
            with contextlib.suppress(asyncio.CancelledError):
                # Get a stream of offers for the target Program.
                client = self.program_discovery.client.program(program)
                stream = await client.monitor(ProgramOffering)

                initial_update.set()

                async for _, offering in stream:
                    self._offers[program] = sorted(offering.offer_windows)
        finally:
            initial_update.set()
            del self._offers[program]
            logger.debug(f"cancelled offer monitor for program {program}")

    def get_offers(self):
        """Return a snapshot of the current offer windows keyed by program name.

        Each program's offer windows are sorted in ascending order.
        """
        return self._offers.copy()


class ProgramDiscovery:
    """Discover Programs and their associated Controllers."""

    def __init__(self):
        self._enabled_programs: AsyncObserver[frozenset[str]] = AsyncObserver(frozenset())
        self._known_programs: AsyncObserver[frozenset[str]] = AsyncObserver(frozenset())
        self._controllers: dict[str, AsyncObserver[frozenset[str]]] = collections.defaultdict(
            lambda: AsyncObserver(frozenset())
        )
        self._monitor_tasks: dict[str, asyncio.Task] = {}
        self._discover_task: asyncio.Task | None = None

    async def start(self, client: SensorKit):
        """Begin monitoring the backend for program enable-state changes, waiting for the first update."""
        self.client = client

        # Begin discovering enabled programs, making sure to wait for the first set of updates.
        ready = asyncio.Event()
        self._discover_task = asyncio.create_task(self._discover_programs(ready))
        await ready.wait()

    async def stop(self):
        """Cancel discovery and every per-program enable-state monitor it started."""
        if self._discover_task is None:
            return

        self._discover_task.cancel()

        for task in self._monitor_tasks.values():
            task.cancel()

        await asyncio.gather(
            self._discover_task, *self._monitor_tasks.values(), return_exceptions=True
        )

        self._monitor_tasks.clear()
        self._discover_task = None

    async def _discover_programs(self, initial_update: asyncio.Event):
        tasks = self._monitor_tasks
        kv = self.client.backend.key_value()

        # Programs present in the initial snapshot. We're "ready" once each of them
        # has been discovered and reported its initial enable state — or immediately
        # if there are none.
        pending = {
            str(entry.key.entity())
            for entry in await kv.get_all(deep=True)
            if entry.key.prop == "ProgramState"
        }
        if not pending:
            initial_update.set()

        # Monitor all registered Programs.
        # FIXME: Need an alternate way to do this without firehose. Cannot assume the backend
        #        allows prefix wildcards to capture '*.ProgramState' (and NATS, in fact, does not).
        #        This most likely means we need to define a dedicated KV prefix for system info,
        #        where we can define keys to explicitly indicate existence, like EntityLease.
        monitor = await kv.monitor_all(deep=True)

        async for entry in monitor:
            if entry.key.prop != "ProgramState":
                continue

            program = str(entry.key.entity())
            client = self.client.program(program)
            events = []

            if entry.deleted():
                # Remove the monitor task associated with the offline program, if any.
                if program in tasks:
                    task = tasks.pop(program)
                    task.cancel()
                    self._known_programs.notify(frozenset(tasks.keys()))
            elif program not in tasks:
                # Start a new enable state monitor for the newly discovered program.
                events.append(asyncio.Event())
                tasks[program] = asyncio.create_task(self._monitor_task(client, events[-1]))
                self._known_programs.notify(frozenset(tasks.keys()))

            # Wait for the initial enablement status of each discovered program.
            await asyncio.gather(*(event.wait() for event in events), return_exceptions=True)

            # Signal ready once every program from the initial snapshot has been seen.
            pending.discard(program)
            if not pending:
                initial_update.set()

    async def _monitor_task(self, client: ProgramClient, initial_update: asyncio.Event):
        monitor = client.monitor_enable_state()
        program = str(client.entity)

        try:
            async for state in monitor.observe():
                observers = [self._enabled_programs]
                logger.debug(f"discovery for {program} observed {state=}")

                if state:
                    observers.append(self._controllers[state.controller])

                for observer in observers:
                    if state and state.enabled:
                        new_set = observer.value.union({program})
                    else:
                        new_set = observer.value.difference({program})

                    observer.notify(new_set)

                initial_update.set()
        finally:
            monitor.stop()

    def enabled_programs(self):
        """Return an async generator yielding the current set of enabled program names on each change."""
        return self._enabled_programs.consume(initial_value=True)

    def known_programs(self):
        """Return an async generator yielding the current set of all known program names on each change."""
        return self._known_programs.consume(initial_value=True)

    def controller_programs(self, controller: str):
        """Return an async generator yielding the set of programs targeting the given controller on each change."""
        return self._controllers[controller].consume(initial_value=True)


class ProgramStateMonitor:
    """Monitor a Program's effective enable-state, taking into account the entity liveness."""

    def __init__(self, client: ProgramClient):
        self.client = client
        self.online = False
        self.enable_state: ProgramEnableState | None = None
        self._observer: AsyncObserver[ProgramEnableState | None] = AsyncObserver(None)

    def start(self):
        """Start background tasks that monitor program liveness and enable-state."""
        self._tasks = (
            asyncio.create_task(self._monitor_liveness()),
            asyncio.create_task(self._monitor_state()),
        )

    def stop(self):
        """Cancel the background monitoring tasks."""
        for t in self._tasks:
            t.cancel()

    def observe(self):
        """Return an async generator yielding the current ProgramEnableState (or None if offline) on each change."""
        return self._observer.consume(initial_value=True)

    def _notify(self):
        if not self.online:
            self._observer.notify(None)
        else:
            self._observer.notify(self.enable_state)

    async def _monitor_liveness(self):
        async for is_online in self.client.observe_online_state():
            self.online = is_online
            self._notify()

    async def _monitor_state(self):
        program = str(self.client.entity)

        try:
            stream = await self.client.monitor_event(ProgramEnableState)
            initial_state = await self.client.kv_get_model(ProgramState)
        except (asyncio.CancelledError, Exception) as e:
            logger.debug(f"Failed to monitor program {program}: {e}")
            raise

        logger.debug(f"monitoring discovered program {program}")
        self.enable_state = initial_state.enable_state
        self._notify()

        async for event in stream:
            if event.timestamp() > self.enable_state.timestamp():
                self.enable_state = event
                self._notify()


class ProgramInterface(EntityInterface):
    """Interface describing an implementation of a program."""

    @abstractmethod
    def on_enable(self, func: Callable[[], None]):
        """Register a callback to invoke when the program is enabled."""
        ...

    @abstractmethod
    def on_disable(self, func: Callable[[], None]):
        """Register a callback to invoke when the program is disabled."""
        ...

    @abstractmethod
    def task_factory(self, func: TaskFactoryFunc) -> TaskFactoryFunc:
        """Register the task factory function that the program calls to generate tasks."""
        ...

    @abstractmethod
    def get_offers(self) -> list[Any]:
        """Return the current list of offer windows."""
        ...

    @abstractmethod
    async def publish_offers(self):
        """Publish the current offer windows to the entity's keyword stream."""
        ...

    @abstractmethod
    def add_offer(self, start: datetime, end: datetime, obj: Any = None):
        """Add an offer window covering the given time range."""
        ...

    @abstractmethod
    def remove_offer(self, start: datetime, end: datetime, obj: Any = None):
        """Remove the offer window covering the given time range."""
        ...

    @abstractmethod
    def clear_offers(self):
        """Remove all offer windows."""
        ...
