# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
from abc import abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Literal

from intervaltree import Interval, IntervalTree
from loguru import logger
from pydantic import BaseModel, Field, model_validator

from sensorkit.backend.event import Event
from sensorkit.backend.request import ExtendedResponse, Request
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.keyword import declare_keyword
from sensorkit.core.entity import EntityClient, EntityInterface, EntityRef
from sensorkit.core.executor import TaskFactoryFunc
from sensorkit.core.task import TaskContextMap, TaskExecution

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit


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


class ProgramTaskingState(Event):
    """Event indicating the Task a Program is currently executing, if any.

    Carries the full `TaskExecution` envelope (not just an id) so observers see the same task shape
    the controller publishes on its `TaskExecutionState`, including the controller-minted
    `task_id`. `None` while the program is between tasks or idle.
    """
    executing_task: TaskExecution | None = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_executing_task(cls, data: Any) -> Any:
        """Upgrade state persisted before the executing task carried a full `TaskExecution`.

        The pre-split `ProgramTaskingStatus` recorded only `executing_task_id` — a bare task id.
        A bare id cannot be rehydrated into a `TaskExecution` envelope, so a legacy id collapses to
        `executing_task=None` (equivalent to "idle"); observers of the old shape only ever saw the
        id, which the program re-publishes the moment it next dispatches a task.
        """
        if not isinstance(data, dict) or "executing_task" in data:
            return data

        if "executing_task_id" in data:
            data = {k: v for k, v in data.items() if k != "executing_task_id"}
            data["executing_task"] = None

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


set_enable_state_request = Request.define(
    "set_enable_state",
    payload=ProgramEnableStateRequest,
)
"""Control the enable state of a Program."""


class ProgramActiveStateRequest(BaseModel):
    """Request that a Program activate or deactivate tasking of the target Controller."""
    action: Literal["start", "stop", "abort"]
    contexts: TaskContextMap = Field(default_factory=TaskContextMap)

    def active_state(self):
        """Return True if the requested action is 'start', False otherwise."""
        return True if self.action == "start" else False


class ProgramActiveStateResult(BaseModel):
    """Result of a request to change the Program active state."""
    success: bool


set_active_state_request = Request.define(
    "set_active_state",
    payload=ProgramActiveStateRequest,
    response=ExtendedResponse,
)
"""Control the active state of a Program."""


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

    async def start_tasking(self, contexts: TaskContextMap | None = None):
        """Request that the Program begin tasking the target Controller."""
        await self.call(
            set_active_state_request,
            ProgramActiveStateRequest(action="start", contexts=contexts or TaskContextMap()),
        )

    async def stop_tasking(self):
        """Request that the Program stop tasking and wait for any in flight task to complete."""
        await self.call(
            set_active_state_request,
            ProgramActiveStateRequest(action="stop"),
        )

    async def abort_tasking(self):
        """Request that the Program stop tasking and abort any in flight task immediately."""
        await self.call(
            set_active_state_request,
            ProgramActiveStateRequest(action="abort"),
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
        self._offers: dict[str, IntervalTree] = {}

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
        self._offers[program] = IntervalTree()

        try:
            # Suppress propagation of cancellation to the parent task group.
            with contextlib.suppress(asyncio.CancelledError):
                # Get a stream of offers for the target Program.
                client = self.program_discovery.client.program(program)
                stream = await client.monitor(ProgramOffering)

                initial_update.set()

                async for _, offering in stream:
                    self._offers[program] = IntervalTree(offering.offer_windows)
        finally:
            initial_update.set()
            del self._offers[program]
            logger.debug(f"cancelled offer monitor for program {program}")

    def get_offers(self):
        """Return a snapshot of the current offer IntervalTrees keyed by program name."""
        return self._offers.copy()


class ProgramDiscovery:
    """Discover Programs and their associated Controllers."""

    def __init__(self):
        self._enabled_programs: AsyncObserver[frozenset[str]] = AsyncObserver(frozenset())
        self._known_programs: AsyncObserver[frozenset[str]] = AsyncObserver(frozenset())
        self._controllers: dict[str, AsyncObserver[frozenset[str]]] = collections.defaultdict(
            lambda: AsyncObserver(frozenset())
        )

    async def start(self, client: SensorKit):
        """Begin monitoring the backend for program enable-state changes, waiting for the first update."""
        self.client = client

        # Begin discovering enabled programs, making sure to wait for the first set of updates.
        ready = asyncio.Event()
        self._discover_task = asyncio.create_task(self._discover_programs(ready))
        await ready.wait()

    async def _discover_programs(self, initial_update: asyncio.Event):
        tasks: dict[str, asyncio.Task] = {}
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
                    await self._known_programs.notify(frozenset(tasks.keys()))
            elif program not in tasks:
                # Start a new enable state monitor for the newly discovered program.
                events.append(asyncio.Event())
                tasks[program] = asyncio.create_task(self._monitor_task(client, events[-1]))
                await self._known_programs.notify(frozenset(tasks.keys()))

            # Wait for the initial enablement status of each discovered program.
            await asyncio.gather(*(event.wait() for event in events), return_exceptions=True)

            # Signal ready once every program from the initial snapshot has been seen.
            pending.discard(program)
            if not pending:
                initial_update.set()

    async def _monitor_task(self, client: ProgramClient, initial_update: asyncio.Event):
        monitor = client.monitor_enable_state()
        program = str(client.entity)

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

                await observer.notify(new_set)

            initial_update.set()

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

    async def _notify(self):
        if not self.online:
            await self._observer.notify(None)
        else:
            await self._observer.notify(self.enable_state)

    async def _monitor_liveness(self):
        async for is_online in self.client.observe_online_state():
            self.online = is_online
            await self._notify()

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
        await self._notify()

        async for event in stream:
            if event.timestamp() > self.enable_state.timestamp():
                self.enable_state = event
                await self._notify()


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
