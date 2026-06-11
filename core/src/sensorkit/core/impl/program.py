from __future__ import annotations

import asyncio
import contextlib
from _contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable, ClassVar, override

from intervaltree import IntervalTree
from loguru import logger

from sensorkit.backend.base import Entity, KVError
from sensorkit.backend.event import Event
from sensorkit.backend.request import CallContext
from sensorkit.core.entity import EntityInfo
from sensorkit.core.executor import TaskFactoryFunc, TaskingLoop
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.program import (
    OfferInterval,
    ProgramActiveState,
    ProgramActiveStateRequest,
    ProgramEnableState,
    ProgramEnableStateRequest,
    ProgramInterface,
    ProgramOffering,
    ProgramState,
    ProgramTaskingStatus,
    set_active_state_request,
    set_enable_state_request,
)
from sensorkit.core.task import TaskContexts


class ProgramOffers:
    """Manages an IntervalTree of offer windows and signals updates to waiters."""

    def __init__(self):
        self._tree = IntervalTree()
        self._updated = asyncio.Event()

    def wait(self):
        """Return an awaitable that resolves when the offer windows have been updated."""
        return self._updated.wait()

    def poll(self):
        """Return True and clear the update flag if a new update is pending, else False."""
        if self._updated.is_set():
            self._updated.clear()
            return True

        return False

    def get_offer_windows(self) -> list[OfferInterval]:
        """Return a sorted list of merged offer windows from the current interval tree."""
        # Create a tree that *references* the offer tree data and merge overlapping intervals. This
        # avoids an extra copy, and we know this is safe because `merge_overlaps` builds a new set
        # of intervals and does not modify `all_intervals`.
        assert_no_change = self._tree.all_intervals.copy()
        ref = IntervalTree(self._tree.all_intervals)
        ref.merge_overlaps()
        assert self._tree.all_intervals == assert_no_change
        return sorted(ref)

    def add(self, start: datetime, end: datetime, obj: Any = None):
        """Add an offer window to the interval tree and signal waiters."""
        self._tree.addi(
            begin=start,
            end=end,
            data=obj,
        )
        self._updated.set()

    def remove(self, start: datetime, end: datetime, obj: Any = None):
        """Remove an offer window from the interval tree, logging a warning if it does not exist."""
        try:
            self._tree.removei(
                begin=start,
                end=end,
                data=obj,
            )
            self._updated.set()
        except ValueError:
            logger.warning(f"Nonexistent offer window removed: {start} -> {end} {obj=}")

    def clear(self):
        if not self._tree.is_empty():
            self._tree.clear()
            self._updated.set()


class ProgramImpl(EntityImpl, ProgramInterface):
    """Helper for implementing server-side functionality of a Program."""

    current: ClassVar[ContextVar[ProgramImpl | None]] = ContextVar("current_program", default=None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Store the user-supplied intervals that are to make up the offered operating windows.
        self._offers = ProgramOffers()

        self._enable_hooks: list[Callable[[], None]] = []
        self._disable_hooks: list[Callable[[], None]] = []
        self._state = ProgramState(
            enable_state=ProgramEnableState(enabled=False),
            active_state=ProgramActiveState(active=False, origin="init"),
            tasking_status=ProgramTaskingStatus(),
        )
        self._state_lock = asyncio.Lock()
        self._task_factory: TaskFactoryFunc | None = None
        self._task_loop: TaskingLoop | None = None

    @override
    async def init_impl(self):
        try:
            # Restore state.
            async with self._state_lock:
                self._state = await self.kv_get_model(ProgramState)
            logger.debug(f"restored program state: {self._state}")
        except KVError:
            logger.debug("initializing program state")
            await self.kv_put_model(self._state)

        if self._state.active_state.active:
            # Our last known state was tasking. We aren't going to automatically start tasking,
            # but we can emit an event indicating the previous tasking state stopped, giving
            # observers an opportunity to react themselves.
            logger.debug("ending lingering tasking active state")
            await self._update_state(
                "active_state",
                ProgramActiveState(active=False, origin="init"),
            )

    @override
    async def attach_impl(self):
        if self._state.enable_state.enabled:
            await self._call_with_context(self._enable_hooks)

        await self.handle_request(set_enable_state_request, self._set_enable_state)
        await self.handle_request(set_active_state_request, self._set_active_state)

    @override
    def on_enable(self, func: Callable[[], None]):
        self._enable_hooks.append(func)
        return func

    @override
    def on_disable(self, func: Callable[[], None]):
        self._disable_hooks.append(func)
        return func

    @override
    def task_factory(self, func: TaskFactoryFunc):
        self._task_factory = func
        return func

    async def _start_loop(self, contexts: TaskContexts):
        if self._task_loop is not None:
            return

        logger.debug(f"starting tasking loop with {contexts=}")

        # Create the tasking loop.
        self._task_loop = TaskingLoop(
            controller=self.sensorkit().controller(
                Entity.at(self._state.enable_state.controller)
            ),
            factory_func=self._task_factory,
            contexts=contexts,
            task_group=self.task_group,
        )

        # Start a background task to make sure the end states are properly handled whether the
        # loop ends by request or by error. We take pains to do this before starting the loop
        # itself to eliminate data races and ensure event ordering.
        queue = asyncio.Queue()

        async def _finalize_loop():
            aio_task = await queue.get()

            try:
                await aio_task
            finally:
                await self._update_state(
                    "active_state",
                    ProgramActiveState(active=False, origin="request")
                    if self._task_loop.stop_requested
                    else ProgramActiveState(active=False, origin="error"),
                )
                logger.info(f"Tasking loop exiting by {self._state.active_state.origin}")
                self._task_loop = None
                self._task_finalizer = None

        self._task_finalizer = self.task_group.create_task(_finalize_loop())

        # Finally, we can start the tasking loop and update our state.
        with self.enter_context():
            logger.info("Starting tasking loop")
            aio_task = self._task_loop.start()

        await self._update_state(
            "active_state",
            ProgramActiveState(active=True, origin="request"),
        )

        # Tell the finalizer about the loop task.
        queue.put_nowait(aio_task)

    async def _stop_loop(self, *, timeout=None):
        if self._task_loop is None:
            return

        await self._update_state(
            "active_state",
            ProgramActiveState(active=True, origin=self._state.active_state.origin, stopping=True),
        )

        # Due to the await above, we have to check again whether the loop is still running, as it
        # may have been concurrently stopped.
        if self._task_loop is None:
            return

        try:
            await self._task_loop.stop(timeout=timeout)
        except (Exception, asyncio.CancelledError) as e:
            logger.warning(f"Error while stopping tasking loop: {e}")

        # Make sure the end state has been updated by the finalizer.
        if self._task_finalizer is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task_finalizer

    async def _set_enable_state(self, request: ProgramEnableStateRequest):
        if not request.enable and not self._state.enable_state.enabled:
            return

        enablement_changed = request.enable ^ self._state.enable_state.enabled
        controller_changed = request.controller != self._state.enable_state.controller

        if not enablement_changed and not controller_changed:
            return

        logger.info(
            f"{'Enabling' if request.enable else 'Disabling'} "
            f"{self.entity} for target {request.controller}"
        )

        await self._update_state(
            "enable_state",
            ProgramEnableState(enabled=request.enable, controller=request.controller),
        )

        if request.enable:
            # Call the enable hook.
            await self._call_with_context(self._enable_hooks)
        else:
            # Disabling the program implies setting the active state low. We consider this an abort
            # case; if the caller wants a graceful stop, they can explicitly stop tasking first.
            await self._stop_loop(timeout=0)

            # Call the disable hook.
            await self._call_with_context(self._disable_hooks)

    async def _set_active_state(
        self,
        request: ProgramActiveStateRequest,
        call: CallContext[None, None],
    ):
        with self.enter_context():
            logger.info(f"Requested to {request.action} the tasking loop")

        if request.action == "start":
            # Cannot activate tasking if we aren't enabled with a target Controller configured.
            if not self._state.enable_state.enabled or not self._state.enable_state.controller:
                call.reject(response=None)
                return

            if (
                self._state.active_state.active
                and request.contexts != self._state.active_state.contexts
            ):
                # Don't support restart-with-different-context in a single request.
                call.reject(response=None)
                return

        call.accept(response=None)

        if request.active_state() == self._state.active_state.active:
            # We are already in the requested state, so claim success.
            await call.succeed(result=None)
            return

        try:
            match request.action:
                case "start":
                    # Start a new tasking loop.
                    with self.enter_context():
                        await self._start_loop(request.contexts)
                case "stop":
                    # Stop the tasking loop gracefully.
                    # TODO: add backstop timeout reflecting maximum task time
                    await call.progress_from_task(
                        self.task_group.create_task(self._stop_loop()),
                        cadence=6,
                        ttl=10,
                    )
                case "abort":
                    # Abort the tasking loop immediately.
                    with self.enter_context():
                        await self._stop_loop(timeout=0)
        except Exception as e:
            with self.enter_context():
                logger.exception("Error setting active state")

            await call.fail(f"{type(e).__name__} setting active state ({e})")
        else:
            await call.succeed(result=None)

    async def _update_state(self, key: str, event: Event):
        async with self._state_lock:
            setattr(self._state, key, event)
            await self.emit_event(event)
            await self.kv_put_model(self._state)

    @override
    def get_offers(self):
        return self._offers.get_offer_windows()

    @override
    async def publish_offers(self):
        """Publish the current set of offers if they have changed."""
        if self._offers.poll():
            await self.publish(
                ProgramOffering(offer_windows=self._offers.get_offer_windows())
            )

    @override
    def add_offer(self, start: datetime, end: datetime, obj: Any = None):
        self._offers.add(start, end, obj)

    @override
    def remove_offer(self, start: datetime, end: datetime, obj: Any = None):
        self._offers.remove(start, end, obj)

    @override
    def clear_offers(self):
        self._offers.clear()

    @override
    def entity_info(self) -> EntityInfo:
        return EntityInfo(entity_type="program", details=None)
