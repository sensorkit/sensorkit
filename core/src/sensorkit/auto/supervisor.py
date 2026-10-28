# SPDX-License-Identifier: Apache-2.0
"""Compare controller state with the state lifecycle procedures expect."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, ClassVar

from loguru import logger

from sensorkit.auto.procedures import ControllerFailure
from sensorkit.common.aio import AsyncObserver, cleanup_future, defer_cancellation
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerMonitor,
    ControllerOperatingState,
    ControllerState,
    ControllerStateUnknown,
    InternalControllerState,
)
from sensorkit.core.task import TaskExecution

if TYPE_CHECKING:
    from sensorkit.auto.failure import FailureState

SETTLED_STATES = frozenset(
    {
        InternalControllerState.OPERATE,
        InternalControllerState.STANDBY,
        InternalControllerState.SHUTDOWN,
    }
)
"""Controller states that can satisfy a normal lifecycle demand."""


class ControllerStateSupervisor:
    """Detect controller states that differ from the state lifecycle procedures expect.

    The supervisor stores a local expectation separately from controller state.
    The expectation has four forms:

    - None before the first settled controller state, allowing startup to adopt that state.
    - TRANSITION while a commanded lifecycle task is pending.
    - A settled state verified by a procedure.
    - UNKNOWN after a failure or an unconfirmed task, until another command or confirmation.

    During a commanded transition, each mismatch has a grace period. Receiving TRANSITION
    clears the deadline, and a later mismatch starts a new grace period. Leaving a settled
    expected state or exceeding the grace period counts one controller failure and sets the
    expectation to UNKNOWN. A context-only change does not count as a failure.

    Monitor updates and fresh reads advance the same controller state by event ID. Older queued
    updates cannot replace a state that a fresh read has already confirmed.
    """

    state_reads: ClassVar[int] = 3
    """Attempts to read controller state after task completion."""

    state_read_delay: ClassVar[float] = 0.5
    """Seconds between failed state reads."""

    def __init__(
        self,
        monitor: ControllerMonitor,
        failure_state: FailureState,
        *,
        transition_grace: float,
    ):
        """Create a supervisor for a started monitor.

        Args:
            monitor: A controller monitor whose start method has completed.
            failure_state: Failure history shared across reactor restarts.
            transition_grace: Seconds a commanded transition may go without the controller
                reporting the expected state.

        Raises:
            ValueError: If `transition_grace` is not positive.
        """
        if not transition_grace > 0:
            raise ValueError(f"transition_grace must be positive, got {transition_grace}")

        self.controller = monitor.controller
        self.failure_state = failure_state
        self.transition_grace = transition_grace
        self.failed = asyncio.Event()
        """Set when a controller failure is detected or supervision ends.

        A new expectation clears this event while supervision is running. Failure history
        remains in `FailureState`. Once supervision ends, the event remains set.
        """
        self._monitor = monitor
        self._observer: AsyncObserver[ControllerOperatingState] = AsyncObserver()
        self._expected: InternalControllerState | None = None
        self._expected_changed = asyncio.Event()
        self._deadline: float | None = None
        self._observed_state: ControllerOperatingState | None = None
        self._ready = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._error: BaseException | None = None

    @property
    def expected(self) -> InternalControllerState | None:
        """The expected state, or None before the first settled controller state."""
        return self._expected

    async def start(self):
        """Start supervision and wait until the initial controller state is processed.

        Failure or cancellation stops supervision and releases its subscriptions.
        The caller remains responsible for stopping the monitor.
        """
        if self._task is not None:
            raise RuntimeError("A controller state supervisor can only be started once")

        self._task = asyncio.create_task(self._run())

        # Notify waiters even if the task is canceled before it starts.
        self._task.add_done_callback(lambda _: self._close())

        try:
            await self._ready.wait()
            self._check_health()
        except BaseException:
            await defer_cancellation(self.stop())
            raise

    async def stop(self):
        """Stop supervision and release its subscriptions.

        The caller remains responsible for stopping the monitor.
        """
        if self._task is None:
            return

        self._task.cancel()
        await asyncio.wait((self._task,))

    def healthy(self) -> bool:
        """Whether supervision is running with valid controller state and a healthy monitor."""
        return (
            self._task is not None
            and not self._task.done()
            and self._observed_state is not None
            and self._monitor.healthy()
        )

    def _check_health(self):
        """Raise if state monitoring or supervision is unavailable."""
        if not self.healthy():
            raise ControllerStateUnknown(
                f"{self.controller.entity} state supervision is unavailable"
            ) from (self._error or self._monitor.error)

    @property
    def state(self) -> ControllerOperatingState:
        """Copy of the newest controller state processed by supervision.

        Raises:
            ControllerStateUnknown: If state monitoring failed or supervision ended.
        """
        self._check_health()
        return self._observed_state.model_copy(deep=True)

    async def refresh(self) -> ControllerState:
        """Read controller state and process its operating state in event order.

        Older updates do not replace a newer processed state. The returned snapshot is the
        complete controller state read from the backend.

        Raises:
            ControllerFailure: If controller state cannot be read after retries.
            ControllerStateUnknown: If state monitoring failed or supervision is unavailable.
        """
        self._check_health()
        last_error = None

        for attempt in range(1, self.state_reads + 1):
            try:
                snapshot = await self.controller.kv_get_model(ControllerState)
            except Exception as error:
                last_error = error
                logger.warning(f"Could not read {self.controller.entity} state ({error!r})")

                if attempt < self.state_reads:
                    await asyncio.sleep(self.state_read_delay)
            else:
                self._check_health()
                self._process_state(snapshot.operating_state)
                return snapshot

        raise ControllerFailure(
            f"Controller state unread for {self.controller.entity}"
        ) from last_error

    async def verify(
        self, state: InternalControllerState, context: KeywordDict | None = None
    ) -> ControllerState:
        """Read fresh state and verify a lifecycle result without changing the expectation.

        Raises:
            ControllerFailure: If state cannot be read or the state or context differs.
            ControllerStateUnknown: If state monitoring failed or supervision is unavailable.
        """
        try:
            snapshot = await self.refresh()

            if not self.state.matches(state, context or KeywordDict()):
                raise ControllerFailure(
                    f"Controller did not reach {state} with the requested lifecycle context"
                )
        except BaseException:
            self.invalidate()
            raise

        return snapshot

    def running_execution(self) -> TaskExecution | None:
        """Return the execution the controller last reported running, if any.

        Raises:
            ControllerStateUnknown: If state monitoring failed or stopped.
        """
        state = self._monitor.state.execution_state
        return state.execution if state.executing else None

    def settled(self, state: InternalControllerState, context: KeywordDict) -> bool:
        """Return whether controller state and context match a valid local expectation.

        No transition may be pending. An unset expectation allows startup adoption.
        An UNKNOWN expectation requires another lifecycle command.

        Raises:
            ControllerStateUnknown: If state monitoring failed or supervision ended.
        """
        return self._expected in (None, state) and self.state.matches(state, context)

    @contextlib.contextmanager
    def state_changes(self) -> Iterator[asyncio.Queue[ControllerOperatingState]]:
        """Subscribe to operating state changes after comparison with the expected state.

        The queue retains only the latest state and shuts down when supervision ends.

        Raises:
            ControllerStateUnknown: If state monitoring failed or supervision ended.
        """
        self._check_health()

        with self._observer.subscription(maxsize=1) as queue:
            yield queue

    def expect_transition(self):
        """Expect the controller to report TRANSITION for a lifecycle task about to be sent."""
        self._expect(InternalControllerState.TRANSITION)

    async def confirm(
        self, state: InternalControllerState, context: KeywordDict | None = None
    ) -> ControllerState:
        """Verify fresh controller state and context, then establish the expectation."""
        snapshot = await self.verify(state, context)
        self._expect(state)
        return snapshot

    def invalidate(self):
        """Set the expectation to UNKNOWN until another lifecycle command or confirmation."""
        self._expect(InternalControllerState.UNKNOWN)

    def _expect(self, state: InternalControllerState):
        self._expected = state
        self._deadline = None
        self._expected_changed.set()
        self._update_settled(asyncio.get_running_loop().time())

        if not self._ended():
            self.failed.clear()

    def _ended(self) -> bool:
        return self._task is not None and self._task.done()

    async def _run(self):
        try:
            with self._monitor.state_changes() as state_changes:
                while True:
                    self._expected_changed.clear()
                    state = await self._next(state_changes)
                    self._process_state(state)
        except (asyncio.QueueShutDown, ControllerStateUnknown):
            # The state accessor raises when state monitoring fails or supervision ends.
            pass
        except Exception as e:
            self._error = e
            logger.opt(exception=e).error(f"{self.controller.entity} state supervision failed")

    def _close(self):
        """Close state subscriptions and wake failure waiters."""
        self._observer.close()
        self.failed.set()
        self._ready.set()
        self._update_settled(asyncio.get_running_loop().time())

    def _update_settled(self, now: float):
        """Update settled time from controller state, expectation, and monitor health."""
        actual = self._observed_state
        self.failure_state.set_settled(
            self.healthy()
            and self._expected in SETTLED_STATES
            and actual.current == self._expected
            and actual.target is None,
            now,
        )

    def _process_state(self, state: ControllerOperatingState | None):
        """Accept newer state updates and compare the processed state with the expectation."""
        current = self._observed_state
        newer = state is not None and (
            current is None or state.event_id.int > current.event_id.int
        )

        if newer:
            self._observed_state = state.model_copy(deep=True)

        if self._observed_state is not None:
            now = asyncio.get_running_loop().time()
            self._evaluate(now)
            self._update_settled(now)
            self._ready.set()

            if newer:
                self._observer.notify(self._observed_state)

    async def _next(
        self,
        state_changes: asyncio.Queue[ControllerOperatingState],
    ) -> ControllerOperatingState | None:
        """Wait for a state update, expectation change, or grace expiry.

        Return None when an expectation change or grace expiry ends the wait without a state.
        """
        if not state_changes.empty() or self._observed_state is None:
            return await state_changes.get()

        loop = asyncio.get_running_loop()
        timeout = None if self._deadline is None else max(0.0, self._deadline - loop.time())
        next_state = asyncio.ensure_future(state_changes.get())
        changed = asyncio.ensure_future(self._expected_changed.wait())

        try:
            await asyncio.wait(
                (next_state, changed), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            changed.cancel()

            # Preserve a state update that arrives while the waiters are being canceled.
            if not next_state.done():
                next_state.cancel()

            await asyncio.wait((next_state, changed))
            cleanup_future(changed)

        if next_state.cancelled():
            return None

        return next_state.result()

    def _evaluate(self, now: float):
        """Compare the latest received controller state with the expected state."""
        actual = self._observed_state

        match self._expected:
            case None:
                if actual.current in SETTLED_STATES and actual.target is None:
                    self._expected = actual.current

                return
            case InternalControllerState.UNKNOWN:
                return
            case InternalControllerState.TRANSITION:
                matched = actual.current == InternalControllerState.TRANSITION
            case expected if actual.current != expected:
                self._fail(f"{self.controller.entity} left {expected} ({actual.current})", now)
                return
            case _:
                matched = True

        if matched:
            self._deadline = None
        elif self._deadline is None:
            self._deadline = now + self.transition_grace
        elif now >= self._deadline:
            self._fail(
                f"{self.controller.entity} reported {actual.current} for longer than "
                f"{self.transition_grace} s during a lifecycle task",
                now,
            )

    def _fail(self, message: str, now: float):
        """Record one controller failure and invalidate the local expectation."""
        logger.error(message)
        self._expected = InternalControllerState.UNKNOWN
        self._deadline = None
        self.failure_state.record_failure(now, ControllerFailure(message))
        self.failed.set()
