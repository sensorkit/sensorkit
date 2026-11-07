# SPDX-License-Identifier: Apache-2.0
"""Tests for comparing observed controller state with the expected state."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
import pytest_asyncio

from sensorkit.auto.failure import FailureState
from sensorkit.auto.procedures import ControllerFailure
from sensorkit.auto.supervisor import ControllerStateSupervisor
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    ControllerMonitor,
    ControllerStateUnknown,
    InternalControllerState,
    KnownControllerState,
)
from sensorkit.core.task import InitTask

OPERATE = InternalControllerState.OPERATE
STANDBY = InternalControllerState.STANDBY
ERROR = InternalControllerState.ERROR
UNKNOWN = InternalControllerState.UNKNOWN
TRANSITION = InternalControllerState.TRANSITION

GRACE = 1.0


class PausedSupervisor(ControllerStateSupervisor):
    """Hold one selected monitor update before supervision processes it."""

    def __init__(self, *args, pause_at: InternalControllerState, **kwargs):
        super().__init__(*args, **kwargs)
        self.pause_at: InternalControllerState | None = pause_at
        self.paused = asyncio.Event()
        self.release = asyncio.Event()
        self.resumed = asyncio.Event()

    async def _next(self, state_changes):
        state = await super()._next(state_changes)

        if state is not None and state.current == self.pause_at:
            self.pause_at = None
            self.paused.set()
            await self.release.wait()
            self.resumed.set()

        return state


@pytest_asyncio.fixture
async def controller(kit, service_context):
    impl = await service_context.register_controller("ctrl1")
    await kit.controller("ctrl1").enable()
    return impl


@pytest_asyncio.fixture
async def gated_init(controller):
    """Register an init handler that runs until released, then reports OPERATE."""
    entered = asyncio.Event()
    release = asyncio.Event()

    @controller.task_handler(InitTask)
    async def init(task):
        entered.set()
        await release.wait()

        await controller.report_state(OPERATE, KeywordDict())

    try:
        yield entered, release
    finally:
        release.set()


@contextlib.asynccontextmanager
async def supervising(kit, *, transition_grace: float = GRACE):
    """Start a monitor and supervisor for ctrl1, stopping both on exit."""
    monitor = ControllerMonitor(kit.controller("ctrl1"))
    await monitor.start()
    supervisor = ControllerStateSupervisor(
        monitor, FailureState(), transition_grace=transition_grace
    )
    await supervisor.start()

    try:
        yield supervisor
    finally:
        await supervisor.stop()
        await monitor.stop()


async def compared(changes: asyncio.Queue, state: InternalControllerState):
    """Wait until the supervisor has compared an update for the given state."""
    async with asyncio.timeout(5.0):
        while (await changes.get()).current != state:
            pass


@contextlib.asynccontextmanager
async def settled_at(kit, controller, state: KnownControllerState, until):
    """Set controller state and yield a supervisor once it expects that state."""
    await controller.report_state(state, KeywordDict())

    async with supervising(kit) as supervisor:
        await until(lambda: supervisor.expected == state)
        yield supervisor


# Startup


@pytest.mark.asyncio
async def test_start_processes_initial_state(kit, controller):
    """An initial settled controller state establishes the expectation for startup adoption."""
    await controller.report_state(OPERATE, KeywordDict())

    async with supervising(kit) as supervisor:
        assert supervisor.expected == OPERATE
        assert supervisor.state.current == OPERATE
        assert supervisor.healthy()
        assert supervisor.settled(OPERATE, KeywordDict())
        assert not supervisor.failed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_start_waits_for_processed_state(kit, controller, cancel):
    """Startup waits for supervision, and cancellation stops it without stopping the monitor."""
    await controller.report_state(STANDBY, KeywordDict())
    monitor = ControllerMonitor(kit.controller("ctrl1"))
    await monitor.start()
    supervisor = PausedSupervisor(
        monitor, FailureState(), transition_grace=GRACE, pause_at=STANDBY
    )
    starting = asyncio.create_task(supervisor.start())

    try:
        async with asyncio.timeout(5.0):
            await supervisor.paused.wait()

        assert not starting.done()
        assert not supervisor.healthy()

        if cancel:
            starting.cancel()

            with pytest.raises(asyncio.CancelledError):
                await starting

            assert supervisor.failed.is_set()
            assert not supervisor.healthy()
        else:
            supervisor.release.set()
            await starting

            assert supervisor.state.current == STANDBY
            assert supervisor.expected == STANDBY
            assert supervisor.healthy()

        assert monitor.operating_state.current == STANDBY
    finally:
        starting.cancel()
        await asyncio.gather(starting, return_exceptions=True)
        await supervisor.stop()
        await monitor.stop()


@pytest.mark.asyncio
async def test_unsettled_report_seeds_nothing(kit, controller, until):
    """ERROR and UNKNOWN controller states leave the expectation unset."""
    await controller.report_state(ERROR)

    async with supervising(kit) as supervisor:
        with supervisor.state_changes() as changes:
            await controller.report_state(UNKNOWN)
            await compared(changes, UNKNOWN)

        assert supervisor.expected is None
        assert not supervisor.settled(OPERATE, KeywordDict())


# Commanded transitions


@pytest.mark.asyncio
async def test_infinite_grace_keeps_state_updates_and_stop_responsive(kit, controller):
    """An unlimited mismatch wait still processes controller updates and stops promptly."""
    await controller.report_state(STANDBY, KeywordDict())

    async with asyncio.timeout(5.0):
        async with supervising(kit, transition_grace=float("inf")) as supervisor:
            supervisor.expect_transition()

            with supervisor.state_changes() as changes:
                await controller.report_state(OPERATE, KeywordDict())
                await compared(changes, OPERATE)

            assert supervisor.healthy()
            assert supervisor.state.current == OPERATE
            assert not supervisor.failed.is_set()


@pytest.mark.asyncio
async def test_queued_transition_cannot_replace_confirmed_state(
    kit, controller, gated_init, until
):
    """A fresh confirmation prevents an older queued transition from causing a failure."""
    entered, release_init = gated_init
    await controller.report_state(STANDBY, KeywordDict())
    monitor = ControllerMonitor(kit.controller("ctrl1"))
    await monitor.start()
    supervisor = PausedSupervisor(
        monitor, FailureState(), transition_grace=GRACE, pause_at=TRANSITION
    )
    call = None

    try:
        await supervisor.start()
        supervisor.expect_transition()
        call = asyncio.ensure_future(kit.controller("ctrl1").execute_task(InitTask()))

        async with asyncio.timeout(5.0):
            await entered.wait()
            await supervisor.paused.wait()

            # Reading state exposes only updates supervision has already processed.
            assert supervisor.state.current == STANDBY

            release_init.set()
            await call
            await until(lambda: monitor.operating_state.current == OPERATE)
            await supervisor.confirm(OPERATE)

            assert supervisor.state.current == OPERATE

            supervisor.release.set()
            await supervisor.resumed.wait()

        assert supervisor.state.current == OPERATE
        assert supervisor.expected == OPERATE
        assert not supervisor.failed.is_set()
        assert supervisor.failure_state.failures == 0
    finally:
        release_init.set()
        supervisor.release.set()

        if call is not None:
            await asyncio.gather(call, return_exceptions=True)

        await supervisor.stop()
        await monitor.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [STANDBY, OPERATE])
async def test_confirmation_verifies_state_and_context(kit, controller, state):
    """Fresh state must match both the requested state and its context."""
    context = KeywordDict()
    context.set_value("dome", "closed")

    @controller.task_handler(InitTask)
    async def init(task):
        pass

    async with supervising(kit) as supervisor:
        supervisor.expect_transition()

        # OPERATE is reached with lifecycle input other than the requested input.
        if state == OPERATE:
            await kit.controller("ctrl1").execute_task(InitTask(), context=context)
        else:
            await controller.report_state(STANDBY, context)

        with pytest.raises(ControllerFailure):
            await supervisor.confirm(OPERATE)

        assert supervisor.state.current == state
        assert supervisor.expected == UNKNOWN


@pytest.mark.asyncio
async def test_unanswered_transition_fails_after_grace(kit, controller, until):
    """A transition the controller never reports fails once the grace period ends."""
    loop = asyncio.get_running_loop()

    async with settled_at(kit, controller, STANDBY, until) as supervisor:
        supervisor.expect_transition()
        expected_at = loop.time()

        async with asyncio.timeout(5.0):
            await supervisor.failed.wait()

        elapsed = loop.time() - expected_at

        assert GRACE <= elapsed < GRACE + 0.5
        assert supervisor.expected == UNKNOWN
        assert supervisor.failure_state.failures == 1


@pytest.mark.asyncio
async def test_long_transition_is_not_a_failure(kit, controller, gated_init, until):
    """A task may exceed the grace duration while actual state remains TRANSITION."""
    entered, release = gated_init

    async with settled_at(kit, controller, STANDBY, until) as supervisor:
        supervisor.expect_transition()
        call = asyncio.ensure_future(kit.controller("ctrl1").execute_task(InitTask()))

        try:
            async with asyncio.timeout(5.0):
                await entered.wait()

            await asyncio.sleep(2 * GRACE)

            assert not supervisor.failed.is_set()

            release.set()
            await call
        finally:
            release.set()
            await asyncio.gather(call, return_exceptions=True)

        await supervisor.confirm(OPERATE)
        await asyncio.sleep(1.5 * GRACE)

        assert not supervisor.failed.is_set()
        assert supervisor.failure_state.failures == 0


@pytest.mark.asyncio
async def test_unconfirmed_target_fails_after_grace(kit, controller, gated_init, until):
    """The caller must confirm the final state within the new grace period."""
    entered, release = gated_init
    loop = asyncio.get_running_loop()

    async with settled_at(kit, controller, STANDBY, until) as supervisor:
        supervisor.expect_transition()

        with supervisor.state_changes() as changes:
            call = asyncio.ensure_future(kit.controller("ctrl1").execute_task(InitTask()))

            try:
                await compared(changes, TRANSITION)

                # Leave TRANSITION after the initial grace duration has passed.
                await asyncio.sleep(GRACE)
                release.set()
                await compared(changes, OPERATE)
                reported_at = loop.time()
            finally:
                release.set()
                await asyncio.gather(call, return_exceptions=True)

        async with asyncio.timeout(5.0):
            await supervisor.failed.wait()

        elapsed = loop.time() - reported_at

    assert GRACE - 0.2 <= elapsed < GRACE + 0.5
    assert supervisor.expected == UNKNOWN
    assert supervisor.failure_state.failures == 1


@pytest.mark.asyncio
async def test_skipped_transition_keeps_first_deadline(kit, controller, until):
    """State updates that skip TRANSITION do not extend the first grace deadline."""
    loop = asyncio.get_running_loop()

    async with settled_at(kit, controller, STANDBY, until) as supervisor:
        supervisor.expect_transition()
        expected_at = loop.time()

        with supervisor.state_changes() as changes:
            await asyncio.sleep(0.6 * GRACE)
            await controller.report_state(OPERATE, KeywordDict())
            await compared(changes, OPERATE)

        async with asyncio.timeout(5.0):
            await supervisor.failed.wait()

        elapsed = loop.time() - expected_at

    assert GRACE <= elapsed < 1.4 * GRACE
    assert supervisor.failure_state.failures == 1


@pytest.mark.asyncio
async def test_skipped_transition_confirmed_in_time(kit, controller, until):
    """A fast task need not expose TRANSITION if its final state is confirmed in time."""

    async with settled_at(kit, controller, STANDBY, until) as supervisor:
        supervisor.expect_transition()

        with supervisor.state_changes() as changes:
            await controller.report_state(OPERATE, KeywordDict())
            await compared(changes, OPERATE)

        await supervisor.confirm(OPERATE)
        await asyncio.sleep(1.5 * GRACE)

        assert supervisor.expected == OPERATE
        assert supervisor.settled(OPERATE, KeywordDict())
        assert not supervisor.failed.is_set()


# Settled states


@pytest.mark.asyncio
async def test_departure_counts_one_failure(kit, controller, until):
    """Leaving a settled expected state counts once, regardless of later state updates."""
    async with settled_at(kit, controller, OPERATE, until) as supervisor:
        failure_state = supervisor.failure_state

        with supervisor.state_changes() as changes:
            await controller.report_state(STANDBY, KeywordDict())
            await compared(changes, STANDBY)

            # The supervisor compares the update before notifying subscribers.
            assert supervisor.failed.is_set()
            assert supervisor.expected == UNKNOWN

            # Another observer detecting the same incident does not count it again.
            failure_state.record_failure(0.0, ControllerFailure("tasking failed"))
            await controller.report_state(ERROR)
            await compared(changes, ERROR)

    assert failure_state.failures == 1
    assert "left operate" in str(failure_state.error)


@pytest.mark.asyncio
async def test_unknown_is_not_repaired_by_a_report(kit, controller, until):
    """Receiving the earlier controller state again does not restore an UNKNOWN expectation."""

    async with settled_at(kit, controller, OPERATE, until) as supervisor:
        with supervisor.state_changes() as changes:
            await controller.report_state(STANDBY, KeywordDict())
            await compared(changes, STANDBY)
            await controller.report_state(OPERATE, KeywordDict())
            await compared(changes, OPERATE)

        assert supervisor.expected == UNKNOWN
        assert not supervisor.settled(OPERATE, KeywordDict())

        # A new expectation clears the failure event without clearing failure history.
        supervisor.expect_transition()

        assert not supervisor.failed.is_set()


@pytest.mark.asyncio
async def test_context_change_is_not_a_failure(kit, controller, until):
    """A settled state with another lifecycle context keeps the expectation."""
    other = KeywordDict()
    other.set_value("dome", "closed")

    async with settled_at(kit, controller, OPERATE, until) as supervisor:
        before = supervisor.failure_state.model_copy()

        with supervisor.state_changes() as changes:
            await controller.report_state(OPERATE, other)
            await compared(changes, OPERATE)

        assert supervisor.expected == OPERATE
        assert supervisor.settled(OPERATE, other)
        assert supervisor.failure_state == before
        assert not supervisor.failed.is_set()


@pytest.mark.asyncio
async def test_unknown_expectation_accepts_any_report(kit, controller, until):
    """An UNKNOWN expectation allows any observed controller state."""

    async with settled_at(kit, controller, OPERATE, until) as supervisor:
        supervisor.invalidate()

        with supervisor.state_changes() as changes:
            await controller.report_state(STANDBY, KeywordDict())
            await compared(changes, STANDBY)

            for state in (UNKNOWN, ERROR):
                await controller.report_state(state)
                await compared(changes, state)

        await asyncio.sleep(1.5 * GRACE)

        assert not supervisor.failed.is_set()
        assert supervisor.failure_state.failures == 0


# Lifetime


@pytest.mark.asyncio
async def test_start_fails_when_monitor_has_stopped(kit, controller):
    """A stopped monitor ends startup without leaving a supervisor running."""
    monitor = ControllerMonitor(kit.controller("ctrl1"))
    await monitor.start()
    await monitor.stop()
    supervisor = ControllerStateSupervisor(monitor, FailureState(), transition_grace=GRACE)

    try:
        async with asyncio.timeout(5.0):
            with pytest.raises(ControllerStateUnknown):
                await supervisor.start()

        assert supervisor.failed.is_set()
        assert not supervisor.healthy()
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_stop_ends_notifications(kit, controller):
    """Stopping wakes subscribers and failure waiters, leaving the monitor running."""
    await controller.report_state(OPERATE, KeywordDict())
    monitor = ControllerMonitor(kit.controller("ctrl1"))
    await monitor.start()

    try:
        supervisor = ControllerStateSupervisor(monitor, FailureState(), transition_grace=GRACE)
        await supervisor.start()

        with supervisor.state_changes() as changes:
            failed = asyncio.ensure_future(supervisor.failed.wait())
            await supervisor.stop()

            with pytest.raises(asyncio.QueueShutDown):
                async with asyncio.timeout(5.0):
                    while True:
                        await changes.get()

            async with asyncio.timeout(5.0):
                await failed

        assert not supervisor.healthy()
        assert monitor.operating_state.current == OPERATE

        with pytest.raises(ControllerStateUnknown):
            _ = supervisor.state

        # A stopped supervisor keeps its failure event set despite a new expectation.
        supervisor.expect_transition()

        assert supervisor.failed.is_set()
    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_monitor_failure_ends_supervision(kit, controller, until):
    """A failed monitor ends supervision and wakes failure waiters."""
    async with supervising(kit) as supervisor:
        await kit.backend.key_value(controller.entity).update("ControllerState", b"{}")

        async with asyncio.timeout(5.0):
            await supervisor.failed.wait()

        assert not supervisor.healthy()

        with pytest.raises(ControllerStateUnknown):
            _ = supervisor.state

    assert supervisor.failure_state.failures == 0
