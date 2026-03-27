"""System tests for ControllerLifecycle driving the full tasking flow.

Tests the critical path: service registration → lifecycle enable →
InitTask → program start_tasking → TaskingLoop → task execution →
stop/shutdown. All tests run on the fake backend.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from sensorkit.auto.lifecycle import ControllerLifecycle, LifecycleStep
from sensorkit.core.controller import InternalControllerState
from sensorkit.core.task import (
    CollectTask,
    InitTask,
    ShutdownTask,
    StandbyTask,
)


async def cleanup(lifecycle: ControllerLifecycle, sc):
    """Cancel the lifecycle background task and shut down the service."""
    lifecycle._main_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await lifecycle._main_task
    await sc.shutdown()


@pytest.mark.asyncio
async def test_operate_full_cycle(kit):
    """Lifecycle drives OPERATE → init → tasking → task execution → SHUTDOWN."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program = await sc.register_program("prog1")

    init_ran = asyncio.Event()
    task_ran = asyncio.Event()
    shutdown_ran = asyncio.Event()
    done = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle_init(task):
        init_ran.set()

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        task_ran.set()

    @controller.task_handler(ShutdownTask)
    async def handle_shutdown(task):
        shutdown_ran.set()

    @program.task_factory
    async def factory():
        if not done.is_set():
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            done.set()

    # Enable controller and program.
    await kit.controller("ctrl1").enable()
    prog_client = kit.program("prog1")
    await prog_client.enable("ctrl1")

    # Create and drive the lifecycle.
    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_client)

    async with asyncio.timeout(10.0):
        await init_ran.wait()
        await task_ran.wait()

    # Transition to SHUTDOWN.
    lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)

    async with asyncio.timeout(15.0):
        await shutdown_ran.wait()
        # Wait for lifecycle to process the task completion and update belief state.
        while lifecycle.belief_state != InternalControllerState.SHUTDOWN:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_operate_multiple_tasks(kit):
    """Program's task_factory yields multiple tasks in sequence."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program = await sc.register_program("prog1")

    init_ran = asyncio.Event()
    task_count = 0
    all_done = asyncio.Event()
    expected_tasks = 3

    @controller.task_handler(InitTask)
    async def handle_init(task):
        init_ran.set()

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        nonlocal task_count
        task_count += 1

    @program.task_factory
    async def factory():
        nonlocal task_count
        if task_count < expected_tasks:
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            if task_count >= expected_tasks:
                all_done.set()

    await kit.controller("ctrl1").enable()
    prog_client = kit.program("prog1")
    await prog_client.enable("ctrl1")

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_client)

    async with asyncio.timeout(15.0):
        await init_ran.wait()
        await all_done.wait()

    assert task_count == expected_tasks
    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_operate_then_shutdown_interrupts_tasking(kit):
    """Changing demand to SHUTDOWN while tasking interrupts the program."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program = await sc.register_program("prog1")

    init_ran = asyncio.Event()
    task_started = asyncio.Event()
    task_can_finish = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle_init(task):
        init_ran.set()

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        task_started.set()
        # Wait until signaled to finish (simulates a running task that cooperates with stop).
        await task_can_finish.wait()

    @controller.task_handler(ShutdownTask)
    async def handle_shutdown(task):
        shutdown_ran.set()

    @program.task_factory
    async def factory():
        yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")

    await kit.controller("ctrl1").enable()
    prog_client = kit.program("prog1")
    await prog_client.enable("ctrl1")

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_client)

    async with asyncio.timeout(10.0):
        await init_ran.wait()
        await task_started.wait()

    # Change demand while the task is still running.
    lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)
    # Allow the task to finish so stop_tasking() can complete.
    task_can_finish.set()

    async with asyncio.timeout(15.0):
        await shutdown_ran.wait()
        while lifecycle.belief_state != InternalControllerState.SHUTDOWN:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_standby_cycle(kit):
    """Lifecycle drives STANDBY → StandbyTask runs on controller."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")

    standby_ran = asyncio.Event()

    @controller.task_handler(StandbyTask)
    async def handle_standby(task):
        standby_ran.set()

    await kit.controller("ctrl1").enable()

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()
    lifecycle.set_demand_state(InternalControllerState.STANDBY)

    async with asyncio.timeout(10.0):
        await standby_ran.wait()
        while lifecycle.belief_state != InternalControllerState.STANDBY:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_interrupt_program_aborts_running_task(kit):
    """A program with interrupt=True has its running task aborted when a new program takes over."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program_a = await sc.register_program("prog_a")
    program_b = await sc.register_program("prog_b")

    task_a_started = asyncio.Event()
    task_a_aborted = asyncio.Event()
    task_b_ran = asyncio.Event()
    done_b = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle_init(task):
        pass

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        if not task_a_started.is_set():
            task_a_started.set()
            try:
                # Block indefinitely — only abort() will unblock this.
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                task_a_aborted.set()
                raise
        else:
            task_b_ran.set()

    @controller.task_handler(ShutdownTask)
    async def handle_shutdown(task):
        shutdown_ran.set()

    @program_a.task_factory
    async def factory_a():
        yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")

    @program_b.task_factory
    async def factory_b():
        if not done_b.is_set():
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            done_b.set()

    await kit.controller("ctrl1").enable()
    prog_a_client = kit.program("prog_a")
    await prog_a_client.enable("ctrl1")
    prog_b_client = kit.program("prog_b")
    await prog_b_client.enable("ctrl1")

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()

    # Start operating with program A with interrupt=True. This means that when program A is
    # interrupted by a new demand, its running task will be aborted instead of stopped gracefully.
    lifecycle.set_demand_state(
        InternalControllerState.OPERATE, program=prog_a_client
    )

    async with asyncio.timeout(10.0):
        await task_a_started.wait()

    # Switch to program B — because program A had interrupt=True, its task is aborted immediately.
    lifecycle.set_demand_state(
        InternalControllerState.OPERATE, program=prog_b_client, interrupt=True
    )

    async with asyncio.timeout(10.0):
        await task_a_aborted.wait()
        await task_b_ran.wait()

    # Transition to SHUTDOWN for clean teardown.
    lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)

    async with asyncio.timeout(15.0):
        await shutdown_ran.wait()
        while lifecycle.belief_state != InternalControllerState.SHUTDOWN:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_non_interrupt_program_waits_for_running_task(kit):
    """Without interrupt=True, switching programs waits for the current task to finish gracefully."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program_a = await sc.register_program("prog_a")
    program_b = await sc.register_program("prog_b")

    task_a_started = asyncio.Event()
    task_a_can_finish = asyncio.Event()
    task_a_finished = asyncio.Event()
    task_b_ran = asyncio.Event()
    done_b = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle_init(task):
        pass

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        if not task_a_started.is_set():
            task_a_started.set()
            # Wait until signaled — stop() will wait for this to complete.
            await task_a_can_finish.wait()
            task_a_finished.set()
        else:
            task_b_ran.set()

    @controller.task_handler(ShutdownTask)
    async def handle_shutdown(task):
        shutdown_ran.set()

    @program_a.task_factory
    async def factory_a():
        yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")

    @program_b.task_factory
    async def factory_b():
        if not done_b.is_set():
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            done_b.set()

    await kit.controller("ctrl1").enable()
    prog_a_client = kit.program("prog_a")
    await prog_a_client.enable("ctrl1")
    prog_b_client = kit.program("prog_b")
    await prog_b_client.enable("ctrl1")

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()

    # Start operating with program A without interrupt (the default).
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_a_client)

    async with asyncio.timeout(10.0):
        await task_a_started.wait()

    # Switch to program B — without interrupt, the lifecycle waits for A's task to finish.
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_b_client)

    # Give the lifecycle a moment to process the demand change. The task should NOT be aborted.
    await asyncio.sleep(0.2)
    assert not task_a_finished.is_set(), "task should still be running (not aborted)"

    # Allow the task to complete naturally.
    task_a_can_finish.set()

    async with asyncio.timeout(10.0):
        await task_a_finished.wait()
        await task_b_ran.wait()

    # Transition to SHUTDOWN for clean teardown.
    lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)

    async with asyncio.timeout(15.0):
        await shutdown_ran.wait()
        while lifecycle.belief_state != InternalControllerState.SHUTDOWN:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_quick_completion_with_long_offer_holds_tasking_loop(kit):
    """A high-priority program's task completes quickly, but the tasking loop keeps
    spinning with NoTaskAvailable retries. The lower-priority program does not get
    scheduled until the operator explicitly changes the demand.

    This documents a key system behavior: offer windows are static scheduling hints.
    When a program exhausts its tasks mid-window, the tasking loop retries every
    5 seconds rather than yielding to the next candidate."""
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program_a = await sc.register_program("prog_a")
    program_b = await sc.register_program("prog_b")

    task_a_ran = asyncio.Event()
    task_b_ran = asyncio.Event()
    done_a = asyncio.Event()
    done_b = asyncio.Event()
    shutdown_ran = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle_init(task):
        pass

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        if not task_a_ran.is_set():
            task_a_ran.set()
        else:
            task_b_ran.set()

    @controller.task_handler(ShutdownTask)
    async def handle_shutdown(task):
        shutdown_ran.set()

    @program_a.task_factory
    async def factory_a():
        if not done_a.is_set():
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            done_a.set()
        # After the first yield, factory_a returns without yielding -> NoTaskAvailable.

    @program_b.task_factory
    async def factory_b():
        if not done_b.is_set():
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            done_b.set()

    await kit.controller("ctrl1").enable()
    prog_a_client = kit.program("prog_a")
    await prog_a_client.enable("ctrl1")
    prog_b_client = kit.program("prog_b")
    await prog_b_client.enable("ctrl1")

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()

    # Start operating with the high-priority program. Its single task completes immediately.
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_a_client)

    async with asyncio.timeout(10.0):
        await task_a_ran.wait()

    # The high-priority task has completed. Give the tasking loop time to cycle back
    # and discover there are no more tasks.
    await asyncio.sleep(1.0)

    # The lifecycle is still in the TASKING step -- the loop is retrying with NoTaskAvailable.
    assert lifecycle.step == LifecycleStep.TASKING
    assert not task_b_ran.is_set(), (
        "low-priority program should not run while lifecycle is occupied with high-priority"
    )

    # In the real system, the operator would re-evaluate the schedule and switch programs.
    # Simulate that by changing the demand to the low-priority program.
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_b_client)

    async with asyncio.timeout(15.0):
        await task_b_ran.wait()

    # Transition to SHUTDOWN for clean teardown.
    lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)

    async with asyncio.timeout(15.0):
        await shutdown_ran.wait()
        while lifecycle.belief_state != InternalControllerState.SHUTDOWN:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)


@pytest.mark.asyncio
async def test_shutdown_task_completes_despite_demand_change(kit):
    """The default graceful stop waits for the running shutdown task to finish.

    Even though the lifecycle stops the ShutdownProc when the demand changes to OPERATE,
    the graceful stop mechanism (shield + cleanup) waits for the actual controller task
    handler to complete before proceeding. The handler does NOT get CancelledError.

    This means the shutdown task runs to completion, then the lifecycle starts the new demand.
    """
    sc = await kit.register_service("ctrl-svc", "0.1.0")
    controller = await sc.register_controller("ctrl1")
    program = await sc.register_program("prog1")

    init_ran = asyncio.Event()
    task_ran = asyncio.Event()
    shutdown_started = asyncio.Event()
    shutdown_can_finish = asyncio.Event()
    shutdown_finished = asyncio.Event()
    done = asyncio.Event()

    @controller.task_handler(InitTask)
    async def handle_init(task):
        init_ran.set()

    @controller.task_handler(CollectTask)
    async def handle_collect(task):
        task_ran.set()

    @controller.task_handler(ShutdownTask)
    async def handle_shutdown(task):
        shutdown_started.set()
        await shutdown_can_finish.wait()
        shutdown_finished.set()

    @program.task_factory
    async def factory():
        if not done.is_set():
            yield CollectTask(task_id=uuid.uuid1(), controller_id="ctrl1")
            done.set()

    await kit.controller("ctrl1").enable()
    prog_client = kit.program("prog1")
    await prog_client.enable("ctrl1")

    lifecycle = ControllerLifecycle()
    lifecycle.start(kit.controller("ctrl1"))
    lifecycle.enable()
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_client)

    async with asyncio.timeout(10.0):
        await init_ran.wait()
        await task_ran.wait()

    # Demand SHUTDOWN — the shutdown task handler will block on the gate.
    lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)

    async with asyncio.timeout(10.0):
        await shutdown_started.wait()

    # While the shutdown task is still running, demand OPERATE again.
    lifecycle.set_demand_state(InternalControllerState.OPERATE, program=prog_client)

    # Give the lifecycle a moment to process the demand change. The ShutdownProc is being
    # gracefully stopped, but the stop waits for cleanup futures (the shielded execute_task).
    await asyncio.sleep(0.3)

    # The shutdown handler should NOT have been cancelled — it's shielded.
    # It should still be blocked on the gate, waiting for us to release it.
    assert not shutdown_finished.is_set(), "shutdown should still be waiting (shielded from cancel)"

    # Release the shutdown handler.
    shutdown_can_finish.set()

    # The shutdown task should complete, and then the lifecycle should proceed to OPERATE.
    async with asyncio.timeout(10.0):
        await shutdown_finished.wait()

    # The pending OPERATE demand should take effect — init should run again.
    init_ran.clear()
    async with asyncio.timeout(15.0):
        await init_ran.wait()
        while lifecycle.belief_state != InternalControllerState.OPERATE:
            await asyncio.sleep(0.05)

    await cleanup(lifecycle, sc)
