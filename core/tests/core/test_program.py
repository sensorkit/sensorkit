from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta

import pytest

from sensorkit.core.program import ProgramOffering, ProgramTaskingState
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.core.task import CollectTask


@pytest.mark.asyncio
async def test_program_offering(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        program = await sc.register_program("myprogram")

    ready = asyncio.Event()
    now = datetime.now().replace(microsecond=0)
    offer = (now, now + timedelta(hours=1))
    program_client = kit.program("myprogram")

    async def monitor_offering():
        stream = await program_client.monitor(ProgramOffering)
        ready.set()

        async for _, data in stream:
            offered = data.offer_windows
            assert len(offered) == 1
            assert offered[0].begin == offer[0]
            assert offered[0].end == offer[1]
            break

    task = asyncio.create_task(monitor_offering())

    async with asyncio.timeout(2.0):
        await program_client.enable("any")
        await ready.wait()

        program.clear_offers()
        program.add_offer(*offer)
        await program.publish_offers()
        await task


@pytest.mark.asyncio
async def test_program_tasking(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")
        program = await sc.register_program("myprogram")

    task_obj = CollectTask(task_id=uuid.uuid1(), controller_id="mycontroller")
    task_ran = False
    enabled = asyncio.Event()
    disabled = asyncio.Event()
    done = asyncio.Event()

    @program.on_enable
    async def program_enable():
        assert ProgramImpl.current.get() is program
        enabled.set()

    @program.on_disable
    async def program_disable():
        assert ProgramImpl.current.get() is program
        disabled.set()

    @program.task_factory
    async def program_tasker():
        assert ProgramImpl.current.get() is program

        if not done.is_set():
            assert not task_ran
            await (yield task_obj)
            assert task_ran
            done.set()

    @controller.task_handler(CollectTask)
    async def run_task(task: CollectTask):
        assert ProgramImpl.current.get() is program
        assert task == task_obj
        nonlocal task_ran
        task_ran = True

    ready = asyncio.Event()
    activated = asyncio.Event()
    stopping = asyncio.Event()
    deactivated = asyncio.Event()
    program_client = kit.program("myprogram")

    async def state_monitor():
        events = await program_client.tasking_change_events()
        ready.set()

        async for event in events:
            if event.active:
                if event.stopping:
                    assert not stopping.is_set()
                    stopping.set()
                else:
                    assert not activated.is_set()
                    activated.set()
            else:
                assert not deactivated.is_set()
                deactivated.set()

    _t = asyncio.create_task(state_monitor())

    async with asyncio.timeout(2.0):
        await kit.controller("mycontroller").enable()
        await ready.wait()
        await program_client.enable("mycontroller")
        await enabled.wait()
        await program_client.start_tasking()
        await activated.wait()
        await done.wait()
        await program_client.stop_tasking()
        await stopping.wait()
        await deactivated.wait()
        await program_client.disable()
        await disabled.wait()


@pytest.mark.asyncio
async def test_program_publishes_tasking_state(kit):
    """While tasking, the program publishes a ProgramTaskingState carrying the live execution,
    then clears it (executing_task=None) once the task completes."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")
        program = await sc.register_program("myprogram")

    task_obj = CollectTask()
    done = asyncio.Event()

    @program.task_factory
    async def program_tasker():
        if not done.is_set():
            await (yield task_obj)
            done.set()

    @controller.task_handler(CollectTask)
    async def run_task(task: CollectTask):
        pass

    ready = asyncio.Event()
    cleared = asyncio.Event()
    program_client = kit.program("myprogram")
    executing: asyncio.Future[ProgramTaskingState] = asyncio.get_running_loop().create_future()

    async def monitor():
        stream = await program_client.monitor_event(ProgramTaskingState)
        ready.set()

        async for event in stream:
            if event.executing_task is not None:
                if not executing.done():
                    executing.set_result(event)
            elif executing.done():
                # The clear must follow an executing state, never precede it.
                cleared.set()
                break

    monitor_task = asyncio.create_task(monitor())

    async with asyncio.timeout(2.0):
        await kit.controller("mycontroller").enable()
        await ready.wait()
        await program_client.enable("mycontroller")
        await program_client.start_tasking()

        event = await executing
        execution = event.executing_task
        # The published state carries the full TaskExecution envelope, not just an id.
        assert execution.task == task_obj
        assert execution.task_id is not None
        assert execution.controller_id == "mycontroller"

        await done.wait()
        await cleared.wait()
        await program_client.stop_tasking()

    await monitor_task
