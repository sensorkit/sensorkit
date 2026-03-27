from __future__ import annotations

import asyncio
from typing import Literal

import pytest
import uuid_utils.compat as uuid
from pydantic import BaseModel

from sensorkit.backend.request import CallError
from sensorkit.common.keyword import declare_keyword
from sensorkit.core.controller import ControllerDevice
from sensorkit.core.device import DeviceClient
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.task import CollectTask, InitTask


@pytest.mark.asyncio
async def test_controller(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    disabled = asyncio.Event()
    enabled = asyncio.Event()
    done = asyncio.Event()

    @controller.on_disable
    async def disable():
        disabled.set()

    @controller.on_enable
    async def on_enable():
        enabled.set()

    @controller.task_handler(InitTask)
    async def run_task(task: InitTask):
        assert ControllerImpl.current.get() is controller
        assert isinstance(task, InitTask)
        done.set()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.disable()
        await disabled.wait()
        await cli.enable()
        await enabled.wait()
        await cli.execute_task(InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"))
        await done.wait()


@pytest.mark.asyncio
async def test_controller_derived_task(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    class MyCollectTask(CollectTask):
        task_type: Literal["my_collect"] = "my_collect"
        my_field: int

    remaining = {MyCollectTask, CollectTask}

    @controller.task_handler(MyCollectTask)
    async def run_custom_collect(task: MyCollectTask):
        assert type(task) is MyCollectTask
        remaining.discard(MyCollectTask)

    @controller.task_handler(CollectTask)
    async def run_generic_collect(task: CollectTask):
        assert type(task) is CollectTask
        remaining.discard(CollectTask)

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.enable()
        await cli.execute_task(CollectTask(task_id=uuid.uuid1(), controller_id="mycontroller"))
        await cli.execute_task(
            MyCollectTask(task_id=uuid.uuid1(), controller_id="mycontroller", my_field=42)
        )
        assert not remaining


@pytest.mark.asyncio
async def test_controller_interrupt(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_task(_: InitTask):
        started.set()
        await can_finish.wait()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        exec_call = cli.execute_task(InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"))
        await exec_call.invoke()
        await started.wait()
        started.clear()

        with pytest.raises(CallError):
            await cli.execute_task(
                InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"),
                interrupt=False,
            )

        assert not started.is_set()

        interrupting_call = cli.execute_task(
            InitTask(task_id=uuid.uuid1(), controller_id="mycontroller"),
            interrupt=True,
        )
        await interrupting_call.invoke()
        await started.wait()

        with pytest.raises(CallError):
            await exec_call

        can_finish.set()
        await interrupting_call


@pytest.mark.asyncio
async def test_controller_abort(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    ready = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_task(_: InitTask):
        ready.set()
        await asyncio.sleep(5)

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        task_id = uuid.uuid1()
        exec_call = cli.execute_task(InitTask(task_id=task_id, controller_id="mycontroller"))
        await exec_call.invoke()
        await ready.wait()

        abort_call = cli.abort_task()
        abort_response = await abort_call.invoke()

        assert abort_response.call_state == "running"
        assert abort_response.aborting
        assert abort_response.task_id == task_id

        await abort_call.wait()
        assert abort_call.result() is None

        with pytest.raises(CallError):
            await exec_call.wait()


@pytest.mark.skip
@pytest.mark.asyncio
@pytest.mark.parametrize("task_id", [uuid.uuid7(), None], ids=("with_id", "without_id"))
async def test_wait_for_task(kit, task_id):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")

    started = asyncio.Event()
    can_finish = asyncio.Event()

    @controller.task_handler(InitTask)
    async def run_task(_: InitTask):
        started.set()
        await can_finish.wait()

    async with asyncio.timeout(1.0):
        cli = kit.controller("mycontroller")
        await cli.enable()

        event = await cli.wait_for_task(task_id=task_id)
        assert event is None if task_id is not None else not event.executing

        exec_call = cli.execute_task(
            InitTask(task_id=task_id or uuid.uuid7(), controller_id="mycontroller")
        )
        await exec_call.invoke()
        await started.wait()

        wait_fut = asyncio.create_task(cli.wait_for_task(task_id=task_id))
        await asyncio.sleep(0)
        assert not wait_fut.done()

        can_finish.set()
        event = await wait_fut
        assert event is not None
        assert event.finished is not None
        assert not event.finished.aborted
        assert not event.executing

        if task_id is not None:
            assert event.task.task_id == task_id

        await exec_call


@pytest.mark.asyncio
async def test_controller_use_device(kit):
    @declare_keyword
    class TestKeyword(BaseModel):
        value: int

    async with asyncio.timeout(2.0):
        sc = await kit.register_service("testservice", "0.1.0")
        controller = await sc.register_controller("mycontroller")
        # We need an actual device registered in the backend for subscriptions to work
        device_impl = await sc.register_device("camera")

    # 1. Register a device for use within the controller.
    # Returns a client instance for interacting with the registered device.
    camera_client = controller.use_device("camera")
    assert isinstance(camera_client, DeviceClient)
    assert camera_client.entity.path == ("camera",)

    # 2. Access the registered device via get_device(name)
    # Returns a ControllerDevice instance.
    camera_device = controller.get_device("camera")
    assert isinstance(camera_device, ControllerDevice)
    assert camera_device.client is camera_client

    # 3. KeyError: If attempting to access a device via get_device(name) that hasn't been registered.
    with pytest.raises(KeyError):
        controller.get_device("unknown")

    # 4. all_devices() returns all registered devices.
    devices = list(controller.all_devices())
    assert len(devices) == 1
    assert devices[0] is camera_device

    # 5. Register device and subscribe to specific keyword types.
    # Subscriptions remain inactive until start_device_subscriptions() is called.
    controller.use_device("camera", subscribe=[TestKeyword])

    # Check that it's still the same device
    assert controller.get_device("camera") is camera_device

    # 6. Verify inactivity before start_device_subscriptions()
    await device_impl.publish(TestKeyword(value=10))
    await asyncio.sleep(0.05)
    assert camera_device.get(TestKeyword) is None

    await controller.start_device_subscriptions()

    # 7. Access cached subscription values from the ControllerDevice instance.
    # Publish a value from the device implementation
    await device_impl.publish(TestKeyword(value=42))

    # Wait for the update to propagate through the subscription
    for _ in range(10):
        if camera_device.get(TestKeyword) is not None:
            break
        await asyncio.sleep(0.01)

    pointing = camera_device.get(TestKeyword)
    assert pointing is not None
    assert pointing.value == 42

    # Also test __getitem__
    assert camera_device[TestKeyword].value == 42

    # 8. Test multiple use_device calls accumulate subscriptions
    @declare_keyword
    class AnotherKeyword(BaseModel):
        other_value: int

    controller.use_device("camera", subscribe=[AnotherKeyword])
    await device_impl.publish(AnotherKeyword(other_value=100))

    # Wait for the update to propagate through the subscription
    for _ in range(10):
        if camera_device.get(AnotherKeyword) is not None:
            break
        await asyncio.sleep(0.01)

    assert camera_device.get(AnotherKeyword).other_value == 100
    # Original keyword still works
    assert camera_device.get(TestKeyword).value == 42

    # 9. stop_device_subscriptions()
    await controller.stop_device_subscriptions()
