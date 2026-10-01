# SPDX-License-Identifier: Apache-2.0
"""Controller failure recovery, service stop and device keyword subscriptions."""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.astro.common import SitePosition
from sensorkit.backend.request import CallError
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.task import TaskInfo
from sensorkit.sensor.client import Sensor
from sensorkit.sensor.execution import WorkflowError
from sensorkit.sensor.impl import SensorConfig, SensorController
from sensorkit.sensor.policies import SensorPolicies
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask
from sensorkit.std.optics import Filter, Filters, SetFilter

from .common import BENCH, TARGET, authored


@pytest.fixture
def handles():
    return {"mount": ("Home", "FollowTarget", "Stop", "Abort"),
            "cam": ("CameraCapture", "Abort"),
            "passive": ("Connect",)}


@pytest.mark.asyncio
@pytest.mark.parametrize("refuse_abort", [False, True])
async def test_a_lifecycle_failure_aborts_all_devices_and_preserves_the_error(
        kit, rig, refuse_abort):
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: mount
            - device: cam
              instrument: true
            - device: passive
        tables:
          home:
            fail_fast: true
            phases:
              - name: home
                entries:
                  - select: {device: mount}
                    ops: Home
        """)
    controller = SensorController()
    controller.sensor = await Sensor.connect(definition, kit)
    controller.tables = {table.name: table for table in definition.tables}
    controller.devices = tuple(rig.devices)
    rig["mount"].refusing["Home"] = 1

    if refuse_abort:
        rig["mount"].refusing["Abort"] = 1

    with pytest.raises(WorkflowError, match="Home refused"):
        await controller.run_table("home")

    assert [c.model_tag() for c in rig["mount"].received] == ["Home", "Abort"]
    assert [c.model_tag() for c in rig["cam"].received] == ["Abort"]
    assert rig["passive"].received == []
    assert not rig.in_flight()


@pytest.mark.asyncio
@pytest.mark.parametrize(("stopped", "cleaned_up"), [(False, True), (True, False)])
async def test_cleanup_skips_a_cancellation_after_service_stop(
        kit, rig, stopped, cleaned_up):
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: mount
        tables:
          standby:
            fail_fast: true
            phases:
              - name: home
                entries:
                  - select: {device: mount}
                    ops: Home
                    id: home-mount
            cleanup:
              - name: park
                armed_by: [home-mount]
                entries:
                  - select: {device: mount}
                    ops: Stop
        """)
    controller = SensorController()
    controller.sensor = await Sensor.connect(definition, kit)
    controller.tables = {table.name: table for table in definition.tables}
    controller.devices = ("mount",)
    rig["mount"].hold("Home")
    task = asyncio.create_task(controller.run_table("standby"))

    async with asyncio.timeout(2.0):
        await rig["mount"].arrival("Home").wait()

        # A stopping service detaches before loop teardown cancels its tasks.
        if stopped:
            await controller.on_detach()

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert bool(rig["mount"].sent("Stop")) == cleaned_up
    assert not rig.in_flight()


@pytest.mark.asyncio
async def test_attach_subscribes_to_reported_keywords_outside_the_pointing_vocabulary(
        service_context, controller_impl):
    wheel = await service_context.register_device("wheel")
    wheel.declare_published_keyword("Filter")
    await wheel.publish_entity_info()
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: wheel
        """)
    await controller_impl.kv_put_model(SensorConfig(
        model=definition.sensor, policies=SensorPolicies()))
    await controller_impl.kv_put_model(SitePosition(
        latitude_degrees=20.0, longitude_degrees=-100.0, altitude_km=1.0))
    controller = SensorController()
    await controller.on_attach()
    await controller_impl.start_device_subscriptions()

    try:
        value = Filter(name="r")

        async with asyncio.timeout(2.0):
            while controller.device_contexts()["wheel"].get(Filter) is None:
                await wheel.publish(value)
                await asyncio.sleep(0)

        assert controller.device_contexts()["wheel"][Filter] == value
    finally:
        await controller_impl.stop_device_subscriptions()


@pytest.mark.asyncio
async def test_collect_headers_keep_task_context_without_keywords_from_other_chains(
        kit, rig, service_context, controller_impl):
    wheel = await service_context.register_device("off-chain")
    wheel.declare_published_keyword("Filter")
    await wheel.publish_entity_info()
    definition = authored("""
        sensor:
          name: bench
          components:
            - device: mount
            - unit: collecting
              components:
                - device: cam
                  instrument: true
            - unit: other
              components:
                - device: off-chain
        """)
    controller = SensorController()
    controller.sensor = await Sensor.connect(definition, kit)
    controller.devices = ("mount", "cam", "off-chain")
    controller.site = SitePosition(latitude_degrees=20.0,
                                   longitude_degrees=-100.0, altitude_km=1.0)

    for key in controller.devices:
        controller_impl.use_device(key, subscribe=[Filter] if key == "off-chain" else [])

    controller_impl.task_handler(StandardCollectTask)(controller.collect_task)
    await controller_impl.start_device_subscriptions()

    try:
        async with asyncio.timeout(2.0):
            while controller.device_contexts()["off-chain"].get(Filter) is None:
                await wheel.publish(Filter(name="unrelated"))
                await asyncio.sleep(0)

        client = kit.controller(str(controller_impl.entity))
        await client.enable()
        task = StandardCollectTask(target=TARGET, camera_params=CameraParameterSet(
            integration_time_seconds=0.1, frame_count=1))
        await client.execute_task(task, context=KeywordDict({"probe": "task context"}))
        capture, = rig["cam"].sent("CameraCapture")

        assert capture.context["probe"] == "task context"
        assert capture.context[TaskInfo].task == task
        assert capture.context[SitePosition] == controller.site
        assert capture.context.get(Filter) is None
    finally:
        await controller_impl.stop_device_subscriptions()


# Filter assignment


async def attached(controller_impl, document: str) -> SensorController:
    """Attach a controller to the sensor document and register its collect handler."""
    definition = authored(document)
    await controller_impl.kv_put_model(SensorConfig(
        model=definition.sensor, policies=SensorPolicies()))
    await controller_impl.kv_put_model(SitePosition(
        latitude_degrees=20.0, longitude_degrees=-100.0, altitude_km=1.0))
    controller = SensorController()
    await controller.on_attach()
    controller_impl.task_handler(StandardCollectTask)(controller.collect_task)

    return controller


def collecting(filter_name: str) -> StandardCollectTask:
    return StandardCollectTask(target=TARGET, camera_params=CameraParameterSet(
        integration_time_seconds=0.1, frame_count=1, filter_name=filter_name))


@pytest.mark.asyncio
async def test_collect_assigns_the_camera_whose_wheel_reports_the_filter(
        kit, rig, service_context, controller_impl):
    for branch, held in (("e", "r"), ("w", "g")):
        await rig.serve(service_context, f"wheel-{branch}", ("SetFilter",),
                        keywords=[Filters(filters=[Filter(name=held)])])
        await rig.serve(service_context, f"cam-{branch}",
                        ("CameraCapture", "Abort"))

    controller = await attached(controller_impl, BENCH.replace("foc-e", "wheel-e"))
    await controller_impl.start_device_subscriptions()

    try:
        # Wait for the filter lists published before the subscriptions started.
        async with asyncio.timeout(2.0):
            while any(controller.device_contexts()[wheel].get(Filters) is None
                      for wheel in ("wheel-e", "wheel-w")):
                await asyncio.sleep(0)

        client = kit.controller(str(controller_impl.entity))
        await client.enable()
        await client.execute_task(collecting("g"))
    finally:
        await controller_impl.stop_device_subscriptions()

    assert rig["wheel-w"].sent("SetFilter") == [SetFilter(filter="g")]
    assert len(rig["cam-w"].sent("CameraCapture")) == 1
    assert rig["wheel-e"].received == []
    assert rig["cam-e"].received == []


@pytest.mark.asyncio
async def test_a_refused_filter_on_an_unknown_wheel_blocks_the_capture(
        kit, rig, service_context, controller_impl):
    wheel = await rig.serve(service_context, "wheel", ("SetFilter",))
    wheel.refusing["SetFilter"] = 1
    await attached(controller_impl, """
        sensor:
          name: bench
          components:
            - device: mount
            - device: wheel
            - device: cam
              instrument: true
        """)
    client = kit.controller(str(controller_impl.entity))
    await client.enable()

    with pytest.raises(CallError):
        await client.execute_task(collecting("g"))

    assert wheel.sent("SetFilter") == [SetFilter(filter="g")]
    assert rig["cam"].sent("CameraCapture") == []
