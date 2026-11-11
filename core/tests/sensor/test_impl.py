# SPDX-License-Identifier: Apache-2.0
"""Controller failure recovery, service stop and device keyword subscriptions."""
from __future__ import annotations

import asyncio

import pytest

from sensorkit.astro.common import RADecPointing, SitePosition
from sensorkit.backend.request import CallError
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.client import SensorKit
from sensorkit.core.controller import ControllerState
from sensorkit.core.entity import EntityInfo, InstrumentInfo
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.task import TaskInfo
from sensorkit.sensor.client import Sensor
from sensorkit.sensor.execution import WorkflowError
from sensorkit.sensor.impl import SensorConfig, SensorController
from sensorkit.sensor.policies import SensorPolicies
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.optics import Filter, Filters, SetFilter

from .common import BENCH, TARGET, Rig, authored


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
async def test_collect_headers_carry_pointing_from_an_undeclared_mount(
        kit, rig, service_context, controller_impl):
    pointing = RADecPointing(right_ascension_hours=6.0, declination_degrees=-30.0)
    await rig.serve(service_context, "telescope",
                    ("Init", "Deinit", "MoveToPark", "SetParkPosition", "Stop",
                     "Home", "FollowTarget", "Abort"),
                    keywords=[pointing], declare=False)
    controller = await attached(controller_impl, """
        sensor:
          components:
            - device: telescope
            - device: cam
              instrument: true
        """)
    await controller_impl.start_device_subscriptions()

    try:
        # Wait for the pointing published before the subscriptions started.
        async with asyncio.timeout(2.0):
            while controller.device_contexts()["telescope"].get(RADecPointing) is None:
                await asyncio.sleep(0)

        client = kit.controller(str(controller_impl.entity))
        await client.enable()
        await client.execute_task(StandardCollectTask(
            target=TARGET, camera_params=CameraParameterSet(
                integration_time_seconds=0.1, frame_count=1)))
    finally:
        await controller_impl.stop_device_subscriptions()

    capture, = rig["cam"].sent("CameraCapture")

    assert capture.context[RADecPointing] == pointing


@pytest.mark.asyncio
async def test_collect_headers_keep_task_context_without_keywords_from_other_chains(
        kit, rig, service_context, controller_impl):
    wheel = await service_context.register_device("off-chain")
    wheel.declare_published_keyword("Filter")
    await wheel.publish_entity_info()
    definition = authored("""
        sensor:
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

    controller_impl.register_instrument("cam", devices=("mount", "cam"))
    controller_impl.add_task_handler(
        StandardCollectTask, execution_callback=controller.collect_task
    )
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
    controller_impl.add_task_handler(
        StandardCollectTask, execution_callback=controller.collect_task
    )

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


# Instrument metadata and selection


REORDERED = """
    sensor:
      components:
        - unit: west
          components:
            - device: cam-w
              instrument: true
            - device: wheel-w
        - device: mount
        - unit: east
          components:
            - device: cam-e
              instrument: true
            - device: foc-e
    """
"""Equivalent BENCH layout with reordered components."""


async def registered(kit, service, document: str) -> None:
    """Register the sensor controller with the supplied device layout."""
    client = kit.controller("sensor")
    await client.kv_put_model(
        SensorConfig(model=authored(document).sensor, policies=SensorPolicies())
    )
    await client.kv_put_model(
        SitePosition(latitude_degrees=20.0, longitude_degrees=-100.0, altitude_km=1.0)
    )
    impl = ControllerImpl.for_service_context(service, "sensor")
    controller = SensorController()
    impl.on_attach(controller.on_attach)
    impl.add_task_handler(StandardCollectTask, execution_callback=controller.collect_task)
    await service.register_impl(impl)


def published(info: EntityInfo) -> dict[str, set[str]]:
    """Return instrument names mapped to their associated device IDs."""
    return {instrument.name: set(instrument.devices) for instrument in info.details.instruments}


def naming(instrument: str, **kw) -> StandardCollectTask:
    return StandardCollectTask(
        target=TARGET,
        camera_params=CameraParameterSet(
            instrument=instrument, integration_time_seconds=0.1, frame_count=1, **kw
        ),
    )


@pytest.mark.asyncio
async def test_each_instrument_placement_is_published_under_its_device_key(
    kit, rig, service_context
):
    keys = [f"cam-{number}" for number in range(10)]

    for key in ("wheel", *keys):
        await rig.serve(service_context, key, ("Connect",))

    await registered(
        kit,
        service_context,
        """
        sensor:
          components:
            - device: mount
            - unit: array
              components:
                - device: wheel
        """
        + "".join(
            f"""
                - device: {key}
                  instrument: true
        """
            for key in keys
        ),
    )
    info = await kit.controller("sensor").kv_get_model(EntityInfo)

    assert published(info) == {key: {"mount", "wheel", key} for key in keys}
    assert set(info.details.controlled_devices) == {"mount", "wheel", *keys}


@pytest.mark.asyncio
async def test_instrument_names_survive_restart_and_reordering(backend):
    seen = []
    captured = []

    # Each iteration starts a fresh service against the same persisted data.
    for document in (BENCH, REORDERED):
        kit = SensorKit(backend)
        service = await kit.register_service("sensor-service", "0.1.0")
        rig = Rig()

        try:
            await rig.serve(service, "mount", ("FollowTarget", "Stop", "Abort"))

            for key in ("foc-e", "wheel-w"):
                await rig.serve(service, key, ("Connect",))

            for key in ("cam-e", "cam-w"):
                await rig.serve(service, key, ("CameraCapture", "Abort"))

            await registered(kit, service, document)
            client = kit.controller("sensor")
            seen.append(published(await client.kv_get_model(EntityInfo)))
            await client.enable()
            await client.execute_task(naming("cam-w"))
            captured.append(
                {key: len(rig[key].sent("CameraCapture")) for key in ("cam-e", "cam-w")}
            )
        finally:
            await rig.release()
            await service.shutdown()

    assert (
        seen
        == [{"cam-e": {"mount", "foc-e", "cam-e"}, "cam-w": {"mount", "wheel-w", "cam-w"}}] * 2
    )
    assert captured == [{"cam-e": 0, "cam-w": 1}] * 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name", ["cam-n", "mount", "cam-e"], ids=["unknown", "not-an-instrument", "lacks-the-filter"]
)
async def test_a_named_target_the_sensor_cannot_honor_sends_nothing(
    kit, rig, service_context, controller_impl, name
):
    for branch, held in (("e", "r"), ("w", "g")):
        await rig.serve(
            service_context,
            f"wheel-{branch}",
            ("SetFilter",),
            keywords=[Filters(filters=[Filter(name=held)])],
        )
        await rig.serve(service_context, f"cam-{branch}", ("CameraCapture", "Abort"))

    controller = await attached(controller_impl, BENCH.replace("foc-e", "wheel-e"))
    await controller_impl.start_device_subscriptions()

    try:
        async with asyncio.timeout(2.0):
            while any(
                controller.device_contexts()[wheel].get(Filters) is None
                for wheel in ("wheel-e", "wheel-w")
            ):
                await asyncio.sleep(0)

        client = kit.controller(str(controller_impl.entity))
        await client.enable()

        with pytest.raises(CallError):
            await client.execute_task(naming(name, filter_name="g"))

        state = await client.kv_get_model(ControllerState)
    finally:
        await controller_impl.stop_device_subscriptions()

    assert name in state.execution_state.finished.error
    assert all(device.received == [] for device in rig.devices.values())


async def served_wheels(rig, service_context, controller_impl) -> SensorController:
    """Attach camera chains with r filters on the east and g filters on the west."""
    for branch, held in (("e", "r"), ("w", "g")):
        await rig.serve(
            service_context,
            f"wheel-{branch}",
            ("SetFilter",),
            keywords=[Filters(filters=[Filter(name=held)])],
        )
        await rig.serve(service_context, f"cam-{branch}", ("CameraCapture", "Abort"))

    controller = await attached(controller_impl, BENCH.replace("foc-e", "wheel-e"))
    await controller_impl.start_device_subscriptions()

    # Wait for subscriptions to receive both filter lists.
    async with asyncio.timeout(2.0):
        while any(
            controller.device_contexts()[wheel].get(Filters) is None
            for wheel in ("wheel-e", "wheel-w")
        ):
            await asyncio.sleep(0)

    return controller


def chain_of(instrument: str) -> set[str]:
    """Return the devices on a bench camera's instrument chain."""
    return {"mount", instrument.replace("cam", "wheel"), instrument}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task", "expected"),
    [(collecting("g"), "cam-w"), (naming("cam-e"), "cam-e")],
    ids=["automatic", "named"],
)
async def test_a_capture_carries_its_assigned_instrument(
    kit, rig, service_context, controller_impl, task, expected
):
    await served_wheels(rig, service_context, controller_impl)

    try:
        client = kit.controller(str(controller_impl.entity))
        await client.enable()
        await client.execute_task(task)
    finally:
        await controller_impl.stop_device_subscriptions()

    (capture,) = rig[expected].sent("CameraCapture")
    instrument = capture.context[InstrumentInfo]

    assert capture.context[Collect].params.instrument == task.camera_params.instrument
    assert instrument.name == expected
    assert set(instrument.devices) == chain_of(expected)
    assert capture.context.resolve("{Instrument.name}") == expected
    assert capture.context.get(Filters) is not None


@pytest.mark.asyncio
async def test_a_capture_takes_its_instrument_from_the_controller_not_its_devices(
    kit, rig, service_context, controller_impl
):
    controller = await served_wheels(rig, service_context, controller_impl)
    controller_impl.register_instrument("guider")
    task = StandardCollectTask(
        target=TARGET,
        camera_params=[
            CameraParameterSet(instrument=key, integration_time_seconds=0.1, frame_count=1)
            for key in ("cam-e", "cam-w")
        ],
    )

    try:
        contexts = controller.device_contexts()
        client = kit.controller(str(controller_impl.entity))
        await client.enable()
        await client.execute_task(task)
    finally:
        await controller_impl.stop_device_subscriptions()

    assert InstrumentInfo(name="guider") in controller_impl.entity_info().details.instruments
    assert all(context.get(InstrumentInfo) is None for context in contexts.values())

    for key in ("cam-e", "cam-w"):
        (capture,) = rig[key].sent("CameraCapture")

        assert (
            capture.context[InstrumentInfo]
            == controller_impl.instrument_context(key)[InstrumentInfo]
        )


@pytest.mark.asyncio
async def test_captures_keep_their_own_instrument_alongside_task_context(
    kit, rig, service_context, controller_impl
):
    await served_wheels(rig, service_context, controller_impl)
    task = StandardCollectTask(
        target=TARGET,
        camera_params=[
            CameraParameterSet(integration_time_seconds=0.1, frame_count=2, filter_name="r"),
            CameraParameterSet(instrument="cam-w", integration_time_seconds=0.1, frame_count=1),
        ],
    )
    submitted = KeywordDict({"probe": "task context"}, InstrumentInfo(name="submitted"))

    try:
        client = kit.controller(str(controller_impl.entity))
        await client.enable()
        await client.execute_task(task, context=submitted)
    finally:
        await controller_impl.stop_device_subscriptions()

    captures = {key: rig[key].sent("CameraCapture") for key in ("cam-e", "cam-w")}

    for key, sent in captures.items():
        for capture in sent:
            assert capture.context[InstrumentInfo].name == key
            assert set(capture.context[InstrumentInfo].devices) == chain_of(key)
            assert capture.context["probe"] == "task context"
            assert capture.context[TaskInfo].task == task
            assert capture.context[Filters].filters[0].name == {"cam-e": "r", "cam-w": "g"}[key]

    assert {
        key: [c.context[Collect].frame_number for c in sent] for key, sent in captures.items()
    } == {"cam-e": [0, 1], "cam-w": [0]}
    assert all(
        "Instrument" not in controller_impl.get_device(key).subscription.cache
        for key in ("mount", "wheel-e", "wheel-w", "cam-e", "cam-w")
    )
    assert published(controller_impl.entity_info()) == {
        key: chain_of(key) for key in ("cam-e", "cam-w")
    }
