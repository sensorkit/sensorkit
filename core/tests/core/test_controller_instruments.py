# SPDX-License-Identifier: Apache-2.0
"""Controller instrument registration, capture context, and task selection."""

from __future__ import annotations

import pytest

import sensorkit.api as sk
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.backend.request import CallError
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.data.context import merge_contexts
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask

TARGET = AltAzTarget(coords=Horizontal(az=90.0, alt=30.0))


def collecting(*instruments: str | None) -> StandardCollectTask:
    return StandardCollectTask(
        target=TARGET,
        camera_params=[
            CameraParameterSet(instrument=name, integration_time_seconds=1.0, frame_count=1)
            for name in instruments
        ],
    )


@pytest.mark.asyncio
async def test_attachment_publishes_registered_instruments(kit, service_context):
    impl = ControllerImpl.for_service_context(service_context, "bench")

    def register():
        impl.use_device("mount")
        impl.register_instrument("wide", devices=("cam-a", kit.device("wheel")))
        impl.register_instrument("narrow", devices=["wheel", "cam-b"])
        impl.register_instrument("guider")

    impl.on_attach(register)
    await service_context.register_impl(impl)
    info = await kit.controller("bench").kv_get_model(sk.EntityInfo)

    assert isinstance(info.details, sk.ControllerDetails)
    assert info.details.instruments == (
        sk.InstrumentInfo(name="wide", devices=("cam-a", "wheel")),
        sk.InstrumentInfo(name="narrow", devices=("wheel", "cam-b")),
        sk.InstrumentInfo(name="guider"),
    )
    # Associations are independent of the controller's device dependencies.
    assert info.details.controlled_devices == ["mount"]


@pytest.mark.asyncio
async def test_registration_rejects_only_an_empty_name(controller_impl):
    with pytest.raises(ValueError, match="at least 1 character"):
        controller_impl.register_instrument("")

    controller_impl.register_instrument("  ")

    assert controller_impl.entity_info().details.instruments == (sk.InstrumentInfo(name="  "),)


@pytest.mark.asyncio
async def test_registration_rejects_a_repeated_name(controller_impl):
    controller_impl.register_instrument("imager", devices=("cam",))

    with pytest.raises(ValueError, match="Cannot register instrument 'imager' twice"):
        controller_impl.register_instrument("imager")

    controller_impl.register_instrument("Imager")

    assert controller_impl.entity_info().details.instruments == (
        sk.InstrumentInfo(name="imager", devices=("cam",)),
        sk.InstrumentInfo(name="Imager"),
    )


@pytest.mark.asyncio
async def test_registration_rejects_one_string_as_devices(controller_impl):
    with pytest.raises(TypeError, match="collection of IDs or clients"):
        controller_impl.register_instrument("imager", devices="cam")


@pytest.mark.asyncio
async def test_a_controller_without_devices_runs_named_collects(kit, service):
    ctrl = sk.declare_controller(name="tcs")
    exposed: list[str | None] = []

    @sk.on_attach(ctrl)
    def register():
        ctrl.register_instrument("imager")

    @sk.task_handler(ctrl)
    async def collect(task: StandardCollectTask):
        unknown = [p.instrument for p in task.exposures if p.instrument not in (None, "imager")]

        if unknown:
            raise ValueError(f"Unregistered instruments {unknown}")

        exposed.extend(p.instrument for p in task.exposures)

    service.add(ctrl)
    await service.start()
    client = kit.controller("tcs")
    info = await client.kv_get_model(sk.EntityInfo)

    assert info.details.instruments == (sk.InstrumentInfo(name="imager"),)
    assert info.details.controlled_devices == []

    await client.enable()
    await client.execute_task(collecting("imager", None))

    with pytest.raises(CallError):
        await client.execute_task(collecting("spectrograph"))

    assert exposed == ["imager", None]


@pytest.mark.asyncio
async def test_an_instrument_context_is_a_fresh_copy_of_its_registration(service_context):
    # Registration supplies context before the controller attaches.
    impl = ControllerImpl.for_service_context(service_context, "bench")
    impl.register_instrument("wide", devices=("cam-a", "wheel"))
    impl.register_instrument("guider")
    registered = impl.entity_info().details.instruments
    context = impl.instrument_context("wide")

    assert list(context) == ["Instrument"]
    assert context[sk.InstrumentInfo] == sk.InstrumentInfo(name="wide", devices=("cam-a", "wheel"))
    assert context.eval("Instrument.devices") == ("cam-a", "wheel")
    assert impl.instrument_context("guider")[sk.InstrumentInfo] == sk.InstrumentInfo(name="guider")

    context[sk.InstrumentInfo].name = "changed"
    context[sk.InstrumentInfo].devices = ()
    context.set_value("probe", "added")
    again = impl.instrument_context("wide")

    assert again is not context
    assert list(again) == ["Instrument"]
    assert again[sk.InstrumentInfo] == sk.InstrumentInfo(name="wide", devices=("cam-a", "wheel"))
    assert impl.entity_info().details.instruments == registered
    assert registered[0] == sk.InstrumentInfo(name="wide", devices=("cam-a", "wheel"))


@pytest.mark.asyncio
async def test_editing_published_instruments_leaves_the_registration_alone(controller_impl):
    controller_impl.register_instrument("wide", devices=("cam-a", "wheel"))
    (published,) = controller_impl.entity_info().details.instruments
    published.name = "changed"
    published.devices = ()
    wide = sk.InstrumentInfo(name="wide", devices=("cam-a", "wheel"))

    assert controller_impl.entity_info().details.instruments == (wide,)
    assert controller_impl.instrument_context("wide")[sk.InstrumentInfo] == wide


@pytest.mark.asyncio
async def test_an_unregistered_instrument_has_no_context(controller_impl):
    with pytest.raises(KeyError, match="No instrument 'imager' is registered"):
        controller_impl.instrument_context("imager")

    controller_impl.register_instrument("imager")

    with pytest.raises(KeyError, match="No instrument 'Imager' is registered"):
        controller_impl.instrument_context("Imager")


@pytest.mark.asyncio
async def test_a_controller_without_devices_builds_capture_contexts(kit, service):
    ctrl = sk.declare_controller(name="tcs")
    captured: list[sk.Context] = []

    @sk.on_attach(ctrl)
    def register():
        ctrl.register_instrument("imager")
        ctrl.register_instrument("guider")

    @sk.task_handler(ctrl)
    async def collect(execution: sk.TaskExecution[StandardCollectTask]):
        captured.extend(
            merge_contexts(execution.context, ctrl.instrument_context(p.instrument or "imager"))
            for p in execution.task.exposures
        )

    service.add(ctrl)
    await service.start()
    client = kit.controller("tcs")
    await client.enable()
    await client.execute_task(
        collecting("guider", None), context=KeywordDict(sk.InstrumentInfo(name="submitted"))
    )

    with pytest.raises(CallError):
        await client.execute_task(collecting("spectrograph"))

    assert [c[sk.InstrumentInfo] for c in captured] == [
        sk.InstrumentInfo(name="guider"),
        sk.InstrumentInfo(name="imager"),
    ]
    assert [c.resolve("{Instrument.name}") for c in captured] == ["guider", "imager"]
    assert all(c[sk.TaskInfo].task == collecting("guider", None) for c in captured)
