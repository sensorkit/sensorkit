# SPDX-License-Identifier: Apache-2.0
"""Autofocus sweep translation and controller execution.

Served devices record focuser moves and captures.
A focus provider publishes analysis for the sweep identified by those captures.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.backend.request import CallError
from sensorkit.core.controller import ControllerState
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.task import TaskInfo
from sensorkit.sensor.autofocus import translate_autofocus
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import AcquisitionRequest, BoundCollect, CollectIntent, pack
from sensorkit.sensor.impl import SensorConfig, SensorController
from sensorkit.sensor.policies import SensorPolicies
from sensorkit.sensor.topology import Placement
from sensorkit.std.collect import CollectInfo
from sensorkit.std.focus import (
    AutoFocus,
    ChangeFocusPosition,
    FocusAnalysis,
    FocusAnalysisSource,
    FocusPosition,
    FocusSweepAnalysis,
    FocusSweepContext,
)
from sensorkit.std.instrument import Binning, ConfigureCameraSensor, ExposureInfo, FrameType
from sensorkit.std.optics import SetFilter

from .common import authored, reached, sensor_of

LAYOUT = """
    sensor:
      components:
        - device: mount
        - unit: east
          components:
            - device: foc
            - device: wheel
            - device: cam
              instrument: true
    """
"""One instrument whose chain holds a focuser and a filter wheel under the mount."""

START = 1000.0
"""Focuser position reported before every sweep."""


@pytest.fixture
def handles():
    return {
        "mount": ("FollowTarget", "Stop", "Abort"),
        "wheel": ("SetFilter", "Abort"),
        "cam": ("CameraCapture", "ConfigureCameraSensor", "Abort"),
    }


@pytest_asyncio.fixture
async def provider(service_context):
    """The configured focus analysis entity."""
    return await focus_provider(service_context, "analysis")


@pytest.fixture
def policies() -> SensorPolicies:
    return SensorPolicies()


@pytest.fixture
def controller() -> SensorController:
    return SensorController()


@pytest_asyncio.fixture
async def sensor(kit, rig, service_context, controller_impl, controller, provider, policies):
    """The client of the attached sensor controller, enabled."""
    await rig.serve(
        service_context,
        "foc",
        ("ChangeFocusPosition", "Abort"),
        keywords=[FocusPosition(position=START)],
    )
    await controller_impl.kv_put_model(
        SensorConfig(model=authored(LAYOUT).sensor, policies=policies)
    )
    await controller_impl.kv_put_model(
        SitePosition(latitude_degrees=20.0, longitude_degrees=-100.0, altitude_km=1.0)
    )
    await controller_impl.kv_put_model(FocusAnalysisSource(entity="analysis"))
    await controller.on_attach()
    controller_impl.add_task_handler(AutoFocus, execution_callback=controller.autofocus_task)
    await controller_impl.start_device_subscriptions()

    try:
        async with asyncio.timeout(2.0):
            while controller.device_contexts()["foc"].get(FocusPosition) is None:
                await asyncio.sleep(0)

        client = kit.controller(str(controller_impl.entity))
        await client.enable()

        yield client
    finally:
        await controller_impl.stop_device_subscriptions()


async def focus_provider(service_context, name: str) -> EntityImpl:
    impl = await service_context.register_impl(
        EntityImpl.for_service_context(service_context, name)
    )

    for keyword in ("FocusAnalysis", "FocusSweepAnalysis"):
        impl.declare_published_keyword(keyword)

    await impl.publish_entity_info()

    return impl


def sweeping(count: int = 3, **kw) -> AutoFocus:
    """A sweep 100 apart, by default 900, 1000, 1100 around `START`."""
    return AutoFocus(
        instrument="cam", step=100.0, count=count, integration_time_seconds=0.1, **kw
    )


def analysis(
    sweep_id: UUID, controller: str, *, instrument: str = "cam", **outcome
) -> FocusSweepAnalysis:
    """A sweep analysis, by default a minimum at 1080."""
    sample = FocusAnalysis(
        controller_id=controller,
        instrument=instrument,
        image_id="frame-0",
        exposure=ExposureInfo(
            date_obs=datetime(2026, 11, 11, tzinfo=UTC), exposure_time=0.1, detector="QHY600M"
        ),
        sweep=FocusSweepContext(
            sweep_id=sweep_id, sample_index=0, sample_count=1, commanded_position=START
        ),
        median_fwhm_pixels=3.0,
        measurement_count=12,
        valid=True,
    )
    fields = {
        "outcome": "success",
        "position": 1080.0,
        "disposition": "equals",
        "rms_residual_pixels": 0.1,
    }

    return FocusSweepAnalysis(
        controller_id=controller,
        instrument=instrument,
        sweep_id=sweep_id,
        samples=[sample],
        **(fields | outcome),
    )


FAILED = {
    "outcome": "failure",
    "position": None,
    "disposition": None,
    "rms_residual_pixels": None,
    "reason": "fit did not converge",
}


def moves(rig) -> list[float]:
    return [command.position for command in rig["foc"].sent("ChangeFocusPosition")]


async def last_capture(rig, count: int) -> UUID:
    """Wait for the sweep's final capture to arrive and return its sweep ID."""
    await reached(rig["cam"].arrival("CameraCapture", count))

    return rig["cam"].sent("CameraCapture")[-1].context[FocusSweepContext].sweep_id


async def finished(client, execution):
    """Wait for a failed or aborted execution and return its final state."""
    with pytest.raises(CallError):
        async with asyncio.timeout(5.0):
            await execution

    return (await client.kv_get_model(ControllerState)).execution_state


@pytest.mark.asyncio
async def test_each_move_completes_before_its_capture_and_best_focus_ends_the_task(
    sensor, rig, provider
):
    positions = [800.0, 900.0, 1000.0, 1100.0, 1200.0]
    execution = await sensor.start_task(sweeping(5))
    sweep_id = await last_capture(rig, len(positions))
    await reached(rig["cam"].ending("CameraCapture", len(positions)))
    await provider.publish(analysis(sweep_id, execution.controller_id))

    async with asyncio.timeout(5.0):
        await execution

    captures = rig["cam"].sent("CameraCapture")
    state = (await sensor.kv_get_model(ControllerState)).execution_state

    assert sweep_id == execution.task_id
    assert moves(rig) == [*positions, 1080.0]
    assert [c.context[FocusSweepContext].commanded_position for c in captures] == positions
    assert [c.context[FocusSweepContext].sample_index for c in captures] == list(range(5))
    assert all(c.context[FocusSweepContext].sample_count == 5 for c in captures)
    assert all(c.frame_type == "light" for c in captures)
    assert all(
        "foc ChangeFocusPosition" not in rig["cam"].overlapping("CameraCapture", n)
        for n in range(1, 6)
    )
    assert all(
        "cam CameraCapture" not in rig["foc"].overlapping("ChangeFocusPosition", n)
        for n in range(1, 6)
    )
    assert [line.split()[0] for line in rig.arrived()] == ["foc", "cam"] * 5 + ["foc"]
    assert rig["mount"].received == []
    assert state.finished.error is False
    assert state.execution.context[FocusSweepAnalysis].position == 1080.0


@pytest.mark.asyncio
async def test_frames_carry_sweep_instrument_controller_and_focus_telemetry(
    sensor, rig, provider, controller
):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    await provider.publish(analysis(sweep_id, execution.controller_id))

    async with asyncio.timeout(5.0):
        await execution

    for capture in rig["cam"].sent("CameraCapture"):
        context = capture.context

        assert context[sk.InstrumentInfo].name == "cam"
        assert set(context[sk.InstrumentInfo].devices) == {"mount", "foc", "wheel", "cam"}
        assert context[TaskInfo].controller_id == execution.controller_id
        assert context[TaskInfo].task_id == sweep_id
        assert context[FocusPosition] is not None
        assert context[SitePosition] == controller.site
        assert context.get(CollectInfo) is None


@pytest.mark.asyncio
async def test_requested_camera_settings_are_applied_once_and_kept(sensor, rig, provider):
    task = sweeping(binning=Binning(x=2, y=2), gain=30.0, readout_mode=1, filter_name="L")
    execution = await sensor.start_task(task)
    sweep_id = await last_capture(rig, 3)
    await provider.publish(analysis(sweep_id, execution.controller_id))

    async with asyncio.timeout(5.0):
        await execution

    assert rig["wheel"].sent("SetFilter") == [SetFilter(filter="L")]
    assert rig["cam"].sent("ConfigureCameraSensor") == [
        ConfigureCameraSensor(binning=Binning(x=2, y=2), gain=30.0, readout_mode=1)
    ]
    assert rig.ended_before("wheel SetFilter", "cam CameraCapture")
    assert rig.ended_before("cam ConfigureCameraSensor", "cam CameraCapture")


@pytest.mark.asyncio
async def test_a_result_published_during_the_last_capture_is_accepted(sensor, rig, provider):
    *passed, last = [rig["cam"].hold("CameraCapture") for _ in range(3)]

    for gate in passed:
        gate.set()

    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    await provider.publish(analysis(sweep_id, execution.controller_id, position=1050.0))
    last.set()

    async with asyncio.timeout(5.0):
        await execution

    assert moves(rig) == [900.0, 1000.0, 1100.0, 1050.0]


@pytest.mark.asyncio
async def test_unrelated_and_stale_results_are_ignored(
    sensor, rig, provider, service_context, controller_impl
):
    other = await focus_provider(service_context, "other")
    controller = str(controller_impl.entity)
    await provider.publish(analysis(uuid4(), controller, position=1010.0))
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)

    for unrelated in (
        analysis(sweep_id, "elsewhere", position=1020.0),
        analysis(sweep_id, controller, instrument="guider", position=1030.0),
        analysis(uuid4(), controller, position=1040.0),
    ):
        await provider.publish(unrelated)

    await other.publish(analysis(sweep_id, controller, position=1050.0))
    await provider.publish(analysis(sweep_id, controller, position=1060.0))

    async with asyncio.timeout(5.0):
        await execution

    assert moves(rig) == [900.0, 1000.0, 1100.0, 1060.0]


@pytest.mark.asyncio
async def test_a_given_center_replaces_the_starting_position(sensor, rig, provider):
    execution = await sensor.start_task(sweeping(center=2000.0))
    sweep_id = await last_capture(rig, 3)
    await provider.publish(analysis(sweep_id, execution.controller_id, position=2050.0))

    async with asyncio.timeout(5.0):
        await execution

    assert moves(rig) == [1900.0, 2000.0, 2100.0, 2050.0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "message"),
    [
        (FAILED, "analysis failed: fit did not converge"),
        ({"position": 1150.0}, "outside the swept range"),
        ({"disposition": "greater_than", "position": 1150.0}, "outside the swept range"),
    ],
    ids=["failed", "outside", "edge-outside"],
)
async def test_an_unusable_analysis_restores_starting_focus_and_fails(
    sensor, rig, provider, outcome, message
):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    await provider.publish(analysis(sweep_id, execution.controller_id, **outcome))

    state = await finished(sensor, execution)

    assert moves(rig) == [900.0, 1000.0, 1100.0, START]
    assert message in state.finished.error
    assert state.finished.aborted is False
    assert state.execution.context[FocusSweepAnalysis].sweep_id == sweep_id


@pytest.mark.asyncio
@pytest.mark.parametrize(("disposition", "edge"), [("greater_than", 1100.0), ("less_than", 900.0)])
async def test_best_focus_beyond_the_sweep_leaves_the_focuser_at_the_edge_and_fails(
    sensor, rig, provider, disposition, edge
):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    await provider.publish(
        analysis(sweep_id, execution.controller_id, disposition=disposition, position=edge)
    )

    state = await finished(sensor, execution)

    assert moves(rig) == [900.0, 1000.0, 1100.0, edge]
    assert f"{disposition.replace('_', ' ')} {edge}" in state.finished.error
    assert "the focuser is left there" in state.finished.error
    assert state.finished.aborted is False
    assert state.execution.context[FocusSweepAnalysis].disposition == disposition


@pytest.mark.asyncio
async def test_a_failed_move_to_the_edge_restores_starting_focus(sensor, rig, provider):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    rig["foc"].refusing["ChangeFocusPosition"] = 1
    await provider.publish(
        analysis(sweep_id, execution.controller_id, disposition="less_than", position=900.0)
    )

    state = await finished(sensor, execution)

    assert moves(rig) == [900.0, 1000.0, 1100.0, 900.0, START]
    assert "ChangeFocusPosition refused" in state.finished.error


@pytest.mark.asyncio
@pytest.mark.parametrize("policies", [SensorPolicies(default_timeout=0.3)])
async def test_a_missing_analysis_times_out_and_restores_starting_focus(sensor, rig):
    execution = await sensor.start_task(sweeping())

    state = await finished(sensor, execution)

    assert moves(rig) == [900.0, 1000.0, 1100.0, START]
    assert "published no analysis" in state.finished.error
    assert state.execution.context.get(FocusSweepAnalysis) is None


@pytest.mark.asyncio
async def test_a_failed_return_keeps_the_original_error(sensor, rig, provider):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    rig["foc"].refusing["ChangeFocusPosition"] = 1
    await provider.publish(analysis(sweep_id, execution.controller_id, **FAILED))

    state = await finished(sensor, execution)

    assert moves(rig) == [900.0, 1000.0, 1100.0, START]
    assert state.finished.error.startswith("Focus sweep analysis failed: fit did not converge")
    assert f"returning focuser 'foc' to {START} also failed" in state.finished.error
    assert "ChangeFocusPosition refused" in state.finished.error


@pytest.mark.asyncio
async def test_a_failed_first_move_prevents_capture_and_restores_starting_focus(sensor, rig):
    rig["foc"].refusing["ChangeFocusPosition"] = 1
    execution = await sensor.start_task(sweeping())

    state = await finished(sensor, execution)

    assert rig["cam"].sent("CameraCapture") == []
    assert moves(rig) == [900.0, START]
    assert "ChangeFocusPosition refused" in state.finished.error
    assert state.finished.aborted is False


@pytest.mark.asyncio
async def test_a_failed_final_move_restores_starting_focus(sensor, rig, provider):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    rig["foc"].refusing["ChangeFocusPosition"] = 1
    await provider.publish(analysis(sweep_id, execution.controller_id))

    state = await finished(sensor, execution)

    assert moves(rig) == [900.0, 1000.0, 1100.0, 1080.0, START]
    assert "ChangeFocusPosition refused" in state.finished.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "message"),
    [("missing", "offline"), ("cam", "does not publish focus analysis")],
)
async def test_an_unusable_source_rejects_the_task_before_any_command(
    sensor, rig, controller_impl, source, message
):
    await controller_impl.kv_put_model(FocusAnalysisSource(entity=source))
    execution = await sensor.start_task(sweeping())

    state = await finished(sensor, execution)

    assert message in state.finished.error
    assert all(device.received == [] for device in rig.devices.values())


@pytest.mark.asyncio
async def test_an_unknown_instrument_rejects_the_task_before_any_command(sensor, rig):
    execution = await sensor.start_task(
        AutoFocus(instrument="guider", step=100.0, count=3, integration_time_seconds=0.1)
    )

    state = await finished(sensor, execution)

    assert "Unknown instrument 'guider'" in state.finished.error
    assert all(device.received == [] for device in rig.devices.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("how", ["abort", "disable", "stop"])
async def test_ending_the_task_during_a_capture_aborts_it_without_returning_focus(
    sensor, rig, controller, how
):
    rig["cam"].hold("CameraCapture")
    execution = await sensor.start_task(sweeping())
    await reached(rig["cam"].arrival("CameraCapture"))

    match how:
        case "abort":
            await sensor.abort_task(execution.task_id)
        case "disable":
            await sensor.disable()
        case "stop":
            # A stopping service declines the cancellation as a domain abort.
            await controller.on_detach()
            await sensor.abort_task(execution.task_id)

    state = await finished(sensor, execution)

    assert state.finished.aborted is True
    assert moves(rig) == [900.0]
    assert len(rig["cam"].sent("Abort")) == 1
    assert rig["mount"].received == []


@pytest.mark.asyncio
async def test_disable_during_cleanup_prevents_the_successor_sweep(sensor, rig):
    rig["cam"].hold("CameraCapture")
    release_abort = rig["cam"].hold("Abort")
    first = await sensor.start_task(sweeping())
    await reached(rig["cam"].arrival("CameraCapture"))

    try:
        second = await sensor.start_task(sweeping(), interrupt=True)
        await reached(rig["cam"].arrival("Abort"))
        await sensor.disable()
    finally:
        release_abort.set()

    state = await finished(sensor, second)

    with pytest.raises(CallError):
        await first

    assert state.execution.task_id == second.task_id
    assert state.finished.error == "Controller is disabled"
    assert state.finished.aborted is False
    assert moves(rig) == [900.0]
    assert len(rig["cam"].sent("CameraCapture")) == 1


@pytest.mark.asyncio
async def test_expiry_during_a_capture_aborts_it_without_returning_focus(sensor, rig):
    rig["cam"].hold("CameraCapture")
    execution = await sensor.start_task(
        sweeping(), expiry_time=datetime.now(UTC) + timedelta(seconds=0.5)
    )

    state = await finished(sensor, execution)

    assert "expired" in state.finished.error
    assert moves(rig) == [900.0]
    assert len(rig["cam"].sent("Abort")) == 1
    assert rig["mount"].received == []


@pytest.mark.asyncio
async def test_aborting_while_waiting_for_analysis_does_not_return_focus(sensor, rig):
    execution = await sensor.start_task(sweeping())
    await reached(rig["cam"].ending("CameraCapture", 3))
    await sensor.abort_task(execution.task_id)

    state = await finished(sensor, execution)

    assert state.finished.aborted is True
    assert moves(rig) == [900.0, 1000.0, 1100.0]


@pytest.mark.asyncio
async def test_aborting_the_return_move_aborts_it_without_another_move(sensor, rig, provider):
    execution = await sensor.start_task(sweeping())
    sweep_id = await last_capture(rig, 3)
    rig["foc"].hold("ChangeFocusPosition")
    await provider.publish(analysis(sweep_id, execution.controller_id, **FAILED))
    await reached(rig["foc"].arrival("ChangeFocusPosition", 4))
    await sensor.abort_task(execution.task_id)

    state = await finished(sensor, execution)

    assert state.finished.aborted is True
    assert moves(rig) == [900.0, 1000.0, 1100.0, START]
    assert len(rig["foc"].sent("Abort")) == 1


# Translation

FOCUS_BENCH = sensor_of(
    """
    components:
      - device: mount
      - unit: ota
        components:
          - device: foc
          - device: wheel
          - device: cam
            instrument: true
    """,
    {
        "mount": (("FollowTarget", "Stop"), ()),
        "foc": (("ChangeFocusPosition",), ()),
        "wheel": (("SetFilter",), ()),
        "cam": (("CameraCapture", "ConfigureCameraSensor"), ()),
    },
)
"""One camera behind a focuser and a wheel, under a mount."""


def named(sensor: BoundSensor) -> dict[str, Placement]:
    return {p.device: p for p in sensor.topology.instruments()}


def counts(intent: CollectIntent) -> list[list[tuple[str, int]]]:
    """Each epoch's requests, as request id and frame count."""
    return [[(request.id, request.count) for request in epoch.units] for epoch in intent.epochs]


def units_of(collect: BoundCollect):
    return [unit for epoch in collect.epochs for unit in epoch.units]


SWEEP_ID = UUID("6f1c2b7e-3d4a-4c1b-9e2f-0a1b2c3d4e5f")


def focusing(*positions: float, **kw) -> CollectIntent:
    sweep = AutoFocus(
        instrument="cam", step=100.0, count=len(positions), integration_time_seconds=2.0, **kw
    )

    return translate_autofocus(
        sweep, list(positions), sweep_id=SWEEP_ID, focuser="foc", instruments=named(FOCUS_BENCH)
    )


def test_autofocus_takes_one_light_frame_per_position_without_pointing():
    intent = focusing(900.0, 1000.0, 1100.0)

    assert counts(intent) == [[("sample-0", 1)], [("sample-1", 1)], [("sample-2", 1)]]
    assert (intent.prepare, intent.cleanup) == ((), ())
    assert [epoch.units[0].keywords for epoch in intent.epochs] == [
        (
            FocusSweepContext(
                sweep_id=SWEEP_ID,
                sample_index=index,
                sample_count=3,
                commanded_position=position,
            ),
        )
        for index, position in enumerate((900.0, 1000.0, 1100.0))
    ]
    assert all(
        epoch.units[0].acquisition
        == AcquisitionRequest(integration_time_s=2.0, frame_type=FrameType.LIGHT, timeout_s=62.0)
        for epoch in intent.epochs
    )


def test_autofocus_moves_the_focuser_and_configures_the_named_chain():
    sweep = focusing(
        900.0, 1000.0, 1100.0, binning=Binning(x=2, y=2), readout_mode=3, filter_name="L"
    )
    packed = pack(sweep, FOCUS_BENCH)
    units = units_of(packed)

    assert [[(s.target.device, s.command) for s in epoch.settings] for epoch in packed.epochs] == [
        [
            ("foc", ChangeFocusPosition(position=position)),
            ("wheel", SetFilter(filter="L")),
            ("cam", ConfigureCameraSensor(binning=Binning(x=2, y=2), readout_mode=3)),
        ]
        for position in (900.0, 1000.0, 1100.0)
    ]
    assert [unit.target.device for unit in units] == ["cam"] * 3
    assert [s.acquisition.keywords[FocusSweepContext].sample_index for s in units] == [0, 1, 2]
    assert all(unit.acquisition.keywords.get(CollectInfo) is None for unit in units)


def test_autofocus_rejects_an_unknown_instrument():
    sweep = AutoFocus(instrument="guider", step=100.0, count=3, integration_time_seconds=1.0)

    with pytest.raises(ValueError, match="Unknown instrument 'guider'"):
        translate_autofocus(
            sweep,
            sweep.positions(1000.0),
            sweep_id=SWEEP_ID,
            focuser="foc",
            instruments=named(FOCUS_BENCH),
        )
