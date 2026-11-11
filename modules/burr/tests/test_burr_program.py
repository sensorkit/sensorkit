# SPDX-License-Identifier: Apache-2.0
"""Instrument validation and task execution by the Burr program."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime

import pytest
import pytest_asyncio

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.astro.target import ICRSTarget, RateTarget
from sensorkit.backend.base import ReplyTimeout
from sensorkit.backend.request import CallRejected
from sensorkit.burr.program import BurrConfig, BurrProgram
from sensorkit.core.controller import InternalControllerState
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.program import ProgramTaskingMonitor
from sensorkit.core.task import TaskExecution, TaskInfo
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileNameTemplate
from sensorkit.std.collect import Collect, StandardCollectTask
from sensorkit.std.instrument import FrameType

INSTRUMENT = " cam east "
WEST = "cam/West"

# The west instrument leaves the filter unspecified and overrides gain.
MAPPED = {
    "filter_name": "r",
    "gain": 100,
    "instruments": {INSTRUMENT: {}, WEST: {"filter_name": None, "gain": 200}},
}

# Coverage requests produce a sidereal collect followed by a streak collect.
SCHEDULE = {
    "twilight_flats": {"collect": False},
    "lunar_background": {"collect": False},
    "photometric_standards": {"collect": False},
    "calsats": {"collect": False},
    "coverage": {"collect": True, "windows": ["night"]},
    "streaks": {"collect": True},
}


def midnight_longitude() -> float:
    """Choose an equatorial site near solar midnight."""
    now = datetime.now(UTC)
    return (180 - (now.hour + now.minute / 60) * 15) % 360 - 180


async def start_controller(
    context,
    name: str,
    *instruments: str,
    withdraw: bool = False,
    fail: bool = False,
    filenames: list[str] | None = None,
) -> list[TaskExecution[StandardCollectTask]]:
    """Start a device-free controller that records tasks and can simulate failures."""
    impl = ControllerImpl.for_service_context(context, name)
    received: list[TaskExecution[StandardCollectTask]] = []

    def register():
        for instrument in instruments:
            impl.register_instrument(instrument)

    async def collect(execution: TaskExecution[StandardCollectTask]):
        received.append(execution)

        if filenames is not None:
            filenames.extend(capture_file_names(execution, impl, instruments[0]))

        if withdraw:
            info = impl.entity_info()
            details = info.details.model_copy(update={"instruments": ()})
            await impl.kv_put_model(info.model_copy(update={"details": details}))

        if fail:
            raise RuntimeError("camera fault")

    impl.add_task_handler(StandardCollectTask, execution_callback=collect)
    impl.on_attach(register)
    await context.register_impl(impl)
    await impl.kv_put_model(
        SitePosition(latitude_degrees=0.0, longitude_degrees=midnight_longitude(), altitude_km=0.0)
    )
    await impl.report_state(InternalControllerState.OPERATE, sk.KeywordDict())
    await context.sensorkit().controller(name).enable()

    return received


async def start_tasking(client) -> ProgramTaskingMonitor:
    """Retry tasking while the enable hook initializes Burr."""
    while True:
        try:
            return await client.start_tasking()
        except CallRejected as e:
            if "enablement is changing" not in str(e):
                raise

        await asyncio.sleep(0.1)


@pytest_asyncio.fixture
async def context(service):
    """Register the service that hosts Burr and its controllers."""
    await service.register()
    return service.context


@pytest_asyncio.fixture
async def burr(kit, service, tmp_path):
    """Start Burr with camera settings and a controller assignment."""

    async def start(collect: dict | None = None, *, assigned: str = "ctrl"):
        program = BurrProgram()
        config = BurrConfig.model_validate(
            {
                "controller": "ctrl",
                "plotting": {"debug": False, "review": False},
                "runtime": {"output_dir": str(tmp_path)},
                "schedule": SCHEDULE,
                "collect": {"instrument": INSTRUMENT} if collect is None else collect,
            }
        )
        await kit.program("burr").kv_put_model(config)
        service.include(program, name="burr")
        await service.start()

        with contextlib.suppress(ReplyTimeout):
            await kit.program("burr").enable(assigned)

        return program

    return start


def requested(execution: TaskExecution[StandardCollectTask]):
    """Read the instrument, filter, gain, and frame type from each sequence."""
    return [(p.instrument, p.filter_name, p.gain, p.frame_type) for p in execution.task.exposures]


def capture_file_names(
    execution: TaskExecution[StandardCollectTask],
    controller: ControllerImpl,
    automatic_instrument: str,
) -> list[str]:
    """Resolve each capture's filename with its registered instrument context."""
    names = []

    for params in execution.task.exposures:
        instrument = controller.instrument_context(params.instrument or automatic_instrument)

        for frame in range(params.frame_count):
            collect = Collect(target=execution.task.target, params=params, frame_number=frame)
            context = Context(execution.context, collect)
            context.update(instrument)
            names.append(context.resolve(context[FileNameTemplate].template, as_type=str))

    return names


@pytest.mark.asyncio
async def test_selected_instrument_reaches_every_collect(kit, context, burr, until):
    received = await start_controller(context, "ctrl", INSTRUMENT)
    program = await burr()
    client = kit.program("burr")

    async with asyncio.timeout(60.0):
        (await start_tasking(client)).close()
        (slot,) = program.scheduler.slots
        await until(lambda: slot.success_count == 1, timeout=60.0)
        await client.stop_tasking()

    sidereal, streak = (execution.task for execution in received[:2])

    assert (type(sidereal.target), type(streak.target)) == (ICRSTarget, RateTarget)
    assert [p.instrument for t in (sidereal, streak) for p in t.exposures] == [INSTRUMENT] * 2
    assert slot.failure_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("collect", "expected"),
    [
        (
            MAPPED,
            [(INSTRUMENT, "r", 100, FrameType.LIGHT), (WEST, None, 200, FrameType.LIGHT)],
        ),
        (None, [(INSTRUMENT, None, None, FrameType.LIGHT)]),
        ({}, [(None, None, None, FrameType.LIGHT)]),
    ],
    ids=["mapped", "named", "automatic"],
)
async def test_instruments_receive_each_collect_and_label_its_files(
    kit, context, burr, until, collect, expected
):
    names = []
    received = await start_controller(context, "ctrl", INSTRUMENT, WEST, filenames=names)
    program = await burr(collect)
    client = kit.program("burr")

    async with asyncio.timeout(60.0):
        (await start_tasking(client)).close()
        (slot,) = program.scheduler.slots
        await until(lambda: slot.success_count == 1, timeout=60.0)
        await client.stop_tasking()

    sidereal, streak = received[:2]

    assert (type(sidereal.task.target), type(streak.task.target)) == (ICRSTarget, RateTarget)
    assert requested(sidereal) == requested(streak) == expected
    assert sidereal.context["BurrContext"] == streak.context["BurrContext"]
    assert sidereal.context[TaskInfo].task_id != streak.context[TaskInfo].task_id
    assert len({name.casefold() for name in names}) == len(names)
    assert not any("/" in name for name in names)
    assert {name.rsplit("_", 2)[1] for name in names} == {
        (instrument or INSTRUMENT).encode().hex() for instrument, *_ in expected
    }
    assert slot.failure_count == 0


@pytest.mark.asyncio
async def test_failed_collect_fails_the_request_for_every_instrument(kit, context, burr, until):
    received = await start_controller(context, "ctrl", INSTRUMENT, WEST, fail=True)
    program = await burr(MAPPED)
    client = kit.program("burr")

    async with asyncio.timeout(60.0):
        (await start_tasking(client)).close()
        (slot,) = program.scheduler.slots
        await until(lambda: slot.failure_count == 1, timeout=60.0)
        await client.stop_tasking()

    first = received[0]
    sequence = first.context["BurrContext"]

    # Failure removes the queued streak task from the same request.
    assert [p.instrument for p in first.task.exposures] == [INSTRUMENT, WEST]
    assert [e for e in received if e.context["BurrContext"] == sequence] == [first]
    assert slot.success_count == 0


@pytest.mark.asyncio
async def test_omitted_instrument_skips_validation(kit, context, burr, until):
    # Automatic selection permits a different controller with no instrument metadata.
    await start_controller(context, "ctrl")
    received = await start_controller(context, "other")
    await burr({}, assigned="other")
    client = kit.program("burr")

    async with asyncio.timeout(60.0):
        (await start_tasking(client)).close()
        await until(lambda: received, timeout=60.0)
        await client.stop_tasking()

    assert received[0].task.exposures[0].instrument is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("unknown", "Controller 'ctrl' has no instrument 'cam east'"),
        ("unknown_configured", "Controller 'ctrl' has no instrument 'cam/West'"),
        ("mismatch", "requires controller 'ctrl', but this program is assigned to 'other'"),
        ("missing", "EntityInfo"),
        ("not_controller", "Entity 'ctrl' has no controller details"),
    ],
)
async def test_invalid_selection_fails_before_any_collect(kit, context, burr, case, error):
    received = await start_controller(context, "ctrl", INSTRUMENT)
    other = await start_controller(context, "other", INSTRUMENT, WEST)
    ctrl = kit.controller("ctrl")

    match case:
        case "missing":
            await kit.backend.key_value(ctrl.entity).delete("EntityInfo")
        case "not_controller":
            await ctrl.kv_put_model(sk.EntityInfo(entity_type="generic", details=None))

    match case:
        case "unknown":
            collect = {"instrument": "cam east"}
        case "unknown_configured":
            collect = MAPPED
        case _:
            collect = None

    program = await burr(collect, assigned="other" if case == "mismatch" else "ctrl")

    async with asyncio.timeout(60.0):
        with await start_tasking(kit.program("burr")) as monitor:
            await monitor.wait()

    (slot,) = program.scheduler.slots

    assert (monitor.fault.fault, monitor.fault.stage) == ("program", "factory")
    assert error in monitor.fault.error
    assert received == other == []
    assert (slot.success_count, slot.failure_count) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("collect", "missing"),
    [(None, f"'{INSTRUMENT}'"), (MAPPED, f"'{INSTRUMENT}', '{WEST}'")],
    ids=["named", "configured"],
)
async def test_withdrawn_instrument_fails_before_queued_collect(
    kit, context, burr, collect, missing
):
    received = await start_controller(context, "ctrl", INSTRUMENT, WEST, withdraw=True)
    program = await burr(collect)

    async with asyncio.timeout(60.0):
        with await start_tasking(kit.program("burr")) as monitor:
            await monitor.wait()

    (slot,) = program.scheduler.slots

    assert (monitor.fault.fault, monitor.fault.stage) == ("program", "factory")
    assert f"Controller 'ctrl' has no instrument {missing}" in monitor.fault.error
    assert len(received) == 1
    assert (slot.success_count, slot.failure_count) == (0, 0)
