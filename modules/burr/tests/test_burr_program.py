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
from sensorkit.std.collect import StandardCollectTask

INSTRUMENT = " cam east "

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
    context, name: str, *instruments: str, withdraw: bool = False
) -> list[StandardCollectTask]:
    """Start a controller with no devices and collect its tasks.

    When requested, withdraw its instrument metadata during the first task.
    """
    impl = ControllerImpl.for_service_context(context, name)
    received: list[StandardCollectTask] = []

    def register():
        for instrument in instruments:
            impl.register_instrument(instrument)

    @impl.task_handler(StandardCollectTask)
    async def collect(task: StandardCollectTask):
        received.append(task)

        if withdraw:
            info = impl.entity_info()
            details = info.details.model_copy(update={"instruments": ()})
            await impl.kv_put_model(info.model_copy(update={"details": details}))

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
    """Start Burr with the requested instrument and controller assignment."""

    async def start(instrument: str | None = INSTRUMENT, *, assigned: str = "ctrl"):
        program = BurrProgram()
        config = BurrConfig.model_validate(
            {
                "controller": "ctrl",
                "plotting": {"debug": False, "review": False},
                "runtime": {"output_dir": str(tmp_path)},
                "schedule": SCHEDULE,
                "collect": {"instrument": instrument},
            }
        )
        await kit.program("burr").kv_put_model(config)
        service.include(program, name="burr")
        await service.start()

        with contextlib.suppress(ReplyTimeout):
            await kit.program("burr").enable(assigned)

        return program

    return start


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

    sidereal, streak = received[:2]

    assert (type(sidereal.target), type(streak.target)) == (ICRSTarget, RateTarget)
    assert [p.instrument for t in (sidereal, streak) for p in t.exposures] == [INSTRUMENT] * 2
    assert slot.failure_count == 0


@pytest.mark.asyncio
async def test_omitted_instrument_skips_validation(kit, context, burr, until):
    # Automatic selection permits a different controller with no instrument metadata.
    await start_controller(context, "ctrl")
    received = await start_controller(context, "other")
    await burr(None, assigned="other")
    client = kit.program("burr")

    async with asyncio.timeout(60.0):
        (await start_tasking(client)).close()
        await until(lambda: received, timeout=60.0)
        await client.stop_tasking()

    assert received[0].exposures[0].instrument is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("unknown", "Controller 'ctrl' has no instrument 'cam east'"),
        ("mismatch", "requires controller 'ctrl', but this program is assigned to 'other'"),
        ("missing", "EntityInfo"),
        ("not_controller", "Entity 'ctrl' has no controller details"),
    ],
)
async def test_invalid_selection_fails_before_any_collect(kit, context, burr, case, error):
    received = await start_controller(context, "ctrl", INSTRUMENT)
    other = await start_controller(context, "other", INSTRUMENT)
    ctrl = kit.controller("ctrl")

    match case:
        case "missing":
            await kit.backend.key_value(ctrl.entity).delete("EntityInfo")
        case "not_controller":
            await ctrl.kv_put_model(sk.EntityInfo(entity_type="generic", details=None))

    program = await burr(
        "cam east" if case == "unknown" else INSTRUMENT,
        assigned="other" if case == "mismatch" else "ctrl",
    )

    async with asyncio.timeout(60.0):
        with await start_tasking(kit.program("burr")) as monitor:
            await monitor.wait()

    (slot,) = program.scheduler.slots

    assert (monitor.fault.fault, monitor.fault.stage) == ("program", "factory")
    assert error in monitor.fault.error
    assert received == other == []
    assert (slot.success_count, slot.failure_count) == (0, 0)


@pytest.mark.asyncio
async def test_withdrawn_instrument_fails_before_queued_collect(kit, context, burr):
    received = await start_controller(context, "ctrl", INSTRUMENT, withdraw=True)
    program = await burr()

    async with asyncio.timeout(60.0):
        with await start_tasking(kit.program("burr")) as monitor:
            await monitor.wait()

    (slot,) = program.scheduler.slots

    assert (monitor.fault.fault, monitor.fault.stage) == ("program", "factory")
    assert "has no instrument" in monitor.fault.error
    assert [t.exposures[0].instrument for t in received] == [INSTRUMENT]
    assert (slot.success_count, slot.failure_count) == (0, 0)
