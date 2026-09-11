# SPDX-License-Identifier: Apache-2.0
"""Interrupting a device whose call was dropped, and the header an acquisition
is sent with.

The mount is a live device on the fake backend, so the Abort crosses the same
request and event machinery a real one does. It records every command it is
sent and answers Abort only when a test releases it, so a test that never
releases it has a device that does not answer.

Every attempt is given a list to record into, which is what a cancelled caller
is left holding.
"""
from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from sensorkit.backend.request import CallError
from sensorkit.core.device import Abort
from sensorkit.sensor import dispatch
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.dispatch import Dispatcher, Interruption
from sensorkit.std.traits import Stop

from .common import REPORTED, snapshot_of

WITHOUT_ABORT = REPORTED["mount"][0]
WITH_ABORT = (*WITHOUT_ABORT, "Abort")

class Mount:
    """What the mount was sent, and how its Abort answers."""

    def __init__(self):
        self.received: list[str] = []
        self.release = asyncio.Event()
        self.refuse = False
        self.aborting = asyncio.Event()
        self.handlers: set[asyncio.Task] = set()


@pytest_asyncio.fixture
async def mount(service_context) -> Mount:
    device = await service_context.register_device("mount")
    mount = Mount()

    @device.command_handler(Abort)
    async def abort(command: Abort):
        mount.received.append("Abort")
        mount.handlers.add(asyncio.current_task())
        mount.aborting.set()
        await mount.release.wait()

        if mount.refuse:
            raise RuntimeError("refused")

    @device.command_handler(Stop)
    async def stop(command: Stop):
        mount.received.append("Stop")

    return mount


@pytest_asyncio.fixture
async def before(kit, mount) -> set[asyncio.Task]:
    """Every task running before an Abort is sent.

    Taken once the mount's event stream is live, since that is started on first
    use and outlives any one call.
    """
    await kit.device("mount").get_event_mux().wait_ready()

    return asyncio.all_tasks()


def dispatcher(kit, topology, commands: tuple[str, ...]) -> Dispatcher:
    """A dispatcher whose mount reports these commands."""
    sensor, _ = BoundSensor.bind(
        topology, snapshot_of({**REPORTED, "mount": (commands, ())}))

    return Dispatcher(sensor, {"mount": kit.device("mount")})


async def assert_released(before: set[asyncio.Task], mount: Mount):
    """Assert that no local call machinery outlived the attempt.

    Ends the mount's own handling first. That sends no further call events, so
    anything still waiting afterward is waiting on this side of the call.
    """
    for handler in mount.handlers:
        handler.cancel()

    if leftover := asyncio.all_tasks() - before - {asyncio.current_task()}:
        _, pending = await asyncio.wait(leftover, timeout=1.0)
        assert not pending


@pytest.mark.asyncio
async def test_a_supported_abort_is_acknowledged(kit, topology, mount):
    mount.release.set()
    recorded: list[Interruption] = []

    async with asyncio.timeout(1.0):
        interruption = await dispatcher(kit, topology, WITH_ABORT).abort(
            "mount", recorded.append)

    assert interruption.outcome == "acknowledged"
    assert interruption.error is None
    assert recorded == [interruption]
    assert mount.received == ["Abort"]


@pytest.mark.asyncio
async def test_an_unsupported_abort_sends_nothing(kit, topology, mount):
    # The mount would answer Abort if sent one, and does report Stop.
    mount.release.set()
    recorded: list[Interruption] = []

    interruption = await dispatcher(kit, topology, WITHOUT_ABORT).abort(
        "mount", recorded.append)

    assert interruption.outcome == "unsupported"
    assert recorded == [interruption]
    assert mount.received == []


@pytest.mark.asyncio
async def test_a_refused_abort_fails_and_is_not_retried(kit, topology, mount):
    mount.refuse = True
    mount.release.set()
    recorded: list[Interruption] = []

    async with asyncio.timeout(1.0):
        interruption = await dispatcher(kit, topology, WITH_ABORT).abort(
            "mount", recorded.append)

    assert interruption.outcome == "failed"
    assert isinstance(interruption.error, CallError)
    assert recorded == [interruption]
    assert mount.received == ["Abort"]


@pytest.mark.asyncio
async def test_an_unanswered_abort_times_out_and_releases_its_call(
        kit, topology, mount, before, monkeypatch):
    monkeypatch.setattr(dispatch, "ABORT_TIMEOUT_S", 0.1)
    recorded: list[Interruption] = []

    async with asyncio.timeout(1.0):
        interruption = await dispatcher(kit, topology, WITH_ABORT).abort(
            "mount", recorded.append)

    assert interruption.outcome == "timed_out"
    assert interruption.error is None
    assert recorded == [interruption]
    assert mount.received == ["Abort"]
    await assert_released(before, mount)


@pytest.mark.parametrize("times", [1, 3])
@pytest.mark.asyncio
async def test_cancellation_waits_for_the_abort_and_keeps_its_result(
        kit, topology, mount, before, times):
    recorded: list[Interruption] = []
    aborting = asyncio.create_task(
        dispatcher(kit, topology, WITH_ABORT).abort("mount", recorded.append))

    async with asyncio.timeout(1.0):
        await mount.aborting.wait()

    for _ in range(times):
        aborting.cancel()
        await asyncio.sleep(0)

    assert not aborting.done()

    assert recorded == []

    mount.release.set()

    async with asyncio.timeout(1.0):
        await asyncio.wait({aborting})

    assert aborting.cancelled()
    assert recorded == [Interruption("acknowledged")]
    assert mount.received == ["Abort"]
    await assert_released(before, mount)


@pytest.mark.parametrize("times", [1, 3])
@pytest.mark.asyncio
async def test_cancellation_during_an_unanswered_abort_keeps_its_timeout(
        kit, topology, mount, monkeypatch, times):
    # Release of the abandoned call is checked below. This one holds the
    # result and the propagation to account.
    monkeypatch.setattr(dispatch, "ABORT_TIMEOUT_S", 0.2)
    recorded: list[Interruption] = []
    aborting = asyncio.create_task(
        dispatcher(kit, topology, WITH_ABORT).abort("mount", recorded.append))

    async with asyncio.timeout(1.0):
        await mount.aborting.wait()

    for _ in range(times):
        aborting.cancel()
        await asyncio.sleep(0)

    async with asyncio.timeout(1.0):
        await asyncio.wait({aborting})

    assert aborting.cancelled()
    assert recorded == [Interruption("timed_out")]
    assert mount.received == ["Abort"]


@pytest.mark.asyncio
async def test_a_failing_recorder_cannot_displace_a_cancellation(
        kit, topology, mount):
    def record(interruption: Interruption):
        raise RuntimeError("storage failed")

    aborting = asyncio.create_task(
        dispatcher(kit, topology, WITH_ABORT).abort("mount", record))

    async with asyncio.timeout(1.0):
        await mount.aborting.wait()

    aborting.cancel()
    mount.release.set()

    async with asyncio.timeout(1.0):
        await asyncio.wait({aborting})

    assert aborting.cancelled()


@pytest.mark.parametrize("times", [1, 3])
@pytest.mark.asyncio
async def test_cancellation_during_an_unanswered_abort_leaves_nothing_running(
        kit, topology, mount, before, monkeypatch, times):
    monkeypatch.setattr(dispatch, "ABORT_TIMEOUT_S", 0.2)
    aborting = asyncio.create_task(
        dispatcher(kit, topology, WITH_ABORT).abort("mount", lambda _: None))

    async with asyncio.timeout(1.0):
        await mount.aborting.wait()

    for _ in range(times):
        aborting.cancel()
        await asyncio.sleep(0)

    async with asyncio.timeout(1.0):
        await asyncio.wait({aborting})

    assert aborting.cancelled()
    assert mount.received == ["Abort"]
    await assert_released(before, mount)


# Headers






"""What each device reports, where the deepest publisher is the camera and the
guide camera is on another chain."""
