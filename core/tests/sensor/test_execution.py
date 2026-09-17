# SPDX-License-Identifier: Apache-2.0
"""Running compiled workflows against live devices.

The rig serves `BENCH`, so commands and Abort cross the same request and event
machinery a real one does. The wheel has no Abort, so interrupting it is
unsupported.

Workflows are either collects, packed and compiled, or step lists lowered by
hand where a case is about one execution rule and a collect would bury it.

Synchronization is by events. Deadlines in the tens of milliseconds are the
behavior under test where they appear.
"""
from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from sensorkit.core.device import Abort, DeviceCommand
from sensorkit.sensor import dispatch
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import (
    CollectIntent,
    CommandRequest,
    RequestEpoch,
    compile_collect,
    pack,
)
from sensorkit.sensor.dispatch import Interruption
from sensorkit.sensor.execution import (
    ExecutionState,
    WorkflowError,
    WorkflowExecutor,
)
from sensorkit.sensor.standard_task import translate
from sensorkit.sensor.workflow import (
    CleanupPlan,
    Dependency,
    ExecutableWorkflow,
    Operation,
    OperatorRule,
    Origin,
    PlannedStep,
    lower,
)
from sensorkit.std.collect import CameraParameterSet, Collect, StandardCollectTask
from sensorkit.std.instrument import CameraCapture
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import SetFilter
from sensorkit.std.traits import Home, Stop

from .common import (
    BENCH,
    TARGET,
    Device,
    Rig,
    abort_only,
    asking,
    authored,
    ended,
    finished,
    observed,
    operation,
    placement,
    pointing,
    ran,
    reached,
    sensor_of,
)


class Intent:
    """Why the caller is cancelling right now, which it may change at any time.

    A predicate reading this answers for the moment it is asked, so a case can
    tell classification on arrival from classification after a drain.
    """

    def __init__(self):
        self.domain = True

    def __call__(self, cancellation: BaseException) -> bool:
        return self.domain


@pytest.fixture(scope="module")
def handles():
    """What each device handles, which is also what it reports."""
    return {
        "mount": ("FollowTarget", "Stop", "Abort"),
        "foc-e": ("ChangeFocusPosition",),
        "wheel-w": ("SetFilter",),
        "cam-e": ("CameraCapture", "Abort"),
        "cam-w": ("CameraCapture", "Abort"),
    }


@pytest.fixture(scope="module")
def sensor(handles) -> BoundSensor:
    return sensor_of(authored(BENCH).sensor,
                     {name: (commands, ()) for name, commands in handles.items()})


@pytest_asyncio.fixture
async def idle(kit, rig) -> set[asyncio.Task]:
    """Every task running before a workflow starts.

    Taken once each device's event stream is live, since that is started on
    first use and outlives any one call.
    """
    for name in rig.devices:
        await kit.device(name).get_event_mux().wait_ready()

    return asyncio.all_tasks()


async def assert_settled(idle: set[asyncio.Task]):
    """Assert that nothing a run started is still running."""
    if leftover := asyncio.all_tasks() - idle - {asyncio.current_task()}:
        _, pending = await asyncio.wait(leftover, timeout=1.0)
        assert not pending


def step(sensor: BoundSensor, name: str, device: str, command: DeviceCommand,
         *after: str, **kw) -> PlannedStep:
    """One step waiting for the success of those it names."""
    return PlannedStep(name=name, origin=Origin(source="t", path=(name,)),
                       group="g", target=placement(sensor, device),
                       command=command,
                       deps=tuple(Dependency(on=a) for a in after), **kw)


def cleanup(name: str, *steps: PlannedStep, **kw) -> CleanupPlan:
    return CleanupPlan(steps=steps, origin=Origin(source=name), **kw)


def lowered(sensor: BoundSensor, *steps: PlannedStep,
            **kw) -> ExecutableWorkflow:
    return lower("t", steps, sensor, **kw)


def stopping(**kw) -> CommandRequest:
    return CommandRequest(command=Stop(), subject="sensor", **kw)


def collected(sensor: BoundSensor, *epochs: RequestEpoch,
              **kw) -> ExecutableWorkflow:
    rules = kw.pop("rules", ())

    return compile_collect(
        pack(CollectIntent(name="demo", epochs=epochs, **kw), sensor), sensor,
        rules=rules)


def events(queue: asyncio.Queue) -> list[tuple[str, str]]:
    """What a subscriber has been given, as command and outcome."""
    seen = []

    while not queue.empty():
        event = queue.get_nowait()
        seen.append((event.operation.command.model_tag(), event.outcome))

    return seen


# Dispatch


@pytest.mark.asyncio
async def test_a_command_is_sent_once_and_reported(executor, rig, sensor,
                                                   observer):
    workflow = lowered(sensor, step(sensor, "stop", "mount", Stop()))

    with observer.subscription() as queue:
        report = await executor.execute(workflow)

    assert report.outcome == "completed"
    assert report.run.ok
    assert report.attempted == {operation(workflow, "Stop")}
    assert report.interruptions == {}
    assert len(rig["mount"].sent("Stop")) == 1
    assert events(queue) == [("Stop", "begin"), ("Stop", "ok")]


@pytest.mark.asyncio
async def test_a_failed_command_is_not_interrupted(executor, rig, sensor,
                                                   observer):
    rig["mount"].refusing["Stop"] = 1
    workflow = lowered(sensor, step(sensor, "stop", "mount", Stop()))

    with observer.subscription() as queue:
        with pytest.raises(WorkflowError, match="in the run") as raised:
            await executor.execute(workflow)

    assert raised.value.report.interruptions == {}
    assert rig["mount"].sent("Abort") == []
    assert events(queue) == [("Stop", "begin"), ("Stop", "failed")]


@pytest.mark.asyncio
async def test_an_expired_deadline_aborts_once_and_keeps_the_timeout(
        executor, rig, sensor, observer, idle):
    rig["mount"].hold("FollowTarget")
    workflow = lowered(sensor, step(sensor, "slew", "mount",
                                    FollowTarget(target=TARGET), timeout_s=0.1))
    slew = operation(workflow, "FollowTarget")

    with observer.subscription() as queue:
        with pytest.raises(WorkflowError) as raised:
            await executor.execute(workflow)

    report = raised.value.report
    (_, error), = report.run.causes

    assert isinstance(error, TimeoutError)
    assert report.interruptions == {slew: Interruption("acknowledged")}
    assert len(rig["mount"].sent("Abort")) == 1
    assert rig["mount"].sent("Stop") == []
    assert events(queue) == [("FollowTarget", "begin"),
                             ("FollowTarget", "failed")]
    await assert_settled(idle)


@pytest.mark.asyncio
async def test_a_refused_abort_is_kept_beside_the_timeout(executor, rig,
                                                          sensor):
    rig["mount"].hold("FollowTarget")
    rig["mount"].refusing["Abort"] = 1
    workflow = lowered(sensor, step(sensor, "slew", "mount",
                                    FollowTarget(target=TARGET), timeout_s=0.1))

    with pytest.raises(WorkflowError) as raised:
        await executor.execute(workflow)

    report = raised.value.report
    (_, error), = report.run.causes
    interruption = report.interruptions[operation(workflow, "FollowTarget")]

    assert isinstance(error, TimeoutError)
    assert interruption.outcome == "failed"
    assert interruption.error is not None
    assert len(rig["mount"].sent("Abort")) == 1


@pytest.mark.asyncio
async def test_an_unsupported_abort_sends_nothing(executor, rig, sensor):
    rig["wheel-w"].hold("SetFilter")
    workflow = lowered(sensor, step(sensor, "filter", "wheel-w",
                                    SetFilter(filter="r"), timeout_s=0.1))

    with pytest.raises(WorkflowError) as raised:
        await executor.execute(workflow)

    assert raised.value.report.interruptions == {
        operation(workflow, "SetFilter"): Interruption("unsupported")}


@pytest.mark.asyncio
async def test_hard_cancellation_after_handoff_aborts_and_propagates(
        executor, rig, sensor, observer, idle):
    rig["mount"].hold("FollowTarget")
    workflow = lowered(sensor, step(sensor, "slew", "mount",
                                    FollowTarget(target=TARGET)),
                       cleanup=(cleanup("halt", step(sensor, "stop", "mount",
                                                     Stop())),))
    slew = operation(workflow, "FollowTarget")
    state = ExecutionState()

    with observer.subscription() as queue:
        running = rig.start(executor.execute(workflow, state=state))
        await reached(rig["mount"].arrival("FollowTarget"))
        running.cancel()
        await ended(running)

    assert running.cancelled()
    assert state.attempted == {slew}
    assert state.interruptions == {slew: Interruption("acknowledged")}
    assert state.run is not None
    assert [r.status for r in state.run.results.values()] == ["cancelled"]
    assert state.cleanup == []
    assert len(rig["mount"].sent("Abort")) == 1
    assert rig["mount"].sent("Stop") == []
    assert events(queue) == [("FollowTarget", "begin"),
                             ("FollowTarget", "cancelled")]
    await assert_settled(idle)


@pytest.mark.asyncio
async def test_cancellation_before_handoff_sends_and_arms_nothing(
        executor, rig, sensor, observer):
    rig["wheel-w"].hold("SetFilter")
    workflow = lowered(
        sensor,
        step(sensor, "slew", "mount", FollowTarget(target=TARGET),
             delay_s=60.0),
        step(sensor, "filter", "wheel-w", SetFilter(filter="r")),
        cleanup=(cleanup("unslew", step(sensor, "stop", "mount", Stop()),
                         armed_by=("slew",)),
                 cleanup("unfilter", step(sensor, "stop", "mount", Stop()),
                         when="cancelled", armed_by=("filter",)),
                 cleanup("failed", step(sensor, "stop", "mount", Stop()),
                         when="failure")))

    with observer.subscription() as queue:
        running = rig.start(
            executor.execute(workflow, in_domain=abort_only))
        # The slew is still in its delay once the filter move has arrived.
        await reached(rig["wheel-w"].arrival("SetFilter"))
        running.cancel("abort")
        report = await finished(running)

    assert report.outcome == "aborted"
    assert report.reason == "abort"
    assert operation(workflow, "SetFilter") in report.attempted
    assert operation(workflow, "FollowTarget") not in report.attempted
    assert ran(report) == ["unfilter"]
    assert rig["mount"].sent("FollowTarget") == []
    assert rig["mount"].sent("Abort") == []
    assert events(queue)[:2] == [("SetFilter", "begin"),
                                 ("SetFilter", "cancelled")]


@pytest.mark.asyncio
async def test_an_authored_abort_is_ordinary_work(executor, rig, sensor,
                                                  monkeypatch):
    monkeypatch.setattr(dispatch, "ABORT_TIMEOUT_S", 0.1)
    rig["mount"].hold("Abort")
    rig["mount"].hold("Abort")
    workflow = lowered(sensor, step(sensor, "abort", "mount", Abort(),
                                    timeout_s=0.1))

    with pytest.raises(WorkflowError) as raised:
        await executor.execute(workflow)

    # Its own deadline expires and it gets the one built-in attempt, whose
    # expiry starts nothing further.
    assert raised.value.report.interruptions == {
        operation(workflow, "Abort"): Interruption("timed_out")}
    assert len(rig["mount"].sent("Abort")) == 2


# Cancellation arriving more than once


@pytest.mark.asyncio
async def test_a_later_hard_cancellation_overrides_a_domain_abort(
        executor, rig, sensor, idle):
    rig["mount"].hold("FollowTarget")
    answering = rig["mount"].hold("Abort")
    workflow = lowered(sensor, step(sensor, "slew", "mount",
                                    FollowTarget(target=TARGET)),
                       cleanup=(cleanup("halt", step(sensor, "stop", "mount",
                                                     Stop())),))
    slew = operation(workflow, "FollowTarget")
    state = ExecutionState()
    running = rig.start(
        executor.execute(workflow, in_domain=abort_only, state=state))

    await reached(rig["mount"].arrival("FollowTarget"))
    running.cancel("abort")
    await reached(rig["mount"].arrival("Abort"))
    running.cancel("shutdown")
    await asyncio.sleep(0)

    # The Abort under way is waited out before anything propagates.
    assert not running.done()

    answering.set()
    await ended(running)

    with pytest.raises(asyncio.CancelledError, match="shutdown"):
        running.result()

    assert state.interruptions == {slew: Interruption("acknowledged")}
    assert state.run is not None
    assert [r.status for r in state.run.results.values()] == ["cancelled"]
    assert state.cleanup == []
    assert len(rig["mount"].sent("Abort")) == 1
    assert rig["mount"].sent("Stop") == []
    await assert_settled(idle)


@pytest.mark.parametrize("times", [1, 3])
@pytest.mark.asyncio
async def test_repeated_cancellation_leaves_nothing_running(
        executor, rig, sensor, idle, times):
    rig["cam-w"].hold("CameraCapture")
    answering = rig["cam-w"].hold("Abort")
    rig["mount"].hold("FollowTarget")
    workflow = lowered(
        sensor,
        step(sensor, "slew", "mount", FollowTarget(target=TARGET)),
        step(sensor, "frame", "cam-w", CameraCapture(integration_time=1.0,
                                                     context=None)))
    state = ExecutionState()
    running = rig.start(executor.execute(workflow, state=state))

    await reached(rig["mount"].arrival("FollowTarget"))
    await reached(rig["cam-w"].arrival("CameraCapture"))

    for _ in range(times):
        running.cancel()
        await asyncio.sleep(0)

    await reached(rig["cam-w"].arrival("Abort"))
    await asyncio.sleep(0)

    assert not running.done()

    answering.set()
    await ended(running)

    assert running.cancelled()
    assert {op.target.device: i.outcome
            for op, i in state.interruptions.items()} == {
        "mount": "acknowledged", "cam-w": "acknowledged"}
    assert len(rig["mount"].sent("Abort")) == len(rig["cam-w"].sent("Abort")) == 1
    await assert_settled(idle)


# Cancellation classified as it arrives

TWICE = pytest.mark.parametrize(("first", "propagated"), [
    (True, "second"),
    (False, "first"),
])
"""A domain abort then a hard cancellation, and a hard one then a domain abort.

Either way the hard cancellation propagates."""


async def cancelled_twice(running: asyncio.Task, device: Device,
                          answering: asyncio.Event, intent: Intent,
                          first: bool):
    """Cancel a run once and again while `device` holds its Abort, then let the
    caller settle on a domain abort before the Abort is answered.

    Classifying only after the drain would read that settled intent for both
    cancellations and return an aborted report.
    """
    intent.domain = first
    running.cancel("first")
    await reached(device.arrival("Abort"))
    intent.domain = not first
    running.cancel("second")
    await asyncio.sleep(0)

    assert not running.done()

    intent.domain = True
    answering.set()
    await ended(running)


@TWICE
@pytest.mark.asyncio
async def test_the_run_classifies_each_cancellation_as_it_arrives(
        executor, rig, sensor, idle, first, propagated):
    rig["mount"].hold("FollowTarget")
    answering = rig["mount"].hold("Abort")
    workflow = lowered(sensor, step(sensor, "slew", "mount",
                                    FollowTarget(target=TARGET)),
                       cleanup=(cleanup("halt", step(sensor, "stop", "mount",
                                                     Stop())),))
    intent, state = Intent(), ExecutionState()
    running = rig.start(
        executor.execute(workflow, in_domain=intent, state=state))

    await reached(rig["mount"].arrival("FollowTarget"))
    await cancelled_twice(running, rig["mount"], answering, intent, first)

    with pytest.raises(asyncio.CancelledError, match=propagated):
        running.result()

    assert [i.outcome for i in state.interruptions.values()] == [
        "acknowledged"]
    assert state.cleanup == []
    assert rig["mount"].sent("Stop") == []
    await assert_settled(idle)


@TWICE
@pytest.mark.asyncio
async def test_cleanup_classifies_each_cancellation_as_it_arrives(
        executor, rig, sensor, idle, first, propagated):
    rig["cam-w"].hold("CameraCapture")
    answering = rig["cam-w"].hold("Abort")
    workflow = lowered(
        sensor, step(sensor, "stop", "mount", Stop()),
        cleanup=(cleanup("held", step(sensor, "a", "cam-w",
                                      CameraCapture(integration_time=1.0,
                                                    context=None))),
                 cleanup("later", step(sensor, "b", "mount", Stop()))))
    intent, state = Intent(), ExecutionState()
    running = rig.start(
        executor.execute(workflow, in_domain=intent, state=state))

    await reached(rig["cam-w"].arrival("CameraCapture"))
    await cancelled_twice(running, rig["cam-w"], answering, intent, first)

    with pytest.raises(asyncio.CancelledError, match=propagated):
        running.result()

    assert ran(state) == ["held"]
    assert state.cleanup[0].run.aborted
    assert not state.cleanup[0].expired
    assert [i.outcome for i in state.interruptions.values()] == [
        "acknowledged"]
    # The main run's one Stop, and not the later cleanup's.
    assert len(rig["mount"].sent("Stop")) == 1
    await assert_settled(idle)


# One state per run


async def refused(executor: WorkflowExecutor, sensor: BoundSensor, rig: Rig,
                  state: ExecutionState):
    """Hand a used state to another run, which must change nothing."""
    run, attempted = state.run, set(state.attempted)
    interruptions, done = dict(state.interruptions), list(state.cleanup)
    sent = list(rig.log)

    with pytest.raises(ValueError, match="records one run"):
        await executor.execute(
            lowered(sensor, step(sensor, "again", "mount", Stop())),
            state=state)

    assert state.run is run
    assert state.attempted == attempted
    assert state.interruptions == interruptions
    assert state.cleanup == done
    assert rig.log == sent


@pytest.mark.asyncio
async def test_a_state_an_empty_run_used_is_refused(executor, rig, sensor):
    state = ExecutionState()
    report = await executor.execute(lowered(sensor), state=state)

    assert report.run.results == {}
    assert state.attempted == set()

    await refused(executor, sensor, rig, state)


@pytest.mark.asyncio
async def test_a_state_a_completed_run_used_is_refused(executor, rig, sensor):
    state = ExecutionState()
    await executor.execute(lowered(sensor, step(sensor, "stop", "mount",
                                                Stop())), state=state)

    await refused(executor, sensor, rig, state)
    assert len(rig["mount"].sent("Stop")) == 1


@pytest.mark.asyncio
async def test_a_state_a_cancelled_run_used_is_refused(executor, rig, sensor):
    rig["mount"].hold("FollowTarget")
    state = ExecutionState()
    running = rig.start(executor.execute(
        lowered(sensor, step(sensor, "slew", "mount",
                             FollowTarget(target=TARGET))), state=state))

    await reached(rig["mount"].arrival("FollowTarget"))
    running.cancel()
    await ended(running)

    assert running.cancelled()

    await refused(executor, sensor, rig, state)
    assert list(state.interruptions.values()) == [
        Interruption("acknowledged")]


@pytest.mark.asyncio
async def test_a_state_a_running_run_holds_is_refused(executor, rig, sensor):
    slewing = rig["mount"].hold("FollowTarget")
    workflow = lowered(sensor, step(sensor, "slew", "mount",
                                    FollowTarget(target=TARGET)))
    state = ExecutionState()
    running = rig.start(executor.execute(workflow, state=state))

    await reached(rig["mount"].arrival("FollowTarget"))
    await refused(executor, sensor, rig, state)
    slewing.set()
    report = await finished(running)

    assert report.run is state.run
    assert report.run.ok
    assert state.attempted == {operation(workflow, "FollowTarget")}
    assert rig["mount"].sent("Stop") == []


# Collect cleanup


@pytest.mark.asyncio
async def test_a_failed_preparation_cleans_up_after_settings_drain(
        executor, rig, sensor, observer):
    wheel, mount = rig["wheel-w"], rig["mount"]
    filtering = wheel.hold("SetFilter")
    slewing = mount.hold("FollowTarget")
    mount.refusing["FollowTarget"] = 1
    workflow = collected(
        sensor,
        RequestEpoch(units=(asking("cam-w", settings=(
            CommandRequest(command=SetFilter(filter="r")),)),)),
        prepare=(pointing(),), cleanup=(stopping(),))

    with observer.subscription() as queue:
        running = rig.start(executor.execute(workflow))

        # The filter change is in flight when preparation fails.
        await reached(wheel.arrival("SetFilter"))
        slewing.set()
        await observed(queue, "mount", "FollowTarget", "failed")

    assert mount.sent("Stop") == []

    filtering.set()

    with pytest.raises(WorkflowError, match="in the run") as raised:
        await finished(running)

    report = raised.value.report

    assert rig["cam-w"].sent("CameraCapture") == []
    assert ran(report) == ["demo/cleanup"]
    assert report.cleanup[0].run.ok
    assert rig.ended_before("wheel-w SetFilter", "mount Stop")
    assert mount.overlapping("Stop") == frozenset()


@pytest.mark.asyncio
async def test_a_fail_fast_acquisition_failure_cleans_up(executor, rig,
                                                         sensor):
    rig["cam-w"].refusing["CameraCapture"] = 1
    workflow = collected(sensor, RequestEpoch(units=(asking("cam-w", 3),)),
                         prepare=(pointing(),), cleanup=(stopping(),))

    with pytest.raises(WorkflowError, match="in the run") as raised:
        await executor.execute(workflow)

    report = raised.value.report

    assert len(rig["cam-w"].sent("CameraCapture")) == 1
    assert ran(report) == ["demo/cleanup"]
    assert len(rig["mount"].sent("Stop")) == 1


@pytest.mark.asyncio
async def test_a_domain_abort_mid_collect_cleans_up(executor, rig, sensor):
    rig["cam-w"].hold("CameraCapture")
    workflow = collected(sensor, RequestEpoch(units=(asking("cam-w", 3),)),
                         prepare=(pointing(),), cleanup=(stopping(),))
    running = rig.start(
        executor.execute(workflow, in_domain=abort_only))

    await reached(rig["cam-w"].arrival("CameraCapture"))
    running.cancel("abort")
    report = await finished(running)
    frame, = (op for op in report.attempted if op.acquisition is not None)

    assert report.outcome == "aborted"
    assert report.reason == "abort"
    assert report.interruptions == {frame: Interruption("acknowledged")}
    assert ran(report) == ["demo/cleanup"]
    assert report.cleanup[0].run.ok
    assert rig.ended_before("cam-w Abort", "mount Stop")


@pytest.mark.asyncio
async def test_hard_cancellation_mid_collect_starts_no_cleanup(executor, rig,
                                                               sensor):
    rig["cam-w"].hold("CameraCapture")
    workflow = collected(sensor, RequestEpoch(units=(asking("cam-w", 3),)),
                         prepare=(pointing(),), cleanup=(stopping(),))
    state = ExecutionState()
    running = rig.start(executor.execute(
        workflow, in_domain=abort_only, state=state))

    await reached(rig["cam-w"].arrival("CameraCapture"))
    running.cancel("shutdown")
    await ended(running)

    assert running.cancelled()
    assert state.cleanup == []
    assert rig["mount"].sent("Stop") == []
    assert [i.outcome for i in state.interruptions.values()] == [
        "acknowledged"]


@pytest.mark.asyncio
async def test_without_preparation_any_attempted_command_arms_cleanup(
        executor, rig, sensor):
    workflow = collected(sensor, RequestEpoch(units=(asking("cam-w"),)),
                         cleanup=(stopping(),))
    report = await executor.execute(workflow)

    assert ran(report) == ["demo/cleanup"]
    assert len(rig["mount"].sent("Stop")) == 1


@pytest.mark.asyncio
async def test_preparation_overridden_arms_no_cleanup(executor, rig, sensor):
    # The frame is attempted, but preparation was authored and never was.
    workflow = collected(
        sensor, RequestEpoch(units=(asking("cam-w"),)),
        prepare=(pointing(),), cleanup=(stopping(),),
        rules=(OperatorRule(reason="already tracking",
                            commands=("FollowTarget",), outcome="ok"),))
    report = await executor.execute(workflow)

    assert report.run.ok
    assert len(rig["cam-w"].sent("CameraCapture")) == 1
    assert rig["mount"].received == []
    assert report.cleanup == ()


@pytest.mark.asyncio
async def test_collect_cleanup_continues_past_a_failed_command(executor, rig,
                                                               sensor):
    rig["mount"].refusing["Stop"] = 1
    workflow = collected(sensor, RequestEpoch(units=(asking("cam-w"),)),
                         prepare=(pointing(),),
                         cleanup=(stopping(), stopping(timeout_s=5.0)),
                         fail_fast=True)

    with pytest.raises(WorkflowError,
                       match="in cleanup 'demo/cleanup', Stop") as raised:
        await executor.execute(workflow)

    report = raised.value.report

    assert report.run.ok
    assert len(rig["mount"].sent("Stop")) == 2
    assert [r.status for r in report.cleanup[0].run.results.values()] == [
        "failed", "ok"]
    assert "in the run" not in str(raised.value)


# Arming and selection


@pytest.mark.asyncio
async def test_arming_follows_attempts_and_nothing_else(executor, rig, sensor):
    workflow = lowered(
        sensor,
        step(sensor, "stop", "mount", Stop()),
        step(sensor, "home", "mount", Home(), unsupported="omit"),
        step(sensor, "skipped", "wheel-w", SetFilter(filter="r")),
        rules=(OperatorRule(reason="not tonight", commands=("SetFilter",),
                            outcome="ok"),),
        cleanup=(cleanup("unconditional", step(sensor, "a", "mount", Stop())),
                 cleanup("never", step(sensor, "b", "mount", Stop()),
                         armed_by=()),
                 cleanup("omitted", step(sensor, "c", "mount", Stop()),
                         armed_by=("home",)),
                 cleanup("overridden", step(sensor, "d", "mount", Stop()),
                         armed_by=("skipped",)),
                 cleanup("attempted", step(sensor, "e", "mount", Stop()),
                         armed_by=("home", "stop"))))
    report = await executor.execute(workflow)

    assert workflow.cleanup[2].armed_by == ()
    assert ran(report) == ["unconditional", "attempted"]
    assert rig["wheel-w"].received == []


@pytest.mark.asyncio
async def test_unconditional_cleanup_runs_with_nothing_attempted(executor, rig,
                                                                 sensor):
    workflow = lowered(
        sensor, step(sensor, "skipped", "wheel-w", SetFilter(filter="r")),
        rules=(OperatorRule(reason="not tonight", commands=("SetFilter",),
                            outcome="ok"),),
        cleanup=(cleanup("unconditional", step(sensor, "a", "mount", Stop())),))
    report = await executor.execute(workflow)

    # The only attempt is the cleanup's own.
    assert report.attempted == {operation(workflow.cleanup[0], "Stop")}
    assert ran(report) == ["unconditional"]
    assert rig["wheel-w"].received == []


# Cleanup outcomes


@pytest.mark.asyncio
async def test_a_cleanup_failure_raises_after_later_cleanup(executor, rig,
                                                            sensor):
    rig["mount"].refusing["Stop"] = 1
    workflow = lowered(
        sensor, step(sensor, "slew", "mount", FollowTarget(target=TARGET)),
        cleanup=(cleanup("first", step(sensor, "a", "mount", Stop())),
                 cleanup("second", step(sensor, "b", "wheel-w",
                                        SetFilter(filter="dark")))))

    with pytest.raises(WorkflowError) as raised:
        await executor.execute(workflow)

    message = str(raised.value)
    report = raised.value.report

    assert message.startswith("t: in cleanup 'first', Stop")
    assert "second" not in message
    assert ran(report) == ["first", "second"]
    assert [c.failed for c in report.cleanup] == [True, False]
    assert len(rig["wheel-w"].sent("SetFilter")) == 1


@pytest.mark.asyncio
async def test_a_main_failure_stays_primary_through_a_cancelled_cleanup(
        executor, rig, sensor):
    rig["mount"].refusing["FollowTarget"] = 1
    rig["wheel-w"].hold("SetFilter")
    workflow = lowered(
        sensor, step(sensor, "slew", "mount", FollowTarget(target=TARGET)),
        cleanup=(cleanup("first", step(sensor, "a", "wheel-w",
                                       SetFilter(filter="dark")),
                         when="failure"),
                 cleanup("second", step(sensor, "b", "mount", Stop()))))
    running = rig.start(
        executor.execute(workflow, in_domain=abort_only))

    await reached(rig["wheel-w"].arrival("SetFilter"))
    running.cancel("abort")

    with pytest.raises(WorkflowError) as raised:
        await finished(running)

    report = raised.value.report

    assert str(raised.value).startswith("t: in the run, FollowTarget")
    assert (report.outcome, report.reason) == ("aborted", "abort")
    assert ran(report) == ["first"]
    assert report.cleanup[0].run.aborted
    assert rig["mount"].sent("Stop") == []


@pytest.mark.asyncio
async def test_cancelling_cleanup_in_domain_starts_no_later_graph(
        executor, rig, sensor):
    rig["cam-w"].hold("CameraCapture")
    workflow = lowered(
        sensor, step(sensor, "stop", "mount", Stop()),
        cleanup=(cleanup("first",
                         step(sensor, "a", "cam-w",
                              CameraCapture(integration_time=1.0,
                                            context=None)),
                         step(sensor, "b", "mount", Stop(), "a")),
                 cleanup("second", step(sensor, "c", "mount", Stop()))))
    running = rig.start(
        executor.execute(workflow, in_domain=abort_only))

    await reached(rig["cam-w"].arrival("CameraCapture"))
    running.cancel("abort")
    report = await finished(running)
    capture = operation(workflow.cleanup[0], "CameraCapture")

    assert (report.outcome, report.reason) == ("aborted", "abort")
    assert ran(report) == ["first"]
    assert report.interruptions == {capture: Interruption("acknowledged")}
    assert len(rig["cam-w"].sent("Abort")) == 1
    # The main run's one Stop, and neither cleanup's.
    assert len(rig["mount"].sent("Stop")) == 1


@pytest.mark.asyncio
async def test_hard_cancellation_during_cleanup_propagates(executor, rig,
                                                           sensor):
    rig["cam-w"].hold("CameraCapture")
    workflow = lowered(
        sensor, step(sensor, "stop", "mount", Stop()),
        cleanup=(cleanup("first", step(sensor, "a", "cam-w",
                                       CameraCapture(integration_time=1.0,
                                                     context=None))),
                 cleanup("second", step(sensor, "c", "mount", Stop()))))
    state = ExecutionState()
    running = rig.start(executor.execute(
        workflow, in_domain=abort_only, state=state))

    await reached(rig["cam-w"].arrival("CameraCapture"))
    running.cancel("shutdown")
    await ended(running)

    assert running.cancelled()
    assert ran(state) == ["first"]
    assert state.cleanup[0].run.aborted
    assert [i.outcome for i in state.interruptions.values()] == [
        "acknowledged"]
    assert len(rig["mount"].sent("Stop")) == 1


@pytest.mark.asyncio
async def test_a_hung_cleanup_is_ended_by_its_own_deadline(executor, rig,
                                                           sensor):
    rig["mount"].hold("FollowTarget")
    workflow = lowered(
        sensor, step(sensor, "stop", "mount", Stop()),
        cleanup=(cleanup("hung", step(sensor, "a", "mount",
                                      FollowTarget(target=TARGET),
                                      timeout_s=30.0),
                         timeout_s=0.1),
                 cleanup("after", step(sensor, "b", "wheel-w",
                                       SetFilter(filter="dark")))))

    # Well inside the command's own deadline, which cannot extend the graph's.
    async with asyncio.timeout(2.0):
        with pytest.raises(WorkflowError,
                           match="in cleanup 'hung', exceeded its 0.1s") as raised:
            await executor.execute(workflow)

    report = raised.value.report
    slew = operation(workflow.cleanup[0], "FollowTarget")

    assert [(c.run.name, c.expired) for c in report.cleanup] == [
        ("hung", True), ("after", False)]
    assert report.interruptions == {slew: Interruption("acknowledged")}
    assert report.outcome == "completed"


@pytest.mark.asyncio
async def test_concurrent_slew_and_filter_drain_before_cleanup(executor, rig,
                                                               sensor):
    wheel, mount = rig["wheel-w"], rig["mount"]
    mount.waits["FollowTarget"] = wheel.arrival("SetFilter")
    wheel.waits["SetFilter"] = mount.arrival("FollowTarget")
    workflow = collected(
        sensor,
        RequestEpoch(units=(asking("cam-w", settings=(
            CommandRequest(command=SetFilter(filter="r")),)),)),
        prepare=(pointing(),), cleanup=(stopping(),))

    # Each waits for the other to arrive, so run in turn they would hang.
    async with asyncio.timeout(2.0):
        report = await executor.execute(workflow)

    assert report.run.ok
    assert rig.at("mount FollowTarget") < rig.at("wheel-w SetFilter ends")
    assert rig.at("wheel-w SetFilter") < rig.at("mount FollowTarget ends")
    assert rig.ended_before("cam-w CameraCapture", "mount Stop")


# Headers


@pytest.mark.asyncio
async def test_a_dispatched_standard_frame_writes_its_frame_number(
        executor, rig, sensor):
    task = StandardCollectTask(
        target=TARGET,
        camera_params=[CameraParameterSet(integration_time_seconds=0.1,
                                          frame_count=3)])
    workflow = compile_collect(pack(translate(task), sensor), sensor)
    frames = [op for op in report_order(workflow) if op.acquisition is not None]
    planned = [(op.command.model_copy(deep=True),
                op.acquisition.keywords[Collect].model_copy())
               for op in frames]
    report = await executor.execute(workflow)
    captured = [c for c in rig[frames[0].target.device].received
                if isinstance(c, CameraCapture)]

    assert report.run.ok
    assert len(captured) == len(frames) == 3

    for sent, op, (command, keyword) in zip(captured, frames, planned,
                                            strict=True):
        written = sent.context[Collect]

        assert written.frame_number == op.acquisition.frame_number
        assert dict(written.get_fits_cards())["FRAMENUM"][0] == (
            op.acquisition.frame_number)
        # Dispatch copied the command, and left what was planned alone.
        assert op.command == command
        assert op.command.context is None
        assert op.acquisition.keywords[Collect] == keyword


def report_order(workflow: ExecutableWorkflow) -> list[Operation]:
    """The run's operations, in the order the compiler emitted them."""
    return [n.payload for n in workflow.graph.nodes
            if isinstance(n.payload, Operation)]


# Observability


@pytest.mark.asyncio
async def test_a_subscriber_sees_each_operation_begin_and_end_in_order(
        executor, sensor, observer):
    workflow = lowered(sensor,
                       step(sensor, "stop", "mount", Stop()),
                       step(sensor, "filter", "wheel-w", SetFilter(filter="r"),
                            "stop"))

    with observer.subscription() as queue:
        await executor.execute(workflow)

    assert events(queue) == [("Stop", "begin"), ("Stop", "ok"),
                             ("SetFilter", "begin"), ("SetFilter", "ok")]


@pytest.mark.asyncio
async def test_a_stalled_subscriber_loses_its_oldest_events(
        executor, sensor, observer):
    workflow = lowered(sensor,
                       step(sensor, "stop", "mount", Stop()),
                       step(sensor, "filter", "wheel-w", SetFilter(filter="r"),
                            "stop"))

    with observer.subscription(maxsize=1) as stalled:
        report = await executor.execute(workflow)

        assert report.run.ok
        assert observer.dropped == 3
        assert events(stalled) == [("SetFilter", "ok")]


@pytest.mark.asyncio
async def test_a_broken_subscriber_does_not_fail_a_run(executor, rig, sensor,
                                                       observer):
    workflow = lowered(sensor, step(sensor, "stop", "mount", Stop()))

    with observer.subscription() as broken:
        broken.shutdown()
        report = await executor.execute(workflow)

    assert report.run.ok
    assert len(rig["mount"].sent("Stop")) == 1


@pytest.mark.asyncio
async def test_a_subscriber_attaching_mid_run_sees_what_follows(
        executor, rig, sensor, observer):
    filtering = rig["wheel-w"].hold("SetFilter")
    workflow = lowered(sensor,
                       step(sensor, "stop", "mount", Stop()),
                       step(sensor, "filter", "wheel-w", SetFilter(filter="r"),
                            "stop"))
    running = rig.start(executor.execute(workflow))

    await reached(rig["wheel-w"].arrival("SetFilter"))

    with observer.subscription() as late:
        filtering.set()
        await finished(running)

    assert events(late) == [("SetFilter", "ok")]
