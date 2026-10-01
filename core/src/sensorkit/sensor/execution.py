# SPDX-License-Identifier: Apache-2.0
"""Run workflow graphs, own cleanup and report aggregate outcomes.

`DagRunner` schedules nodes. This executor supplies dispatch, interruption
recording and cleanup sequencing. Eligible, armed cleanup runs after the main
graph drains, including after a fail-fast failure.

The caller classifies each cancellation as domain abort or hard cancellation.
Domain abort returns an aborted report unless the main run already failed. Hard
cancellation drains in-flight work, skips further cleanup and propagates. A
later hard cancellation still takes precedence over an earlier domain abort.

Cleanup graphs run sequentially under individual timeouts. Cancelling cleanup
stops the sequence and drains current work; cleanup has no further cleanup. A
timed-out graph fails, but subsequent eligible graphs may still run.
`ExecutionState` retains attempts, node results and Abort outcomes even when
hard cancellation propagates. These records do not prove hardware state or
downstream data durability.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal

import sensorkit.api as sk
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.dag import (
    DagRunner,
    Dispatch,
    Node,
    NodeResult,
    RunReport,
    log_summary,
)
from sensorkit.core.device import DeviceClient
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.dispatch import (
    DeviceContexts,
    Dispatcher,
    Interruption,
    InterruptionRecorder,
    OperationEvent,
)
from sensorkit.sensor.topology import DeviceKey
from sensorkit.sensor.workflow import (
    Cleanup,
    ExecutableWorkflow,
    Operation,
)

type WorkflowOutcome = Literal["completed", "aborted"]
"""Outcome of a returned report; hard cancellation propagates instead."""

type AbortPredicate = Callable[[BaseException], bool]
"""Caller predicate distinguishing a domain abort from hard cancellation.

True claims a cancellation as a domain abort; false or no predicate causes hard
cancellation. Classify each arrival separately until one is declined. The
caller supplies intent because both arrive as `CancelledError`.
"""


@dataclass(frozen=True)
class CleanupReport:
    """One cleanup graph's results and whether its total timeout expired.

    Timeout marks failure even if no node reports an error. Cancellation or
    timeout marks the underlying run aborted.
    """

    cleanup: Cleanup
    run: RunReport
    expired: bool = False

    @property
    def failed(self) -> bool:
        """Test for total timeout expiry or required node failures."""
        return self.expired or bool(self.run.failures)


@dataclass
class ExecutionState:
    """Caller-owned, single-use records of one execution.

    Attempts are recorded when dispatch begins. Overrides and cancelled
    delays send no command and do not count as attempts. Cleanup triggers use
    operation identity.

    `run` and `cleanup` hold node results; `interruptions` separately holds
    Abort outcomes. Keep this state to inspect partial results after hard
    cancellation. Reuse is rejected even if the first execution recorded no
    attempts.
    """

    attempted: set[Operation] = field(default_factory=set)
    interruptions: dict[Operation, Interruption] = field(default_factory=dict)
    run: RunReport | None = None
    cleanup: list[CleanupReport] = field(default_factory=list)
    _used: bool = field(default=False, init=False, repr=False)

    def begin(self, workflow: ExecutableWorkflow) -> dict[int, NodeResult]:
        """Reserve this state and seed the main report with the runner's
        result mapping.

        Returns:
            The mutable mapping where the runner records node results.

        Raises:
            ValueError: This state was already used; existing records are
                unchanged.
        """
        if self._used:
            raise ValueError(
                f"{workflow.name}: an ExecutionState records one run, and this one already has"
            )

        self._used = True
        self.run = RunReport(workflow.name, workflow.graph, {})

        return self.run.results

    def interrupted(self, operation: Operation) -> InterruptionRecorder:
        """Return a callback that stores this operation's Abort outcome."""

        def record(interruption: Interruption) -> None:
            self.interruptions[operation] = interruption

        return record


@dataclass(frozen=True)
class WorkflowReport:
    """Main-run results, cleanup results, attempts and interruption
    outcomes.

    Cleanup reports retain execution order. `reason` records the first claimed
    cancellation for an aborted workflow, otherwise `None`.
    """

    name: str
    run: RunReport
    cleanup: tuple[CleanupReport, ...] = ()
    attempted: frozenset[Operation] = frozenset()
    interruptions: Mapping[Operation, Interruption] = field(default_factory=dict)
    outcome: WorkflowOutcome = "completed"
    reason: str | None = None


class WorkflowError(Exception):
    """A required workflow or cleanup failure, with the complete workflow
    report.

    Main-run failures are reported before cleanup failures.
    """

    def __init__(self, message: str, report: WorkflowReport):
        super().__init__(message)
        self.report = report

    @classmethod
    def from_report(cls, report: WorkflowReport) -> WorkflowError:
        """Build an error describing main and cleanup causes, including
        required skipped work.
        """
        sections = []

        if report.run.failures:
            sections.append(f"in the run, {'; '.join(_causes(report.run))}")

        for done in report.cleanup:
            if done.failed:
                expiry = (
                    f"exceeded its {done.cleanup.timeout_s}s deadline" if done.expired else None
                )
                causes = [*([expiry] if expiry else []), *_causes(done.run)]
                sections.append(f"in cleanup '{done.cleanup.origin}', {'; '.join(causes)}")

        return cls(f"{report.name}: {'. '.join(sections)}", report)


class WorkflowExecutor:
    """Run lifecycle or collect workflows against a bound sensor's clients.

    Each execution uses a separate state and dispatcher. The observer spans
    runs. Direct execution permits concurrent runs. `Sensor` permits one run
    at a time and checks that it issued the workflow.
    """

    def __init__(
        self,
        sensor: BoundSensor,
        clients: Mapping[DeviceKey, DeviceClient],
        *,
        events: AsyncObserver[OperationEvent] | None = None,
    ):
        self.sensor = sensor
        self.clients = clients
        self.events = events

    async def execute(
        self,
        workflow: ExecutableWorkflow,
        *,
        contexts: DeviceContexts | None = None,
        base: sk.Context | None = None,
        in_domain: AbortPredicate | None = None,
        state: ExecutionState | None = None,
    ) -> WorkflowReport:
        """Run a compiled workflow, drain calls and attempt eligible
        cleanup.

        Args:
            workflow: Compiled main and cleanup graphs.
            contexts: Current device contexts, sampled for each acquisition.
            base: Context shared by every acquisition in this run.
            in_domain: Predicate claiming cancellations as domain aborts. False
                or no predicate means hard cancellation.
            state: Fresh caller-owned state for inspecting results after
                cancellation.

        Returns:
            An aggregate report, marked aborted for a claimed domain
            cancellation.

        Raises:
            ValueError: The supplied state was already used; dispatch has not
                begun.
            WorkflowError: Required main work failed or did not run, or
                required cleanup failed without a domain abort.
            asyncio.CancelledError: A cancellation was declined, after draining
                in-flight work and without starting further cleanup.
        """
        state = ExecutionState() if state is None else state

        # Reserve state before yielding so concurrent reuse cannot pass.
        results = state.begin(workflow)
        dispatcher = Dispatcher(
            self.sensor, self.clients, contexts=contexts, base=base, events=self.events
        )
        runner = DagRunner(_dispatching(dispatcher, state))
        reasons: list[str] = []

        def claimed(cancellation: BaseException) -> bool:
            if in_domain is None or not in_domain(cancellation):
                return False

            reasons.append(str(cancellation) or "cancelled")
            return True

        state.run = await runner.execute(
            workflow.graph, name=workflow.name, absorbed=claimed, results=results
        )
        log_summary(state.run)

        ended = await _cleaned_up(workflow.cleanup, state.run, state, runner, claimed)
        aborted = state.run.aborted or ended
        report = WorkflowReport(
            name=workflow.name,
            run=state.run,
            cleanup=tuple(state.cleanup),
            attempted=frozenset(state.attempted),
            interruptions=dict(state.interruptions),
            outcome="aborted" if aborted else "completed",
            reason=reasons[0] if aborted and reasons else None,
        )

        # Domain abort preserves established main failures but returns cleanup outcomes.
        if state.run.failures or (not aborted and any(c.failed for c in report.cleanup)):
            raise WorkflowError.from_report(report)

        return report


def _dispatching(dispatcher: Dispatcher, state: ExecutionState) -> Dispatch:
    """Build node dispatch that records operations and resolves ordering
    nodes locally.
    """

    async def dispatch(node: Node) -> object:
        match node.payload:
            case Operation() as operation:
                return await dispatcher.perform(
                    operation, node, state.attempted.add, state.interrupted(operation)
                )
            case _:
                return None

    return dispatch


async def _cleaned_up(
    cleanups: tuple[Cleanup, ...],
    run: RunReport,
    state: ExecutionState,
    runner: DagRunner,
    claimed: Callable[[BaseException], bool],
) -> bool:
    """Run eligible, armed cleanup graphs sequentially until cancellation.

    Select by the drained main run's outcome and recorded attempts.

    Returns:
        Whether caller cancellation ended the sequence as a domain abort.

    Raises:
        asyncio.CancelledError: A declined cancellation, after storing the
            current cleanup report and draining its graph.
    """
    failed = bool(run.failures)

    for cleanup in cleanups:
        if not (cleanup.selected(failed, run.aborted) and cleanup.armed(state.attempted)):
            continue

        done, cancelled, declined = await _bounded(cleanup, runner, claimed)
        state.cleanup.append(done)
        log_summary(done.run)

        if declined is not None:
            raise declined

        if cancelled:
            return True

    return False


async def _bounded(
    cleanup: Cleanup, runner: DagRunner, claimed: Callable[[BaseException], bool]
) -> tuple[CleanupReport, bool, asyncio.CancelledError | None]:
    """Own one cleanup task through timeout, cancellation and final drain.

    Classify caller cancellations until one is declined. Forward cancellations
    and timeout expiry to the runner, which absorbs them and drains its nodes.
    After cancellation, wait for interruption handling without a separate
    bound.

    Returns:
        Cleanup report, whether caller cancellation arrived, and the first
        declined cancellation, if any.
    """
    name = str(cleanup.origin)
    results: dict[int, NodeResult] = {}
    running = asyncio.create_task(
        runner.execute(cleanup.graph, name=name, absorbed=lambda _: True, results=results)
    )
    loop = asyncio.get_running_loop()
    expiry = loop.time() + cleanup.timeout_s
    expired = cancelled = False
    declined: asyncio.CancelledError | None = None

    while not running.done():
        bounded = not (expired or cancelled)

        try:
            await asyncio.wait({running}, timeout=expiry - loop.time() if bounded else None)
        except asyncio.CancelledError as e:
            cancelled = True

            if declined is None and not claimed(e):
                declined = e

            running.cancel()
            continue

        if bounded and not running.done():
            expired = True
            running.cancel()

    # Synthesize a report if cancellation prevented the graph task from starting.
    run = (
        RunReport(name, cleanup.graph, results, aborted=True)
        if running.cancelled()
        else running.result()
    )

    return CleanupReport(cleanup, run, expired), cancelled, declined


def _causes(report: RunReport) -> list[str]:
    """Describe up to three root causes and count required nodes that never
    ran.
    """
    causes = report.causes
    missing = len(report.failures) - sum(not n.optional for n, _ in causes)
    parts = [f"{n.label.strip()} ({e})" for n, e in causes[:3]]

    if len(causes) > 3:
        parts.append(f"and {len(causes) - 3} more")

    if missing:
        parts.append(f"{missing} required operation(s) did not run")

    return parts
