# SPDX-License-Identifier: Apache-2.0
"""Connect a sensor session and plan workflows independently of execution.

    session = await Sensor.connect(definition, sensorkit)
    workflow = session.plan_lifecycle(definition.tables[0])
    report = await session.execute(workflow)

The session accepts lifecycle tables and collect intents directly. To use
policies, compose their deadline rules before connecting, then compose tables
against the bound sensor. Standard-task translation is also optional.
"""

from __future__ import annotations

import weakref
from collections.abc import Mapping

import sensorkit.api as sk
from sensorkit.backend.base import KeyNotFound
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.entity import DeviceDetails, EntityInfo
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.collect import CollectIntent, compile_collect, pack
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.dispatch import DeviceContexts, OperationEvent
from sensorkit.sensor.execution import (
    AbortPredicate,
    ExecutionState,
    WorkflowExecutor,
    WorkflowReport,
)
from sensorkit.sensor.lifecycle import LifecycleWorkflow, compile_lifecycle
from sensorkit.sensor.topology import DeviceKey
from sensorkit.sensor.workflow import ExecutableWorkflow, OperatorRule


class Sensor:
    """A connected sensor that plans workflows and executes them one at a time.

    Only workflows planned by this session may execute through it. Discarded
    plans are not retained. Callers must not mutate the definition, binding or
    compiled workflows. The session checks workflow identity without checking
    for mutations to nested values.
    """

    def __init__(
        self, definition: SensorDefinition, sensor: BoundSensor, executor: WorkflowExecutor
    ):
        self._definition = definition
        self._sensor = sensor
        self._executor = executor
        self._issued: weakref.WeakSet[ExecutableWorkflow] = weakref.WeakSet()
        self._running = False

    @property
    def definition(self) -> SensorDefinition:
        """Return the definition supplying this session's structure and
        deadline rules.
        """
        return self._definition

    @property
    def sensor(self) -> BoundSensor:
        """Return the bound sensor used for planning and execution."""
        return self._sensor

    @property
    def executor(self) -> WorkflowExecutor:
        """Return the executor and its operation-event observer.

        Calling it directly bypasses session issuance and overlap checks.
        """
        return self._executor

    @classmethod
    async def connect(cls, definition: SensorDefinition, sensorkit: sk.SensorKit) -> Sensor:
        """Validate the definition, discover configured devices and bind
        their capabilities.

        Validate the definition before reading device reports. The caller owns
        the supplied client and backend and remains responsible for closing
        them.

        Args:
            definition: Authored definition with any generated deadlines
                composed.
            sensorkit: Client used to access configured devices.

        Returns:
            A session bound to the discovered device details.

        Raises:
            ValueError: The definition is invalid, an entity is not a device, a
                report is missing, or a declared trait is unregistered or
                unsatisfied.
            BackendError: A device report could not be read from the backend.
        """
        topology = definition.check()
        keys = tuple(p.device for p in topology.placements())
        clients = {key: sensorkit.device(key) for key in keys}
        reported: dict[str, DeviceDetails] = {}

        for key, client in clients.items():
            try:
                info = await client.kv_get_model(EntityInfo)
            except KeyNotFound:
                continue

            if not isinstance(info.details, DeviceDetails):
                raise ValueError(f"'{key}' is not a device")

            reported[key] = info.details

        # Report all missing devices in one binding error.
        sensor = BoundSensor.bind(topology, reported)
        executor = WorkflowExecutor(sensor, clients, events=AsyncObserver[OperationEvent]())

        return cls(definition, sensor, executor)

    def plan_lifecycle(
        self, workflow: LifecycleWorkflow, *, rules: tuple[OperatorRule, ...] = ()
    ) -> ExecutableWorkflow:
        """Compile and issue a lifecycle table using this definition's
        deadlines.

        Optional operator rules come from the caller. No commands are sent.

        Raises:
            ValueError: The table cannot compile against this sensor.
        """
        return self._issue(
            compile_lifecycle(
                workflow, self._sensor, deadlines=self._definition.deadlines, rules=rules
            )
        )

    def plan_collect(
        self,
        intent: CollectIntent,
        *,
        rules: tuple[OperatorRule, ...] = (),
        device_keywords: Mapping[DeviceKey, KeywordDict] | None = None,
    ) -> ExecutableWorkflow:
        """Pack, compile and issue a collect intent using this definition's
        deadlines.

        `device_keywords` supplies the keywords for planning selections. A
        missing mapping, device or keyword is treated as absent. Operator rules
        use the session's binding independently of this mapping. Planning
        chooses all hardware before any commands are sent.

        Raises:
            ValueError: The intent cannot pack or compile against this sensor.
        """
        bound = pack(intent, self._sensor, device_keywords=device_keywords)

        return self._issue(
            compile_collect(bound, self._sensor, deadlines=self._definition.deadlines, rules=rules)
        )

    async def execute(
        self,
        workflow: ExecutableWorkflow,
        *,
        contexts: DeviceContexts | None = None,
        base: sk.Context | None = None,
        in_domain: AbortPredicate | None = None,
        state: ExecutionState | None = None,
    ) -> WorkflowReport:
        """Run an issued workflow, keeping the session occupied through
        cleanup and drain.

        Overlapping runs are rejected.

        Args:
            workflow: Workflow planned by this session.
            contexts: Current device contexts, sampled for each acquisition.
            base: Context shared by every acquisition in the run.
            in_domain: Predicate claiming cancellations as domain aborts.
            state: Fresh caller-owned state for inspection after hard
                cancellation.

        Returns:
            The executor's aggregate workflow report.

        Raises:
            ValueError: The workflow was not issued here, another run is
                active, or state was already used. Refusal dispatches nothing
                and preserves state.
            WorkflowError: Required main work failed or did not run, or
                required cleanup failed without a domain abort.
            asyncio.CancelledError: A declined cancellation, after local work
                drains.
        """
        # Check issuance and overlap before yielding or touching execution state.
        if workflow not in self._issued:
            raise ValueError(
                f"{workflow.name}: this session did not issue the workflow; "
                f"plan it through the session that runs it"
            )

        if self._running:
            raise ValueError(f"{workflow.name}: another run on this session has not ended")

        self._running = True

        try:
            return await self._executor.execute(
                workflow, contexts=contexts, base=base, in_domain=in_domain, state=state
            )
        finally:
            self._running = False

    def _issue(self, workflow: ExecutableWorkflow) -> ExecutableWorkflow:
        """Register a workflow by identity without retaining it strongly."""
        self._issued.add(workflow)

        return workflow
