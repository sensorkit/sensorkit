# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: F401
"""Plan and execute workflows for a sensor's devices and instruments.

`SensorDefinition` describes the device structure, lifecycle tables and deadline
rules. Validation checks the document without contacting devices; binding then
checks reported capabilities and establishes each device's traits.

The lifecycle and collect compilers produce planned steps with dependencies.
Shared lowering resolves capability omissions, deadlines and operator rules,
then builds graphs for `sensorkit.common.dag` to schedule and execute.

`Sensor` connects to devices and exposes planning and execution as
separate calls. `audit` explains definitions and compiled workflows. The optional
`policies` and `standard_task` adapters generate lifecycle tables and collect
intents for the same compilers.

`SensorController` serves a structure and policies from the `sensors`
configuration section as a controller.
"""

from sensorkit.sensor.binding import (
    BindingReport,
    BoundSensor,
    CapabilitySnapshot,
)
from sensorkit.sensor.collect import (
    AcquisitionRequest,
    BoundCollect,
    BoundEpoch,
    Collect,
    CollectIntent,
    CommandRequest,
    Epoch,
    InstrumentRequest,
    PackingConflict,
    PlannedAcquisition,
    RequestEpoch,
    SettingUnsatisfiable,
    compile_collect,
    pack,
)
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.dispatch import (
    AttemptRecorder,
    DeviceContexts,
    Dispatcher,
    Interruption,
    InterruptionOutcome,
    InterruptionRecorder,
    OperationEvent,
    OperationOutcome,
)
from sensorkit.sensor.execution import (
    AbortPredicate,
    CleanupReport,
    ExecutionState,
    WorkflowError,
    WorkflowExecutor,
    WorkflowOutcome,
    WorkflowReport,
)
from sensorkit.sensor.lifecycle import (
    CleanupSpec,
    Entry,
    Join,
    LifecycleWorkflow,
    OpSpec,
    Phase,
    compile_lifecycle,
)
from sensorkit.sensor.policies import (
    SensorPolicies,
    compose_deadlines,
    compose_tables,
)
from sensorkit.sensor.selection import (
    AnySelection,
    DeviceFacts,
    PlacementFacts,
    Selection,
    SelectionError,
)
from sensorkit.sensor.topology import (
    Component,
    Device,
    DeviceKey,
    Placement,
    Port,
    Selector,
    Structure,
    StructurePath,
    TagKey,
    Topology,
    TraitKey,
    Unit,
)
from sensorkit.sensor.workflow import (
    Acquisition,
    Cleanup,
    CleanupPlan,
    DeadlineRule,
    DeadlineTarget,
    Dependency,
    ExecutableWorkflow,
    Omission,
    Operation,
    OperationId,
    OperatorRule,
    Origin,
    PlannedStep,
    RequestId,
    RoutedCommand,
    Scope,
    StepName,
    Subject,
    lower,
    resolve_deadline,
)
