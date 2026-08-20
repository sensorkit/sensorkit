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
