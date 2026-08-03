# SPDX-License-Identifier: Apache-2.0
"""Standard sensor control.

Two implementations of one configuration section. `legacy` is the hand-written
orchestration sites run today; `client` and `impl` are the workflow
implementation, split so that the orchestration is usable without a controller
and the controller is a thin face over it. `impl` holds the service entrypoint
that chooses between them.

`__all__` is the public API.
"""

from sensorkit.std.sensor.client import Sensor, connect_sensor
from sensorkit.std.sensor.compat import Capabilities, add_compat_context
from sensorkit.std.sensor.config import (
    Implementation,
    SensorConfig,
    SensorDevices,
    SensorPolicies,
)
from sensorkit.std.sensor.derive import (
    capability_index,
    derive_plan,
    derive_structure,
    derive_tables,
    timeouts,
)
from sensorkit.std.sensor.dispatch import (
    DeviceContexts,
    Dispatcher,
    FrameKeywords,
    Handler,
    Listener,
    OpOutcome,
    compile_supported,
    unresolved,
    unsupported,
)
from sensorkit.std.sensor.impl import StandardSensor, sensor_control_service
from sensorkit.std.sensor.legacy import LegacyDevices, LegacySensor

__all__ = [
    # config: the sensors: section
    "Implementation", "SensorConfig", "SensorDevices", "SensorPolicies",
    # derive: the section as a workflow plan
    "capability_index", "derive_plan", "derive_structure", "derive_tables",
    "timeouts",
    # dispatch: workflow ops as device commands
    "DeviceContexts", "Dispatcher", "FrameKeywords", "Handler", "Listener",
    "OpOutcome", "compile_supported", "unresolved", "unsupported",
    # client: the sensor, with no controller
    "Sensor", "connect_sensor",
    # impl: the sensor as a service
    "StandardSensor", "sensor_control_service",
    # legacy: retired one release after the workflow implementation ships
    "LegacyDevices", "LegacySensor",
    # compat: retired when the UI stops reading them
    "Capabilities", "add_compat_context",
]
