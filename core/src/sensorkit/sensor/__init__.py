# SPDX-License-Identifier: Apache-2.0
"""Standard sensor control.

Two implementations of one configuration section. `legacy` is the hand-written
orchestration sites run today; `client` and `impl` are the workflow
implementation, split so that the orchestration is usable without a controller
and the controller is a thin face over it. `impl` holds the service entrypoint
that chooses between them.

The structural model, the lifecycle and collect compilers, and the graph runner
live in `sensorkit.workflow`, which knows nothing about devices or the backend.
This package supplies the observatory vocabulary and binds workflow ops to real
device commands.

`__all__` is the public API.
"""

from sensorkit.sensor.client import Sensor, connect_sensor, pointed_at
from sensorkit.sensor.compat import Capabilities, add_compat_context
from sensorkit.sensor.config import (
    Implementation,
    SensorConfig,
    SensorDevices,
    SensorPolicies,
)
from sensorkit.sensor.derive import (
    capability_index,
    derive_plan,
    derive_structure,
    derive_tables,
    timeouts,
)
from sensorkit.sensor.dispatch import (
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
from sensorkit.sensor.impl import StandardSensor, sensor_control_service
from sensorkit.sensor.legacy import LegacyDevices, LegacySensor
from sensorkit.sensor.translate import (
    SCIENCE,
    SIDEREAL,
    Assignment,
    Translation,
    camera_settings,
    candidates,
    commanded,
    commanded_target,
    descriptor,
    duration,
    exposure_request,
    fits,
    frame_targets,
    pack,
    requests,
    resolve_wave,
    schedule,
    target_id,
    translate,
)

__all__ = [
    # config: the sensors: section
    "Implementation", "SensorConfig", "SensorDevices", "SensorPolicies",
    # derive: the section as a workflow plan
    "capability_index", "derive_plan", "derive_structure", "derive_tables",
    "timeouts",
    # dispatch: workflow ops as device commands
    "DeviceContexts", "Dispatcher", "FrameKeywords", "Handler", "Listener",
    "OpOutcome", "compile_supported", "unresolved", "unsupported",
    # translate: a standard collect task as a request
    "SCIENCE", "SIDEREAL", "Assignment", "Translation", "camera_settings",
    "candidates", "commanded", "commanded_target", "descriptor", "duration",
    "exposure_request", "fits", "frame_targets", "pack", "requests",
    "resolve_wave", "schedule", "target_id", "translate",
    # client: the sensor, with no controller
    "Sensor", "connect_sensor", "pointed_at",
    # impl: the sensor as a service
    "StandardSensor", "sensor_control_service",
    # legacy: retired one release after the workflow implementation ships
    "LegacyDevices", "LegacySensor",
    # compat: retired when the UI stops reading them
    "Capabilities", "add_compat_context",
]
