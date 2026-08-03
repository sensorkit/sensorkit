# SPDX-License-Identifier: Apache-2.0
"""A sensor as a client object: the workflow implementation, with no controller.

`Sensor` owns everything the workflow library needs to bring a set of devices up,
take frames through them, and put them back down — the derived plan, the device
clients it dispatches through, and what each of those devices reports it can do.
It holds no controller and no service, so a script can drive one directly:

    sensor = await connect_sensor(config)
    await sensor.init()

`StandardSensor` is the same object wearing a Controller's task handlers.

The one thing a client cannot supply for itself is a frame's metadata, since
keyword subscriptions belong to a controller. So `collect` takes the per-device
contexts as a callable and samples it per frame; a caller with nothing to say
passes nothing and gets frames with only the task's own context on them.
"""

from __future__ import annotations

from collections.abc import Mapping

import sensorkit.api as sk
from sensorkit.core.client import SensorKit
from sensorkit.core.device import DeviceClient
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.sensor.config import SensorConfig
from sensorkit.std.sensor.derive import capability_index, derive_plan
from sensorkit.std.sensor.dispatch import DeviceContexts
from sensorkit.workflow import CapabilityIndex, DeviceRef, RunReport, SensorPlan


class Sensor:
    """A logical sensor, driven through a compiled workflow plan.

    Construct with `connect_sensor`, which resolves what each device reports it
    can do. That resolution is what makes a plan dispatchable, so an instance
    that exists is one whose devices are all known.
    """

    def __init__(
        self,
        config: SensorConfig,
        plan: SensorPlan,
        devices: Mapping[DeviceRef, DeviceClient],
        capabilities: CapabilityIndex,
    ):
        self.config = config
        self.plan = plan
        self.devices = devices
        self.capabilities = capabilities

    async def init(self) -> RunReport:
        """Bring the sensor up, undoing the attempt if it fails part way."""
        raise NotImplementedError

    async def standby(self) -> RunReport:
        """Bring the sensor to standby."""
        raise NotImplementedError

    async def shutdown(self) -> RunReport:
        """Take the sensor down."""
        raise NotImplementedError

    async def recover(self) -> RunReport:
        """Reconnect to every device and halt whatever is moving."""
        raise NotImplementedError

    async def stop(self) -> RunReport:
        """Halt everything that moves under its own power."""
        raise NotImplementedError

    async def collect(
        self,
        task: StandardCollectTask,
        *,
        contexts: DeviceContexts | None = None,
        base: sk.Context | None = None,
    ) -> RunReport:
        """Resolve a collect task against this sensor and run it.

        Args:
            task: The collect to perform.
            contexts: Per-device context, sampled once per frame.
            base: Context common to every frame, such as the task's own keywords.
        """
        raise NotImplementedError


async def connect_sensor(
    config: SensorConfig,
    sensorkit: SensorKit | None = None,
) -> Sensor:
    """Build a `Sensor` for a configuration, resolving its devices.

    Every configured device must have published its details, since what a device
    reports it supports is what a plan's ops resolve against. A device that has
    never registered therefore fails here rather than at the first task that
    needs it.

    Args:
        config: The sensors: entry describing this sensor.
        sensorkit: An existing client, or None to connect one.

    Returns:
        A sensor ready to run its plan.

    Raises:
        KVError: A configured device has published no details.
    """
    sensorkit = sensorkit or await sk.connect()
    devices = {ref: sensorkit.device(ref) for ref in config.devices.refs()}
    details = {ref: await client.get_details() for ref, client in devices.items()}

    return Sensor(
        config=config,
        plan=derive_plan(config),
        devices=devices,
        capabilities=capability_index(details),
    )
