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

import asyncio
import contextlib
from collections.abc import Mapping

import sensorkit.api as sk
from sensorkit.core.client import SensorKit
from sensorkit.core.device import DeviceClient
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.sensor.config import SensorConfig
from sensorkit.std.sensor.derive import capability_index, derive_plan, timeouts
from sensorkit.std.sensor.dispatch import DeviceContexts, Dispatcher, compile_supported
from sensorkit.workflow import (
    CapabilityIndex,
    DeviceRef,
    LifecycleError,
    LifecycleRunner,
    RunReport,
    SensorPlan,
)


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
        self.lifecycle = LifecycleRunner(
            Dispatcher(plan, devices, capabilities, timeouts(config.policies)))

    async def init(self) -> RunReport:
        """Bring the sensor up, undoing the attempt if it fails part way."""
        return await self.bring_up("init")

    async def standby(self) -> RunReport:
        """Bring the sensor to standby."""
        return await self.bring_up("standby")

    async def shutdown(self) -> RunReport:
        """Take the sensor down."""
        return await self.run("shutdown")

    async def recover(self) -> RunReport:
        """Reconnect to every device and halt whatever is moving."""
        return await self.run("recover")

    async def stop(self) -> RunReport:
        """Halt everything that moves under its own power."""
        return await self.run("stop")

    async def run(self, table: str) -> RunReport:
        """Compile one of the plan's tables against its devices, and run it.

        Args:
            table: Which of the plan's tables to run.

        Returns:
            What every step did, degraded outcomes included.

        Raises:
            LifecycleError: A required step failed or never ran.
        """
        graph = compile_supported(self.plan.devices, self.plan.tables[table],
                                  self.capabilities)

        return await self.lifecycle.execute(graph, name=table)

    async def bring_up(self, table: str) -> RunReport:
        """Run a table that leaves the sensor operating, halting it if it does not.

        Nothing is reversed automatically, and a table that undoes another is just
        a table run deliberately — so the composition is here, where a reader sees
        both halves at once, rather than folded into a graph.

        A cancelled bring-up halts too. What the dispatcher stops is the op that
        was in flight, and an operator aborting a bring-up wants everything the
        table already started brought to rest.
        """
        try:
            return await self.run(table)
        except (LifecycleError, asyncio.CancelledError):
            # Shielded because the halt matters most exactly when the run is being
            # torn down: unshielded, it would be cancelled before it left.
            with contextlib.suppress(BaseException):
                await asyncio.shield(self.run("stop"))

            raise

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
