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
from sensorkit.astro.target import Target
from sensorkit.core.client import SensorKit
from sensorkit.core.device import DeviceClient
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.mount import FollowTarget
from sensorkit.std.sensor.config import SensorConfig
from sensorkit.std.sensor.derive import capability_index, derive_plan, timeouts
from sensorkit.std.sensor.dispatch import DeviceContexts, Dispatcher, compile_supported
from sensorkit.std.sensor.translate import translate
from sensorkit.std.traits import Stop
from sensorkit.workflow import (
    CapabilityIndex,
    Collect,
    CollectRunner,
    DeviceRef,
    LifecycleError,
    LifecycleRunner,
    RequestResolver,
    RunReport,
    SensorPlan,
)


def pointed_at(collect: Collect) -> DeviceRef | None:
    """The device a resolved collect commanded its target on.

    Read off the resolution rather than routed a second time, so the device that
    is halted is the one that was pointed, however the request addressed it.
    """
    return next((ref for step in collect.steps
                 for ref, value in step.settings.items()
                 if isinstance(value, FollowTarget)), None)


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
        self.resolver = RequestResolver(plan.topology, plan.devices, capabilities,
                                        plan.aliases)
        self.lifecycle = LifecycleRunner(self.dispatcher())

    def dispatcher(self, **header) -> Dispatcher:
        """The op hook, plus whatever a collect gives its frames' headers.

        A lifecycle run supplies none of that and a collect supplies all of it,
        which is why a dispatcher is built per run rather than held: it carries no
        run state of its own, so the only thing to vary is what it is told.
        """
        return Dispatcher(self.plan, self.devices, self.capabilities,
                          timeouts(self.config.policies), **header)

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

        Returns:
            What every setting and every frame did.

        Raises:
            ValueError: The task does not resolve against this sensor — no
                instrument satisfies it, or a command it implies reaches no device.
            LifecycleError: A required setting or frame failed.
        """
        translation = translate(task, self.resolver)
        collect = self.resolver.to_collect(translation.steps, name="collect")
        runner = CollectRunner(self.dispatcher(
            contexts=contexts, base=base, frame_keywords=translation.keywords))

        await self.acquire(collect, translation.acquire)

        try:
            report = await runner.run(self.plan.topology, self.plan.devices,
                                      collect)
        finally:
            await self.halt_pointing(collect)

        if report.failures and not report.aborted:
            # The causes, not the cascade: a failed setting skips exactly the
            # frames it invalidated, and listing those buries the one line that
            # says why.
            causes = "; ".join(f"{node.label.strip()} ({error})"
                               for node, error in report.causes[:3])
            raise LifecycleError(
                f"collect: {causes or 'required frames did not run'}", report)

        return report

    async def acquire(self, collect: Collect, target: Target | None) -> None:
        """Reach the target a collect's first step cannot reach for itself.

        Only a collect whose opening frames are sidereal has one: holding sidereal
        holds whatever the mount already has, so a collect that begins that way
        would otherwise hold wherever the last task left it. Unshielded and
        unguarded — a collect that never acquired its target has nothing worth
        taking frames of.
        """
        ref = pointed_at(collect)

        if target is None or ref is None or ref not in self.devices:
            return

        await self.devices[ref].command(FollowTarget(target=target))

    async def halt_pointing(self, collect: Collect) -> None:
        """Stop whatever a collect was pointing, now that its frames are over.

        Tracking outlives the last frame, and the collect layer has no expression
        for teardown — a step is a configuration epoch, not a way to say "and then
        stop" — so the halt is composed here, where a reader sees it beside the run
        that needed it.

        Best effort, and shielded, because it matters most exactly when a collect
        is being torn down: a halt that fails must not displace the failure or the
        cancellation already on its way out, and the report is what says what
        happened.
        """
        ref = pointed_at(collect)

        if ref is None or ref not in self.devices:
            return

        with contextlib.suppress(BaseException):
            await asyncio.shield(self.devices[ref].command(Stop()))


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
