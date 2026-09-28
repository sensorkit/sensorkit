# SPDX-License-Identifier: Apache-2.0
"""Serve a sensor definition as a controller.

Tasks run as workflows planned and executed by a `Sensor`. Site policies
supply the lifecycle tables and deadline rules.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Literal

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.common.keyword import get_keyword_type
from sensorkit.sensor.binding import BoundSensor
from sensorkit.sensor.client import Sensor
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.dispatch import DeviceContexts, Dispatcher
from sensorkit.sensor.execution import WorkflowError, WorkflowReport
from sensorkit.sensor.lifecycle import LifecycleWorkflow
from sensorkit.sensor.policies import (
    SensorPolicies,
    compose_deadlines,
    compose_tables,
)
from sensorkit.sensor.selection import Supports
from sensorkit.sensor.standard_task import translate
from sensorkit.sensor.topology import DeviceKey, Placement, Structure
from sensorkit.sensor.workflow import ExecutableWorkflow
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.enclosure import CloseEnclosure
from sensorkit.std.instrument import ChangeRotatorPosition
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import ChangeFocusPosition, CloseMirrorCover, SetFilter


class SensorConfig(BaseModel):
    """A sensor's structure and the site policies it runs under."""

    model: Structure
    policies: SensorPolicies


@sk.declare_controller
class SensorController:
    """A controller running lifecycle and collect tasks as sensor workflows."""

    stopping: bool = False

    @sk.on_attach
    async def on_attach(self):
        controller = sk.controller()
        config = await controller.kv_get_model(SensorConfig)
        self.site = await controller.kv_get_model(SitePosition)

        definition = compose_deadlines(SensorDefinition(sensor=config.model),
                                       config.policies.deadlines())
        self.sensor = await Sensor.connect(definition, controller.sensorkit())

        # Tables compose after binding so generated entries are pruned to the
        # equipment actually present.
        bound = self.sensor.sensor
        self.tables: Mapping[str, LifecycleWorkflow] = {
            table.name: table
            for table in compose_tables(definition, config.policies.tables(),
                                        bound)}

        # Subscriptions start after attach; each device supplies whichever
        # header keywords it reports publishing.
        placements = bound.topology.placements()
        self.devices = tuple(placement.device for placement in placements)

        for placement in placements:
            controller.use_device(placement.device, subscribe=[
                keyword for name in sorted(bound.keywords(placement.device))
                if (keyword := get_keyword_type(name)) is not None])

        # Compat only, for UI code not yet reading ControllerInfo and
        # SensorConfig.
        await controller.kv_put_model(Capabilities(
            tasks=controller.entity_info().details.supported_tasks,
            devices=SensorDevices.infer(bound)))

    @sk.on_detach
    async def on_detach(self):
        self.stopping = True

    def device_contexts(self) -> Mapping[DeviceKey, sk.Context]:
        """Return each device's latest subscribed keywords, unmerged.

        Dispatch merges only the contexts along an instrument's chain.
        """
        controller = sk.controller()

        return {key: controller.get_device(key).subscription.snapshot()
                for key in self.devices}

    async def execute(self, workflow: ExecutableWorkflow, *,
                      contexts: DeviceContexts | None = None,
                      base: sk.Context | None = None) -> WorkflowReport:
        """Execute a workflow, treating cancellation as a domain abort unless
        the service is stopping.

        Raises:
            WorkflowError: Required work or cleanup failed.
            asyncio.CancelledError: The workflow was aborted, once its cleanup
                ends, or the service is stopping.
        """
        report = await self.sensor.execute(
            workflow, contexts=contexts, base=base,
            in_domain=lambda _: not self.stopping)

        # Core records a task as aborted only if the handler is cancelled.
        if report.outcome == "aborted":
            raise asyncio.CancelledError(report.reason)

        return report

    async def run_table(self, name: str):
        """Plan and execute a lifecycle table, aborting every device if it
        fails.

        Raises:
            WorkflowError: Required work or cleanup failed.
            asyncio.CancelledError: The table was aborted.
        """
        try:
            await self.execute(self.sensor.plan_lifecycle(self.tables[name]))
        except WorkflowError:
            # Failed lifecycle commands may leave hardware running after responding.
            dispatcher = Dispatcher(self.sensor.sensor, self.sensor.executor.clients)
            await asyncio.gather(*(dispatcher.abort(device, lambda _: None)
                                   for device in self.devices))
            raise

    @sk.task_handler
    async def init_task(self, task: sk.InitTask):
        """Bring the sensor up to operate."""
        await self.run_table("init")
        logger.info(f"Sensor '{sk.controller().entity}' is ready to operate")

    @sk.task_handler
    async def standby_task(self, task: sk.StandbyTask):
        """Put the sensor in standby."""
        await self.run_table("standby")
        logger.info(f"Sensor '{sk.controller().entity}' is standing by")

    @sk.task_handler
    async def shutdown_task(self, task: sk.ShutdownTask):
        """Shut the sensor down."""
        await self.run_table("shutdown")

    @sk.task_handler
    async def recover_task(self, task: sk.RecoverTask):
        """Reconnect every device, then stop any motion."""
        await self.run_table("recover")

    @sk.task_handler
    async def collect_task(self, task: StandardCollectTask):
        """Point at the task's target and capture its exposures."""
        workflow = self.sensor.plan_collect(translate(task))
        base = sk.Context(task.execution.context, self.site)

        await self.execute(workflow, contexts=self.device_contexts, base=base)


class SensorDevices(BaseModel):
    """Compat only. Devices by role, with per-camera roles paired by position.

    An empty key marks a camera without that device.
    """

    mount: str | None = None
    camera: list[str] = Field(default_factory=list)
    focuser: list[str] = Field(default_factory=list)
    rotator: str | None = None
    filter_wheel: list[str] = Field(default_factory=list)
    mirror_cover: str | None = None
    dome: str | None = None

    @classmethod
    def infer(cls, sensor: BoundSensor) -> SensorDevices:
        """Assign roles by the commands each device reports.

        Each instrument is a camera. Its focuser and filter wheel are the first
        supporting devices along its chain.
        """
        topology = sensor.topology
        placements = topology.placements()
        cameras = topology.instruments()

        def supporting(command: type[sk.DeviceCommand],
                       choices: tuple[Placement, ...]) -> DeviceKey | None:
            return next(iter(Supports(supports=command).refs(choices, sensor)), None)

        def paired(command: type[sk.DeviceCommand]) -> list[str]:
            keys = [supporting(command, topology.chain(camera)) or ""
                    for camera in cameras]

            return keys if any(keys) else []

        return cls(
            mount=supporting(FollowTarget, placements),
            camera=[camera.device for camera in cameras],
            focuser=paired(ChangeFocusPosition),
            rotator=supporting(ChangeRotatorPosition, placements),
            filter_wheel=paired(SetFilter),
            mirror_cover=supporting(CloseMirrorCover, placements),
            dome=supporting(CloseEnclosure, placements))


class Capabilities(BaseModel):
    """Compat only. A sensor controller's tasks and devices by role."""

    type: Literal["controller"] = "controller"
    tasks: list[str]
    devices: SensorDevices


sk.declare_config_section(
    "sensors",
    list[SensorConfig],
    id_source="by_subkey",
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def sensor_service(service: sk.Service):
    service.include(SensorController)
    await service.run()
