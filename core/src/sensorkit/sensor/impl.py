# SPDX-License-Identifier: Apache-2.0
"""Serve a sensor definition as a controller.

Tasks run as workflows planned and executed by a `Sensor`. Site policies
supply the lifecycle tables and deadline rules.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from typing import Literal

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.sensor.autofocus import (
    Focuser,
    final_focus_position,
    first_sweep_analysis,
    focus_source,
    translate_autofocus,
)
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
from sensorkit.sensor.topology import Placement, Structure
from sensorkit.sensor.workflow import ExecutableWorkflow
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.enclosure import CloseEnclosure
from sensorkit.std.focus import AutoFocus, ChangeFocusPosition, FocusSweepAnalysis
from sensorkit.std.instrument import ChangeRotatorPosition
from sensorkit.std.mount import FollowTarget
from sensorkit.std.optics import CloseMirrorCover, SetFilter


class SensorConfig(BaseModel):
    """A sensor's structure and the site policies it runs under."""

    model: Structure
    policies: SensorPolicies


@sk.declare_controller
class SensorController:
    """Run lifecycle, collect, and autofocus tasks as sensor workflows."""

    stopping: bool = False
    instruments: Mapping[str, Placement] = {}
    """Collection targets by instrument name."""
    running_autofocus: asyncio.Task | None = None
    """Running autofocus task canceled by controller disable."""

    @sk.on_attach
    async def on_attach(self):
        controller = sk.controller()
        config = await controller.kv_get_model(SensorConfig)
        self.site = await controller.kv_get_model(SitePosition)
        self.policies = config.policies
        controller.on_disable(self.on_disable)

        definition = compose_deadlines(
            SensorDefinition(sensor=config.model), config.policies.deadlines()
        )
        self.sensor = await Sensor.connect(definition, controller.sensorkit())

        # Tables compose after binding so generated entries are pruned to the
        # equipment actually present.
        bound = self.sensor.sensor
        self.tables: Mapping[str, LifecycleWorkflow] = {
            table.name: table
            for table in compose_tables(definition, config.policies.tables(), bound)
        }

        # Subscriptions start after attach. Only keywords named by a matched trait
        # are subscribed, so custom keywords are never subscribed implicitly.
        placements = bound.topology.placements()
        self.devices = tuple(placement.device for placement in placements)

        for placement in placements:
            controller.use_device(placement.device)
            controller.get_device(placement.device).subscription.add_trait_keywords(
                bound.details[placement.device]
            )

        self.instruments = {target.device: target for target in bound.topology.instruments()}

        for name, target in self.instruments.items():
            chain = bound.topology.chain(target)
            controller.register_instrument(name, devices=[p.device for p in chain])

        # Publish device roles for consumers of the legacy capability model.
        await controller.kv_put_model(
            Capabilities(
                tasks=controller.entity_info().details.supported_tasks,
                devices=SensorDevices.infer(bound),
            )
        )

    @sk.on_detach
    async def on_detach(self):
        self.stopping = True

    async def on_disable(self):
        if self.running_autofocus is not None:
            self.running_autofocus.cancel("Controller disabled")

    def device_contexts(self) -> Mapping[str, sk.Context]:
        """Return each device's latest subscribed keywords, unmerged.

        Dispatch merges only the contexts along an instrument's chain.
        """
        controller = sk.controller()

        return {key: controller.get_device(key).subscription.snapshot() for key in self.devices}

    def instrument_context(self, target: Placement) -> sk.Context:
        """Read registered context for the assigned capture target."""
        # Sensor instrument names are camera device IDs.
        return sk.controller().instrument_context(target.device)

    async def execute(
        self,
        workflow: ExecutableWorkflow,
        *,
        contexts: DeviceContexts | None = None,
        base: sk.Context | None = None,
    ) -> WorkflowReport:
        """Execute a workflow, treating cancellation as a domain abort unless
        the service is stopping.

        Raises:
            WorkflowError: Required work or cleanup failed.
            asyncio.CancelledError: The workflow was aborted, once its cleanup
                ends, or the service is stopping.
        """
        report = await self.sensor.execute(
            workflow,
            contexts=contexts,
            instrument_contexts=self.instrument_context,
            base=base,
            in_domain=lambda _: not self.stopping,
        )

        # Core records a task as aborted only if the handler is cancelled.
        if report.outcome == "aborted":
            raise asyncio.CancelledError(report.reason)

        return report

    async def run_table(self, name: str):
        """Plan and execute a lifecycle table, attempting Abort on every
        device after failure.

        Raises:
            WorkflowError: Required work or cleanup failed.
            asyncio.CancelledError: The table was aborted.
        """
        try:
            await self.execute(self.sensor.plan_lifecycle(self.tables[name]))
        except WorkflowError:
            # Failed lifecycle commands may leave hardware running after responding.
            dispatcher = Dispatcher(self.sensor.sensor, self.sensor.executor.clients)
            await asyncio.gather(
                *(dispatcher.abort(device, lambda _: None) for device in self.devices)
            )
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
        """Attempt to reconnect every device, then stop motion."""
        await self.run_table("recover")

    @sk.task_handler
    async def collect_task(self, execution: sk.TaskExecution[StandardCollectTask]):
        """Point at the task's target and capture its exposures.

        Named parameter sets keep their instrument throughout the collect.
        Planning uses one snapshot of subscribed device keywords. Frame headers
        sample them again at dispatch, along with the assigned instrument's
        registered context.
        """
        workflow = self.sensor.plan_collect(
            translate(execution.task, instruments=self.instruments),
            device_keywords=self.device_contexts(),
        )
        base = sk.Context(execution.context, self.site)

        await self.execute(workflow, contexts=self.device_contexts, base=base)

    @sk.task_handler
    async def autofocus_task(self, execution: sk.TaskExecution[AutoFocus]):
        """Execute one fixed sweep and apply a best-focus estimate.

        The task ID identifies the sweep. Analysis comes from the configured
        `FocusAnalysisSource` and is retained in the task's working context.
        Pointing and tracking are preserved. Requested camera and filter
        settings remain applied.

        If best focus lies beyond the sweep, leave the focuser at the nearer
        sampled boundary and fail the task. A later sweep can start there.
        Capture, analysis, and final-move failures attempt to restore the starting
        focus position. Cancellation, expiry, controller disable, and service
        shutdown cancel active work without a return move.
        """
        task = execution.task

        async with self._autofocus_lifetime(execution):
            focuser = Focuser.on(
                self.sensor.sensor, task.instrument, self.instruments, self.device_contexts()
            )
            positions = task.positions(focuser.start)
            workflow = self.sensor.plan_collect(
                translate_autofocus(
                    task,
                    positions,
                    sweep_id=execution.task_id,
                    focuser=focuser.device,
                    instruments=self.instruments,
                ),
                device_keywords=self.device_contexts(),
            )
            source = await focus_source(sk.controller())

            try:
                analysis = await self._run_focus_sweep(execution, workflow, source)
                execution.context.set(analysis)
                await self._move_focus(focuser.move(final_focus_position(analysis, positions)))
            except Exception as failure:
                await self._restore_focus(focuser, failure)
                raise

            if analysis.disposition != "equals":
                raise RuntimeError(
                    f"Best focus is {analysis.disposition.replace('_', ' ')} "
                    f"{analysis.position}, beyond the swept range; the focuser is left there"
                )

    @contextlib.asynccontextmanager
    async def _autofocus_lifetime(self, execution: sk.TaskExecution) -> AsyncIterator[None]:
        """Bound autofocus by execution expiry and controller disable."""
        expiry = execution.expiry_time
        seconds = None if expiry is None else (expiry - datetime.now(UTC)).total_seconds()
        self.running_autofocus = asyncio.current_task()

        try:
            async with asyncio.timeout(seconds) as lifetime:
                yield
        except TimeoutError:
            if lifetime.expired():
                raise TimeoutError(f"The task expired at {expiry}") from None

            raise
        finally:
            self.running_autofocus = None

    async def _run_focus_sweep(
        self,
        execution: sk.TaskExecution[AutoFocus],
        workflow: ExecutableWorkflow,
        source: sk.EntityClient,
    ) -> FocusSweepAnalysis:
        """Capture the sweep and wait for matching analysis.

        Subscribe before capturing the first frame. After the last capture,
        allow the default command timeout for matching analysis.
        Task expiry can shorten this wait.

        Raises:
            WorkflowError: Required work failed.
            TimeoutError: No matching analysis arrived in time.
        """
        async with first_sweep_analysis(
            source,
            controller=execution.controller_id,
            instrument=execution.task.instrument,
            sweep_id=execution.task_id,
        ) as analysis:
            base = sk.Context(execution.context, self.site)
            await self.execute(workflow, contexts=self.device_contexts, base=base)
            seconds = self.policies.default_timeout

            try:
                async with asyncio.timeout(seconds):
                    return await analysis
            except TimeoutError:
                raise TimeoutError(
                    f"'{source.entity}' published no analysis of sweep {execution.task_id} "
                    f"within {seconds} s"
                ) from None

    async def _move_focus(self, move: LifecycleWorkflow) -> None:
        """Move the focuser under its command deadline.

        Raises:
            WorkflowError: The move failed.
        """
        await self.execute(self.sensor.plan_lifecycle(move))

    async def _restore_focus(self, focuser: Focuser, failure: Exception) -> None:
        """Attempt to restore starting focus after a capture, analysis, or move failure.

        Raises:
            RuntimeError: Restoration failed. The message includes both failures,
                with the original failure retained as the cause.
        """
        # Expiry and absorbed aborts also leave the task cancelling.
        if asyncio.current_task().cancelling():
            return

        try:
            await self._move_focus(focuser.move(focuser.start))
        except Exception as error:
            raise RuntimeError(
                f"{failure}; returning focuser '{focuser.device}' to {focuser.start} "
                f"also failed: {error}"
            ) from failure


class SensorDevices(BaseModel):
    """Legacy device roles, with focusers and filter wheels paired to cameras.

    Each paired list follows camera order. An empty key marks a camera without
    that device. The list is empty when no camera has that device.
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

        def supporting(
            command: type[sk.DeviceCommand], choices: tuple[Placement, ...]
        ) -> str | None:
            return next(iter(Supports(supports=command).refs(choices, sensor)), None)

        def paired(command: type[sk.DeviceCommand]) -> list[str]:
            keys = [supporting(command, topology.chain(camera)) or "" for camera in cameras]

            return keys if any(keys) else []

        return cls(
            mount=supporting(FollowTarget, placements),
            camera=[camera.device for camera in cameras],
            focuser=paired(ChangeFocusPosition),
            rotator=supporting(ChangeRotatorPosition, placements),
            filter_wheel=paired(SetFilter),
            mirror_cover=supporting(CloseMirrorCover, placements),
            dome=supporting(CloseEnclosure, placements),
        )


class Capabilities(BaseModel):
    """Legacy model of a sensor controller's tasks and device roles."""

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
