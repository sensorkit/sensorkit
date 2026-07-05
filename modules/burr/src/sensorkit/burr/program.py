# SPDX-License-Identifier: Apache-2.0
import asyncio
import collections
import contextlib
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import burr.core.config
from burr.core.config import (
    AppConfig,
    PlottingConfig,
    RuntimeConfig,
    ScheduleConfig,
)
from burr.models.observation import ObservationRequest
from burr.models.run import BurrRun
from burr.models.site import LightingCondition, SiteMetadata
from burr.run.utils import get_or_create_run
from burr.scheduler.factory import build_scheduler
from burr.scheduler.scheduler import Scheduler
from burr.scheduler.slot import TaskSlot
from burr.task_source.calsats.manager import CalibrationSatelliteManager
from burr.task_source.coverage.manager import SkyCoverageManager
from burr.task_source.flats.manager import FlatFieldManager
from burr.task_source.lunar.manager import LunarBackgroundManager
from burr.task_source.photometry.manager import PhotometricStandardsManager
from burr.task_source.protocol import TaskSource
from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.astro.common import SitePosition
from sensorkit.burr.tasks import build_sk_tasks


class BurrConfig(BaseModel):
    controller: sk.ControllerRef
    plotting: PlottingConfig = Field(default_factory=PlottingConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    minimum_altitude_degrees: float = 0.0

    async def to_app_config(self, kit: sk.SensorKit):
        self.controller.resolve(kit)
        pos = await self.controller.require().kv_get_model(SitePosition)
        return AppConfig(
            stage="stage",
            plotting=self.plotting,
            runtime=self.runtime,
            site=SiteMetadata(
                name=self.controller.name or "unknown",
                latitude=pos.latitude_degrees,
                longitude=pos.longitude_degrees,
                altitude_km=pos.altitude_km,
                minimum_altitude_degrees=self.minimum_altitude_degrees,
            ),
            schedule=self.schedule,
        )


sk.declare_config_section(
    "burr",
    list[BurrConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


@sk.declare_program
class BurrProgram:
    """Scheduling program backed by the burr scheduler.

    Builds burr task sources (sky coverage, calibration satellites, twilight flats,
    lunar background, photometric standards), advertises observation intervals as
    offers, and translates scheduled burr slots into sensorkit collect tasks.
    """

    run: BurrRun | None = None
    sources: dict[str, TaskSource] | None = None
    scheduler: Scheduler | None = None
    _offer_task: asyncio.Task | None = None

    @sk.on_attach
    async def startup(self):
        self._queue: collections.deque[PendingTask] = collections.deque()

        # Get configuration.
        program = sk.program()
        self.config = await program.kv_get_model(BurrConfig)
        self.app_config = await self.config.to_app_config(program.sensorkit())
        logger.debug(f"burr service starting for site {self.app_config.site.name}")

        # FIXME: Burr needs a way of setting config
        burr.core.config._config_instance = self.app_config

    @sk.on_enable
    async def enable(self):
        program = sk.program()

        # Set the assigned controller name in burr's internal config.
        # FIXME: SK API needs a public way to get at the assigned controller.
        self.app_config.site.name = program._state.enable_state.controller

        def init_burr_sync():
            config = self.app_config
            self.run = get_or_create_run(site=config.site, verbose=False, new_config=config)
            self.sources = dict(self._build_task_sources())
            self.scheduler = build_scheduler(config, self.run, self.sources)

        # Initialize burr.
        await asyncio.to_thread(init_burr_sync)

        # Publish offers.
        async def offer_loop(cadence: float):
            while True:
                for start, end in self._offer_intervals():
                    program.add_offer(start, end)

                logger.debug("burr publishing offers")
                await program.publish_offers()
                await asyncio.sleep(cadence)

        await self._cleanup_offer_task()
        self._offer_task = asyncio.create_task(offer_loop(cadence=1800))
        self.scheduler.log_status(
            self._current_condition(),
            offer_intervals=[(int.begin, int.end) for int in program.get_offers()],
        )

    @sk.on_disable
    async def disable(self):
        self.run = None
        self.sources = None
        self.scheduler = None
        await self._cleanup_offer_task()

    @sk.task_factory
    async def next_task(self):
        if not self._queue:
            await asyncio.to_thread(self._enqueue_next_request)

            if not self._queue:
                return

        pending = self._queue.popleft()
        logger.debug(
            f"yielding SK task for {pending.slot.name} "
            f"(request {pending.request.task_id}, "
            f"{'final' if pending.is_last_of_request else 'intermediate'} of fan-out)"
        )

        error: Exception | None = None
        start_time = time.monotonic()

        try:
            await (yield pending.task)
        except Exception as e:
            error = e

            # Drain the rest of the tasks for this request.
            while self._queue and not pending.is_last_of_request:
                pending = self._queue.popleft()

        if pending.is_last_of_request:
            duration = time.monotonic() - start_time

            # The request's last SK task has completed — record completion.
            # Treat any non-None result as success; the result type may or
            # may not expose success flags depending on sensorkit version.
            def finalize():
                if error:
                    pending.slot.record_failure()
                    pending.slot.source.on_request_failed(pending.request, str(error) or "unknown")
                else:
                    pending.slot.record_success(duration, self._current_condition())
                    pending.slot.source.on_request_completed(pending.request, duration)

            await asyncio.to_thread(finalize)

    def _build_task_sources(self):
        config = self.config.schedule

        if config.twilight_flats.collect:
            yield "twilight_flats", FlatFieldManager(self.run)

        if config.lunar_background.collect:
            yield "lunar_background", LunarBackgroundManager(self.run)

        if config.photometric_standards.collect:
            yield "photometric_standards", PhotometricStandardsManager(self.run)

        if config.coverage.collect:
            yield "coverage", SkyCoverageManager(self.run)

        if config.calsats.collect:
            yield "calsats", CalibrationSatelliteManager(self.run)

    def _required_conditions(self) -> set[LightingCondition]:
        """Union of suitable_conditions across all registered task sources."""
        conditions: set[LightingCondition] = set()

        for slot in self.scheduler.slots:
            conditions.update(slot.source.suitable_conditions)

        return conditions

    def _offer_intervals(self) -> list[tuple[datetime, datetime]]:
        """Merged offer windows for the currently registered sources."""
        return self.run.lighting_schedule.merged_windows_for(self._required_conditions())

    def _current_condition(self):
        now = datetime.now(UTC)
        try:
            return self.run.lighting_schedule.get_lighting_condition(now)
        except Exception as e:
            logger.warning(f"could not determine lighting condition: {e}")
            return LightingCondition.NIGHT

    def _enqueue_next_request(self):
        condition = self._current_condition()
        candidates = self.scheduler.ranked_suitable(condition)
        logger.info(f"SK requested next task (condition={condition.value})")

        if not candidates:
            logger.info(
                f"no suitable task source for condition={condition.value} — "
                "nothing to generate this cycle"
            )
            return

        logger.info(
            f"considering {len(candidates)} suitable slot(s) in priority order: "
            f"{[s.name for s in candidates]}"
        )

        chosen_slot = None
        chosen_request = None

        for slot in candidates:
            task_id = str(uuid.uuid4())

            try:
                request = slot.source.generate_request(task_id=task_id)
            except Exception as e:
                logger.exception(
                    f"{slot.name}.generate_request raised: {e} — "
                    "recording failure, trying next slot"
                )
                slot.record_failure()
                continue

            if request is None:
                logger.info(
                    f"{slot.name}.generate_request returned None "
                    "(nothing to produce right now) — trying next slot"
                )
                slot.record_failure()
                continue

            chosen_slot = slot
            chosen_request = request
            break

        if chosen_slot is None:
            logger.info("every suitable slot returned None this cycle — nothing to generate")
            return

        logger.info(
            f"selected {chosen_slot.name} → generating request {chosen_request.task_id} "
            f"target={type(chosen_request.target).__name__} "
            f"exposures={len(chosen_request.exposure_seconds)} "
            f"modes={[s.tracking_mode.value for s in chosen_request.exposure_map]}"
        )

        try:
            sk_tasks = list(
                build_sk_tasks(
                    self.run,
                    self.app_config,
                    chosen_request,
                )
            )
        except Exception as e:
            logger.exception(f"could not build SK tasks for {chosen_slot.name} request: {e}")
            chosen_slot.record_failure()
            return

        logger.info(
            f"{chosen_slot.name} request {chosen_request.task_id}: enqueued "
            f"{len(sk_tasks)} SK task(s) from {len(chosen_request.exposure_seconds)} "
            f"exposure(s) × {len(chosen_request.exposure_map)} mode(s)"
        )

        for i, t in enumerate(sk_tasks):
            self._queue.append(
                PendingTask(
                    task=t,
                    slot=chosen_slot,
                    request=chosen_request,
                    is_last_of_request=(i == len(sk_tasks) - 1),
                )
            )

    async def _cleanup_offer_task(self):
        if self._offer_task:
            self._offer_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await self._offer_task

            self._offer_task = None


@dataclass
class PendingTask:
    """A single SK task waiting to be handed to the executor, plus its
    request-level bookkeeping so `_next_task` knows when to record
    completion."""

    task: sk.TaskSubmission
    slot: TaskSlot
    request: ObservationRequest
    is_last_of_request: bool


@sk.service_entrypoint(version=sk.VERSION)
async def main(service: sk.Service):
    service.include(BurrProgram())
    await service.run()
