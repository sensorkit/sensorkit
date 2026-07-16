# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field

from sensorkit.astro.common import SitePosition
from sensorkit.astro.observer import EarthObserver
from sensorkit.auto.constraint import Constraint, ConstraintManager
from sensorkit.auto.lifecycle import ControllerLifecycle
from sensorkit.auto.mode import Mode, ModeList
from sensorkit.auto.scheduler import ProgramConfig, Scheduler, debug_print_schedule
from sensorkit.backend.base import KeyNotFound
from sensorkit.common.aio import AsyncObserver
from sensorkit.common.graph import StateElection
from sensorkit.core.client import SensorKit
from sensorkit.core.controller import InternalControllerState
from sensorkit.core.program import ControllerOffers, ProgramClient, ProgramDiscovery, ProgramState
from sensorkit.core.task import TaskContextMap, TaskContextOverlay


class ControllerConfig(BaseModel):
    """Configuration for a single Controller managed by the VirtualOperator."""
    depends_on: list[str] = Field(default_factory=list)
    estimated_startup_time: float = 0.0
    estimated_shutdown_time: float = 0.0
    modes: ModeList = Field(default_factory=list)
    constraints: list[Constraint] = Field(default_factory=list)
    tasking: list[ProgramConfig] = Field(default_factory=list)
    contexts: TaskContextOverlay = Field(default_factory=TaskContextOverlay)

    _name: str | None = None
    _mode_index: dict[str, Mode]
    _program_index: dict[str, ProgramConfig]

    @property
    def name(self):
        """The name of this controller, set by the parent ControllerConfigMap validator."""
        if self._name is None:
            raise RuntimeError("name not set")

        return self._name

    @name.setter
    def name(self, value: str):
        """Set the name of this controller."""
        self._name = value

    def mode(self, name: str):
        """Return the Mode configuration with the given name."""
        return self._mode_index[name]

    def program_config(self, program: str):
        """Return the ProgramConfig for the given program entity name."""
        return self._program_index[program]

    def model_post_init(self, __context: Any):
        self._mode_index = {mode.name: mode for mode in self.modes}
        self._program_index = {config.program: config for config in self.tasking}

    def propagate_config(self, contexts: TaskContextOverlay):
        """Merge agent-level task contexts into this controller and its program configs."""
        contexts.propagate(self.contexts)

        for config in self.tasking:
            self.contexts.propagate(config.contexts)


class ControllerDriver:
    """Operate a single Controller."""

    def __init__(
        self,
        config: ControllerConfig,
        program_discovery: ProgramDiscovery,
        program_manager: ProgramStateManager,
    ):
        self.config = config
        self.program_manager = program_manager

        # Build task context maps. These are user-configured context keywords passed through
        # to program tasking and direct task executions.
        self._context_maps: dict[str | None, TaskContextMap] = {
            None: config.contexts.build(),
            **{cfg.program: cfg.contexts.build() for cfg in config.tasking},
        }

        # Create the Controller task offers monitor.
        self.controller_offers = ControllerOffers(config.name, program_discovery)

        # Create a scheduler.
        self.scheduler = Scheduler(
            modes=config.modes,
            program_configs=config.tasking,
        )

        # Create constraint manager.
        self.constraints = ConstraintManager(config.constraints)

        # Create the lifecycle object.
        self.lifecycle = ControllerLifecycle()

    async def start(
        self,
        kit: SensorKit,
        *,
        task_group: Any = asyncio,
    ):
        """Start the lifecycle, constraints, offers monitor, and background tasks for this driver."""
        self.kit = kit
        controller = kit.controller(self.config.name)
        logger.info(f"Driver for {self.config.name} starting")

        # Start the lifecycle loop. This won't result in commanding until the lifecycle is enabled
        # and a demand state is set.
        self.lifecycle.start(controller, task_group=task_group)

        # Start checking the configured constraints.
        await self.constraints.start(task_group=task_group, ready_timeout=10.0, kit=kit)

        # Start monitoring associated Programs for offers.
        await self.controller_offers.start(task_group=task_group)

        # Query controller site position.
        while True:
            try:
                site = await controller.kv_get_model(SitePosition)
                break
            except KeyNotFound:
                logger.debug(f"waiting for {self.config.name} SitePosition")
                await asyncio.sleep(1)

        self.observer = await EarthObserver.get(
            lat_deg=site.latitude_degrees, lon_deg=site.longitude_degrees
        )

        logger.debug(f"driver for {self.config.name} ready")

    async def stop(self):
        """Stop the lifecycle and all background tasks for this driver."""
        await self.lifecycle.stop()

    def evaluate_state(self, election: StateElection):
        """Update the schedule and cast election votes for schedule and constraints."""
        # Build mode evaluation context.
        mode_context = {
            "time_range_parsers": {
                "sunrise": self.observer.get_sunrise_time,
                "sunset": self.observer.get_sunset_time,
            },
        }

        # Update the schedule.
        # FIXME: We do the full call twice here to ensure `tasking_available` and `after_activity`
        #        criteria pick up changes in a single iteration. This double-evaluation should be
        #        done more efficiently within the Scheduler itself.
        for _ in range(2):
            self.scheduler.update(
                offers_dict=self.controller_offers.get_offers(),
                enabled_programs=self.program_manager.enabled_programs(),
                mode_context=mode_context,
            )

        if os.getenv("AGENT_DEBUG_SCHEDULE", None):
            debug_print_schedule(self.scheduler, print_func=logger.debug)

        # Check the schedule. Lookahead by startup time.
        now = datetime.now(UTC)
        intent = self.scheduler.get_intent(now)
        if not intent and self.config.estimated_startup_time:
            lookahead = now + timedelta(seconds=self.config.estimated_startup_time)
            intent = self.scheduler.get_intent(lookahead)

        election.vote(
            "schedule",
            subject=self.config.name,
            vote=True if intent else None,
        )

        # Check constraints.
        election.vote(
            "constraint",
            subject=self.config.name,
            vote=False if self.constraints.is_constrained() else None,
        )

    def set_demand(self, operate: bool):
        """Apply the elected operate/shutdown demand to the lifecycle, returning True if changed."""
        state: InternalControllerState = InternalControllerState.SHUTDOWN
        program: ProgramClient | None = None

        if operate:
            # Determine the target state (operate or standby) and which program to use, if any. If
            # the controller's scheduler has something in mind, its intended state and program are
            # used.
            if intent := self.scheduler.get_intent(datetime.now(UTC)):
                state = (
                    InternalControllerState.OPERATE
                    if self.config.mode(intent.mode).state == "operate"
                    else InternalControllerState.STANDBY
                )

                if state == InternalControllerState.OPERATE and intent.programs:
                    # Always use the highest-priority program given by the scheduler. Note that
                    # things like offline programs, temporary failure cooldowns, and other program
                    # availability constraints are already accounted for in the schedule. Any
                    # exclusions or re-prioritization here would not be visible in the published
                    # schedule, thus should be avoided. The full listing of candidate programs
                    # exists only for user informational purposes.
                    program = self.kit.program(intent.programs[0])
            else:
                # We are not scheduled to operate, so this must be a forced override scenario.
                # Presently overrides alone cannot specify STANDBY.
                state = InternalControllerState.OPERATE

        # Determine the appropriate contexts and set the demand state. The contexts are a dict of
        # dicts, mapping task types to the combined user configuration that applies for that task
        # for this controller and program (if any).

        if program:
            program_name = str(program.entity)
            program_config = self.config.program_config(program_name)
            contexts = self._context_maps[program_name]

            # Only interrupt if configured to do so and the controller is in the OPERATE state.
            # This means lifecycle tasks (e.g. InitTask) will not be interrupted.
            interrupt = (
                program_config.interrupt
                if self.lifecycle.belief_state == InternalControllerState.OPERATE
                else False
            )
        else:
            contexts = self._context_maps[None]
            interrupt = False

        changed = self.lifecycle.set_demand_state(
            state,
            contexts=contexts,
            program=program,
            interrupt=interrupt,
        )

        if changed:
            logger.debug(
                f"{self.config.name} -> {str(state)} program={program.entity if program else None}"
            )

        return changed


# FIXME: This is an AI implementation of this class; should be reviewed.
class ProgramStateManager:
    """Manages the desired enable state for programs and drives remote state."""

    def __init__(self, programs: Iterable[str]):
        self.all_programs = frozenset(programs)
        self._desired_enable: dict[
            str, str | None
        ] = {}  # program -> target_controller or None if disabled
        self._global_enabled = True
        self.on_change = AsyncObserver[str | None](None)
        self._controller: dict[str, list[str]] = {}

    def assign_controller(self, program: str, controller: str = None):
        """Register a controller as a candidate for a program."""
        if program not in self._controller:
            self._controller[program] = []
        if controller not in self._controller[program]:
            self._controller[program].append(controller)

    async def enable(self, program: str):
        """Mark *program* as enabled for its first registered candidate controller."""
        candidates = self._controller.get(program, [])
        if not candidates:
            return
        target = candidates[0]
        current = self._desired_enable.get(program)

        if current and current != target:
            raise RuntimeError(f"Program {program} already enabled for {current}")

        if current == target:
            return

        self._desired_enable[program] = target
        await self.on_change.notify(program)

    async def disable(self, program: str):
        """Mark *program* as disabled, removing it from the enabled set."""
        if self._desired_enable.get(program) is not None:
            del self._desired_enable[program]
            await self.on_change.notify(program)

    async def global_enable(self):
        """Enable the global scheduling gate, allowing all individually-enabled programs to run."""
        if not self._global_enabled:
            self._global_enabled = True
            await self.on_change.notify(None)

    async def global_disable(self):
        """Disable the global scheduling gate, preventing all programs from running."""
        if self._global_enabled:
            self._global_enabled = False
            await self.on_change.notify(None)

    def is_enabled(self, program: str) -> bool:
        """Return True if *program* is individually enabled and the global gate is open."""
        return self._global_enabled and self._desired_enable.get(program) is not None

    def get_target_controller(self, program: str) -> str | None:
        """Return the target controller entity name for *program*, or None if disabled."""
        if not self._global_enabled:
            return None
        return self._desired_enable.get(program)

    async def start(
        self,
        client: SensorKit,
        discovery: ProgramDiscovery,
        task_group: asyncio.TaskGroup,
    ):
        """Start the background task that drives remote program enable/disable state."""
        self.client = client
        self.discovery = discovery
        self._retries = set()
        self._program_locks: dict[str, asyncio.Lock] = {}

        # Start monitoring tasks.
        task_group.create_task(self._watch_discovery())
        task_group.create_task(self._watch_changes())

    async def _handle_program(self, program: str):
        if program not in self._program_locks:
            self._program_locks[program] = asyncio.Lock()

        async with self._program_locks[program]:
            program_client = self.client.program(program)

            try:
                state = await program_client.kv_get_model(ProgramState)
                target = self.get_target_controller(program)

                if target:
                    if not state.enable_state.enabled or state.enable_state.controller != target:
                        logger.info(f"Enabling program {program} for {target}")
                        await program_client.enable(target)
                else:
                    if state.enable_state.enabled:
                        logger.info(f"Disabling program {program}")
                        await program_client.disable()
            except Exception as e:
                logger.error(f"Failed to drive program {program} state: {e}")

                async def delayed_retry(delay: float = 10.0):
                    logger.debug(f"retrying in {delay} sec")
                    await asyncio.sleep(delay)
                    await self._handle_program(program)

                self._retries.add(asyncio.create_task(delayed_retry()))

            for task in list(self._retries):
                if task.done():
                    self._retries.discard(task)

    async def _watch_discovery(self):
        previous_discovered = set()

        async for programs in self.discovery.known_programs():
            changed = programs.symmetric_difference(previous_discovered)

            for p in changed:
                if p in self.all_programs:
                    await self._handle_program(p)

            previous_discovered = programs

    async def _watch_changes(self):
        async for program in self.on_change.consume(initial_value=True):
            if program is None:
                # Global change, re-evaluate all programs.
                for p in self.all_programs:
                    await self._handle_program(p)
            else:
                await self._handle_program(program)

    def enabled_programs(self):
        """Return the set of currently enabled program entity names."""
        return set(self._desired_enable.keys()) if self._global_enabled else set()


class VirtualOperator:
    """Orchestrate a set of Controllers."""

    def __init__(self, controllers: Iterable[ControllerConfig]):
        # Create the state election object. This election is the final say on whether a Controller
        # operates or not. It unites operating schedule determined from mode configuration and
        # program offerings, constraint evaluation, dependency relationships, and overrides.
        self.election = StateElection({config.name: config.depends_on for config in controllers})

        # Create a discovery object to find Programs and their target Controllers.
        self.discovery = ProgramDiscovery()

        # Create a manager to handle program enablement.
        self.programs = ProgramStateManager(
            tasking.program for config in controllers for tasking in config.tasking
        )

        for ctrl in controllers:
            for prog in ctrl.tasking:
                self.programs.assign_controller(prog.program, ctrl.name)

        # Create objects to operate each of the configured Controllers.
        self.drivers = {
            config.name: ControllerDriver(config, self.discovery, self.programs)
            for config in controllers
        }

    async def start(self, client: SensorKit, *, task_group: Any = asyncio):
        """Start discovery, the program state manager, all drivers, and the main operator loop."""
        # Start discovery.
        await self.discovery.start(client)

        # Start the program state manager.
        await self.programs.start(client, self.discovery, task_group=task_group)

        # Start all drivers.
        # FIXME: One driver's failure to start should not affect others.
        await asyncio.gather(
            *(driver.start(client, task_group=task_group) for driver in self.drivers.values())
        )

        # Start the main loop.
        logger.info("Starting virtual operator main loop")
        self._loop_task = task_group.create_task(self._virtual_operator())

    async def stop(self):
        """Cancel the main loop and stop all controller drivers."""
        self._loop_task.cancel()

        await asyncio.gather(
            *(driver.stop() for driver in self.drivers.values()),
            return_exceptions=True,
        )

        with contextlib.suppress(asyncio.CancelledError):
            await self._loop_task

    async def _virtual_operator(self):
        while True:
            # Tell each driver to update its state election votes.
            for driver in self.drivers.values():
                driver.evaluate_state(self.election)

            # Evaluate the election to determine the up/down demand.
            demand = self.election.evaluate()

            # If there is no explicit demand for a given controller, we default to down.
            for driver in self.drivers.values():
                demand.setdefault(driver.config.name, False)

            for name, state in demand.items():
                if name not in self.drivers:
                    # FIXME: This should be a config error -- can't depend on a Controller that
                    #        is not being orchestrated by the Agent.
                    continue

                # Tell the driver to make it so.
                self.drivers[name].set_demand(state)

            await asyncio.sleep(2)
