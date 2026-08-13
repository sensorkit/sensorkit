# SPDX-License-Identifier: Apache-2.0
"""System tests for VirtualOperator driving election, demand, and lifecycle.

Tests the agent layer: VirtualOperator with election, demand evaluation,
program discovery, and lifecycle management.
"""

from __future__ import annotations

import asyncio
import contextlib
import json

import pytest

from sensorkit.astro.common import SitePosition
from sensorkit.auto.agent import (
    AgentControllerInfo,
    AgentOperatingState,
    AgentSchedulerState,
    AgentState,
)
from sensorkit.auto.constraint import GenericConstraint
from sensorkit.auto.operator import ControllerConfig, VirtualOperator
from sensorkit.auto.scheduler import ProgramConfig
from sensorkit.backend.base import Entity
from sensorkit.common.condition import EqualsCondition
from sensorkit.core.controller import InternalControllerState
from sensorkit.core.program import ProgramDiscovery, ProgramState
from sensorkit.core.task import InitTask, ShutdownTask

_test_position = SitePosition(latitude_degrees=42.0, longitude_degrees=123.0, altitude_km=3.0)


@pytest.mark.asyncio
async def test_operator_override_drives_operate(kit, service_context):
    """Override vote=True drives lifecycle to OPERATE and runs InitTask."""
    init_ran = asyncio.Event()

    # Create operator with one controller, no modes/constraints, one program reference.
    config = ControllerConfig(tasking=[ProgramConfig(program="prog1")])
    config.name = "ctrl1"
    operator = VirtualOperator([config])

    async with asyncio.timeout(5.0):
        # Register services
        controller = await service_context.register_controller("ctrl1")
        await controller.kv_put_model(_test_position)

        @controller.task_handler(InitTask)
        async def handle_init(task):
            init_ran.set()

        await service_context.register_program("prog1")
        await kit.program("prog1").enable("ctrl1")

        await kit.controller("ctrl1").enable()

        await operator.start(client=kit, task_group=asyncio)

    # Build agent state with global control enabled and override demand for ctrl1.
    state = AgentState(
        operating_state=AgentOperatingState(
            global_control_enabled=True,
            controllers={"ctrl1": AgentControllerInfo(
                control_enabled=True,
                elected_state=None,
                demand_override=True,
            )},
        ),
        scheduler_state=AgentSchedulerState(scheduling_enabled=False),
    )

    try:
        driver = operator.drivers["ctrl1"]

        # The operator loop runs every 2 seconds. Wait for init to run, then verify the lifecycle
        # reached OPERATE.
        async with asyncio.timeout(10.0):
            await state.apply_to_operator(operator, {"ctrl1": config})
            await init_ran.wait()

            while driver.lifecycle.belief_state != InternalControllerState.OPERATE:
                await asyncio.sleep(0.05)
    finally:
        async with asyncio.timeout(10.0):
            await operator.stop()


@pytest.mark.asyncio
async def test_operator_override_false_shuts_down(kit, service_context):
    """Override vote=False drives lifecycle to SHUTDOWN after OPERATE."""
    init_ran = asyncio.Event()
    shutdown_ran = asyncio.Event()

    config = ControllerConfig(tasking=[ProgramConfig(program="prog1")])
    config.name = "ctrl1"
    operator = VirtualOperator([config])

    async with asyncio.timeout(5.0):
        controller = await service_context.register_controller("ctrl1")
        await controller.kv_put_model(_test_position)

        await service_context.register_program("prog1")
        await kit.program("prog1").enable("ctrl1")

        @controller.task_handler(InitTask)
        async def handle_init(task):
            init_ran.set()

        @controller.task_handler(ShutdownTask)
        async def handle_shutdown(task):
            shutdown_ran.set()

        await kit.controller("ctrl1").enable()

        await operator.start(client=kit, task_group=asyncio)

    # Start with override=True to drive to OPERATE.
    state = AgentState(
        operating_state=AgentOperatingState(
            global_control_enabled=True,
            controllers={"ctrl1": AgentControllerInfo(
                control_enabled=True,
                elected_state=None,
                demand_override=True,
            )},
        ),
        scheduler_state=AgentSchedulerState(scheduling_enabled=False),
    )

    try:
        async with asyncio.timeout(10.0):
            await state.apply_to_operator(operator, {"ctrl1": config})
            await init_ran.wait()

        # The operator drives the controller down before any demand is applied, so a ShutdownTask has
        # already run by now. Clear the event so the wait below observes the override-driven shutdown.
        shutdown_ran.clear()

        # Now set override=False to drive to SHUTDOWN.
        state = AgentState(
            operating_state=AgentOperatingState(
                global_control_enabled=True,
                controllers={"ctrl1": AgentControllerInfo(
                    control_enabled=True,
                    elected_state=None,
                    demand_override=False,
                )},
            ),
            scheduler_state=AgentSchedulerState(scheduling_enabled=False),
        )

        driver = operator.drivers["ctrl1"]

        async with asyncio.timeout(15.0):
            await state.apply_to_operator(operator, {"ctrl1": config})
            await shutdown_ran.wait()

            while driver.lifecycle.belief_state != InternalControllerState.SHUTDOWN:
                await asyncio.sleep(0.05)
    finally:
        async with asyncio.timeout(10.0):
            await operator.stop()


@pytest.mark.asyncio
async def test_active_constraint_aborts_running_task(kit, backend, service_context):
    """A constraint going active aborts the running task instead of waiting for it to finish."""
    init_started = asyncio.Event()
    init_aborted = asyncio.Event()

    config = ControllerConfig(
        constraints=[
            GenericConstraint(
                entity="weather",
                keyword="Rain",
                field="state",
                condition=EqualsCondition(threshold="Wet"),
            )
        ],
    )
    config.name = "ctrl1"
    operator = VirtualOperator([config])

    async def report(value: str):
        """Publish a weather reading for the constraint check task to consume."""
        await backend.stream(Entity.at("weather")).publish(
            "Rain", json.dumps({"state": value}).encode()
        )

    async def report_until_cancelled(value: str):
        """Keep a reading flowing, covering the gap before the check task subscribes."""
        while True:
            await report(value)
            await asyncio.sleep(0.05)

    async with asyncio.timeout(5.0):
        controller = await service_context.register_controller("ctrl1")
        await controller.kv_put_model(_test_position)

        @controller.task_handler(InitTask)
        async def handle_init(task):
            # This task never completes on its own, so an abort is the only way out of it.
            init_started.set()

            try:
                await asyncio.sleep(float("inf"))
            except asyncio.CancelledError:
                init_aborted.set()
                raise

        @controller.task_handler(ShutdownTask)
        async def handle_shutdown(task):
            pass

        await kit.controller("ctrl1").enable()

    state = AgentState(
        operating_state=AgentOperatingState(
            global_control_enabled=True,
            controllers={"ctrl1": AgentControllerInfo(
                control_enabled=True,
                elected_state=None,
                demand_override=True,
            )},
        ),
        scheduler_state=AgentSchedulerState(scheduling_enabled=False),
    )

    # Constraints are fail-closed and the driver waits on them before it is ready, so clear
    # weather has to be flowing before the operator starts. Two readings are needed, since the
    # first one through only establishes a baseline for the condition.
    clear_weather = asyncio.create_task(report_until_cancelled("Dry"))

    try:
        async with asyncio.timeout(20.0):
            await operator.start(client=kit, task_group=asyncio)
            await state.apply_to_operator(operator, {"ctrl1": config})
            await init_started.wait()

        # Turn the constraint high with the init task still in flight.
        clear_weather.cancel()
        await report("Wet")

        async with asyncio.timeout(20.0):
            await init_aborted.wait()

            driver = operator.drivers["ctrl1"]

            while driver.lifecycle.belief_state != InternalControllerState.SHUTDOWN:
                await asyncio.sleep(0.05)
    finally:
        clear_weather.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await clear_weather

        async with asyncio.timeout(10.0):
            await operator.stop()


# TODO: Move this to program/discovery tests.
@pytest.mark.asyncio
async def test_program_discovery_finds_registered_program(kit, service_context):
    """ProgramDiscovery detects a registered program and its enable state."""
    await service_context.register_program("myprog")

    # Enable the program for a controller.
    prog_client = kit.program("myprog")
    await prog_client.enable("ctrl1")

    # Start ProgramDiscovery and verify it finds the program.
    discovery = ProgramDiscovery()
    await discovery.start(kit)

    async with asyncio.timeout(5.0):
        async for programs in discovery.enabled_programs():
            if "myprog" in programs:
                break

    # Check controller-specific discovery.
    async with asyncio.timeout(5.0):
        async for programs in discovery.controller_programs("ctrl1"):
            if "myprog" in programs:
                break

    await discovery.stop()


@pytest.mark.asyncio
async def test_late_discovered_program_gets_enabled(kit, service_context):
    """A program that appears after the operator starts is still enabled.

    Regression test: ProgramStateManager.watch_discovery() previously consumed
    only the set of *enabled* programs.  A late-arriving program that starts
    disabled never changed that set, so handle_program() was never called for
    it.  Switching to known_programs (all discovered, regardless of enable
    state) ensures late arrivals are reconciled.
    """
    async with asyncio.timeout(1.0):
        # Register a service and ONE program before the operator starts.
        await service_context.register_program("early_prog")
        controller = await service_context.register_controller("ctrl1")
        await controller.kv_put_model(_test_position)

    # Build an operator that expects two programs.
    config = ControllerConfig(
        tasking=[
            ProgramConfig(program="early_prog"),
            ProgramConfig(program="late_prog"),
        ],
    )
    config.name = "ctrl1"
    operator = VirtualOperator([config])

    # Apply state that desires both programs enabled.
    state = AgentState(
        operating_state=AgentOperatingState(global_control_enabled=False),
        scheduler_state=AgentSchedulerState(scheduling_enabled=True),
    )

    async with asyncio.timeout(5.0):
        await state.apply_to_operator(operator, {"ctrl1": config})

        # Start the operator — only early_prog is discoverable at this point.
        await operator.start(client=kit, task_group=asyncio)

    try:
        async with asyncio.timeout(5.0):
            # Verify early_prog is enabled.
            while True:
                ps = await kit.program("early_prog").kv_get_model(ProgramState)
                if ps.enable_state.enabled:
                    break
                await asyncio.sleep(0.1)

            # Now register the late program (simulates a service starting slowly).
            await service_context.register_program("late_prog")

            # The operator should discover it and enable it via handle_program().
            while True:
                ps = await kit.program("late_prog").kv_get_model(ProgramState)
                if ps.enable_state.enabled:
                    break
                await asyncio.sleep(0.1)
    finally:
        async with asyncio.timeout(10.0):
            await operator.stop()
