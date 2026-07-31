# SPDX-License-Identifier: Apache-2.0
"""System tests for VirtualOperator driving election, demand, and lifecycle.

Tests the agent layer: VirtualOperator with election, demand evaluation,
program discovery, and lifecycle management.
"""

from __future__ import annotations

import asyncio

import pytest

from sensorkit.astro.common import SitePosition
from sensorkit.auto.agent import (
    AgentControllerInfo,
    AgentOperatingState,
    AgentSchedulerState,
    AgentState,
)
from sensorkit.auto.operator import ControllerConfig, VirtualOperator
from sensorkit.auto.scheduler import ProgramConfig
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

    driver = operator.drivers["ctrl1"]

    # The operator loop runs every 2 seconds. Wait for init to run, then verify the lifecycle
    # reached OPERATE.
    async with asyncio.timeout(10.0):
        await state.apply_to_operator(operator, {"ctrl1": config})
        await init_ran.wait()

        while driver.lifecycle.belief_state != InternalControllerState.OPERATE:
            await asyncio.sleep(0.05)

    await operator.stop()
    await service_context.shutdown()


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

    await operator.stop()
    await service_context.shutdown()


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
    await service_context.shutdown()


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

        await operator.stop()
        await service_context.shutdown()
