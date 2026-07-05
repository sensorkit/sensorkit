# SPDX-License-Identifier: Apache-2.0
import asyncclick as click

from sensorkit.auto.agent import AgentControllerInfo, AgentState
from sensorkit.cli.utils import entity_option, with_kit


@click.group("agent")
async def agent_group():
    """Agent management commands."""
    pass


@agent_group.command("include")
@click.argument("program_name")
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def include(kit, program_name: str, entity: str):
    """Include a program in the agent's management."""
    from sensorkit.auto.agent import AgentConfigureRequest, agent_configure_request

    await kit.entity(entity).call(
        agent_configure_request,
        AgentConfigureRequest(remove_program_exclusions={program_name}),
    )
    click.echo(f"Removed scheduling exclusion for '{program_name}'")


@agent_group.command("exclude")
@click.argument("program_name")
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def exclude(kit, program_name: str, entity: str):
    """Exclude a program from the agent's management."""
    from sensorkit.auto.agent import AgentConfigureRequest, agent_configure_request

    await kit.entity(entity).call(
        agent_configure_request,
        AgentConfigureRequest(add_program_exclusions={program_name}),
    )
    click.echo(f"Added scheduling exclusion for '{program_name}'")


@agent_group.command("global-control")
@click.argument("state", type=click.Choice(["on", "off"], case_sensitive=False))
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def global_control(kit, state: str, entity: str):
    """Enable or disable global control."""
    from sensorkit.auto.agent import AgentConfigureRequest, agent_configure_request

    enabled = state.lower() == "on"
    await kit.entity(entity).call(
        agent_configure_request,
        AgentConfigureRequest(global_control_enabled=enabled),
    )
    click.echo(f"Global control was set to {state}")


@agent_group.command("control")
@click.argument("controller")
@click.argument("state", type=click.Choice(["on", "off"], case_sensitive=False))
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def controller_control(kit, controller: str, state: str, entity: str):
    """Enable or disable control for a specific controller."""
    from sensorkit.auto.agent import AgentConfigureRequest, agent_configure_request

    enabled = state.lower() == "on"
    await kit.entity(entity).call(
        agent_configure_request,
        AgentConfigureRequest(controller_control_enabled={controller: enabled}),
    )
    click.echo(f"Control for '{controller}' was set to {state}")


@agent_group.command("override")
@click.argument("controller")
@click.argument("state", type=click.Choice(["up", "down", "none"], case_sensitive=False))
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def controller_override(kit, controller: str, state: str, entity: str):
    """Set or clear demand override for a specific controller."""
    from sensorkit.auto.agent import AgentConfigureRequest, agent_configure_request

    state = state.lower()
    override = True if state == "up" else False if state == "down" else None

    await kit.entity(entity).call(
        agent_configure_request,
        AgentConfigureRequest(controller_demand_override={controller: override}),
    )
    click.echo(f"Demand override for '{controller}' was set to {state.upper()}")


@agent_group.command("scheduling")
@click.argument("state", type=click.Choice(["on", "off"], case_sensitive=False))
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def scheduling(kit, state: str, entity: str):
    """Enable or disable scheduling."""
    from sensorkit.auto.agent import AgentConfigureRequest, agent_configure_request

    enabled = state.lower() == "on"
    await kit.entity(entity).call(
        agent_configure_request, AgentConfigureRequest(scheduling_enabled=enabled)
    )
    click.echo(f"Scheduling was set to {state}")


@agent_group.command("status")
@entity_option(default="agent", help="Agent entity name")
@with_kit
async def status(kit, entity: str):
    """View the agent's current status and configuration."""
    from sensorkit.auto.agent import AgentState

    try:
        state = await kit.entity(entity).kv_get_model(AgentState)
    except Exception as e:
        click.echo(f"Error: Could not recover agent state: {e}", err=True)
        return

    click.echo(f"{'Agent:':<18} {entity}")

    # Global settings
    click.echo(
        f"{'Global Control:':<18} {'ON' if state.operating_state.global_control_enabled else 'OFF'}"
    )
    click.echo(
        f"{'Scheduling:':<18} {'ON' if state.scheduler_state.scheduling_enabled else 'OFF'}\n"
    )

    if state.operating_state.controllers:
        for i, (name, info) in enumerate(state.operating_state.controllers.items(), 1):
            _print_controller_info(i, name, info, state)
            click.echo()
    else:
        click.echo("No controllers registered in agent.\n")

    # Excluded programs
    if state.scheduler_state.excluded_programs:
        click.echo("Excluded Programs:")

        for program in sorted(state.scheduler_state.excluded_programs):
            click.echo(f"  - {program}")
    else:
        click.echo("No excluded programs.")


def _print_controller_info(i: int, name: str, info: AgentControllerInfo, state: AgentState):
    control = "ON" if info.control_enabled else "OFF"

    if not state.operating_state.global_control_enabled:
        control += "*"

    override = "NONE"

    if info.demand_override is True:
        override = "UP"
    elif info.demand_override is False:
        override = "DOWN"

    elected = "NONE"

    if info.elected_state is True:
        elected = "UP"
    elif info.elected_state is False:
        elected = "DOWN"

    click.echo(f"{f'Controller {i}:':<18} {name}\n")
    click.echo(f"  Control:         {control}")
    click.echo(f"  State override:  {override}")
    click.echo(f"  Elected state:   {elected}")
    click.echo()

    if schedule := state.scheduler_state.schedule.get(name):
        from intervaltree import IntervalTree

        from sensorkit.auto.scheduler import debug_print_schedule

        debug_print_schedule(
            IntervalTree.from_tuples(schedule),
            lookahead_mins=5,
            print_func=lambda x: click.echo(f"  {x}"),
        )
    else:
        click.echo("  No schedule present.")
