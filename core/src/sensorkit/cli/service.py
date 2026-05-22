import asyncclick as click
from rich.console import Console

from sensorkit.cli.utils import with_kit


@click.group("service")
async def service_group():
    """Service management commands."""


@service_group.command("run")
@click.argument("name", metavar="name", nargs=1)
@click.argument("spec", metavar="python_module[:entrypoint]", nargs=1)
@click.option("-r", "--restart", is_flag=True, help="Automatically restart service on failure")
async def service_run(name, spec, restart):
    """
    Run a SensorKit service.
    """
    from loguru import logger

    from sensorkit.api.entrypoint import ServiceEntrypoint, ShutdownSignal, run_services
    from sensorkit.backend.base import KVError
    from sensorkit.backend.lease import LeaseUnavailableError
    from sensorkit.common.logging import configure_logging

    # Configure logging
    configure_logging()

    try:
        entrypoints = {
            name: ServiceEntrypoint.from_spec(spec, load_file=True)
        }
    except (ValueError, ModuleNotFoundError):
        raise click.UsageError(f"Could not find an entrypoint at '{spec}'")

    try:
        # Find and run the service entrypoint.
        await run_services(
            entrypoints,
            max_restarts=None if restart else 0
        )
    except* KVError as eg:
        logger.opt(exception=eg.exceptions[0]).debug("service config error")
        click.secho(
            "Could not find required data (is there missing configuration?)",
            fg="red",
            err=True,
        )
    except* LeaseUnavailableError:
        click.secho(
            "Service cannot start due to a conflict (is this service already running?)",
            fg="red",
            err=True,
        )
    except* ShutdownSignal:
        logger.debug("service exiting due to shutdown signal")
    except* Exception as eg:
        logger.opt(exception=eg.exceptions[0]).debug("service exiting with error")
        click.secho("Service exiting with error", fg="red", err=True)


@service_group.command("ls")
@with_kit
async def service_list(kit):
    """List services registered in the backend."""
    console = Console()
    services = await kit.list_services()

    for _, status in services.items():
        sr = status.service.info
        deco = "bold white" if status.online else "bold yellow"
        console.print(
            f"[{deco}]{sr.name} (version {sr.version}): {'online' if status.online else 'offline'}[/{deco}]"
        )
