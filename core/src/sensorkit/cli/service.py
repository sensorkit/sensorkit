# SPDX-License-Identifier: Apache-2.0
import asyncclick as click
from rich.console import Console

from sensorkit.cli.utils import with_kit


@click.group("service")
async def service_group():
    """Service management commands."""


@service_group.command("run")
@click.argument("name", metavar="name", nargs=1)
@click.argument("spec", metavar="[python_module[:entrypoint]]", nargs=1, required=False)
@click.option(
    "-c", "--config", "config_file",
    metavar="[config_file]",
    default=None,
    help="Config file to resolve the service implementation from (defaults to sensorkit.yaml)",
)
@click.option("-r", "--restart", is_flag=True, help="Automatically restart service on failure")
async def service_run(name, spec, config_file, restart):
    """
    Run a SensorKit service.

    Resolves the implementation's python module path from configuration based on
    NAME. If the config cannot be loaded, the path must be supplied explicitly as
    the second argument.
    """
    import yaml
    from loguru import logger
    from pydantic import ValidationError

    import sensorkit.api as sk
    from sensorkit.api.entrypoint import ServiceEntrypoint, ShutdownSignal, run_services
    from sensorkit.backend.base import KVError
    from sensorkit.backend.lease import LeaseUnavailableError
    from sensorkit.common.logging import configure_logging
    from sensorkit.config.parser import (
        ConfigError,
        ConfigSectionUnknown,
        ConfigVersionUnsupported,
    )

    # Configure logging
    configure_logging()

    # Try to load config. A missing file is nonfatal (the spec must then be
    # supplied explicitly); any other load error is fatal.
    try:
        sk.set_config_location(config_file)
        config = await sk.load_config()
    except FileNotFoundError as e:
        logger.opt(exception=e).debug("config file not found")
        config = None
    except yaml.YAMLError as e:
        raise click.ClickException(f"invalid YAML in config: {e}") from e
    except ConfigVersionUnsupported as e:
        raise click.ClickException(f"unsupported config version: {e}") from e
    except ConfigSectionUnknown as e:
        raise click.ClickException(f"unknown config section: {e}") from e
    except ConfigError as e:
        raise click.ClickException(f"config processing failed: {e}") from e
    except ValidationError as e:
        raise click.ClickException(f"config validation failed: {e}") from e

    if config is not None:
        configured_spec = next(
            (svc.python_module for svc in config.services if svc.id == name and svc.python_module),
            None,
        )

        if configured_spec is None:
            raise click.UsageError(
                f"Could not resolve a python module for service '{name}' from configuration"
            )

        if spec is not None and spec != configured_spec:
            raise click.UsageError(
                f"An explicit python module is not allowed when configuration is loaded; "
                f"the implementation for service '{name}' is resolved from config "
                f"('{configured_spec}')"
            )

        spec = configured_spec
    elif spec is None:
        raise click.UsageError(
            f"No configuration found; supply the implementation for service '{name}' "
            "explicitly as 'python_module[:entrypoint]'"
        )

    try:
        entrypoints = {
            name: ServiceEntrypoint.from_spec(spec, load_file=True)
        }
    except (ValueError, ModuleNotFoundError) as e:
        raise click.UsageError(f"Could not find an entrypoint at '{spec}'") from e

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
