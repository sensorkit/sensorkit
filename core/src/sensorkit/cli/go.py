import asyncio
import os
import signal
import textwrap

import asyncclick as click
from pydantic import BaseModel, Field, ValidationError

from sensorkit.cli.utils import handle_errors

DEFAULT_CONFIG_FILE = "services.yaml"


class ServiceDefinition(BaseModel):
    """Definition of a service to launch."""
    name: str
    module: str
    func: str | None = None

    @classmethod
    def from_shorthand(cls, in_str: str):
        """Parse a ``name:module[:entrypoint]`` shorthand string into a ServiceDefinition."""
        name, module, *func = in_str.split(":")
        func = func[0] if func else None
        return cls(name=name, module=module, func=func)


class ServiceConfig(BaseModel):
    """Collection of service definitions loaded from a YAML config file."""
    services: list[ServiceDefinition] = Field(default_factory=list)


@click.command("go")
@click.option(
    "--config", "-c", "config_file",
    metavar="[config_file]",
    required=False,
    type=click.Path(exists=True),
)
@click.option(
    "--daemon", "-d",
    is_flag=True,
    help="Run in daemon mode",
)
@click.option(
    "--restart", "-r",
    is_flag=True,
    help="Automatically restart services on failure",
)
@click.option(
    "--log-file",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default=None,
    help="If set, all log output (stdout/stderr, exceptions, and logger calls) also goes here.",
)
@click.option(
    "--log-file-append",
    default=True,
    type=bool,
    help="When using --log-file, append to the file (default) or overwrite it.",
)
@click.option(
    "--log-level",
    default="INFO",
    help="Log level for all log output (file and stdout/stderr)."
)
@click.option(
    "--add-service",
    multiple=True,
    metavar="name:module[:entrypoint]",
    help="Service name and entrypoint spec",
)
@click.option(
    "--shutdown-timeout",
    type=float,
    default=180.0,
    help=(
        "Max seconds per service for graceful shutdown. Bounds the entire deinit "
        "phase (entity @sk.on_detach callbacks) plus backend teardown. Pick a value "
        "that comfortably covers your slowest hardware deinit (e.g. dome close, "
        "mount park)."
    ),
)
async def go_command(
    daemon: bool,
    restart: bool,
    config_file: str | None,
    log_file: str | None,
    log_file_append: bool,
    log_level: str,
    add_service: tuple[str],
    shutdown_timeout: float,
):
    """Launch and manage services."""
    import yaml
    from loguru import logger

    from sensorkit.api.entrypoint import ServiceEntrypoint, run_services
    from sensorkit.common.logging import configure_logging

    if daemon:
        raise NotImplementedError("--daemon flag not yet implemented")

    # Configure logging
    configure_logging(
        level=log_level,
        log_file=log_file,
        log_file_append=log_file_append,
    )

    # Load config.
    config: ServiceConfig | None = None
    validation_errors: list[ValidationError] = []

    try:
        with open(config_file or DEFAULT_CONFIG_FILE, "r") as f:
            # Find the first valid configuration in the input YAML.
            config_data = yaml.safe_load_all(f)

            for data in config_data:
                try:
                    # Use the first valid configuration in the document.
                    config = ServiceConfig.model_validate(data)
                    break
                except ValidationError as e:
                    validation_errors.append(e)
    except Exception as e:
        # If this load was explicitly requested (not a default), we fail here.
        if config_file is not None:
            info = textwrap.indent(str(e), prefix="  ")
            logger.opt(exception=e).debug("error loading config file")
            click.secho(f"Could not load '{config_file}':\n{info}", fg="red", err=True)
            return

        # Otherwise, fall through.

    if config is None:
        if validation_errors:
            info = f"\n".join(
                "  {doc}: {msg} near '{loc[0]}'".format(
                    doc=i,
                    **details,
                )
                for i, error in enumerate(validation_errors, 1)
                for details in error.errors()
            )
            click.secho(f"Configuration is invalid:\n{info}", fg="red", err=True)
            return

        config = ServiceConfig()

    # Include additional services from command line args.
    for shorthand in add_service:
        try:
            config.services.append(ServiceDefinition.from_shorthand(shorthand))
        except ValueError as e:
            raise click.UsageError(f"Invalid argument: {shorthand}") from e

    # Find entrypoint objects.
    entrypoints: dict[str, ServiceEntrypoint] = {}

    for svc in config.services:
        try:
            entrypoints[svc.name] = ServiceEntrypoint.from_spec(
                svc.module + (f":{svc.func}" if svc.func else ""),
                load_file=True,
            )
        except Exception as e:
            logger.opt(exception=e).debug("error loading entrypoint")
            click.secho(
                f"Failed to find entrypoint for '{svc.name}': {e}",
                fg="red",
                err=True,
            )
            return

    if not entrypoints:
        click.secho("No services defined!", fg="red", err=True)
        return

    FORCE_QUIT_PRESSES = 3

    async def install_escalation_handler():
        # `run_services` installs its own SIGINT/SIGTERM handler synchronously before its
        # first await. By the time this task gets to run, that handler is in place; we
        # wrap it to add per-press feedback and a force-quit escape hatch.
        await asyncio.sleep(0)
        inner = signal.getsignal(signal.SIGINT)
        count = 0

        def escalate(sig, frame):
            nonlocal count
            count += 1
            remaining = FORCE_QUIT_PRESSES - count

            if count == 1:
                logger.info(
                    f"Shutdown signal received; allowing up to ~{shutdown_timeout * 2:.0f}s "
                    f"per service for clean shutdown. "
                    f"Press Ctrl-C {remaining} more times to force quit."
                )
                if callable(inner):
                    inner(sig, frame)
            elif count < FORCE_QUIT_PRESSES:
                logger.warning(
                    f"Shutdown in progress — press Ctrl-C {remaining} more time"
                    f"{'s' if remaining != 1 else ''} to force quit"
                )
            else:
                logger.error("Force quit")
                os._exit(130)  # 128 + SIGINT

        signal.signal(signal.SIGINT, escalate)
        signal.signal(signal.SIGTERM, escalate)

    async def run():
        asyncio.create_task(install_escalation_handler())
        await run_services(
            entrypoints,
            max_restarts=-1 if restart else 0,
            shutdown_timeout=shutdown_timeout,
        )

    await handle_errors(run)
