import asyncio
import pathlib
import textwrap

import asyncclick as click
import yaml
from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.cli.config import config_load
from sensorkit.cli.utils import report_errors
from sensorkit.common.logging import add_debug_logger, configure_logging
from sensorkit.config.parser import parse_config

DEFAULT_CONFIG_FILE = "services.yaml"
DEFAULT_UNIFIED_FILE = "sensorkit.yaml"


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

    @classmethod
    def from_yaml_file(cls, config_file: pathlib.Path):
        """Load service configuration from a YAML file.

        Args:
            config_file: Path to the config file, or None to use the default.

        Returns:
            ServiceConfig instance, or None if the file does not exist.

        Raises:
            ValidationError: If the input configuration is invalid.
            YAMLError: If the config file cannot be parsed.
            OSError: If the config file cannot be read.
        """
        try:
            with config_file.open("r") as f:
                return cls.model_validate(yaml.safe_load(f))
        except FileNotFoundError:
            return None


def load_unified_config(config_file: pathlib.Path):
    """Load service configuration from a YAML file."""
    try:
        with config_file.open("r") as f:
            parsed = parse_config(yaml.safe_load(f))
            config = ServiceConfig()

            for svc in parsed.services:
                if svc.python_module:
                    config.services.append(
                        ServiceDefinition.from_shorthand(f"{svc.id}:{svc.python_module}")
                    )

            return config
    except FileNotFoundError:
        return None


async def read_config_file(config_file: str | None):
    """Find the config file, if any."""
    config: ServiceConfig | None = None
    is_unified = False
    errs = []

    try:
        file = config_file or DEFAULT_CONFIG_FILE
        config = ServiceConfig.from_yaml_file(pathlib.Path(file))
    except Exception as e:
        errs.append(e)

    if not config:
        file = config_file or DEFAULT_UNIFIED_FILE

        try:
            # Try the unified config format.
            sk.import_plugins()
            config = load_unified_config(pathlib.Path(file))
            is_unified = True
        except Exception as e:
            errs.append(e)

    if not config and errs:
        raise ExceptionGroup("could not load configuration", errs)

    return config, is_unified


def _logger_formatter(record):
    color = record["extra"].get("entity_color", "magenta")
    return (
        "{extra[entity_type]} "
        f" <{color}>{{extra[entity]}}</> "
        " <level>{message}</level>\n"
    )


def _logger_patcher(record):
    current = sk.entity()

    if current:
        entity = current.entity

        if sk.device():
            entity_type = "💠 "
            entity_color = "fg 57"
        elif sk.controller():
            entity_type = "📡 "
            entity_color = "fg 39"
        elif sk.program():
            entity_type = "⏭️ "
            entity_color = "fg 36"
        elif entity == "agent":
            entity_type = "🤖 "
            entity_color = "fg 84"
        else:
            entity_type = "🟣 "
            entity_color = "fg 144"
    else:
        entity = ".".join(record["name"].split(".")[1:])
        entity_type = "⚙️ "
        entity_color = "dim"

    record["extra"].update(
        entity=entity,
        entity_type=entity_type,
        entity_color=entity_color,
    )


@click.command("go")
@click.option(
    "--config", "-c", "config_file",
    metavar="[config_file]",
    required=False,
    type=click.Path(exists=True),
)
@click.option(
    "--load-config", "-l",
    is_flag=True,
    help="Automatically load configuration (unified config format only)",
)
@click.option(
    "--log-file",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default=None,
    help="If set, debug log output goes here instead of the default location.",
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
    help="Log level for console output."
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
@click.pass_context
async def go_command(
    ctx: click.Context,
    load_config: bool,
    config_file: str | None,
    log_file: str | None,
    log_file_append: bool,
    log_level: str,
    add_service: tuple[str],
    shutdown_timeout: float,
):
    """Launch and manage services."""
    from sensorkit.api.entrypoint import ServiceEntrypoint, run_services

    # Read service configuration from file.
    try:
        config, is_unified = await read_config_file(config_file)
    except ExceptionGroup as e:
        logger.opt(exception=e).debug("could not load either config format")
        info = textwrap.indent(f"{e.exceptions}", prefix="  ")
        click.secho(f"Could not read configuration:\n{info}", fg="red", err=True)
        return

    if not config:
        config = ServiceConfig()
    elif is_unified and load_config:
        print()
        print(" ▶️ Loading configuration...")
        print()

        # Run the 'config load' subcommand to load config.
        try:
            async with asyncio.timeout(5.0):
                await ctx.invoke(config_load, file=config_file or DEFAULT_UNIFIED_FILE, verbose=1)
        except Exception as e:
            click.secho(f"{type(e).__name__} while loading configuration", fg="red", err=True)
            return

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

    configure_logging(level=log_level, format=_logger_formatter)
    logger.configure(patcher=_logger_patcher)
    debug_log_dest = add_debug_logger(file=log_file, append=log_file_append)

    print()
    print(" ▶️ Logging to", debug_log_dest)
    print()
    print(" ▶️ Starting services...")
    print()

    with report_errors():
        await run_services(entrypoints, max_restarts=None, shutdown_timeout=shutdown_timeout)
