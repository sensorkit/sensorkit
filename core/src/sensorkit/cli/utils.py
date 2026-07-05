# SPDX-License-Identifier: Apache-2.0
import functools
from contextlib import contextmanager
from typing import Any, Callable

import asyncclick as click
from rich.console import Console

from sensorkit.api.entrypoint import ShutdownSignal

console = Console()


def common_options(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to add common SensorKit options to a command."""
    # Placeholder for common options like --config, --log-level etc.
    # For now, we can just return f
    return f


def entity_option(default: str | None = None, required: bool = False, help: str = "Entity name"):
    """Standardized entity option."""
    return click.option("-e", "--entity", default=default, required=required, help=help)


@contextmanager
def report_errors():
    from sensorkit.backend.base import BackendError, KVError
    from sensorkit.backend.lease import LeaseUnavailableError

    try:
        yield
    except* ShutdownSignal:
        # Fall through to normal shutdown.
        pass
    except* KVError as eg:
        console.print(f"[red]Error:[/red] Configuration or state error: {eg.exceptions[0]}")
        raise click.Abort() from eg
    except* LeaseUnavailableError as eg:
        console.print("[red]Error:[/red] Service or entity is already running!")
        raise click.Abort() from eg
    except* BackendError as eg:
        console.print(f"[red]Error:[/red] Backend unavailable: {eg.exceptions[0]}")
        raise click.Abort() from eg
    except* Exception as eg:
        for e in eg.exceptions:
            console.print(f"[red]Unexpected {type(e).__name__}[/red]")

        console.print_exception()
        raise click.Abort() from eg


def with_kit(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that provides a 'kit' instance to the command and handles errors."""

    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        from sensorkit.api.bootstrap import connect

        with report_errors():
            kit = await connect()
            return await f(kit, *args, **kwargs)

    return wrapper
