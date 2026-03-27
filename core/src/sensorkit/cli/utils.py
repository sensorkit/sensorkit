import functools
from typing import Any, Callable, TypeVar

import asyncclick as click
from rich.console import Console

T = TypeVar("T")

console = Console()

def common_options(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to add common SensorKit options to a command."""
    # Placeholder for common options like --config, --log-level etc.
    # For now, we can just return f
    return f

def entity_option(default: str | None = None, required: bool = False, help: str = "Entity name"):
    """Standardized entity option."""
    return click.option("-e", "--entity", default=default, required=required, help=help)

async def handle_errors(coro: Callable[..., Any]):
    """Wrapper to handle common SensorKit errors and display them using rich."""
    from sensorkit.backend.base import KVError
    from sensorkit.backend.lease import LeaseUnavailableError
    
    try:
        return await coro()
    except* KVError as eg:
        console.print(f"[bold red]Error:[/bold red] Database or configuration error: {eg.exceptions[0]}")
        raise click.Abort()
    except* LeaseUnavailableError:
        console.print("[bold red]Error:[/bold red] Resource is busy or already in use.")
        raise click.Abort()
    except* Exception as eg:
        console.print(f"[bold red]Unexpected Error:[/bold red] {eg.exceptions[0]}")
        raise click.Abort()

def with_kit(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that provides a 'kit' instance to the command and handles errors."""
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        from sensorkit.api.bootstrap import connect
        
        async def run():
            kit = await connect()
            return await f(kit, *args, **kwargs)
            
        return await handle_errors(run)
    return wrapper
