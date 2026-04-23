import asyncio
import concurrent.futures
import contextlib
import signal
import time
from collections.abc import Callable, Coroutine, Mapping
from random import random
from typing import Any

from loguru import logger

from sensorkit.api.declarative import Service
from sensorkit.common.aio import scoped_waiter
from sensorkit.common.importutil import obj_from_spec

type ServiceEntrypointFunc = Callable[[Service], Coroutine[Any, Any, None]]


class ServiceEntrypoint:
    """Proxy for a service entrypoint function."""

    @classmethod
    def from_spec(cls, spec: str, load_file: bool = False):
        """Finds a service entrypoint in a module or python file."""
        return obj_from_spec(spec=spec, base=ServiceEntrypoint, load_file=load_file)

    def __init__(self, func: ServiceEntrypointFunc, version: str):
        self._func = func
        self._version = version

    async def run(self, name: str):
        """Start the service with *name* and return ``(service, task)`` once it is running."""
        # Invoke the user entrypoint function passing in a new Service instance.
        service = Service(name, self._version)
        task = asyncio.create_task(self._func(service))

        try:
            # Make sure the entrypoint function is proper and actually starts the service.
            await asyncio.wait(
                [service.started, task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not service.started.done():
                if e := task.exception():
                    logger.debug(f"Service '{name}' failed to start: {type(e).__name__}: {e}")
                    raise e
                else:
                    raise RuntimeError(f"Entrypoint '{self._func.__name__}' did not start the service!")

            await service.started

            # Wait for the service to start up or for the task to fail.
            await asyncio.wait(
                [service.running, task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not service.running.done():
                raise task.exception() or RuntimeError("Task ended before service inited")

            await service.running

            return service, task
        except asyncio.CancelledError:
            task.cancel()

            try:
                await task
            except Exception as e:
                logger.opt(exception=e).debug("exception during service shutdown")

            raise

    def __call__(self, *args, **kwargs):
        raise RuntimeError("Service entrypoint cannot be called directly")


def service_entrypoint(*, version: str):
    """Returns a decorator for defining a service entrypoint."""
    def decorator(func: ServiceEntrypointFunc):
        return ServiceEntrypoint(func, version=version)

    return decorator


class ShutdownSignal(Exception):
    """Raised to signal an orderly service shutdown across threads."""


async def run_services(
    entrypoints: Mapping[str, ServiceEntrypoint],
    trap_signals: bool = True,
    **kwargs,
):
    """Run a set of service entrypoints."""
    # We have to use a `concurrent.futures.Future` here so it can be used by different event loops
    # running on different threads.
    shutdown = concurrent.futures.Future()

    def signal_shutdown(sig=None, _frame=None):
        if sig:
            logger.debug(f"Caught shutdown signal {sig}")

        if not shutdown.done():
            shutdown.set_exception(ShutdownSignal())

    if trap_signals:
        signal.signal(signal.SIGINT, signal_shutdown)
        signal.signal(signal.SIGTERM, signal_shutdown)

    # Run all services, each in its own thread with its own event loop per invocation.
    try:
        await asyncio.gather(
            asyncio.shield(asyncio.wrap_future(shutdown)),
            *(
                asyncio.to_thread(_service_loop, name, entrypoint, shutdown, **kwargs)
                for name, entrypoint in entrypoints.items()
            )
        )
    except Exception:
        # Shut down other services.
        signal_shutdown()
        raise


def _service_loop(
    name: str,
    entrypoint: ServiceEntrypoint,
    shutdown_signal: concurrent.futures.Future,
    max_restarts: int = -1,
    startup_failure_threshold: float = 30.0,
    restart_backoff_min: float = 5.0,
    restart_backoff_max: float = 300.0,
    restart_backoff_random: float = 5.0,
    restart_backoff_factor: float = 1.8,
    startup_timeout: float = 300.0,
    shutdown_timeout: float = 5.0,
):
    restarts = 0
    backoff = 0.0
    last_err: Exception | None = None

    try:
        # Loop while we have not had too many consecutive failed restarts.
        while max_restarts == -1 or restarts <= max_restarts:
            # Backoff if required, waking early if the shutdown signal fires.
            if backoff > 0.0 and not shutdown_signal.done():
                logger.info(f"Waiting {backoff:1.0f} sec before restarting {name} ...")

            try:
                # Check for the shutdown signal, performing backoff wait as necessary. If this does
                # not raise, the service stopped gracefully.
                shutdown_signal.result(timeout=backoff)
                break
            except concurrent.futures.TimeoutError:
                pass
            except Exception as e:
                # The service stopped abnormally.
                last_err = e
                break

            # Run the service in a fresh event loop.
            logger.info(f"Starting {name}")
            start_time = time.monotonic()
            last_err = None

            try:
                asyncio.run(
                    _service_proc(
                        name, entrypoint, shutdown_signal, startup_timeout, shutdown_timeout
                    )
                )
            except Exception as e:
                logger.error(f"Service {name} exited due to {type(e).__name__}")
                last_err = e

            # Decide whether the previous run was a failure.
            prev_elapsed = time.monotonic() - start_time

            if prev_elapsed < startup_failure_threshold:
                # Increment our restart counter and calculate exponential backoff.
                restarts += 1
                backoff = backoff * restart_backoff_factor + random() * restart_backoff_random
                backoff = min(max(backoff, restart_backoff_min), restart_backoff_max)
            else:
                # This wasn't considered a failed startup, so reset our consecutive restart counter
                # and zero out the backoff wait.
                restarts = 1
                backoff = 0.0
    except Exception as e:
        logger.error(f"Terminating {name} service loop due to {type(e).__name__}")
        last_err = e
    finally:
        logger.debug(f"service loop for {name} exiting (last_err is {type(last_err).__name__}))")

        # Make sure all other services exit, too.
        if not shutdown_signal.done():
            shutdown_signal.set_result(None)

        if last_err:
            raise last_err


async def _service_proc(
    name: str,
    entrypoint: ServiceEntrypoint,
    shutdown_signal: concurrent.futures.Future,
    startup_timeout: float,
    shutdown_timeout: float,
):
    service: Service | None = None
    service_task: asyncio.Task | None = None

    # Bind the cross-thread shutdown signal to this event loop.
    shutdown = asyncio.wrap_future(shutdown_signal)

    try:
        async with scoped_waiter(asyncio.shield(shutdown)) as shutdown_wait:
            startup = asyncio.create_task(entrypoint.run(name))

            await asyncio.wait(
                [startup, shutdown_wait],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=startup_timeout,
            )

            if shutdown_wait.done():
                startup.cancel()

                with contextlib.suppress(asyncio.CancelledError):
                    await startup

                return

            if not startup.done():
                raise TimeoutError(f"Service failed to start within {startup_timeout:.0f} seconds")

            service, service_task = startup.result()

            await asyncio.wait(
                [service_task, shutdown_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not shutdown_wait.done():
                await service_task
    finally:
        if service and not service.shutdown.done():
            logger.info(f"Shutting down {name}")

            # Try an orderly shutdown first.
            with contextlib.suppress(Exception):
                async with asyncio.timeout(shutdown_timeout):
                    await service.stop()

        # Clean up any service tasks remaining, in case of abnormal shutdown.
        with contextlib.suppress(Exception):
            async with asyncio.timeout(shutdown_timeout):
                await service.cleanup()

        # Kill the user entrypoint task.
        if service_task and not service_task.done():
            service_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await service_task
