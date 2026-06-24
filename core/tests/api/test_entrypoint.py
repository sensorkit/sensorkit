import asyncio
import concurrent.futures
import contextlib
import os
import signal
import threading

import pytest

from sensorkit.api.declarative import Service
from sensorkit.api.entrypoint import (
    ServiceEntrypoint,
    ShutdownSignal,
    _service_loop,
    run_services,
    service_entrypoint,
)


def test_service_entrypoint_decorator():
    @service_entrypoint(version="1.2.3")
    async def my_service(svc: Service):
        await svc.run()

    assert isinstance(my_service, ServiceEntrypoint)
    assert my_service._version == "1.2.3"


def test_entrypoint_direct_call_raises():
    @service_entrypoint(version="0.1.0")
    async def my_service(svc: Service):
        await svc.run()

    with pytest.raises(RuntimeError, match="cannot be called directly"):
        my_service()


@pytest.mark.asyncio
async def test_entrypoint_run():
    """Happy path: entrypoint calls service.run(), returns (service, task)."""
    os.environ["SENSORKIT_BACKEND"] = "fake"

    ep = ServiceEntrypoint(
        func=lambda svc: svc.run(),
        version="0.1.0",
    )

    async with asyncio.timeout(2.0):
        service, task = await ep.run("test-svc")

    assert service.running.done()
    assert service.name == "test-svc"

    await service.context.shutdown()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_entrypoint_no_run_raises():
    """Entrypoint returns without calling service.run() — should raise RuntimeError."""
    os.environ["SENSORKIT_BACKEND"] = "fake"

    async def bad_entrypoint(svc: Service):
        pass  # never calls svc.run()

    ep = ServiceEntrypoint(func=bad_entrypoint, version="0.1.0")

    with pytest.raises(RuntimeError, match="did not start the service"):
        async with asyncio.timeout(2.0):
            await ep.run("test-svc")


@pytest.mark.asyncio
async def test_entrypoint_error_before_run():
    """Entrypoint raises before calling service.run() — exception propagates."""
    os.environ["SENSORKIT_BACKEND"] = "fake"

    async def failing_entrypoint(svc: Service):
        raise ValueError("startup failed")

    ep = ServiceEntrypoint(func=failing_entrypoint, version="0.1.0")

    with pytest.raises(ValueError, match="startup failed"):
        async with asyncio.timeout(2.0):
            await ep.run("test-svc")


@pytest.mark.asyncio
async def test_service_loop_shutdown():
    """Service starts, then shutdown signal causes clean exit."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    started = threading.Event()

    async def entrypoint_func(svc: Service):
        await svc.start()
        started.set()
        await svc.shutdown

    ep = ServiceEntrypoint(func=entrypoint_func, version="0.1.0")
    shutdown = concurrent.futures.Future()

    loop_task = asyncio.create_task(
        asyncio.to_thread(_service_loop, "test-svc", ep, shutdown, shutdown_timeout=1.0)
    )

    assert await asyncio.to_thread(started.wait, 5.0), "service did not start"

    # Signal shutdown.
    shutdown.set_result(None)

    async with asyncio.timeout(5.0):
        await loop_task


@pytest.mark.asyncio
async def test_service_loop_restart_on_failure():
    """Service crashes on first attempt, succeeds on second."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    call_count = 0
    running = threading.Event()

    async def flaky_entrypoint(svc: Service):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("first attempt fails")

        running.set()
        await svc.run()

    ep = ServiceEntrypoint(func=flaky_entrypoint, version="0.1.0")
    shutdown = concurrent.futures.Future()

    loop_task = asyncio.create_task(
        asyncio.to_thread(
            _service_loop,
            "test-svc",
            ep,
            shutdown,
            shutdown_timeout=1.0,
            restart_backoff_min=0.01,
            restart_backoff_max=0.05,
            restart_backoff_random=0.0,
        )
    )

    assert await asyncio.to_thread(running.wait, 5.0), "service did not restart"
    assert call_count == 2

    shutdown.set_exception(Exception("shutdown"))
    async with asyncio.timeout(5.0):
        with pytest.raises(Exception, match="shutdown"):
            await loop_task


@pytest.mark.asyncio
async def test_service_loop_max_restarts():
    """Service always fails — loop exits after max_restarts."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    call_count = 0

    async def always_fails(svc: Service):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("boom")

    ep = ServiceEntrypoint(func=always_fails, version="0.1.0")
    shutdown = concurrent.futures.Future()

    with pytest.raises(RuntimeError, match="boom"):
        async with asyncio.timeout(5.0):
            await asyncio.to_thread(
                _service_loop,
                "test-svc",
                ep,
                shutdown,
                max_restarts=2,
                restart_backoff_min=0.01,
                restart_backoff_max=0.05,
                restart_backoff_random=0.0,
            )

    # Initial attempt + 2 restarts = 3 total calls.
    assert call_count == 3


@pytest.mark.asyncio
async def test_service_loop_long_run_resets_restarts():
    """A run exceeding startup_failure_threshold resets the restart counter."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    done = threading.Event()
    call_count = 0

    async def brief_runs(svc: Service):
        nonlocal call_count
        call_count += 1
        await svc.start()

        if call_count < 3:
            # Delay long enough to exceed the startup_failure_threshold.
            await asyncio.sleep(0.05)
        else:
            # Done, stop restarting.
            done.set()
            await asyncio.sleep(float('inf'))

        await svc.stop()

    ep = ServiceEntrypoint(func=brief_runs, version="0.1.0")
    shutdown = concurrent.futures.Future()

    loop_task = asyncio.create_task(
        asyncio.to_thread(
            _service_loop,
            "test-svc",
            ep,
            shutdown,
            max_restarts=1,
            shutdown_timeout=1.0,
            startup_failure_threshold=0,
            restart_backoff_min=0,
            restart_backoff_max=0,
            restart_backoff_random=0,
        )
    )

    async with asyncio.timeout(5.0):
        assert await asyncio.to_thread(done.wait, 5.0), "service did not reach 3rd run"
        assert call_count >= 3
        shutdown.set_result(None)
        await loop_task


@pytest.mark.asyncio
async def test_run_services_shutdown():
    """run_services shuts down cleanly when SIGINT is received."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    started = threading.Event()

    async def my_svc(svc: Service):
        await svc.start()
        started.set()
        await svc.shutdown

    ep = ServiceEntrypoint(func=my_svc, version="0.1.0")

    # Save original signal handlers — run_services overrides them.
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    try:
        svc_task = asyncio.create_task(
            run_services(
                {"test-svc": ep},
                trap_signals=True,
                shutdown_timeout=1.0,
            )
        )

        # Wait for the service to be fully running in its background thread.
        assert await asyncio.to_thread(started.wait, 5.0), "service did not start"

        # Trigger the signal-based shutdown path.
        signal.raise_signal(signal.SIGINT)

        with pytest.raises(ShutdownSignal):
            async with asyncio.timeout(5.0):
                await svc_task
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)