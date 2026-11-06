# SPDX-License-Identifier: Apache-2.0
import asyncio
import concurrent.futures
import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from sensorkit.api.declarative import Service
from sensorkit.api.entrypoint import (
    ServiceEntrypoint,
    ServiceExit,
    ShutdownSignal,
    _service_loop,
    run_services,
    service_entrypoint,
)

FAST_RESTARTS = {
    "shutdown_timeout": 1.0,
    "restart_backoff_min": 0.01,
    "restart_backoff_max": 0.05,
    "restart_backoff_random": 0.0,
}


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


# ---------------------------------------------------------------------------
# Exits raised by service tasks
#
# Service loops run in executor threads to isolate exits from the test event loop.
# ---------------------------------------------------------------------------


async def raise_in_task(exc: BaseException):
    """Raise from a child task, which stops the service event loop like a uvicorn exit."""

    async def child():
        raise exc

    await asyncio.create_task(child())


def test_service_loop_restarts_only_the_exiting_service():
    """A SystemExit restarts its own service while the companion service keeps running."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    starts = {"exiting": 0, "companion": 0}
    teardowns = []
    exiting_restarted = threading.Event()
    companion_running = threading.Event()

    async def exiting(svc: Service):
        starts["exiting"] += 1
        await svc.start()

        try:
            if starts["exiting"] == 1:
                await asyncio.to_thread(companion_running.wait, 5.0)
                await raise_in_task(SystemExit(1))

            exiting_restarted.set()
            await svc.shutdown
        finally:
            teardowns.append("exiting")

    async def companion(svc: Service):
        starts["companion"] += 1
        await svc.start()
        companion_running.set()

        try:
            await svc.shutdown
        finally:
            teardowns.append("companion")

    shutdown = concurrent.futures.Future()

    with concurrent.futures.ThreadPoolExecutor(2) as executor:
        loops = [
            executor.submit(
                _service_loop,
                name,
                ServiceEntrypoint(func=func, version="0.1.0"),
                shutdown,
                max_restarts=1,
                **FAST_RESTARTS,
            )
            for name, func in (("exiting", exiting), ("companion", companion))
        ]

        try:
            assert exiting_restarted.wait(10.0), "exiting service did not restart"
            assert starts == {"exiting": 2, "companion": 1}
            assert teardowns == ["exiting"]
            assert not any(loop.done() for loop in loops)
        finally:
            # Unblocks the executor exit even if an assertion fails.
            if not shutdown.done():
                shutdown.set_result(None)

        assert [loop.result(timeout=10.0) for loop in loops] == [None, None]

    assert sorted(teardowns) == ["companion", "exiting", "exiting"]


@pytest.mark.parametrize("max_restarts", [0, 1])
@pytest.mark.parametrize("exit_type", [SystemExit, KeyboardInterrupt])
def test_service_loop_exhausted_exit_raises_service_exit(max_restarts, exit_type):
    """Exhausted restarts raise ServiceExit caused by the last exit, after its teardown."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    starts = 0
    teardowns = 0

    async def exiting(svc: Service):
        nonlocal starts, teardowns
        starts += 1
        await svc.start()

        try:
            await raise_in_task(exit_type(starts))
        finally:
            teardowns += 1

    shutdown = concurrent.futures.Future()

    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        loop = executor.submit(
            _service_loop,
            "exiting",
            ServiceEntrypoint(func=exiting, version="0.1.0"),
            shutdown,
            max_restarts=max_restarts,
            **FAST_RESTARTS,
        )

        try:
            error = loop.exception(timeout=10.0)
            assert shutdown.done()
        finally:
            # Unblocks the executor exit even if the loop does not finish in time.
            if not shutdown.done():
                shutdown.set_result(None)

    assert isinstance(error, ServiceExit)
    assert isinstance(error.__cause__, exit_type)
    assert error.__cause__.args == (max_restarts + 1,)
    assert starts == teardowns == max_restarts + 1


@pytest.mark.asyncio
async def test_run_services_failure_waits_for_teardown():
    """A terminal service failure propagates only after the other services are torn down."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    companion_running = threading.Event()
    teardowns = []

    async def failing(svc: Service):
        await svc.start()
        await asyncio.to_thread(companion_running.wait, 5.0)
        raise RuntimeError("boom")

    async def companion(svc: Service):
        await svc.start()
        companion_running.set()

        try:
            await svc.shutdown
        finally:
            # Blocks so teardown outlasts the failure handling on the main loop.
            time.sleep(0.2)
            teardowns.append("companion")

    with pytest.raises(RuntimeError, match="boom"):
        async with asyncio.timeout(10.0):
            await run_services(
                {
                    "failing": ServiceEntrypoint(func=failing, version="0.1.0"),
                    "companion": ServiceEntrypoint(func=companion, version="0.1.0"),
                },
                trap_signals=False,
                max_restarts=0,
                shutdown_timeout=1.0,
            )

    assert teardowns == ["companion"]


def test_main_thread_exit_waits_for_service_teardown():
    """A main-thread SystemExit cancels run_services, which tears services down before exit."""
    script = Path(__file__).with_name("main_thread_exit.py")

    result = subprocess.run(
        [sys.executable, str(script)],
        env=os.environ | {"SENSORKIT_BACKEND": "fake"},
        capture_output=True,
        text=True,
        timeout=30.0,
    )

    assert result.returncode == 3, result.stderr
    assert result.stdout.splitlines() == ["companion torn down"]
