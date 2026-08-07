# SPDX-License-Identifier: Apache-2.0
import asyncio

import pytest

from sensorkit.common.aio import AsyncLoop, AsyncObserver, AsyncValueLatch


@pytest.mark.asyncio
async def test_async_value_latch():
    latch = AsyncValueLatch(0)

    assert latch.value == 0
    assert latch.pending_value == 0
    assert not latch.pending_change()

    # Stage a change
    changed = latch.update(1)
    assert changed is True
    assert latch.pending_change()
    assert latch.pending_value == 1

    # Apply the change
    applied = latch.apply()
    assert applied == 1
    assert latch.value == 1
    assert not latch.pending_change()


@pytest.mark.asyncio
async def test_async_value_latch_pending_wait():
    latch = AsyncValueLatch("a")

    # No-op update when same value and only_if_different=True (default)
    assert not latch.update("a")
    assert not latch.pending_change()

    # Waiter should block until an update occurs
    started = asyncio.Event()
    resumed = asyncio.Event()

    async def waiter():
        started.set()
        await latch.wait_until_pending()
        resumed.set()

    task = asyncio.create_task(waiter())
    await started.wait()

    # Still waiting
    await asyncio.sleep(0)
    assert not resumed.is_set()

    # Trigger a pending change with a new value
    assert latch.update("b")
    await asyncio.wait_for(resumed.wait(), timeout=1)

    # Apply the change
    assert latch.apply() == "b"
    assert latch.value == "b"

    # Updating with same value does nothing when only_if_different=True
    assert latch.update("b") is False
    assert not latch.pending_change()

    # Forcing update with only_if_different=False should set pending
    assert latch.update("b", only_if_different=False) is True
    assert latch.pending_change()

    # Cleanup
    if not task.done():
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        assert task.done()


@pytest.mark.asyncio
async def test_async_observer():
    obs = AsyncObserver(0)
    q1 = obs.subscribe(initial_value=True)
    q2 = obs.subscribe(initial_value=False)

    async with asyncio.timeout(1):
        assert obs.value == 0
        assert await q1.get() == 0
        q1.task_done()
        assert q2.empty()

        await obs.notify(1)
        assert obs.value == 1
        assert await q1.get() == 1
        q1.task_done()
        assert await q2.get() == 1
        q2.task_done()

        # Unsubscribe one queue; it should not receive further notifications.
        obs.unsubscribe(q2)
        assert q2 not in obs._observers

        await obs.notify(2)
        assert await q1.get() == 2
        q1.task_done()

        with pytest.raises(asyncio.QueueShutDown):
            await q2.get()

        # Test unset initial value.
        obs = AsyncObserver()

        with pytest.raises(RuntimeError):
            _ = obs.value

        assert obs.subscribe(initial_value=True).empty()

        await obs.notify(42)
        assert obs.value == 42


@pytest.mark.asyncio
async def test_async_observer_generator():
    obs = AsyncObserver("x")
    queue = asyncio.Queue()
    ready = asyncio.Event()

    async def reader(n: int):
        async for v in obs.consume(initial_value=True):
            ready.set()
            queue.put_nowait(v)
            n -= 1

            if n <= 0:
                break

    # Create a task to read two notifications.
    task = asyncio.create_task(reader(n=2))
    await ready.wait()

    # Reach in and get a reference to our generator's underlying queue.
    underlying_queue = next(iter(obs._observers))

    async with asyncio.timeout(1):
        # The initial value should already be there.
        assert await queue.get() == "x"

        # Add a second value, which will cause the reader to exit its generator.
        await obs.notify("y")
        assert await queue.get() == "y"

        # Wait for the reader task to end. We need to force a suspend with a sleep(0) in order
        # to ensure the `consume()` generator cleanup happens!
        await task
        await asyncio.sleep(0)

        # Make sure everything cleaned up.
        assert len(obs._observers) == 0
        await underlying_queue.join()

        with pytest.raises(asyncio.QueueShutDown):
            await underlying_queue.get()


@pytest.mark.asyncio
async def test_async_loop_start_stop():
    ticks = 0

    async def body():
        nonlocal ticks
        ticks += 1

    loop = AsyncLoop(body, interval=0.01)
    assert not loop.active

    assert loop.start() is loop
    assert loop.active

    async with asyncio.timeout(1):
        while ticks < 3:
            await asyncio.sleep(0.01)

    await loop.stop()
    assert not loop.active

    # No further iterations once stopped.
    settled = ticks
    await asyncio.sleep(0.05)
    assert ticks == settled


@pytest.mark.asyncio
async def test_async_loop_start_is_idempotent():
    ticks = 0

    async def body():
        nonlocal ticks
        ticks += 1

    loop = AsyncLoop(body, interval=0.01)

    # Starting repeatedly must not leave orphaned tasks behind, which a single stop
    # would fail to cancel.
    loop.start()
    loop.start()
    loop.start()

    async with asyncio.timeout(1):
        while ticks < 2:
            await asyncio.sleep(0.01)

    await loop.stop()

    settled = ticks
    await asyncio.sleep(0.05)
    assert ticks == settled


@pytest.mark.asyncio
async def test_async_loop_restart_after_stop():
    ticks = 0

    async def body():
        nonlocal ticks
        ticks += 1

    loop = AsyncLoop(body, interval=0.01)
    loop.start()

    async with asyncio.timeout(1):
        while ticks < 1:
            await asyncio.sleep(0.01)

    await loop.stop()
    stopped_at = ticks

    loop.start()
    assert loop.active

    async with asyncio.timeout(1):
        while ticks == stopped_at:
            await asyncio.sleep(0.01)

    await loop.stop()


@pytest.mark.asyncio
async def test_async_loop_continues_after_error():
    attempts = 0

    async def body():
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    loop = AsyncLoop(body, interval=0.01)
    loop.start()

    async with asyncio.timeout(1):
        while attempts < 3:
            await asyncio.sleep(0.01)

    assert loop.active
    await loop.stop()


@pytest.mark.asyncio
async def test_async_loop_stop_without_start():
    async def body():
        await asyncio.sleep(0)

    loop = AsyncLoop(body, interval=0.01)

    # Stopping is safe before a first start, and again after a stop.
    await loop.stop()
    loop.start()
    await loop.stop()
    await loop.stop()

    assert not loop.active
