# SPDX-License-Identifier: Apache-2.0
import asyncio
import contextlib
import pathlib

import pytest

from sensorkit.common import filewatch
from sensorkit.common.filewatch import FileEvent, FileEventKind


@pytest.fixture(autouse=True)
def _reset_manager():
    """Ensure each test starts and ends with no watches on the shared observer."""
    filewatch.manager._reset()
    yield
    filewatch.manager._reset()


async def _next(gen, timeout: float = 5.0) -> FileEvent:
    return await asyncio.wait_for(gen.__anext__(), timeout)


@pytest.mark.asyncio
async def test_watch_dir_reports_created_file(tmp_path: pathlib.Path):
    async with filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}) as gen:
        # Entering the block establishes the subscription; give the OS watch a moment to arm.
        await asyncio.sleep(0.2)

        target = tmp_path / "hello.txt"
        target.write_text("hi")

        event = await _next(gen)
        assert event.kind is FileEventKind.CREATED
        assert event.path == target
        assert not event.is_directory


@pytest.mark.asyncio
async def test_watch_dir_existing_scan(tmp_path: pathlib.Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")

    async with filewatch.watch_dir(
        tmp_path, kinds={FileEventKind.EXISTING}, existing=True, recursive=False
    ) as gen:
        seen = {(await _next(gen)).path, (await _next(gen)).path}

    assert seen == {tmp_path / "a.txt", tmp_path / "b.txt"}


@pytest.mark.asyncio
async def test_watch_dir_non_recursive_filters_subdirs(tmp_path: pathlib.Path):
    sub = tmp_path / "sub"
    sub.mkdir()

    async with filewatch.watch_dir(
        tmp_path, kinds={FileEventKind.CREATED}, recursive=False
    ) as gen:
        await asyncio.sleep(0.2)

        # A nested file should be filtered out; only the top-level one is reported.
        (sub / "deep.txt").write_text("deep")
        await asyncio.sleep(0.3)
        top = tmp_path / "top.txt"
        top.write_text("top")

        assert (await _next(gen)).path == top


@pytest.mark.asyncio
async def test_two_watches_same_dir_share_observer(tmp_path: pathlib.Path):
    real_dir = str(tmp_path.resolve())

    async with (
        filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}) as gen_a,
        filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}) as gen_b,
    ):
        # Both subscriptions are established eagerly on entering the blocks above.
        assert len(filewatch.manager._watches[real_dir].subscribers) == 2

        await asyncio.sleep(0.2)

        target = tmp_path / "shared.txt"
        target.write_text("x")

        assert (await _next(gen_a)).path == target
        assert (await _next(gen_b)).path == target

    # Both gone -> registry empty.
    assert real_dir not in filewatch.manager._watches


@pytest.mark.asyncio
async def test_close_reclaims_emitter(tmp_path: pathlib.Path):
    async with filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}):
        observer = filewatch.manager._observer
        assert observer is not None and observer.emitters

    # Leaving the block drops the registry entry synchronously, but the physical watch is
    # torn down on the reaper thread, so the emitter is only gone once that has drained.
    filewatch.manager._pending.join()

    assert not observer.emitters


@pytest.mark.asyncio
async def test_unsubscribes_stream_that_was_never_iterated(tmp_path: pathlib.Path):
    # Entering subscribes, so a block left before the stream's first __anext__ -- whose
    # body, and therefore any cleanup placed in it, never runs -- must still unsubscribe.
    real_dir = str(tmp_path.resolve())

    async with filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}):
        assert real_dir in filewatch.manager._watches

    assert real_dir not in filewatch.manager._watches


@pytest.mark.asyncio
async def test_unsubscribes_when_block_raises(tmp_path: pathlib.Path):
    real_dir = str(tmp_path.resolve())

    with pytest.raises(RuntimeError):
        async with filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}):
            assert real_dir in filewatch.manager._watches
            raise RuntimeError("boom")

    assert real_dir not in filewatch.manager._watches


@pytest.mark.asyncio
async def test_resubscribe_before_teardown_drains(tmp_path: pathlib.Path):
    # Re-arming the directory races the queued teardown of the watch just released. Since
    # an equal ObservedWatch shares one emitter, a teardown that ignored the race would
    # kill the new subscriber's watch instead.
    async with filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}):
        pass

    async with filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}) as gen:
        filewatch.manager._pending.join()
        await asyncio.sleep(0.2)

        target = tmp_path / "after.txt"
        target.write_text("x")

        assert (await _next(gen)).path == target


@pytest.mark.asyncio
async def test_exit_stack_composes_watches(tmp_path: pathlib.Path):
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()

    async with contextlib.AsyncExitStack() as stack:
        streams = [
            await stack.enter_async_context(
                filewatch.watch_dir(d, kinds={FileEventKind.CREATED})
            )
            for d in (left, right)
        ]
        await asyncio.sleep(0.2)

        (left / "l.txt").write_text("l")
        (right / "r.txt").write_text("r")

        assert (await _next(streams[0])).path == left / "l.txt"
        assert (await _next(streams[1])).path == right / "r.txt"

    assert not filewatch.manager._watches


@pytest.mark.asyncio
async def test_recursive_after_nonrecursive_raises(tmp_path: pathlib.Path):
    async with filewatch.watch_dir(tmp_path, recursive=False):
        # A recursive consumer of a directory already watched non-recursively is rejected,
        # and since subscription happens on entry, the rejection surfaces there.
        with pytest.raises(ValueError):
            async with filewatch.watch_dir(tmp_path, recursive=True):
                pass


@pytest.mark.asyncio
async def test_nonrecursive_consumer_shares_recursive_watch(tmp_path: pathlib.Path):
    real_dir = str(tmp_path.resolve())

    # A recursive directory watch is established first, then a non-recursive watch on the
    # same directory shares it without error (it filters).
    async with (
        filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}, recursive=True) as gen_r,
        filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}, recursive=False) as gen_n,
    ):
        assert filewatch.manager._watches[real_dir].recursive is True
        assert len(filewatch.manager._watches[real_dir].subscribers) == 2

        await asyncio.sleep(0.2)

        target = tmp_path / "f.fits"
        target.write_text("data")

        assert (await _next(gen_r)).path == target
        assert (await _next(gen_n)).path == target


@pytest.mark.asyncio
async def test_wait_for_file_already_exists(tmp_path: pathlib.Path):
    target = tmp_path / "present.txt"
    target.write_text("here")

    await asyncio.wait_for(filewatch.wait_for_file(target), 5.0)


@pytest.mark.asyncio
async def test_wait_for_file_appears(tmp_path: pathlib.Path):
    target = tmp_path / "later.txt"

    waiter = asyncio.ensure_future(filewatch.wait_for_file(target))
    await asyncio.sleep(0.2)
    assert not waiter.done()

    target.write_text("now")
    await asyncio.wait_for(waiter, 5.0)
