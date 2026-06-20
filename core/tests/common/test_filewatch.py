import asyncio
import contextlib
import pathlib

import pytest

from sensorkit.common import filewatch
from sensorkit.common.filewatch import FileEvent, FileEventKind


async def _drain_cancel(task, gen):
    """Cancel a primed __anext__ task and finalize its generator."""
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    with contextlib.suppress(Exception):
        await gen.aclose()


@pytest.fixture(autouse=True)
def _reset_manager():
    """Ensure each test starts and ends with a torn-down shared observer."""
    filewatch.manager._reset()
    yield
    filewatch.manager._reset()


async def _next(gen, timeout: float = 5.0) -> FileEvent:
    return await asyncio.wait_for(gen.__anext__(), timeout)


@pytest.mark.asyncio
async def test_watch_dir_reports_created_file(tmp_path: pathlib.Path):
    gen = filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED})

    # Prime the subscription (first __anext__ subscribes, then blocks on the queue).
    task = asyncio.ensure_future(_next(gen))
    await asyncio.sleep(0.2)

    target = tmp_path / "hello.txt"
    target.write_text("hi")

    event = await task
    assert event.kind is FileEventKind.CREATED
    assert event.path == target
    assert not event.is_directory

    await gen.aclose()


@pytest.mark.asyncio
async def test_watch_dir_existing_scan(tmp_path: pathlib.Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")

    gen = filewatch.watch_dir(
        tmp_path, kinds={FileEventKind.EXISTING}, existing=True, recursive=False
    )

    seen = {(await _next(gen)).path, (await _next(gen)).path}
    assert seen == {tmp_path / "a.txt", tmp_path / "b.txt"}

    await gen.aclose()


@pytest.mark.asyncio
async def test_watch_dir_non_recursive_filters_subdirs(tmp_path: pathlib.Path):
    sub = tmp_path / "sub"
    sub.mkdir()

    gen = filewatch.watch_dir(
        tmp_path, kinds={FileEventKind.CREATED}, recursive=False
    )
    task = asyncio.ensure_future(_next(gen))
    await asyncio.sleep(0.2)

    # A nested file should be filtered out; only the top-level one is reported.
    (sub / "deep.txt").write_text("deep")
    await asyncio.sleep(0.3)
    top = tmp_path / "top.txt"
    top.write_text("top")

    event = await task
    assert event.path == top

    await gen.aclose()


@pytest.mark.asyncio
async def test_two_watches_same_dir_share_observer(tmp_path: pathlib.Path):
    gen_a = filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED})
    gen_b = filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED})

    task_a = asyncio.ensure_future(_next(gen_a))
    task_b = asyncio.ensure_future(_next(gen_b))
    await asyncio.sleep(0.2)

    # One physical watch for the directory, two logical subscribers.
    real_dir = str(tmp_path.resolve())
    assert len(filewatch.manager._watches[real_dir].subscribers) == 2

    target = tmp_path / "shared.txt"
    target.write_text("x")

    event_a = await task_a
    event_b = await task_b
    assert event_a.path == target
    assert event_b.path == target

    await gen_a.aclose()
    await gen_b.aclose()

    # Both gone -> registry empty.
    assert real_dir not in filewatch.manager._watches


@pytest.mark.asyncio
async def test_recursive_after_nonrecursive_raises(tmp_path: pathlib.Path):
    gen_a = filewatch.watch_dir(tmp_path, recursive=False)
    task_a = asyncio.ensure_future(_next(gen_a))
    await asyncio.sleep(0.2)  # let gen_a subscribe (recursive=False)

    # A recursive consumer of a directory already watched non-recursively is rejected.
    gen_b = filewatch.watch_dir(tmp_path, recursive=True)
    with pytest.raises(ValueError):
        await gen_b.__anext__()

    await _drain_cancel(task_a, gen_a)


@pytest.mark.asyncio
async def test_nonrecursive_consumer_shares_recursive_watch(tmp_path: pathlib.Path):
    # A recursive directory watch is established first...
    gen_r = filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}, recursive=True)
    task_r = asyncio.ensure_future(_next(gen_r))
    await asyncio.sleep(0.2)

    # ...then a non-recursive watch on the same directory shares it without error (it filters).
    gen_n = filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}, recursive=False)
    task_n = asyncio.ensure_future(_next(gen_n))
    await asyncio.sleep(0.2)

    real_dir = str(tmp_path.resolve())
    assert filewatch.manager._watches[real_dir].recursive is True
    assert len(filewatch.manager._watches[real_dir].subscribers) == 2

    target = tmp_path / "f.fits"
    target.write_text("data")
    event_r = await task_r
    event_n = await task_n
    assert event_r.path == target
    assert event_n.path == target

    await gen_r.aclose()
    await gen_n.aclose()


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
