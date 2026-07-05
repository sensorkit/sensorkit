# SPDX-License-Identifier: Apache-2.0
import asyncio
import pathlib

import pytest

from sensorkit.common import filewatch
from sensorkit.common.filewatch import FileEvent, FileEventKind


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
    # Awaiting watch_dir establishes the subscription; give the OS watch a moment to arm.
    gen = await filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED})
    await asyncio.sleep(0.2)

    target = tmp_path / "hello.txt"
    target.write_text("hi")

    event = await _next(gen)
    assert event.kind is FileEventKind.CREATED
    assert event.path == target
    assert not event.is_directory

    await gen.aclose()


@pytest.mark.asyncio
async def test_watch_dir_existing_scan(tmp_path: pathlib.Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")

    gen = await filewatch.watch_dir(
        tmp_path, kinds={FileEventKind.EXISTING}, existing=True, recursive=False
    )

    seen = {(await _next(gen)).path, (await _next(gen)).path}
    assert seen == {tmp_path / "a.txt", tmp_path / "b.txt"}

    await gen.aclose()


@pytest.mark.asyncio
async def test_watch_dir_non_recursive_filters_subdirs(tmp_path: pathlib.Path):
    sub = tmp_path / "sub"
    sub.mkdir()

    gen = await filewatch.watch_dir(
        tmp_path, kinds={FileEventKind.CREATED}, recursive=False
    )
    await asyncio.sleep(0.2)

    # A nested file should be filtered out; only the top-level one is reported.
    (sub / "deep.txt").write_text("deep")
    await asyncio.sleep(0.3)
    top = tmp_path / "top.txt"
    top.write_text("top")

    event = await _next(gen)
    assert event.path == top

    await gen.aclose()


@pytest.mark.asyncio
async def test_two_watches_same_dir_share_observer(tmp_path: pathlib.Path):
    gen_a = await filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED})
    gen_b = await filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED})

    # Both subscriptions are established eagerly by the awaits above.
    real_dir = str(tmp_path.resolve())
    assert len(filewatch.manager._watches[real_dir].subscribers) == 2

    await asyncio.sleep(0.2)

    target = tmp_path / "shared.txt"
    target.write_text("x")

    event_a = await _next(gen_a)
    event_b = await _next(gen_b)
    assert event_a.path == target
    assert event_b.path == target

    await gen_a.aclose()
    await gen_b.aclose()

    # Both gone -> registry empty.
    assert real_dir not in filewatch.manager._watches


@pytest.mark.asyncio
async def test_recursive_after_nonrecursive_raises(tmp_path: pathlib.Path):
    gen_a = await filewatch.watch_dir(tmp_path, recursive=False)

    # A recursive consumer of a directory already watched non-recursively is rejected, and
    # since subscription happens during the await, the rejection surfaces there.
    with pytest.raises(ValueError):
        await filewatch.watch_dir(tmp_path, recursive=True)

    await gen_a.aclose()


@pytest.mark.asyncio
async def test_nonrecursive_consumer_shares_recursive_watch(tmp_path: pathlib.Path):
    # A recursive directory watch is established first...
    gen_r = await filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}, recursive=True)

    # ...then a non-recursive watch on the same directory shares it without error (it filters).
    gen_n = await filewatch.watch_dir(tmp_path, kinds={FileEventKind.CREATED}, recursive=False)

    real_dir = str(tmp_path.resolve())
    assert filewatch.manager._watches[real_dir].recursive is True
    assert len(filewatch.manager._watches[real_dir].subscribers) == 2

    await asyncio.sleep(0.2)

    target = tmp_path / "f.fits"
    target.write_text("data")
    event_r = await _next(gen_r)
    event_n = await _next(gen_n)
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
