# SPDX-License-Identifier: Apache-2.0
"""Sequence batching in the SenpaiAnalyzer's consume loop."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from tests_common import make_result

import sensorkit.senpai.analyzer as analyzer_mod
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.models import SenpaiConfig


def make_analyzer(process_sequence=True):
    """Build an analyzer with the SENPAI pipeline mocked out."""
    config = SenpaiConfig(
        senpai_config="/nonexistent/senpai.yaml",
        senpai_output_dir="/nonexistent/out",
        process_sequence=process_sequence,
    )
    with patch("sensorkit.senpai.analyzer.SenpaiPipeline"):
        analyzer = analyzer_mod.SenpaiAnalyzer(config)

    analyzer._entity = MagicMock()
    analyzer._entity.publish = AsyncMock()
    analyzer._pipeline.process_frames = MagicMock(
        side_effect=lambda inputs, from_sequence=False: [
            make_result(file_path=inp.file_path, from_sequence=from_sequence)
            for inp in inputs
        ]
    )
    return analyzer


def frame(name="frame.fits", **fields):
    """A (context, data) pair as the DataGraph sink would yield it."""
    context = Context({k: v for k, v in fields.items() if v is not None})
    context.set(FileInfo(path=name))
    return context, b"\x00"


async def run(analyzer, items, tail_s=0.0):
    """Feed the analyzer's consume loop a finite stream of frames."""

    async def consume():
        for item in items:
            yield item
        if tail_s:
            # Keep the stream open with no new frames (a stalled sequence).
            await asyncio.sleep(tail_s)

    graph = MagicMock()
    graph.app_sink.return_value.consume = consume
    analyzer._entity.data_graph = AsyncMock(return_value=graph)
    await analyzer._process_frames()


async def run_pushed(analyzer, feed):
    """Feed frames on a schedule through a push-style sink (like the real AppSink).

    `feed` is a list of (delay_s, item); items queue up even while the consume
    loop is blocked processing, mirroring the sink's buffering.
    """
    queue: asyncio.Queue = asyncio.Queue()

    async def producer():
        for delay_s, item in feed:
            await asyncio.sleep(delay_s)
            await queue.put(item)
        await queue.put(None)

    async def consume():
        while True:
            item = await queue.get()
            if item is None:
                return
            yield item

    graph = MagicMock()
    graph.app_sink.return_value.consume = consume
    analyzer._entity.data_graph = AsyncMock(return_value=graph)
    producer_task = asyncio.create_task(producer())
    try:
        await analyzer._process_frames()
    finally:
        producer_task.cancel()


class TestBatching:
    @pytest.mark.asyncio
    async def test_per_frame_when_sequence_processing_off(self):
        analyzer = make_analyzer(process_sequence=False)
        await run(analyzer, [frame(task_id="task-a", frame_num=0, frame_count=3)])

        analyzer._pipeline.process_frames.assert_called_once()
        inputs, from_sequence = analyzer._pipeline.process_frames.call_args.args
        assert len(inputs) == 1
        assert from_sequence is False
        assert analyzer._entity.publish.await_count == 1

    @pytest.mark.asyncio
    async def test_per_frame_without_collect_identity(self):
        analyzer = make_analyzer()
        await run(analyzer, [frame(name="stray.fits")])

        analyzer._pipeline.process_frames.assert_called_once()
        inputs, from_sequence = analyzer._pipeline.process_frames.call_args.args
        assert inputs[0].task_id is None
        assert from_sequence is False
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_batch_closes_on_frame_count(self):
        analyzer = make_analyzer()
        await run(
            analyzer,
            [
                frame(name=f"a{n}.fits", task_id="task-a", frame_num=n, frame_count=3)
                for n in range(3)
            ],
        )

        analyzer._pipeline.process_frames.assert_called_once()
        inputs, from_sequence = analyzer._pipeline.process_frames.call_args.args
        assert [inp.frame_num for inp in inputs] == [0, 1, 2]
        assert from_sequence is True
        assert analyzer._entity.publish.await_count == 3
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_single_frame_collect_is_sequence_derived(self):
        """A complete frame_count=1 collect counts as the sequence result."""
        analyzer = make_analyzer()
        await run(analyzer, [frame(task_id="task-a", frame_num=0, frame_count=1)])

        inputs, from_sequence = analyzer._pipeline.process_frames.call_args.args
        assert len(inputs) == 1
        assert from_sequence is True

    @pytest.mark.asyncio
    async def test_batches_keyed_by_task_id(self):
        analyzer = make_analyzer()
        await run(
            analyzer,
            [
                frame(name="a0.fits", task_id="task-a", frame_num=0, frame_count=2),
                frame(name="b0.fits", task_id="task-b", frame_num=0, frame_count=2),
                frame(name="b1.fits", task_id="task-b", frame_num=1, frame_count=2),
                frame(name="a1.fits", task_id="task-a", frame_num=1, frame_count=2),
            ],
        )

        calls = analyzer._pipeline.process_frames.call_args_list
        assert len(calls) == 2
        first, second = (call.args[0] for call in calls)
        assert {inp.task_id for inp in first} == {"task-b"}
        assert {inp.task_id for inp in second} == {"task-a"}

    @pytest.mark.asyncio
    async def test_stalled_batch_processed_partially(self, monkeypatch):
        # exptime (0.01 s) + margin sets the stall limit; the open stream then
        # goes quiet for much longer than that.
        monkeypatch.setattr(analyzer_mod, "_STALL_MARGIN_S", 0.05)
        analyzer = make_analyzer()
        await run(
            analyzer,
            [frame(task_id="task-a", frame_num=0, frame_count=3, exptime=0.01)],
            tail_s=0.5,
        )

        analyzer._pipeline.process_frames.assert_called_once()
        inputs, from_sequence = analyzer._pipeline.process_frames.call_args.args
        assert len(inputs) == 1
        assert from_sequence is True
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_healthy_batch_not_flushed_early(self, monkeypatch):
        # Stall limit is comfortably longer than the stream's quiet tail: the
        # incomplete batch must still be open (not processed) at stream end.
        monkeypatch.setattr(analyzer_mod, "_STALL_DEFAULT_S", 30.0)
        analyzer = make_analyzer()
        await run(
            analyzer,
            [frame(task_id="task-a", frame_num=0, frame_count=3)],
            tail_s=0.2,
        )

        analyzer._pipeline.process_frames.assert_not_called()
        assert "task-a" in analyzer._batches

    @pytest.mark.asyncio
    async def test_slow_processing_does_not_fragment_other_batches(self, monkeypatch):
        """A frame arriving while SENPAI blocks the loop must not read as a stall.

        Batch B closes and processes for far longer than batch A's stall limit;
        A's second frame lands (and queues) during that run. The sweep must not
        flush A partially — it gets its queued frame first and closes normally.
        """
        monkeypatch.setattr(analyzer_mod, "_STALL_MARGIN_S", 0.05)
        analyzer = make_analyzer()

        def slow_process(inputs, from_sequence=False):
            time.sleep(0.3)  # in to_thread; the event loop stays free
            return [make_result(file_path=inp.file_path) for inp in inputs]

        analyzer._pipeline.process_frames = MagicMock(side_effect=slow_process)

        def seq_frame(name, task, num):
            return frame(name=name, task_id=task, frame_num=num, frame_count=2, exptime=0.01)

        await run_pushed(
            analyzer,
            [
                (0.0, seq_frame("a0.fits", "task-a", 0)),
                (0.0, seq_frame("b0.fits", "task-b", 0)),
                (0.0, seq_frame("b1.fits", "task-b", 1)),
                # Lands while _process(task-b) is still running, well past
                # task-a's ~0.06 s stall limit.
                (0.1, seq_frame("a1.fits", "task-a", 1)),
            ],
        )

        calls = [call.args[0] for call in analyzer._pipeline.process_frames.call_args_list]
        assert sorted(len(inputs) for inputs in calls) == [2, 2]

    @pytest.mark.asyncio
    async def test_numeric_task_identity_coerced(self):
        """A numeric task-id header must still key batches and survive publishing."""
        analyzer = make_analyzer()
        await run(analyzer, [frame(task_id=42, frame_num=0, frame_count=1)])

        inputs, _ = analyzer._pipeline.process_frames.call_args.args
        assert inputs[0].task_id == "42"

    @pytest.mark.asyncio
    async def test_redelivered_frame_ignored(self):
        """A file announced twice must not enter its batch twice.

        A Docker bind mount raises `created` both when the file appears and when
        its data lands. Counting the second copy would close the batch a frame
        early and hand SENPAI two frames with one timestamp — a zero frame gap,
        hence an infinite track rate.
        """
        analyzer = make_analyzer()
        a0 = frame(name="a0.fits", task_id="task-a", frame_num=0, frame_count=3)
        await run(
            analyzer,
            [
                a0,
                a0,  # same file, announced again
                frame(name="a1.fits", task_id="task-a", frame_num=1, frame_count=3),
                frame(name="a2.fits", task_id="task-a", frame_num=2, frame_count=3),
            ],
        )

        analyzer._pipeline.process_frames.assert_called_once()
        inputs, _ = analyzer._pipeline.process_frames.call_args.args
        assert [inp.file_path for inp in inputs] == ["a0.fits", "a1.fits", "a2.fits"]

    @pytest.mark.asyncio
    async def test_redelivery_after_batch_closed_does_not_reopen_it(self):
        """The last frame's second announcement must not open a fresh batch."""
        analyzer = make_analyzer()
        a1 = frame(name="a1.fits", task_id="task-a", frame_num=1, frame_count=2)
        await run(
            analyzer,
            [
                frame(name="a0.fits", task_id="task-a", frame_num=0, frame_count=2),
                a1,  # closes the batch
                a1,  # arrives after it closed
            ],
        )

        analyzer._pipeline.process_frames.assert_called_once()
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_done_memory_stays_bounded(self):
        analyzer = make_analyzer()
        for n in range(analyzer_mod._DONE_MEMORY + 10):
            analyzer._mark_done(f"task-{n}")

        assert len(analyzer._done) == analyzer_mod._DONE_MEMORY
        assert "task-0" not in analyzer._done          # oldest evicted
        assert f"task-{analyzer_mod._DONE_MEMORY + 9}" in analyzer._done

    @pytest.mark.asyncio
    async def test_publish_failure_does_not_drop_remaining_results(self):
        analyzer = make_analyzer()
        analyzer._entity.publish = AsyncMock(side_effect=[RuntimeError("too large"), None])
        await run(
            analyzer,
            [
                frame(name=f"a{n}.fits", task_id="task-a", frame_num=n, frame_count=2)
                for n in range(2)
            ],
        )

        assert analyzer._entity.publish.await_count == 2
