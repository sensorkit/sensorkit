# SPDX-License-Identifier: Apache-2.0
"""Sequence batching in the SenpaiAnalyzer's consume loop."""

import asyncio
import contextlib
import io

import pytest
from astropy.io import fits

import sensorkit.senpai.analyzer as analyzer_mod
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.senpai.models import SenpaiConfig, SenpaiResult

from .fakes import FakePipeline


@pytest.fixture
def pipeline(monkeypatch) -> FakePipeline:
    """Stand the SENPAI engine down so the analyzer runs against a recording pipeline."""

    fake = FakePipeline()
    monkeypatch.setattr(analyzer_mod, "SenpaiPipeline", lambda *args, **kwargs: fake)

    return fake


@pytest.fixture
def make_analyzer(pipeline, device_impl, data_graph):
    """Return a factory building an analyzer on the live entity and its real DataGraph."""

    def build(process_sequence=True) -> analyzer_mod.SenpaiAnalyzer:
        analyzer = analyzer_mod.SenpaiAnalyzer(
            SenpaiConfig(
                senpai_config="/nonexistent/senpai.yaml",
                senpai_output_dir="/nonexistent/out",
                process_sequence=process_sequence,
            )
        )
        analyzer._entity = device_impl
        return analyzer

    return build


@pytest.fixture
def analyzer(make_analyzer) -> analyzer_mod.SenpaiAnalyzer:
    return make_analyzer()


def frame(name="frame.fits", data=b"\x00", **fields):
    """A (context, data) pair as the camera writing into the graph would produce it."""
    context = Context({k: v for k, v in fields.items() if v is not None})
    context.set(FileInfo(path=name))
    return context, data


def fits_bytes(imagetyp):
    """A minimal FITS payload carrying an IMAGETYP card."""
    hdu = fits.PrimaryHDU()
    hdu.header["IMAGETYP"] = imagetyp
    buf = io.BytesIO()
    hdu.writeto(buf)
    return buf.getvalue()


def feed(graph, *frames):
    """Announce frames on the graph's source; they reach the sink the analyzer consumes."""
    source = graph.app_source()

    for context, data in frames:
        source.produce(context, data)


@contextlib.asynccontextmanager
async def consuming(analyzer):
    """Run the analyzer's consume loop for the duration of the block."""
    task = asyncio.create_task(analyzer._process_frames())

    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def until_runs(pipeline, count, timeout=5.0):
    """Wait until the pipeline has been run `count` times.

    Raises:
        TimeoutError: if it is not run that often in time.
    """
    async with asyncio.timeout(timeout):
        while len(pipeline.runs) < count:
            await asyncio.sleep(0.01)


class TestBatching:
    @pytest.mark.asyncio
    async def test_per_frame_when_sequence_processing_off(
        self, make_analyzer, pipeline, data_graph, recorder
    ):
        analyzer = make_analyzer(process_sequence=False)
        published = await recorder()

        async with consuming(analyzer):
            feed(
                data_graph,
                frame(task_id="task-a", frame_num=0, frame_count=3, controller_name="ctrl-a"),
            )
            await until_runs(pipeline, 1)
            await published.wait_for(SenpaiResult)

        inputs, from_sequence = pipeline.runs[0]
        assert len(inputs) == 1
        assert inputs[0].controller_name == "ctrl-a"
        assert from_sequence is False
        assert len(published.of(SenpaiResult)) == 1

    @pytest.mark.asyncio
    async def test_calibration_frames_skipped(self, analyzer, pipeline, data_graph):
        """dark/bias/flat frames never reach the pipeline; lights still do."""
        async with consuming(analyzer):
            feed(
                data_graph,
                frame(name="d.fits", data=fits_bytes("dark")),
                frame(name="b.fits", data=fits_bytes("bias")),
                frame(name="f.fits", data=fits_bytes("flat")),
                frame(name="l.fits", data=fits_bytes("light")),
            )
            await until_runs(pipeline, 1)

        assert [inp.file_path for inputs, _ in pipeline.runs for inp in inputs] == ["l.fits"]

    @pytest.mark.asyncio
    async def test_per_frame_without_collect_identity(self, analyzer, pipeline, data_graph):
        async with consuming(analyzer):
            feed(data_graph, frame(name="stray.fits"))
            await until_runs(pipeline, 1)

        inputs, from_sequence = pipeline.runs[0]
        assert inputs[0].task_id is None
        assert from_sequence is False
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_batch_closes_on_frame_count(self, analyzer, pipeline, data_graph, recorder):
        published = await recorder()

        async with consuming(analyzer):
            feed(
                data_graph,
                *(
                    frame(name=f"a{n}.fits", task_id="task-a", frame_num=n, frame_count=3)
                    for n in range(3)
                ),
            )
            await until_runs(pipeline, 1)
            await published.wait_for(SenpaiResult, count=3)

        inputs, from_sequence = pipeline.runs[0]
        assert [inp.frame_num for inp in inputs] == [0, 1, 2]
        assert from_sequence is True
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_single_frame_collect_is_sequence_derived(self, analyzer, pipeline, data_graph):
        """A complete frame_count=1 collect counts as the sequence result."""
        async with consuming(analyzer):
            feed(data_graph, frame(task_id="task-a", frame_num=0, frame_count=1))
            await until_runs(pipeline, 1)

        inputs, from_sequence = pipeline.runs[0]
        assert len(inputs) == 1
        assert from_sequence is True

    @pytest.mark.asyncio
    async def test_batches_keyed_by_task_id(self, analyzer, pipeline, data_graph):
        async with consuming(analyzer):
            feed(
                data_graph,
                frame(name="a0.fits", task_id="task-a", frame_num=0, frame_count=2),
                frame(name="b0.fits", task_id="task-b", frame_num=0, frame_count=2),
                frame(name="b1.fits", task_id="task-b", frame_num=1, frame_count=2),
                frame(name="a1.fits", task_id="task-a", frame_num=1, frame_count=2),
            )
            await until_runs(pipeline, 2)

        first, second = (inputs for inputs, _ in pipeline.runs)
        assert {inp.task_id for inp in first} == {"task-b"}
        assert {inp.task_id for inp in second} == {"task-a"}

    @pytest.mark.asyncio
    async def test_stalled_batch_processed_partially(
        self, analyzer, pipeline, data_graph, monkeypatch
    ):
        # exptime (0.01 s) + margin sets the stall limit; nothing follows the frame, so the
        # batch goes quiet for much longer than that.
        monkeypatch.setattr(analyzer_mod, "_STALL_MARGIN_S", 0.05)

        async with consuming(analyzer):
            feed(data_graph, frame(task_id="task-a", frame_num=0, frame_count=3, exptime=0.01))
            await until_runs(pipeline, 1)

        inputs, from_sequence = pipeline.runs[0]
        assert len(inputs) == 1
        assert from_sequence is True
        assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_healthy_batch_not_flushed_early(
        self, analyzer, pipeline, data_graph, monkeypatch
    ):
        # Stall limit is comfortably longer than the quiet spell that follows: the incomplete
        # batch must still be open (not processed) when we look.
        monkeypatch.setattr(analyzer_mod, "_STALL_DEFAULT_S", 30.0)

        async with consuming(analyzer):
            feed(data_graph, frame(task_id="task-a", frame_num=0, frame_count=3))
            await asyncio.sleep(0.2)

            assert pipeline.runs == []
            assert "task-a" in analyzer._batches

    @pytest.mark.asyncio
    async def test_slow_processing_does_not_fragment_other_batches(
        self, analyzer, pipeline, data_graph, monkeypatch
    ):
        """A frame arriving while SENPAI blocks the loop must not read as a stall.

        Batch B closes and processes for far longer than batch A's stall limit; A's second frame
        lands (and queues) during that run. The sweep must not flush A partially — it gets its
        queued frame first and closes normally.
        """
        monkeypatch.setattr(analyzer_mod, "_STALL_MARGIN_S", 0.05)
        pipeline.delay = 0.3

        def seq_frame(name, task, num):
            return frame(name=name, task_id=task, frame_num=num, frame_count=2, exptime=0.01)

        async with consuming(analyzer):
            feed(
                data_graph,
                seq_frame("a0.fits", "task-a", 0),
                seq_frame("b0.fits", "task-b", 0),
                seq_frame("b1.fits", "task-b", 1),
            )
            # Lands while _process(task-b) is still running, well past task-a's ~0.06 s limit.
            await asyncio.sleep(0.1)
            feed(data_graph, seq_frame("a1.fits", "task-a", 1))
            await until_runs(pipeline, 2)

        assert pipeline.batch_sizes() == [2, 2]

    @pytest.mark.asyncio
    async def test_numeric_task_identity_coerced(self, analyzer, pipeline, data_graph):
        """A numeric task-id header must still key batches and survive publishing."""
        async with consuming(analyzer):
            feed(data_graph, frame(task_id=42, frame_num=0, frame_count=1))
            await until_runs(pipeline, 1)

        inputs, _ = pipeline.runs[0]
        assert inputs[0].task_id == "42"

    @pytest.mark.asyncio
    async def test_redelivered_frame_ignored(self, analyzer, pipeline, data_graph):
        """A file announced twice must not enter its batch twice.

        A Docker bind mount raises `created` both when the file appears and when its data lands.
        Counting the second copy would close the batch a frame early and hand SENPAI two frames
        with one timestamp — a zero frame gap, hence an infinite track rate.
        """
        a0 = frame(name="a0.fits", task_id="task-a", frame_num=0, frame_count=3)

        async with consuming(analyzer):
            feed(
                data_graph,
                a0,
                a0,  # same file, announced again
                frame(name="a1.fits", task_id="task-a", frame_num=1, frame_count=3),
                frame(name="a2.fits", task_id="task-a", frame_num=2, frame_count=3),
            )
            await until_runs(pipeline, 1)

        inputs, _ = pipeline.runs[0]
        assert [inp.file_path for inp in inputs] == ["a0.fits", "a1.fits", "a2.fits"]

    @pytest.mark.asyncio
    async def test_redelivery_after_batch_closed_does_not_reopen_it(
        self, analyzer, pipeline, data_graph
    ):
        """The last frame's second announcement must not open a fresh batch."""
        a1 = frame(name="a1.fits", task_id="task-a", frame_num=1, frame_count=2)

        async with consuming(analyzer):
            feed(
                data_graph,
                frame(name="a0.fits", task_id="task-a", frame_num=0, frame_count=2),
                a1,  # closes the batch
                a1,  # arrives after it closed
            )
            await until_runs(pipeline, 1)
            # Let the redelivery be consumed before checking that it opened nothing.
            await asyncio.sleep(0.05)

            assert len(pipeline.runs) == 1
            assert not analyzer._batches

    @pytest.mark.asyncio
    async def test_done_memory_stays_bounded(self, analyzer):
        for n in range(analyzer_mod._DONE_MEMORY + 10):
            analyzer._mark_done(f"task-{n}")

        assert len(analyzer._done) == analyzer_mod._DONE_MEMORY
        assert "task-0" not in analyzer._done  # oldest evicted
        assert f"task-{analyzer_mod._DONE_MEMORY + 9}" in analyzer._done

    @pytest.mark.asyncio
    async def test_publish_failure_does_not_drop_remaining_results(
        self, analyzer, pipeline, data_graph, device_impl, recorder
    ):
        published = await recorder()
        entity_publish = device_impl.publish
        failures = [RuntimeError("payload too large")]

        async def publish(model):
            if failures:
                raise failures.pop()

            return await entity_publish(model)

        device_impl.publish = publish

        async with consuming(analyzer):
            feed(
                data_graph,
                *(
                    frame(name=f"a{n}.fits", task_id="task-a", frame_num=n, frame_count=2)
                    for n in range(2)
                ),
            )
            await until_runs(pipeline, 1)
            result = await published.wait_for(SenpaiResult)

        # The first result's publish raised; the second still reached the stream.
        assert result.file_path == "a1.fits"
        assert len(published.of(SenpaiResult)) == 1
