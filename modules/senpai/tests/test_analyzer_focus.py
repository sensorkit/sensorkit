# SPDX-License-Identifier: Apache-2.0
"""Focus sweep analysis by a running SenpaiAnalyzer, using the real engine."""

import importlib.resources
import io
from datetime import UTC, datetime
from uuid import UUID

import pytest
import pytest_asyncio
from astropy.io import fits

import sensorkit.api as sk
import sensorkit.senpai.analyzer as analyzer_mod
import sensorkit.senpai.processors as processors_mod
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo
from sensorkit.data.graph import DataGraph
from sensorkit.data.local import AppSink, AppSource
from sensorkit.senpai.models import SenpaiConfig, SenpaiResult
from sensorkit.std.focus import (
    AutoFocus,
    FocusAnalysis,
    FocusPosition,
    FocusProvider,
    FocusSweepAnalysis,
    FocusSweepContext,
)
from sensorkit.std.instrument import ExposureInfo

from .images import star_field_fits

SWEEP_ID = UUID("6f1c2b7e-3d4a-4c1b-9e2f-0a1b2c3d4e5f")
TASK_ID = UUID("0b8e6a52-91f4-4b7d-8c3e-5d6f7a8b9c0d")
POSITIONS = [1000.0, 1050.0, 1100.0, 1150.0, 1200.0, 1250.0, 1300.0]
BEST = 1140.0


def star_width(position: float) -> float:
    """Width of the synthetic stars at a focuser position, narrowest at `BEST`."""
    return 3.0 + ((position - BEST) / 75.0) ** 2


def focus_frame(
    index: int,
    position: float = 1000.0,
    *,
    count: int = len(POSITIONS),
    controller: str | None = "ctrl-a",
    instrument: str | None = "cam-east",
    exposure: bool = True,
    data: bytes | None = None,
):
    """A sweep frame with typed capture metadata and a FITS image."""
    context = Context(
        FileInfo(path=f"{controller}-focus-{index}.fits"),
        FocusSweepContext(
            sweep_id=SWEEP_ID,
            sample_index=index,
            sample_count=count,
            commanded_position=position,
        ),
        FocusPosition(position=position + 0.5),
    )

    if controller is not None:
        context.set_value("controller_name", controller)

    if instrument is not None:
        context.set(sk.InstrumentInfo(name=instrument, devices=(instrument,)))

    if exposure:
        context.set(
            ExposureInfo(
                date_obs=datetime(2026, 11, 10, 8, 30, tzinfo=UTC),
                exposure_time=0.01,
                detector="QHY600M",
            )
        )

    return context, star_field_fits(star_width(position)) if data is None else data


def fits_focus_frame(index: int, position: float = 1000.0, *, count: int = len(POSITIONS)):
    """A sweep frame whose capture metadata travels only as FITS cards, as read from a file."""
    task = AutoFocus(instrument="cam-east", positions=POSITIONS, integration_time_seconds=0.01)
    written = (
        FocusSweepContext(
            sweep_id=SWEEP_ID, sample_index=index, sample_count=count, commanded_position=position
        ),
        sk.TaskInfo(task=task, task_id=SWEEP_ID, controller_id="ctrl-a"),
        sk.InstrumentInfo(name="cam-east", devices=("foc", "cam")),
        ExposureInfo(
            date_obs=datetime(2026, 11, 10, 8, 30, tzinfo=UTC),
            exposure_time=0.01,
            detector="QHY600M",
        ),
        FocusPosition(position=position + 0.5),
    )
    cards = [card for keyword in written for card in keyword.get_fits_cards()]

    return (
        Context(FileInfo(path=f"fits-focus-{index}.fits")),
        star_field_fits(star_width(position), cards=cards),
    )


@pytest_asyncio.fixture
async def senpai(kit, service, tmp_path, recorder):
    """Run the analyzer as a service entity and return a feeder for its DataGraph."""
    engine_config = importlib.resources.files("senpai") / "resources" / "config" / "local.yaml"
    analyzer = analyzer_mod.SenpaiAnalyzer(
        SenpaiConfig(senpai_config=str(engine_config), senpai_output_dir=str(tmp_path))
    )

    await kit.entity("senpai").kv_put_model(
        DataGraph(nodes={"source": AppSource(output=["sink"]), "sink": AppSink()})
    )
    service.include(analyzer, name="senpai")
    await service.start()

    graph = await analyzer._entity.data_graph()
    published = await recorder("senpai")

    def feed(*frames):
        for context, data in frames:
            graph.app_source().produce(context, data)

        return published

    feed.analyzer = analyzer

    return feed


@pytest.mark.asyncio
async def test_entity_is_a_focus_provider(kit, senpai):
    info = await kit.entity("senpai").kv_get_model(sk.EntityInfo)

    assert FocusProvider.match(info.details)
    assert "SenpaiResult" in info.details.published_keywords


@pytest.mark.asyncio
async def test_sweep_locates_best_focus(senpai):
    order = [3, 0, 6, 1, 5, 2, 4]
    published = senpai(*(focus_frame(index, POSITIONS[index]) for index in order))

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert (result.outcome, result.disposition) == ("success", "equals")
    assert result.position == pytest.approx(BEST, abs=15.0)
    assert result.controller_id == sk.ControllerRef("ctrl-a")
    assert result.instrument == "cam-east"
    assert [sample.sweep.sample_index for sample in result.samples] == list(range(7))
    assert all(sample.valid for sample in result.samples)
    assert [sample.reported_position for sample in result.samples] == [p + 0.5 for p in POSITIONS]
    assert result.samples[3].median_fwhm_pixels < result.samples[0].median_fwhm_pixels
    assert len(published.of(FocusAnalysis)) == 7
    assert published.of(FocusAnalysis)[0].sweep.sample_index == 3


@pytest.mark.asyncio
async def test_sweep_metadata_is_restored_from_fits_cards(senpai):
    published = senpai(*(fits_focus_frame(index, POSITIONS[index]) for index in range(7)))

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert (result.outcome, result.disposition) == ("success", "equals")
    assert result.position == pytest.approx(BEST, abs=15.0)
    assert (result.controller_id, result.instrument) == (sk.ControllerRef("ctrl-a"), "cam-east")
    assert [sample.reported_position for sample in result.samples] == [p + 0.5 for p in POSITIONS]
    assert {sample.exposure.detector for sample in result.samples} == {"QHY600M"}
    assert published.of(SenpaiResult) == []


def test_sweep_cards_are_read_from_a_compressed_image():
    context, data = fits_focus_frame(2, POSITIONS[2])
    buffer = io.BytesIO()

    with fits.open(io.BytesIO(data)) as frames:
        header = frames[0].header.copy()
        header.strip()
        fits.HDUList(
            [fits.PrimaryHDU(), fits.CompImageHDU(data=frames[0].data, header=header)]
        ).writeto(buffer)

    restored = processors_mod.sweep_context(context, buffer.getvalue())

    assert restored[FocusSweepContext].sample_index == 2
    assert restored[sk.InstrumentInfo].name == "cam-east"
    assert restored["controller_name"] == "ctrl-a"
    assert restored[FocusPosition].position == POSITIONS[2] + 0.5
    assert restored[ExposureInfo].exposure_time == 0.01


def test_frame_without_sweep_cards_is_not_a_sweep_frame():
    context = Context(FileInfo(path="science.fits"))

    assert processors_mod.sweep_context(context, star_field_fits(4.0)) is None


@pytest.mark.asyncio
async def test_typed_sweep_context_outranks_fits_cards(senpai):
    context, data = fits_focus_frame(0, count=1)
    typed, _ = focus_frame(0, count=1, controller="ctrl-typed")
    context.update(typed)

    published = senpai((context, data))
    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert result.controller_id == sk.ControllerRef("ctrl-typed")


@pytest.mark.asyncio
async def test_malformed_fits_sweep_frame_skips_science_analysis(senpai):
    context, _ = fits_focus_frame(0, count=1)
    malformed = star_field_fits(4.0, cards=[("FOCSWEEP", "not-a-uuid")])

    published = senpai((context, malformed), focus_frame(0, count=1))
    await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert published.of(SenpaiResult) == []
    assert len(published.of(FocusAnalysis)) == 1


@pytest.mark.asyncio
async def test_sweep_frames_bypass_science_batching(senpai):
    context, data = focus_frame(0, count=1)
    task = AutoFocus(instrument="cam-east", positions=[1000.0], integration_time_seconds=0.01)
    context.set(sk.TaskInfo(task=task, task_id=TASK_ID, controller_id="ctrl-task"))
    context.set_value("task_id", str(TASK_ID))
    context.set_value("frame_count", 3)

    published = senpai((context, data))
    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    # Task metadata names the controller ahead of the scalar context value.
    assert result.controller_id == sk.ControllerRef("ctrl-task")
    assert published.of(SenpaiResult) == []
    assert not senpai.analyzer._standard.batches


@pytest.mark.asyncio
async def test_duplicate_delivery_is_measured_once(senpai):
    frames = [focus_frame(index, POSITIONS[index]) for index in range(7)]
    published = senpai(frames[0], frames[0], *frames[1:])

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert result.outcome == "success"
    assert len(published.of(FocusAnalysis)) == 7


@pytest.mark.asyncio
async def test_conflicting_sample_fails_sweep(senpai, until):
    published = senpai(
        focus_frame(0, count=3),
        focus_frame(1, count=4),
        focus_frame(2, count=3),
    )

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)
    await until(lambda: not senpai.analyzer._sweeps.sweeps)

    assert (result.outcome, result.position) == ("failure", None)
    assert "expected 3" in result.reason
    assert [sample.sweep.sample_index for sample in result.samples] == [0]
    assert len(published.of(FocusAnalysis)) == 1


@pytest.mark.asyncio
async def test_late_sample_after_terminal_result_is_ignored(senpai):
    published = senpai(focus_frame(0, count=1))
    await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    senpai(focus_frame(0, count=1), focus_frame(0, controller="ctrl-b", count=1))
    await published.wait_for(FocusSweepAnalysis, count=2, timeout=30.0)

    # The second terminal result belongs to the other controller's sweep.
    assert [str(r.controller_id.name) for r in published.of(FocusSweepAnalysis)] == [
        "ctrl-a",
        "ctrl-b",
    ]
    assert len(published.of(FocusAnalysis)) == 2


@pytest.mark.asyncio
async def test_sweeps_of_two_controllers_stay_separate(senpai):
    published = senpai(
        *(
            focus_frame(index, POSITIONS[index], controller=controller)
            for index in range(7)
            for controller in ("ctrl-a", "ctrl-b")
        )
    )

    await published.wait_for(FocusSweepAnalysis, count=2, timeout=60.0)
    results = published.of(FocusSweepAnalysis)

    assert {r.controller_id.name for r in results} == {"ctrl-a", "ctrl-b"}
    assert all(r.disposition == "equals" and len(r.samples) == 7 for r in results)


@pytest.mark.asyncio
async def test_stalled_sweep_fails_with_evidence(senpai, monkeypatch):
    monkeypatch.setattr(processors_mod, "STALL_MARGIN_S", 0.05)

    published = senpai(focus_frame(1), focus_frame(0))
    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert result.outcome == "failure"
    assert "stalled with 2 of 7" in result.reason
    assert [sample.sweep.sample_index for sample in result.samples] == [0, 1]
    assert not senpai.analyzer._sweeps.sweeps


@pytest.mark.asyncio
async def test_missing_exposure_fails_sweep(senpai):
    published = senpai(focus_frame(0, count=3), focus_frame(1, count=3, exposure=False))

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert "sample 1 has no exposure metadata" in result.reason
    assert len(result.samples) == 1


@pytest.mark.asyncio
async def test_unreadable_image_is_an_invalid_sample(senpai):
    published = senpai(focus_frame(0, count=1, data=b"not a FITS image"))

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)
    sample = published.latest(FocusAnalysis)

    assert (sample.valid, sample.median_fwhm_pixels) == (False, None)
    assert sample.reason.startswith("measurement failed")
    assert "got 0" in result.reason
    assert result.samples == [sample]


@pytest.mark.asyncio
async def test_starless_image_is_an_invalid_sample(senpai):
    published = senpai(focus_frame(0, count=1, data=star_field_fits(4.0, stars=0)))

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)
    sample = published.latest(FocusAnalysis)

    assert (sample.valid, sample.median_fwhm_pixels, sample.measurement_count) == (False, None, 0)
    assert sample.reason is not None
    assert result.outcome == "failure"
    assert result.samples == [sample]


@pytest.mark.asyncio
async def test_frame_without_instrument_is_dropped(senpai):
    published = senpai(focus_frame(0, count=1, instrument=None), focus_frame(0, count=1))

    result = await published.wait_for(FocusSweepAnalysis, timeout=30.0)

    assert result.instrument == "cam-east"
    assert len(published.of(FocusSweepAnalysis)) == 1
    assert len(published.of(FocusAnalysis)) == 1
