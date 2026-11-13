# SPDX-License-Identifier: Apache-2.0
"""A deployment's configured camera graph writes both science and focus frames.

The Huntsman producer graph and task contexts are read from the tracked deployment, with
only the output directory replaced.
"""

import asyncio
import contextlib
import io
import pathlib
from datetime import UTC, datetime
from uuid import UUID

import numpy as np
import pytest
import yaml
from astropy.io import fits

import sensorkit.api as sk
import sensorkit.data.filesys  # noqa: F401  registers write_file
from sensorkit.astro.common import AltAzPointing, RADecPointing, SitePosition
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.core.task import TaskContextOverlay
from sensorkit.data.context import Context
from sensorkit.data.fits import ArrayInfo
from sensorkit.data.graph import DataGraph
from sensorkit.std.collect import CameraParameterSet, CollectInfo, StandardCollectTask
from sensorkit.std.focus import AutoFocus, FocusPosition, FocusSweepContext
from sensorkit.std.instrument import ExposureInfo
from sensorkit.std.mount import AxisRate, AxisRates, MountAxis

DEPLOYMENT = pathlib.Path(__file__).parents[3] / "deploy" / "huntsman" / "huntsman.yaml"

TASK_ID = UUID("0b8e6a52-91f4-4b7d-8c3e-5d6f7a8b9c0d")

AUTOFOCUS = AutoFocus(instrument="ASI183MM", step=100.0, count=3, integration_time_seconds=1.0)

EXPOSURES = CameraParameterSet(integration_time_seconds=1.0, frame_count=5, filter_name="r")

COLLECT = StandardCollectTask(
    target=AltAzTarget(coords=Horizontal(az=90.0, alt=30.0)), camera_params=EXPOSURES
)


def configured(directory: pathlib.Path) -> tuple[DataGraph, TaskContextOverlay]:
    """Return the camera's producer graph, writing under `directory`, and the task contexts."""
    config = yaml.safe_load(DEPLOYMENT.read_text(encoding="utf-8"))
    producer = next(flow["producer"] for flow in config["data_flow"] if "producer" in flow)
    *ops, write = producer["simple"]
    graph = DataGraph.model_validate({"simple": [*ops, {**write, "directory": str(directory)}]})

    return graph, TaskContextOverlay.model_validate(config["automation"]["contexts"])


def frame(task: sk.Task, contexts: TaskContextOverlay, *keywords) -> Context:
    """Capture context as the controller and camera supply it, for a 1-second light frame."""
    rates = AxisRates(**{axis.value: AxisRate(axis=axis, velocity=0.0) for axis in MountAxis})

    return Context(
        contexts.build().get(task.task_type),
        sk.TaskInfo(task=task, task_id=TASK_ID, controller_id="Huntsman"),
        sk.InstrumentInfo(name="ASI183MM", devices=("ASI183MM",)),
        ExposureInfo(
            date_obs=datetime(2026, 11, 11, 10, tzinfo=UTC),
            exposure_time=1.0,
            detector="ZWO ASI183MM",
        ),
        SitePosition(latitude_degrees=-33.8, longitude_degrees=151.1, altitude_km=0.1),
        AltAzPointing(altitude_degrees=60.0, azimuth_degrees=90.0),
        RADecPointing(right_ascension_hours=6.0, declination_degrees=-30.0),
        rates,
        *keywords,
    )


async def written(device_impl, tmp_path: pathlib.Path, context: Context) -> pathlib.Path:
    """Run one frame through the configured graph and return the file it wrote."""
    graph, _ = configured(tmp_path)
    await device_impl.kv_put_model(graph)
    running = await device_impl.data_graph()
    pixels = np.arange(64, dtype=np.uint16).reshape(8, 8)
    context.set(ArrayInfo.from_array(pixels))

    try:
        running.app_source().produce(context, pixels.tobytes())

        # A file appears before the writer finishes it.
        async with asyncio.timeout(5.0):
            while True:
                for path in tmp_path.glob("*.fits"):
                    with contextlib.suppress(Exception):
                        _header(path)
                        return path

                await asyncio.sleep(0.02)
    finally:
        await running.stop()


def _header(path: pathlib.Path) -> fits.Header:
    """Return the image header of a tile-compressed file."""
    with fits.open(io.BytesIO(path.read_bytes())) as frames:
        return frames[1].header.copy()


@pytest.mark.asyncio
async def test_a_focus_frame_without_collect_metadata_is_written(device_impl, tmp_path):
    _, contexts = configured(tmp_path)
    sweep = FocusSweepContext(
        sweep_id=TASK_ID, sample_index=2, sample_count=3, commanded_position=1000.0
    )
    context = frame(AUTOFOCUS, contexts, sweep, FocusPosition(position=1000.5))

    path = await written(device_impl, tmp_path, context)
    header = _header(path)

    assert path.name == f"{TASK_ID}_focus2.fits"
    assert FocusSweepContext.from_fits_header(header) == sweep
    assert (header["INSTRUME"], header["SKCTRL"], header["FOCPOS"]) == (
        "ASI183MM",
        "Huntsman",
        1000.5,
    )
    assert "FILTER" not in header
    assert "FRAMECNT" not in header
    assert header["SITELAT"] == -33.8


@pytest.mark.asyncio
async def test_a_science_frame_keeps_its_collect_cards_and_name(device_impl, tmp_path):
    _, contexts = configured(tmp_path)
    collect = CollectInfo(target=COLLECT.target, params=EXPOSURES, frame_number=3)

    path = await written(device_impl, tmp_path, frame(COLLECT, contexts, collect))
    header = _header(path)

    assert path.name == f"{TASK_ID}_3.fits"
    assert (header["FILTER"], header["FRAMECNT"], header["FRAMENUM"]) == ("r", 5, 3)
    assert "FOCSWEEP" not in header
