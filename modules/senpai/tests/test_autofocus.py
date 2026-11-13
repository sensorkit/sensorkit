# SPDX-License-Identifier: Apache-2.0
"""An autofocus task analyzed by a running SenpaiAnalyzer through written FITS files.

The camera writes each frame with a producer DataGraph, and the analyzer reads it back
by watching the directory, so sweep metadata crosses the file as FITS cards.
"""

import asyncio
import contextlib
import importlib.resources
from datetime import UTC, datetime

import pytest
import pytest_asyncio

import sensorkit.senpai.analyzer as analyzer_mod
from sensorkit.astro.common import SitePosition
from sensorkit.core.client import SensorKit
from sensorkit.core.controller import ControllerState
from sensorkit.core.device import DeviceCommand
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.data.context import Context
from sensorkit.data.filesys import FileInfo, ReadFile, WatchDirectory, WriteFile
from sensorkit.data.fits import ArrayInfo, ArrayToFITS, BuildFITSHeader
from sensorkit.data.graph import DataGraph
from sensorkit.data.local import AppSink, AppSource
from sensorkit.senpai.models import SenpaiConfig, SenpaiResult
from sensorkit.sensor.definition import SensorDefinition
from sensorkit.sensor.impl import SensorConfig, SensorController
from sensorkit.sensor.policies import SensorPolicies
from sensorkit.std.focus import (
    AutoFocus,
    ChangeFocusPosition,
    FocusAnalysis,
    FocusAnalysisSource,
    FocusPosition,
    FocusSweepAnalysis,
)
from sensorkit.std.instrument import CameraCapture, ExposureInfo
from sensorkit.std.mount import FollowTarget
from sensorkit.std.traits import Stop

from .images import star_field
from .test_analyzer_focus import BEST, POSITIONS, star_width

LAYOUT = """
    sensor:
      components:
        - device: mount
        - unit: ota
          components:
            - device: foc
            - device: cam
              instrument: true
    """


class Bench:
    """A mount, a focuser and a camera whose stars widen away from `BEST`."""

    def __init__(self):
        self.position = 900.0
        self.received: dict[str, list[DeviceCommand]] = {"mount": [], "foc": [], "cam": []}

    async def serve(self, service_context, raw):
        mount = await service_context.register_device("mount")

        async def point(command: DeviceCommand):
            self.received["mount"].append(command)

        for command in (FollowTarget, Stop):
            mount.command_handler(command)(point)

        await mount.publish_entity_info()
        self.focuser = await service_context.register_device("foc")
        self.focuser.command_handler(ChangeFocusPosition)(self.move)
        self.focuser.declare_published_keyword("FocusPosition")
        await self.focuser.publish_entity_info()
        await self.focuser.publish(FocusPosition(position=self.position))
        camera = await service_context.register_device("cam")
        camera.command_handler(CameraCapture)(self.capture)
        await camera.publish_entity_info()
        await camera.kv_put_model(
            DataGraph(
                nodes={
                    "source": AppSource(output=["header"]),
                    "header": BuildFITSHeader(include="all", output=["fits"]),
                    "fits": ArrayToFITS(output=["write"]),
                    "write": WriteFile(directory=str(raw)),
                }
            )
        )
        self.graph = await camera.data_graph()

    async def move(self, command: ChangeFocusPosition):
        self.received["foc"].append(command)
        self.position = command.position
        await self.focuser.publish(FocusPosition(position=self.position))

    async def capture(self, command: CameraCapture):
        self.received["cam"].append(command)
        pixels = star_field(star_width(self.position))
        context = Context(command.context)
        context.set(
            ExposureInfo(
                date_obs=datetime.now(UTC),
                exposure_time=command.integration_time,
                detector="QHY600M",
                image_type=command.frame_type,
            ),
            ArrayInfo.from_array(pixels),
            FileInfo(path=f"focus-{len(self.received['cam'])}.fits"),
        )
        self.graph.app_source().produce(context, pixels.tobytes())


@pytest_asyncio.fixture
async def service_context(backend):
    """A second service, beside the analyzer's, hosting the bench and the controller."""
    context = await SensorKit(backend).register_service("bench", "0.1.0")

    yield context

    with contextlib.suppress(Exception):
        await context.shutdown()


@pytest_asyncio.fixture
async def bench(kit, service, service_context, tmp_path, recorder):
    """Serve the bench, start the analyzer on its frames, and attach the controller."""
    raw = tmp_path / "raw"
    bench = Bench()
    await bench.serve(service_context, raw)

    engine_config = importlib.resources.files("senpai") / "resources" / "config" / "local.yaml"
    await kit.entity("senpai").kv_put_model(
        DataGraph(
            nodes={
                "source": WatchDirectory(directory=str(raw), match="*.fits", output=["read"]),
                "read": ReadFile(output=["sink"]),
                "sink": AppSink(),
            }
        )
    )
    service.include(
        analyzer_mod.SenpaiAnalyzer(
            SenpaiConfig(
                senpai_config=str(engine_config), senpai_output_dir=str(tmp_path / "senpai")
            )
        ),
        name="senpai",
    )
    await service.start()
    bench.published = await recorder("senpai")

    client = kit.controller("sensor")
    await client.kv_put_model(
        SensorConfig(model=SensorDefinition.from_yaml(LAYOUT).sensor, policies=SensorPolicies())
    )
    await client.kv_put_model(
        SitePosition(latitude_degrees=20.0, longitude_degrees=-100.0, altitude_km=1.0)
    )
    await client.kv_put_model(FocusAnalysisSource(entity="senpai"))
    impl = ControllerImpl.for_service_context(service_context, "sensor")
    controller = SensorController()
    impl.on_attach(controller.on_attach)
    impl.add_task_handler(AutoFocus, execution_callback=controller.autofocus_task)
    await service_context.register_impl(impl)

    async with asyncio.timeout(5.0):
        while impl.get_device("foc").subscription.snapshot().get(FocusPosition) is None:
            await asyncio.sleep(0.01)

    await client.enable()
    bench.client = client

    yield bench

    await bench.graph.stop()


@pytest.mark.asyncio
async def test_autofocus_moves_to_the_best_focus_senpai_finds(bench):
    task = AutoFocus(
        instrument="cam",
        step=50.0,
        count=len(POSITIONS),
        center=(POSITIONS[0] + POSITIONS[-1]) / 2,
        integration_time_seconds=0.01,
    )

    async with asyncio.timeout(120.0):
        await bench.client.execute_task(task)

    state = (await bench.client.kv_get_model(ControllerState)).execution_state
    analysis = state.execution.context[FocusSweepAnalysis]
    *swept, final = [command.position for command in bench.received["foc"]]

    assert swept == POSITIONS
    assert final == analysis.position == pytest.approx(BEST, abs=15.0)
    assert (analysis.outcome, analysis.disposition) == ("success", "equals")
    assert analysis.sweep_id == state.execution.task_id
    assert (analysis.controller_id.name, analysis.instrument) == ("sensor", "cam")
    assert [sample.sweep.commanded_position for sample in analysis.samples] == POSITIONS
    assert {sample.exposure.detector for sample in analysis.samples} == {"QHY600M"}
    assert len(bench.published.of(FocusAnalysis)) == len(POSITIONS)
    assert bench.published.of(SenpaiResult) == []
    assert bench.received["mount"] == []
    assert bench.position == final
