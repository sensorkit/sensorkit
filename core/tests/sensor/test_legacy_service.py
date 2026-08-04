# SPDX-License-Identifier: Apache-2.0
"""System tests for LegacySensor (std controller) with mock device services.

Tests the actual LegacySensor class with mock mount/camera/dome/mirror_cover
devices. All entities run on a single declarative Service with the fake backend.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from sensorkit.api.declarative import Service, command_handler, declare_device
from sensorkit.astro.common import SitePosition
from sensorkit.core.task import InitTask, ShutdownTask
from sensorkit.sensor import SensorConfig, SensorDevices
from sensorkit.sensor.legacy import LegacySensor
from sensorkit.std import Deinit, Init, Stop
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover


async def run_service(svc: Service):
    """Start the service and wait until it's fully running."""
    os.environ["SENSORKIT_BACKEND"] = "fake"
    await svc.start()


@pytest.mark.asyncio
async def test_std_init_and_shutdown():
    """LegacySensor init calls mount Init; shutdown calls mount Deinit."""
    mount_init_called = asyncio.Event()
    mount_deinit_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        mount_init_called.set()

    @command_handler(mount)
    async def handle_deinit(cmd: Deinit):
        mount_deinit_called.set()

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=SitePosition(latitude_degrees=0.0, longitude_degrees=0.0, altitude_km=0.0),
    )
    sc = LegacySensor(config=config)

    svc = Service("test-std", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    await run_service(svc)

    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()

        # InitTask should trigger mount Init.
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )

        async with asyncio.timeout(5.0):
            await mount_init_called.wait()

        # ShutdownTask should trigger mount Deinit.
        await cli.execute_task(
            ShutdownTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )

        async with asyncio.timeout(5.0):
            await mount_deinit_called.wait()

    finally:
        await svc.stop()


@pytest.mark.asyncio
async def test_std_init_with_optional_devices():
    """Init with dome and mirror_cover calls Open on both, shutdown calls Close."""
    dome_open_called = asyncio.Event()
    dome_close_called = asyncio.Event()
    mirror_open_called = asyncio.Event()
    mirror_close_called = asyncio.Event()
    mount_init_called = asyncio.Event()
    mount_deinit_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")
    dome = declare_device(name="mock-dome")
    mirror_cover = declare_device(name="mock-mirror")

    @command_handler(mount)
    async def handle_mount_init(cmd: Init):
        mount_init_called.set()

    @command_handler(mount)
    async def handle_mount_deinit(cmd: Deinit):
        mount_deinit_called.set()

    @command_handler(mount)
    async def handle_mount_stop(cmd: Stop):
        pass

    @command_handler(dome)
    async def handle_dome_init(cmd: Init):
        pass

    @command_handler(dome)
    async def handle_dome_deinit(cmd: Deinit):
        pass

    @command_handler(dome)
    async def handle_dome_open(cmd: OpenEnclosure):
        dome_open_called.set()

    @command_handler(dome)
    async def handle_dome_close(cmd: CloseEnclosure):
        dome_close_called.set()

    @command_handler(dome)
    async def handle_dome_stop(cmd: Stop):
        pass

    @command_handler(mirror_cover)
    async def handle_mirror_open(cmd: OpenMirrorCover):
        mirror_open_called.set()

    @command_handler(mirror_cover)
    async def handle_mirror_close(cmd: CloseMirrorCover):
        mirror_close_called.set()

    @command_handler(mirror_cover)
    async def handle_mirror_stop(cmd: Stop):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(
            mount="mock-mount",
            camera="mock-camera",
            dome="mock-dome",
            mirror_cover="mock-mirror",
        ),
        site_position=SitePosition(latitude_degrees=0.0, longitude_degrees=0.0, altitude_km=0.0),
    )
    sc = LegacySensor(config=config)

    svc = Service("test-std-full", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.add(dome)
    svc.add(mirror_cover)
    svc.include(sc, name="test-sensor")

    await run_service(svc)

    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()

        # InitTask should trigger dome Open, mount Init, mirror_cover Open.
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )

        async with asyncio.timeout(5.0):
            await dome_open_called.wait()
            await mount_init_called.wait()
            await mirror_open_called.wait()

        # ShutdownTask should trigger mirror_cover Close, mount Deinit, dome Close.
        await cli.execute_task(
            ShutdownTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )

        async with asyncio.timeout(5.0):
            await mirror_close_called.wait()
            await mount_deinit_called.wait()
            await dome_close_called.wait()

    finally:
        await svc.stop()
