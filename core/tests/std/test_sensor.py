# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from sensorkit.api.declarative import Service, command_handler, declare_device
from sensorkit.astro.common import RADecPointing, SitePosition
from sensorkit.astro.coords import Equatorial, Horizontal
from sensorkit.astro.target import AltAzTarget, ICRSTarget
from sensorkit.core.task import (
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
)
from sensorkit.std import (
    CameraCapture,
    CameraParameterSet,
    Capabilities,
    CloseEnclosure,
    CloseMirrorCover,
    ConfigureCameraSensor,
    Connect,
    Deinit,
    FollowTarget,
    Init,
    OpenEnclosure,
    OpenMirrorCover,
    Sensor,
    SensorConfig,
    SensorControl,
    SensorDevices,
    SensorPolicies,
    SetFilter,
    StandardCollectTask,
    Stop,
)


async def run_service(svc: Service):
    os.environ["SENSORKIT_BACKEND"] = "fake"
    svc_task = asyncio.create_task(svc.run())
    await svc.running
    return svc_task


async def cleanup_service(svc: Service, svc_task: asyncio.Task, sc: SensorControl):
    if hasattr(sc, "_start_pointing_monitor") and sc._start_pointing_monitor:
        sc._start_pointing_monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sc._start_pointing_monitor
    await svc.context.shutdown()
    with contextlib.suppress(asyncio.CancelledError):
        await svc_task


def _site():
    return SitePosition(latitude_degrees=33.0, longitude_degrees=-117.0, altitude_km=0.5)


def _mock_impl():
    """Return a mock ControllerImpl whose control_device() yields AsyncMock clients."""
    impl = AsyncMock()
    clients = {}

    def _control_device(name, *, subscribe=None):
        if name not in clients:
            client = AsyncMock()
            client.command = AsyncMock()
            clients[name] = client
        return clients[name]

    impl.use_device = _control_device
    return impl, clients


def _bypass_pointing_monitor(sc: SensorControl):
    """Cancel the pointing monitor and seed mount_pointing so collect doesn't block."""
    if hasattr(sc, "_start_pointing_monitor") and sc._start_pointing_monitor:
        sc._start_pointing_monitor.cancel()
    done = asyncio.get_event_loop().create_future()
    done.set_result(None)
    sc._start_pointing_monitor = done
    sc.mount_pointing = {
        "ra": 180.0, "dec": 45.0,
        "alt": 60.0, "az": 120.0,
        "ra_rate": 0.001, "dec_rate": 0.002,
        "alt_rate": 0.003, "az_rate": 0.004,
    }


# TODO: Move these to model tests.
def _ra(deg):
    return RADecPointing(right_ascension_hours=deg / 15, declination_degrees=0.0)


def _dec(deg):
    return RADecPointing(right_ascension_hours=0.0, declination_degrees=deg)


def test_ra_to_hms_zero():
    assert _ra(0).ra_hms == "0 0 0.0"


def test_ra_to_hms_positive():
    # 90° RA → 6h 0m 0s
    assert _ra(90.0).ra_hms == "6 0 0.0"


def test_ra_to_hms_negative():
    assert _ra(-15.0).ra_hms == "-1 0 0.0"


def test_ra_to_hms_fractional():
    # 22.5° → 1h 30m 0s
    assert _ra(22.5).ra_hms == "1 30 0.0"


def test_ra_to_hms_full_circle():
    assert _ra(360.0).ra_hms == "24 0 0.0"


def test_ra_to_hms_with_seconds():
    # 15.5° → 1h 2m 0s
    parts = _ra(15.5).ra_hms.split()
    assert int(parts[0]) == 1
    assert int(parts[1]) == 2
    assert float(parts[2]) == pytest.approx(0.0, abs=0.01)


def test_dec_to_dms_zero():
    assert _dec(0).dec_dms == "0 0 0.0"


def test_dec_to_dms_positive_integer():
    assert _dec(45.0).dec_dms == "45 0 0.0"


def test_dec_to_dms_negative_integer():
    assert _dec(-30.0).dec_dms == "-30 0 0.0"


def test_dec_to_dms_positive_with_arcmin():
    # 45.5° → 45° 30' 0"
    assert _dec(45.5).dec_dms == "45 30 0.0"


def test_dec_to_dms_positive_with_arcsec():
    # 45° 30' 30" → 45.508333...°
    parts = _dec(45.0 + 30 / 60 + 30 / 3600).dec_dms.split()
    assert int(parts[0]) == 45
    assert int(parts[1]) == 30
    assert float(parts[2]) == pytest.approx(30.0, abs=0.01)


def test_dec_to_dms_negative_with_fraction():
    parts = _dec(-30.5).dec_dms.split()
    assert int(parts[0]) == -30
    assert int(parts[1]) == 30
    assert float(parts[2]) == pytest.approx(0.0, abs=1e-9)


def test_sensor_devices_required_only():
    d = SensorDevices(mount="m1", camera="c1")
    assert d.mount == "m1"
    assert d.camera == "c1"
    assert d.focuser is None
    assert d.dome is None


def test_sensor_devices_all_optional():
    d = SensorDevices(
        mount="m", camera="c", focuser="f", rotator="r",
        filter_wheel="fw", mirror_cover="mc", dome="d",
    )
    assert d.dome == "d"
    assert d.filter_wheel == "fw"


def test_sensor_policies_defaults():
    p = SensorPolicies()
    assert p.concurrent_dome_and_mount_init is False
    assert p.concurrent_dome_and_mount_deinit is False
    assert p.concurrent_dome_init_open is False
    assert p.concurrent_dome_deinit_close is False
    assert p.dome_open_close_timeout == 120.0
    assert p.dome_init_timeout == 300.0
    assert p.dome_deinit_timeout == 300.0
    assert p.mount_init_timeout == 30.0
    assert p.minimum_target_altitude_degrees is None


def test_sensor_policies_custom():
    p = SensorPolicies(
        concurrent_dome_and_mount_init=True,
        mount_init_timeout=60.0,
        minimum_target_altitude_degrees=15.0,
    )
    assert p.concurrent_dome_and_mount_init is True
    assert p.mount_init_timeout == 60.0
    assert p.minimum_target_altitude_degrees == 15.0


def test_sensor_config_minimal():
    cfg = SensorConfig(
        controller_name="s1",
        devices=SensorDevices(mount="m", camera="c"),
        site_position=_site(),
    )
    assert cfg.controller_name == "s1"
    assert isinstance(cfg.policies, SensorPolicies)


def test_sensor_config_with_policies():
    cfg = SensorConfig(
        controller_name="s1",
        devices=SensorDevices(mount="m", camera="c"),
        site_position=_site(),
        policies=SensorPolicies(mount_init_timeout=99.0),
    )
    assert cfg.policies.mount_init_timeout == 99.0


def test_capabilities():
    devices = SensorDevices(mount="m", camera="c")
    cap = Capabilities(tasks=["sensor_init", "sensor_collect"], devices=devices)
    assert cap.type == "controller"
    assert "sensor_init" in cap.tasks


@pytest.mark.asyncio
async def test_sensor_required_devices_only(service_context):
    impl = await service_context.register_controller("ctrl")
    devices = SensorDevices(mount="mount", camera="camera")
    sensor = Sensor(impl, devices, SensorPolicies())
    clients = {str(dev.client.entity): dev.client for dev in impl.all_devices()}

    assert sensor.mount is clients["mount"]
    assert sensor.camera is clients["camera"]
    assert sensor.focuser is None
    assert sensor.dome is None
    assert sensor.mirror_cover is None
    assert sensor.filter_wheel is None
    assert len(impl.all_devices()) == 2


@pytest.mark.asyncio
async def test_sensor_all_devices(service_context):
    impl = await service_context.register_controller("ctrl")
    devices = SensorDevices(
        mount="m", camera="c", focuser="f", rotator="r",
        filter_wheel="fw", mirror_cover="mc", dome="d",
    )
    sensor = Sensor(impl, devices, SensorPolicies())
    assert len(impl.all_devices()) == 7


@pytest.mark.asyncio
async def test_sensor_init_mount():
    impl, clients = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())
    await sensor.init_mount()
    clients["m"].command.assert_awaited_once()
    cmd = clients["m"].command.call_args[0][0]
    assert isinstance(cmd, Init)


@pytest.mark.asyncio
async def test_sensor_deinit_mount():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())
    await sensor.deinit_mount()
    cmd = sensor.mount.command.call_args[0][0]
    assert isinstance(cmd, Deinit)


@pytest.mark.asyncio
async def test_sensor_init_dome_present():
    """Sequential policy: Init fully completes before the aperture is opened."""
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c", dome="d"), SensorPolicies())
    await sensor.init_dome()

    cmds = [call[0][0] for call in sensor.dome.command.call_args_list]
    assert [type(c) for c in cmds] == [Init, OpenEnclosure]


@pytest.mark.asyncio
async def test_sensor_init_dome_concurrent():
    """Concurrent policy: both commands are issued, order unconstrained."""
    impl, _ = _mock_impl()
    sensor = Sensor(
        impl,
        SensorDevices(mount="m", camera="c", dome="d"),
        SensorPolicies(concurrent_dome_init_open=True),
    )
    await sensor.init_dome()

    cmds = [call[0][0] for call in sensor.dome.command.call_args_list]
    assert {type(c) for c in cmds} == {Init, OpenEnclosure}


@pytest.mark.asyncio
async def test_sensor_init_dome_absent():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())
    await sensor.init_dome()  # should be a no-op


@pytest.mark.asyncio
async def test_sensor_deinit_dome_present():
    """Stop is hoisted here so a module Deinit can never abort the close mid-flight."""
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c", dome="d"), SensorPolicies())
    await sensor.deinit_dome()

    cmds = [call[0][0] for call in sensor.dome.command.call_args_list]
    assert [type(c) for c in cmds] == [Stop, CloseEnclosure, Deinit]


@pytest.mark.asyncio
async def test_sensor_deinit_dome_concurrent():
    impl, _ = _mock_impl()
    sensor = Sensor(
        impl,
        SensorDevices(mount="m", camera="c", dome="d"),
        SensorPolicies(concurrent_dome_deinit_close=True),
    )
    await sensor.deinit_dome()

    cmds = [call[0][0] for call in sensor.dome.command.call_args_list]
    assert type(cmds[0]) is Stop
    assert {type(c) for c in cmds[1:]} == {CloseEnclosure, Deinit}


@pytest.mark.asyncio
async def test_sensor_deinit_dome_closes_despite_failed_stop():
    """A dome that rejects Stop must still be closed."""
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c", dome="d"), SensorPolicies())

    async def _command(cmd):
        if isinstance(cmd, Stop):
            raise RuntimeError("stop unsupported")

    sensor.dome.command.side_effect = _command
    await sensor.deinit_dome()

    cmds = [call[0][0] for call in sensor.dome.command.call_args_list]
    assert [type(c) for c in cmds] == [Stop, CloseEnclosure, Deinit]


@pytest.mark.asyncio
async def test_sensor_deinit_dome_absent():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())
    await sensor.deinit_dome()  # should be a no-op


@pytest.mark.asyncio
async def test_sensor_init_mirror_cover_present():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c", mirror_cover="mc"), SensorPolicies())
    await sensor.init_mirror_cover()
    cmd = sensor.mirror_cover.command.call_args[0][0]
    assert isinstance(cmd, OpenMirrorCover)


@pytest.mark.asyncio
async def test_sensor_init_mirror_cover_absent():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())
    await sensor.init_mirror_cover()  # should be a no-op


@pytest.mark.asyncio
async def test_sensor_deinit_mirror_cover_present():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c", mirror_cover="mc"), SensorPolicies())
    await sensor.deinit_mirror_cover()
    cmd = sensor.mirror_cover.command.call_args[0][0]
    assert isinstance(cmd, CloseMirrorCover)


@pytest.mark.asyncio
async def test_sensor_stop_all_required_only():
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())
    await sensor.stop_all()
    cmd = sensor.mount.command.call_args[0][0]
    assert isinstance(cmd, Stop)


@pytest.mark.asyncio
async def test_sensor_stop_all_with_optional():
    impl, _ = _mock_impl()
    sensor = Sensor(
        impl,
        SensorDevices(mount="m", camera="c", dome="d", mirror_cover="mc"),
        SensorPolicies(),
    )
    await sensor.stop_all()
    for dev in (sensor.mount, sensor.dome, sensor.mirror_cover):
        cmd = dev.command.call_args[0][0]
        assert isinstance(cmd, Stop)


@pytest.mark.asyncio
async def test_sensor_stop_all_suppresses_exceptions():
    impl, _ = _mock_impl()
    sensor = Sensor(
        impl,
        SensorDevices(mount="m", camera="c", dome="d"),
        SensorPolicies(),
    )
    sensor.mount.command.side_effect = RuntimeError("mount error")
    sensor.dome.command.side_effect = RuntimeError("dome error")
    # Should not raise
    await sensor.stop_all()


@pytest.mark.asyncio
async def test_sensor_init_all_sequential():
    """Default policies: dome → mount → mirror cover sequentially."""
    impl, _ = _mock_impl()
    policies = SensorPolicies(
        concurrent_dome_and_mount_init=False,
        concurrent_mount_and_mirror_cover_init=False,
    )
    sensor = Sensor(
        impl,
        SensorDevices(mount="m", camera="c", dome="d", mirror_cover="mc"),
        policies,
    )

    async with asyncio.TaskGroup() as tg:
        await sensor.init_all(tg)

    # The dome takes both an Init and an OpenEnclosure.
    assert sensor.dome.command.await_count == 2
    assert sensor.mount.command.await_count == 1
    assert sensor.mirror_cover.command.await_count == 1


@pytest.mark.asyncio
async def test_sensor_init_all_concurrent():
    """With concurrent policies all three init calls still complete."""
    impl, _ = _mock_impl()
    policies = SensorPolicies(
        concurrent_dome_and_mount_init=True,
        concurrent_mount_and_mirror_cover_init=True,
    )
    sensor = Sensor(
        impl,
        SensorDevices(mount="m", camera="c", dome="d", mirror_cover="mc"),
        policies,
    )

    async with asyncio.TaskGroup() as tg:
        await sensor.init_all(tg)

    # The dome takes both an Init and an OpenEnclosure.
    assert sensor.dome.command.await_count == 2
    assert sensor.mount.command.await_count == 1
    assert sensor.mirror_cover.command.await_count == 1


@pytest.mark.asyncio
async def test_sensor_init_all_no_optional_devices():
    """init_all with only mount+camera still inits the mount."""
    impl, _ = _mock_impl()
    sensor = Sensor(impl, SensorDevices(mount="m", camera="c"), SensorPolicies())

    async with asyncio.TaskGroup() as tg:
        await sensor.init_all(tg)

    assert sensor.mount.command.await_count == 1


@pytest.mark.asyncio
async def test_sensor_control_init():
    mount_init_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        mount_init_called.set()

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-init", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        async with asyncio.timeout(5.0):
            await mount_init_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_standby():
    mount_init_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        mount_init_called.set()

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-standby", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            StandbyTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        async with asyncio.timeout(5.0):
            await mount_init_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_shutdown():
    mount_deinit_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        pass

    @command_handler(mount)
    async def handle_deinit(cmd: Deinit):
        mount_deinit_called.set()

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-shutdown", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        await asyncio.sleep(0.1)

        await cli.execute_task(
            ShutdownTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        async with asyncio.timeout(5.0):
            await mount_deinit_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_shutdown_concurrent_dome():
    mount_deinit_called = asyncio.Event()
    dome_close_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")
    dome = declare_device(name="mock-dome")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        pass

    @command_handler(mount)
    async def handle_deinit(cmd: Deinit):
        mount_deinit_called.set()

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    @command_handler(dome)
    async def handle_dome_init(cmd: Init):
        pass

    @command_handler(dome)
    async def handle_dome_deinit(cmd: Deinit):
        pass

    @command_handler(dome)
    async def handle_dome_open(cmd: OpenEnclosure):
        pass

    @command_handler(dome)
    async def handle_dome_close(cmd: CloseEnclosure):
        dome_close_called.set()

    @command_handler(dome)
    async def handle_dome_stop(cmd: Stop):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera", dome="mock-dome"),
        site_position=_site(),
        policies=SensorPolicies(concurrent_dome_and_mount_deinit=True),
    )
    sc = SensorControl(config=config)
    svc = Service("test-shutdown-concurrent", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.add(dome)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        await asyncio.sleep(0.1)

        await cli.execute_task(
            ShutdownTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        async with asyncio.timeout(5.0):
            await mount_deinit_called.wait()
            await dome_close_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_recover():
    connect_called = asyncio.Event()
    stop_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        pass

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        stop_called.set()

    @command_handler(mount)
    async def handle_connect(cmd: Connect):
        connect_called.set()

    @command_handler(camera)
    async def handle_cam_stop(cmd: Stop):
        pass

    @command_handler(camera)
    async def handle_cam_connect(cmd: Connect):
        pass

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-recover", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            RecoverTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        async with asyncio.timeout(5.0):
            await connect_called.wait()
            await stop_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_collect_icrs():
    """Collect with an ICRS target sends FollowTarget, CameraCapture, then Stop."""
    follow_called = asyncio.Event()
    capture_called = asyncio.Event()
    stop_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        pass

    @command_handler(mount)
    async def handle_follow(cmd: FollowTarget):
        follow_called.set()

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        stop_called.set()

    @command_handler(camera)
    async def handle_capture(cmd: CameraCapture):
        capture_called.set()

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-collect", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()

        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        await asyncio.sleep(0.2)

        # Bypass the pointing monitor that requires live keyword streams.
        _bypass_pointing_monitor(sc)

        task = StandardCollectTask(
            task_id=uuid.uuid4(),
            controller_id="test-sensor",
            target=ICRSTarget(coords=Equatorial(ra=180.0, dec=45.0)),
            camera_params=CameraParameterSet(
                integration_time_seconds=1.0,
                frame_count=1,
            ),
            end_time=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
        await cli.execute_task(task)

        async with asyncio.timeout(10.0):
            await follow_called.wait()
            await capture_called.wait()
            await stop_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_collect_with_filter_and_binning():
    """Collect sets filter and binning when camera_params specify them."""
    filter_called = asyncio.Event()
    binning_called = asyncio.Event()
    capture_called = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")
    fw = declare_device(name="mock-fw")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        pass

    @command_handler(mount)
    async def handle_follow(cmd: FollowTarget):
        pass

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    @command_handler(fw)
    async def handle_filter(cmd: SetFilter):
        assert cmd.filter == "Luminance"
        filter_called.set()

    @command_handler(camera)
    async def handle_binning(cmd: ConfigureCameraSensor):
        assert cmd.binning.x == 2
        assert cmd.binning.y == 2
        binning_called.set()

    @command_handler(camera)
    async def handle_capture(cmd: CameraCapture):
        capture_called.set()

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(
            mount="mock-mount",
            camera="mock-camera",
            filter_wheel="mock-fw",
        ),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-collect-opts", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.add(fw)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        await asyncio.sleep(0.2)

        _bypass_pointing_monitor(sc)

        task = StandardCollectTask(
            task_id=uuid.uuid4(),
            controller_id="test-sensor",
            target=AltAzTarget(coords=Horizontal(az=180.0, alt=60.0)),
            camera_params=CameraParameterSet(
                integration_time_seconds=0.5,
                frame_count=1,
                binning_x=2,
                binning_y=2,
                filter_name="Luminance",
            ),
            end_time=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
        await cli.execute_task(task)

        async with asyncio.timeout(10.0):
            await filter_called.wait()
            await binning_called.wait()
            await capture_called.wait()
    finally:
        await cleanup_service(svc, svc_task, sc)


@pytest.mark.asyncio
async def test_sensor_control_collect_multiple_frames():
    """Collect with frame_count=3 sends 3 CameraCapture commands."""
    capture_count = 0
    all_captured = asyncio.Event()

    mount = declare_device(name="mock-mount")
    camera = declare_device(name="mock-camera")

    @command_handler(mount)
    async def handle_init(cmd: Init):
        pass

    @command_handler(mount)
    async def handle_follow(cmd: FollowTarget):
        pass

    @command_handler(mount)
    async def handle_stop(cmd: Stop):
        pass

    @command_handler(camera)
    async def handle_capture(cmd: CameraCapture):
        nonlocal capture_count
        capture_count += 1
        if capture_count == 3:
            all_captured.set()

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-collect-frames", "0.1.0")
    svc.add(mount)
    svc.add(camera)
    svc.include(sc, name="test-sensor")

    svc_task = await run_service(svc)
    try:
        cli = svc.client.controller("test-sensor")
        await cli.enable()
        await cli.execute_task(
            InitTask(task_id=uuid.uuid4(), controller_id="test-sensor")
        )
        await asyncio.sleep(0.2)

        _bypass_pointing_monitor(sc)

        task = StandardCollectTask(
            task_id=uuid.uuid4(),
            controller_id="test-sensor",
            target=ICRSTarget(coords=Equatorial(ra=0.0, dec=0.0)),
            camera_params=CameraParameterSet(
                integration_time_seconds=0.1,
                frame_count=3,
            ),
            end_time=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
        await cli.execute_task(task)

        async with asyncio.timeout(10.0):
            await all_captured.wait()

        assert capture_count == 3
    finally:
        await cleanup_service(svc, svc_task, sc)
