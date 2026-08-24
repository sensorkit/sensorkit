# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from sensorkit.api import CallError
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


SENSOR_COMMANDS = (
    Init,
    Deinit,
    Stop,
    OpenEnclosure,
    CloseEnclosure,
    OpenMirrorCover,
    CloseMirrorCover,
)
"""Every command a Sensor issues while bringing its devices up and down."""

DOME_DEINIT = [Stop, CloseEnclosure, Deinit]
"""What a dome receives from deinit_dome when it runs to completion."""

DOME_DEINIT_CUT_SHORT = [Stop, CloseEnclosure]
"""What a dome receives from deinit_dome when it refuses to close."""


class DeviceLog:
    """Records the commands one device received.

    Command types added to `reject` are recorded and then refused, standing in for hardware that
    does not implement them. Commands are also appended to `journal`, which the devices of one
    stack share so that ordering across them can be checked.
    """

    def __init__(self, name: str, journal: list[tuple[str, type]]):
        self.name = name
        self.commands = []
        self.journal = journal
        self.reject: set[type] = set()

    async def handle(self, cmd):
        self.commands.append(cmd)
        self.journal.append((self.name, type(cmd)))

        if type(cmd) in self.reject:
            raise RuntimeError(f"{type(cmd).__name__} unsupported")

    def types(self) -> list[type]:
        """The type of each command received, in order."""
        return [type(cmd) for cmd in self.commands]

    def count(self, command: type) -> int:
        """How many commands of the given type were received."""
        return self.types().count(command)


def recording_device(name: str, journal: list[tuple[str, type]]) -> tuple[declare_device, DeviceLog]:
    """Declare a device that answers every sensor command by recording it."""
    device = declare_device(name=name)
    log = DeviceLog(name, journal)

    for command in SENSOR_COMMANDS:

        async def handler(cmd, _log=log):
            await _log.handle(cmd)

        handler.__annotations__ = {"cmd": command}
        handler.__name__ = f"handle_{name}_{command.__name__}"
        command_handler(device)(handler)

    return device, log


class SensorStack:
    """A running service whose devices record every command a Sensor sends them."""

    DEVICES = ("mount", "camera", "dome", "cover")

    def __init__(self, controller, logs: dict[str, DeviceLog], journal: list[tuple[str, type]]):
        self.controller = controller
        self.logs = logs
        self.journal = journal

    def sensor(self, policies: SensorPolicies | None = None, **devices) -> Sensor:
        """Build a Sensor over the named subset of the running devices."""
        return Sensor(self.controller, SensorDevices(**devices), policies or SensorPolicies())

    def control(self, policies: SensorPolicies | None = None, **devices) -> SensorControl:
        """Build a SensorControl driving the named subset of the running devices.

        SensorControl normally builds its Sensor while attaching to a live controller, which a
        task handler invoked on its own never gets; this supplies the equivalent one.
        """
        policies = policies or SensorPolicies()
        control = SensorControl(
            config=SensorConfig(
                controller_name="ctrl",
                devices=SensorDevices(**devices),
                site_position=_site(),
                policies=policies,
            )
        )
        control.sensor = self.sensor(policies, **devices)
        return control

    def log(self, name: str) -> DeviceLog:
        return self.logs[name]

    def sequence(self) -> list[tuple[str, type]]:
        """Every command every device received, in the order they arrived."""
        return list(self.journal)


@pytest_asyncio.fixture
async def sensor_stack():
    svc = Service("test-sensor-devices", "0.1.0")
    logs = {}
    journal = []

    for name in SensorStack.DEVICES:
        device, log = recording_device(name, journal)
        svc.add(device)
        logs[name] = log

    svc_task = await run_service(svc)
    controller = await svc.context.register_controller("ctrl")

    yield SensorStack(controller, logs, journal)

    await svc.context.shutdown()

    with contextlib.suppress(asyncio.CancelledError):
        await svc_task


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
    Sensor(impl, devices, SensorPolicies())
    assert len(impl.all_devices()) == 7


@pytest.mark.asyncio
async def test_sensor_init_mount(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera")
    await sensor.init_mount()

    assert sensor_stack.log("mount").types() == [Init]


@pytest.mark.asyncio
async def test_sensor_deinit_mount(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera")
    await sensor.deinit_mount()

    assert sensor_stack.log("mount").types() == [Deinit]


@pytest.mark.asyncio
async def test_sensor_init_dome_present(sensor_stack):
    """Sequential policy: Init fully completes before the aperture is opened."""
    sensor = sensor_stack.sensor(mount="mount", camera="camera", dome="dome")
    await sensor.init_dome()

    assert sensor_stack.log("dome").types() == [Init, OpenEnclosure]


@pytest.mark.asyncio
async def test_sensor_init_dome_concurrent(sensor_stack):
    """Concurrent policy: both commands are issued, order unconstrained."""
    sensor = sensor_stack.sensor(
        SensorPolicies(concurrent_dome_init_open=True),
        mount="mount",
        camera="camera",
        dome="dome",
    )
    await sensor.init_dome()

    assert set(sensor_stack.log("dome").types()) == {Init, OpenEnclosure}


@pytest.mark.asyncio
async def test_sensor_init_dome_absent(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera")
    await sensor.init_dome()  # should be a no-op

    assert sensor_stack.log("dome").types() == []


@pytest.mark.asyncio
async def test_sensor_deinit_dome_present(sensor_stack):
    """Stop is hoisted here so a module Deinit can never abort the close mid-flight."""
    sensor = sensor_stack.sensor(mount="mount", camera="camera", dome="dome")
    await sensor.deinit_dome()

    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_deinit_dome_concurrent(sensor_stack):
    sensor = sensor_stack.sensor(
        SensorPolicies(concurrent_dome_deinit_close=True),
        mount="mount",
        camera="camera",
        dome="dome",
    )
    await sensor.deinit_dome()

    issued = sensor_stack.log("dome").types()
    assert issued[0] is Stop
    assert set(issued[1:]) == {CloseEnclosure, Deinit}


@pytest.mark.asyncio
async def test_sensor_deinit_dome_closes_despite_failed_stop(sensor_stack):
    """A dome that rejects Stop must still be closed."""
    sensor_stack.log("dome").reject.add(Stop)

    sensor = sensor_stack.sensor(mount="mount", camera="camera", dome="dome")
    await sensor.deinit_dome()

    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_deinit_dome_absent(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera")
    await sensor.deinit_dome()  # should be a no-op

    assert sensor_stack.log("dome").types() == []


@pytest.mark.asyncio
async def test_sensor_init_mirror_cover_present(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera", mirror_cover="cover")
    await sensor.init_mirror_cover()

    assert sensor_stack.log("cover").types() == [OpenMirrorCover]


@pytest.mark.asyncio
async def test_sensor_init_mirror_cover_absent(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera")
    await sensor.init_mirror_cover()  # should be a no-op

    assert sensor_stack.log("cover").types() == []


@pytest.mark.asyncio
async def test_sensor_deinit_mirror_cover_present(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera", mirror_cover="cover")
    await sensor.deinit_mirror_cover()

    assert sensor_stack.log("cover").types() == [CloseMirrorCover]


@pytest.mark.asyncio
async def test_sensor_stop_all_required_only(sensor_stack):
    sensor = sensor_stack.sensor(mount="mount", camera="camera")
    await sensor.stop_all()

    assert sensor_stack.log("mount").types() == [Stop]


@pytest.mark.asyncio
async def test_sensor_stop_all_with_optional(sensor_stack):
    sensor = sensor_stack.sensor(
        mount="mount", camera="camera", dome="dome", mirror_cover="cover"
    )
    await sensor.stop_all()

    for name in ("mount", "dome", "cover"):
        assert sensor_stack.log(name).types() == [Stop]


@pytest.mark.asyncio
async def test_sensor_stop_all_suppresses_exceptions(sensor_stack):
    sensor_stack.log("mount").reject.add(Stop)
    sensor_stack.log("dome").reject.add(Stop)

    sensor = sensor_stack.sensor(mount="mount", camera="camera", dome="dome")
    # Should not raise
    await sensor.stop_all()

    assert sensor_stack.log("dome").types() == [Stop]


@pytest.mark.asyncio
async def test_sensor_init_all_sequential(sensor_stack):
    """Default policies: dome → mount → mirror cover sequentially."""
    sensor = sensor_stack.sensor(
        SensorPolicies(
            concurrent_dome_and_mount_init=False,
            concurrent_mount_and_mirror_cover_init=False,
        ),
        mount="mount",
        camera="camera",
        dome="dome",
        mirror_cover="cover",
    )

    async with asyncio.TaskGroup() as tg:
        await sensor.init_all(tg)

    # The dome takes both an Init and an OpenEnclosure.
    assert sensor_stack.log("dome").types() == [Init, OpenEnclosure]
    assert sensor_stack.log("mount").types() == [Init]
    assert sensor_stack.log("cover").types() == [OpenMirrorCover]


@pytest.mark.asyncio
async def test_sensor_init_all_concurrent(sensor_stack):
    """With concurrent policies all three init calls still complete."""
    sensor = sensor_stack.sensor(
        SensorPolicies(
            concurrent_dome_and_mount_init=True,
            concurrent_mount_and_mirror_cover_init=True,
        ),
        mount="mount",
        camera="camera",
        dome="dome",
        mirror_cover="cover",
    )

    async with asyncio.TaskGroup() as tg:
        await sensor.init_all(tg)

    # The dome takes both an Init and an OpenEnclosure.
    assert set(sensor_stack.log("dome").types()) == {Init, OpenEnclosure}
    assert sensor_stack.log("mount").types() == [Init]
    assert sensor_stack.log("cover").types() == [OpenMirrorCover]


@pytest.mark.asyncio
async def test_sensor_init_all_no_optional_devices(sensor_stack):
    """init_all with only mount+camera still inits the mount."""
    sensor = sensor_stack.sensor(mount="mount", camera="camera")

    async with asyncio.TaskGroup() as tg:
        await sensor.init_all(tg)

    assert sensor_stack.log("mount").types() == [Init]


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


def shutdown_task():
    return ShutdownTask(task_id=uuid.uuid4(), controller_id="ctrl")


def full_sensor(sensor_stack, policies: SensorPolicies) -> SensorControl:
    """A SensorControl over every device a shutdown touches."""
    return sensor_stack.control(
        policies, mount="mount", camera="camera", dome="dome", mirror_cover="cover"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("always_deinit_dome", [False, True])
@pytest.mark.parametrize("concurrent", [False, True])
async def test_sensor_shutdown_deinits_every_device(sensor_stack, always_deinit_dome, concurrent):
    sc = full_sensor(
        sensor_stack,
        SensorPolicies(
            always_deinit_dome=always_deinit_dome,
            concurrent_dome_and_mount_deinit=concurrent,
        ),
    )
    await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("cover").types() == [CloseMirrorCover]
    assert sensor_stack.log("mount").types() == [Deinit]
    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_shutdown_closes_the_dome_last(sensor_stack):
    """The mirror cover and mount come down before the dome starts closing over them."""
    sc = full_sensor(sensor_stack, SensorPolicies())
    await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.sequence() == [
        ("cover", CloseMirrorCover),
        ("mount", Deinit),
        ("dome", Stop),
        ("dome", CloseEnclosure),
        ("dome", Deinit),
    ]


@pytest.mark.asyncio
async def test_sensor_shutdown_mount_failure_leaves_dome_alone(sensor_stack):
    sensor_stack.log("mount").reject.add(Deinit)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=False))
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("mount").types() == [Deinit]
    assert sensor_stack.log("dome").types() == []


@pytest.mark.asyncio
async def test_sensor_shutdown_mount_failure_still_deinits_dome(sensor_stack):
    """The dome comes down anyway, and the mount failure is still reported."""
    sensor_stack.log("mount").reject.add(Deinit)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=True))
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_shutdown_mirror_cover_failure_stops_shutdown(sensor_stack):
    sensor_stack.log("cover").reject.add(CloseMirrorCover)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=False))
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("mount").types() == []
    assert sensor_stack.log("dome").types() == []


@pytest.mark.asyncio
async def test_sensor_shutdown_mirror_cover_failure_still_deinits_dome(sensor_stack):
    """A stuck mirror cover is the earliest step, so it must not strand the dome open."""
    sensor_stack.log("cover").reject.add(CloseMirrorCover)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=True))
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("mount").types() == [Deinit]
    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_shutdown_dome_failure_propagates(sensor_stack):
    sensor_stack.log("dome").reject.add(CloseEnclosure)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=True))
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("mount").types() == [Deinit]
    assert sensor_stack.log("dome").types() == DOME_DEINIT_CUT_SHORT


@pytest.mark.asyncio
async def test_sensor_shutdown_groups_failures_from_several_steps(sensor_stack):
    sensor_stack.log("cover").reject.add(CloseMirrorCover)
    sensor_stack.log("mount").reject.add(Deinit)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=True))
    with pytest.raises(ExceptionGroup) as excinfo:
        await sc.sensor_shutdown(shutdown_task())

    assert len(excinfo.value.exceptions) == 2
    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_shutdown_reports_every_failed_step(sensor_stack):
    sensor_stack.log("cover").reject.add(CloseMirrorCover)
    sensor_stack.log("mount").reject.add(Deinit)
    sensor_stack.log("dome").reject.add(CloseEnclosure)

    sc = full_sensor(sensor_stack, SensorPolicies(always_deinit_dome=True))
    with pytest.raises(ExceptionGroup) as excinfo:
        await sc.sensor_shutdown(shutdown_task())

    assert len(excinfo.value.exceptions) == 3
    assert sensor_stack.log("dome").types() == DOME_DEINIT_CUT_SHORT


@pytest.mark.asyncio
@pytest.mark.parametrize("always_deinit_dome", [False, True])
async def test_sensor_shutdown_concurrent_awaits_dome_despite_mount_failure(
    sensor_stack, always_deinit_dome
):
    """The dome deinit must finish before the handler returns, not run on detached from it."""
    sensor_stack.log("mount").reject.add(Deinit)

    sc = full_sensor(
        sensor_stack,
        SensorPolicies(
            always_deinit_dome=always_deinit_dome,
            concurrent_dome_and_mount_deinit=True,
        ),
    )
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("dome").types() == DOME_DEINIT


@pytest.mark.asyncio
async def test_sensor_shutdown_concurrent_groups_dome_and_mount_failures(sensor_stack):
    sensor_stack.log("mount").reject.add(Deinit)
    sensor_stack.log("dome").reject.add(CloseEnclosure)

    sc = full_sensor(
        sensor_stack,
        SensorPolicies(always_deinit_dome=True, concurrent_dome_and_mount_deinit=True),
    )
    with pytest.raises(ExceptionGroup) as excinfo:
        await sc.sensor_shutdown(shutdown_task())

    assert len(excinfo.value.exceptions) == 2
    assert sensor_stack.log("dome").types() == DOME_DEINIT_CUT_SHORT


@pytest.mark.asyncio
async def test_sensor_shutdown_concurrent_mirror_cover_failure_skips_both(sensor_stack):
    sensor_stack.log("cover").reject.add(CloseMirrorCover)

    sc = full_sensor(
        sensor_stack,
        SensorPolicies(always_deinit_dome=False, concurrent_dome_and_mount_deinit=True),
    )
    with pytest.raises(CallError):
        await sc.sensor_shutdown(shutdown_task())

    assert sensor_stack.log("mount").types() == []
    assert sensor_stack.log("dome").types() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("always_deinit_dome", [False, True])
@pytest.mark.parametrize("concurrent", [False, True])
async def test_sensor_shutdown_cancellation_is_not_absorbed(
    sensor_stack, always_deinit_dome, concurrent
):
    """Cancelling a shutdown must not be recorded as a step failure."""
    sc = full_sensor(
        sensor_stack,
        SensorPolicies(
            always_deinit_dome=always_deinit_dome,
            concurrent_dome_and_mount_deinit=concurrent,
        ),
    )

    reached_mount = asyncio.Event()

    async def hang():
        reached_mount.set()
        await asyncio.Event().wait()

    sc.sensor.deinit_mount = hang

    task = asyncio.create_task(sc.sensor_shutdown(shutdown_task()))
    await reached_mount.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


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


@pytest.mark.asyncio
async def test_sensor_control_collect_with_readout_mode_and_gain():
    """Collect sends readout_mode and gain to the camera when specified."""
    configs: list = []
    got_readout = asyncio.Event()
    got_gain = asyncio.Event()
    capture_called = asyncio.Event()

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
    async def handle_config(cmd: ConfigureCameraSensor):
        configs.append(cmd)
        if cmd.readout_mode is not None:
            got_readout.set()
        if cmd.gain is not None:
            got_gain.set()

    @command_handler(camera)
    async def handle_capture(cmd: CameraCapture):
        capture_called.set()

    config = SensorConfig(
        controller_name="test-sensor",
        devices=SensorDevices(mount="mock-mount", camera="mock-camera"),
        site_position=_site(),
    )
    sc = SensorControl(config=config)
    svc = Service("test-collect-readout-gain", "0.1.0")
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
            target=ICRSTarget(coords=Equatorial(ra=10.0, dec=20.0)),
            camera_params=CameraParameterSet(
                integration_time_seconds=0.5,
                frame_count=1,
                readout_mode=1,
                gain=100.0,
            ),
            end_time=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
        await cli.execute_task(task)

        async with asyncio.timeout(10.0):
            await got_readout.wait()
            await got_gain.wait()
            await capture_called.wait()

        assert [c.readout_mode for c in configs if c.readout_mode is not None] == [1]
        assert [c.gain for c in configs if c.gain is not None] == [100.0]
    finally:
        await cleanup_service(svc, svc_task, sc)
