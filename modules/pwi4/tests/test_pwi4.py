import asyncio
import os
from contextlib import ExitStack

import pytest
from testcontainers.generic.server import DockerContainer
from testcontainers.nats import NatsContainer

import sensorkit.api as sk
from sensorkit.models.devices import (
    AltAz,
    Connect,
    DisableAxis,
    Disconnect,
    EnableAxis,
    FollowTarget,
    Home,
    MountAxis,
    MoveToPark,
    RADec,
    Stop,
)
from sensorkit.pwi4.mount import MountDevice

localonly = pytest.mark.skipif(
    os.getenv("ENV", "").lower() != "local",
    reason="local only test; enable by setting `ENV=local`",
)


@localonly
@pytest.mark.asyncio
async def test_pwi_mount_service():
    with ExitStack() as stack:
        pwi = stack.enter_context(
            DockerContainer(
                "registry.e-o.solutions/dmac/sensorkit/pwi4-simulator:latest"
            ).with_exposed_ports(8220)
        )
        nats = stack.enter_context(NatsContainer().with_command("-js"))
        await asyncio.sleep(5)

        nats_url = nats.nats_uri()
        os.environ["NATS_PORT"] = "4222"
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.nats"
        os.environ["SENSORKIT_BACKEND_ARG"] = nats_url

        pwi_host = pwi.get_container_host_ip()
        pwi_port = pwi.get_exposed_port(8220)

        @sk.service_entrypoint(version="0.0.0")
        async def planewaves_mount_entrypoint(service: sk.Service):
            service.include(
                MountDevice(device_host=pwi_host, device_port=pwi_port),
                name="mount",
            )
            await service.run()

        mount_service, _ = await planewaves_mount_entrypoint.run("mount_service")

        mount_device = mount_service.client.device("mount")

        cmd = await mount_device.command(Connect())
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(EnableAxis(axis=MountAxis.ALTITUDE))
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(EnableAxis(axis=MountAxis.AZIMUTH))
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(Home())
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(MoveToPark())
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(
            FollowTarget(target=AltAz(altitude_degrees=20, azimuth_degrees=90))
        )
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(
            FollowTarget(target=RADec(right_ascension_hours=38, declination_degrees=80))
        )
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(Stop())
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(DisableAxis(axis=MountAxis.ALTITUDE))
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(DisableAxis(axis=MountAxis.AZIMUTH))
        await cmd.done()
        assert cmd.end_event.success

        cmd = await mount_device.command(Disconnect())
        await cmd.done()
        assert cmd.end_event.success

        await mount_service.context.shutdown()
        await mount_service.shutdown.wait()
