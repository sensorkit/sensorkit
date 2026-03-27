import asyncio
import signal
from contextlib import ExitStack

import uvicorn
from fastapi import FastAPI
from loguru import logger
from testcontainers.core.container import DockerContainer
from testcontainers.nats import NatsContainer

from sensorkit.ascom.camera import CameraService
from sensorkit.ascom.dome import DomeService
from sensorkit.ascom.focuser import FocuserService
from sensorkit.ascom.mount import MountService
from sensorkit.ascom.observing_conditions import ObservingConditionsService
from sensorkit.ascom.rotator import RotatorService
from sensorkit.backend.nats import NATSBackendImpl
from sensorkit.core.client import SensorKit
from sensorkit.models.device.camera import AnyCameraCommand
from sensorkit.models.device.dome import AnyDomeCommand
from sensorkit.models.device.mount import AnyMountCommand


async def run_client(nats_uri: str):
    sk = SensorKit(backend=await NATSBackendImpl.create(nats_uri))
    camera = sk.device("camera")
    mount = sk.device("mount")
    dome = sk.device("dome")
    oc = sk.device("observing_conditions")
    focuser = sk.device("focuser")
    rotator = sk.device("rotator")

    async def dome_status():
        while True:
            dome_monitor = await dome.monitor_all()
            async for k, v in dome_monitor:
                logger.info(f"{k.prop}: {v}")

    async def mount_status():
        while True:
            mount_monitor = await mount.monitor_all()
            async for k, v in mount_monitor:
                logger.info(f"{k.prop}: {v}")

    async def camera_status():
        while True:
            camera_monitor = await camera.monitor_all()
            async for k, v in camera_monitor:
                logger.info(f"{k.prop}: {v}")

    async def oc_status():
        while True:
            oc_monitor = await oc.monitor_all()
            async for k, v in oc_monitor:
                logger.info(f"{k.prop}: {v}")

    async def focus_status():
        while True:
            focus_monitor = await focuser.monitor_all()
            async for k, v in focus_monitor:
                logger.info(f"{k.prop}: {v}")

    async def rotator_status():
        while True:
            rotator_monitor = await rotator.monitor_all()
            async for k, v in rotator_monitor:
                logger.info(f"{k.prop}: {v}")

    asyncio.create_task(dome_status())
    asyncio.create_task(mount_status())
    asyncio.create_task(camera_status())
    asyncio.create_task(oc_status())
    asyncio.create_task(focus_status())
    asyncio.create_task(rotator_status())

    app = FastAPI()

    @app.post("/camera")
    async def camera_command(cmd: AnyCameraCommand):
        logger.info(f"SensorKit Client recieved camera command: {cmd.command_id}")
        c = await camera.command(cmd)
        print(c)

    @app.post("/mount")
    async def mount_command(cmd: AnyMountCommand):
        logger.info(f"SensorKit Client recieved mount command: {cmd.command_id}")
        c = await mount.command(cmd)
        await c.done()
        print(c)

    @app.post("/dome")
    async def dome_command(cmd: AnyDomeCommand):
        logger.info(f"SensorKit Client recieved dome command: {cmd.command_id}")
        c = await dome.command(cmd)
        await c.done()
        print(c)

    await uvicorn.Server(uvicorn.Config(app=app, port=8000)).serve()


async def run_service(nats_uri: str, ascom_uri):
    logger.info("Starting Up")
    sk = SensorKit(backend=await NATSBackendImpl.create(nats_uri))
    sc = await sk.register_service("demo", "0.1.0")

    camera = await CameraService.create(sc, "camera", device_url=ascom_uri)
    mount = await MountService.create(sc, "mount", device_url=ascom_uri)
    dome = await DomeService.create(sc, "dome", device_url=ascom_uri)
    focuser = await FocuserService.create(sc, "focuser", device_url=ascom_uri)
    rotator = await RotatorService.create(sc, "rotator", device_url=ascom_uri)

    observing_conditions = await ObservingConditionsService.create(sc, "observing_conditions", device_url=ascom_uri)

    def shutdown_signal(sig, frame):
        asyncio.run_coroutine_threadsafe(sc.shutdown(), asyncio.get_event_loop())

    signal.signal(signal.SIGINT, shutdown_signal)
    signal.signal(signal.SIGTERM, shutdown_signal)

    # Wait until service is shut down.
    await sc.join()

    logger.info("Shutting Down")

    await asyncio.gather(camera.shutdown(), mount.shutdown(), dome.shutdown(), observing_conditions.shutdown(), focuser.shutdown(), rotator.shutdown())


def main():
    async def async_main():
        with ExitStack() as stack:
            ascom_sim = stack.enter_context(
                DockerContainer(
                    image="registry.e-o.solutions/dmac/sensorkit/sensorkit-infrastructure/ascom-alpaca-simulators:0.0.1"
                )
                .with_command(
                    "dotnet ascom.alpaca.simulators.dll --urls=http://*:32323"
                )
                .with_exposed_ports(32323)
            )
            ascom_sim_url = f"{ascom_sim.get_container_host_ip()}:{ascom_sim.get_exposed_port(32323)}"

            nats = stack.enter_context(NatsContainer().with_command("-js"))
            nats_url = nats.nats_uri()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        run_service(nats_url, ascom_sim_url),
                    ),
                    asyncio.create_task(run_client(nats_url)),
                ),
                return_when=asyncio.FIRST_COMPLETED,
            )

    asyncio.run(async_main())


if __name__ == "__main__":
    main()
