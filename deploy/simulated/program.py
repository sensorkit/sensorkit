import random
import uuid
from datetime import UTC, datetime, timedelta

import sensorkit.api as sk
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask


@sk.declare_program
class SimProgram:

    @sk.on_attach
    async def startup(self):
        now = datetime.now(UTC)
        sk.program().add_offer(now, now + timedelta(days=1))
        await sk.program().publish_offers()

    @sk.task_factory
    async def task_factory(self):
        return StandardCollectTask(
            task_id=uuid.uuid1(),
            controller_id="SimulatedSensor",
            target=AltAzTarget(coords=Horizontal(random.randint(0, 30), 85)),
            camera_params=CameraParameterSet(
                integration_time_seconds=5.0,
                frame_count=3,
            ),
            end_time=datetime.now(UTC) + timedelta(minutes=2),
        )


@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(SimProgram)
    await service.run()
