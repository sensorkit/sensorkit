# SPDX-License-Identifier: Apache-2.0
"""The sensor service, and the Controller face of a `Sensor`.

One entrypoint serves both implementations. Which one a site runs is a field on
its own configuration entry rather than a second service to register, so the two
are reachable from an unchanged `sensors:` section and rolling back is an edit
rather than a redeploy.
"""

from __future__ import annotations

from collections.abc import Mapping

from loguru import logger

import sensorkit.api as sk
from sensorkit.astro.common import AltAzPointing, RADecPointing
from sensorkit.sensor.client import connect_sensor
from sensorkit.sensor.compat import Capabilities
from sensorkit.sensor.config import Implementation, SensorConfig
from sensorkit.sensor.legacy import LegacySensor
from sensorkit.std.collect import StandardCollectTask
from sensorkit.std.mount import AxisRates
from sensorkit.workflow import DeviceRef

MOUNT_KEYWORDS = [AltAzPointing, RADecPointing, AxisRates]
"""What a frame's header wants from the mount, and what a collect waits on."""


@sk.declare_controller
class StandardSensor:
    """A Controller over a `Sensor`.

    Thin on purpose: the orchestration is the client's, and what this adds is the
    two things a client cannot have — task handlers, and the keyword
    subscriptions a frame's header is built from.
    """

    def __init__(self, config: SensorConfig):
        self.config = config

    @sk.on_attach
    async def controller_init(self):
        controller = sk.controller()

        # TODO: Phase out when UI code is updated to use ControllerInfo and SensorConfig for this
        #       info.
        await controller.kv_put_model(
            Capabilities(
                tasks=[h.__name__ for h in controller._task_handlers.keys()],
                devices=self.config.devices,
            )
        )

        # Registers the subscriptions device_contexts() reads. The clients this
        # returns are the ones connect_sensor resolves for itself, since a
        # SensorKit caches them per entity.
        for ref in self.config.devices.refs():
            controller.use_device(
                ref,
                subscribe=MOUNT_KEYWORDS if ref == self.config.devices.mount else None,
            )

        self.sensor = await connect_sensor(self.config, controller.sensorkit())

        await controller.kv_put_model(self.config.site_position)

    def device_contexts(self) -> Mapping[DeviceRef, sk.Context]:
        """What each device currently reports, one context apiece.

        Left unmerged: which of these belong on a given frame is a question about
        that instrument's optical path, and the sensor is what knows the path.
        """
        controller = sk.controller()

        return {
            ref: controller.get_device(ref).subscription.snapshot()
            for ref in self.config.devices.refs()
        }

    @sk.task_handler
    async def sensor_init(self, task: sk.InitTask):
        """Attempt to start the sensor."""
        await self.sensor.init()
        logger.info(f"Sensor '{sk.controller().entity}' is ready to operate")

    @sk.task_handler
    async def sensor_standby(self, task: sk.StandbyTask):
        """Put the sensor in standby mode."""
        await self.sensor.standby()
        logger.info(f"Sensor '{sk.controller().entity}' is standing by")

    @sk.task_handler
    async def sensor_collect(self, task: StandardCollectTask):
        """Execute a StandardCollectTask: slew, configure camera, capture frames."""
        controller = sk.controller()
        await self.sensor.collect(
            task,
            contexts=self.device_contexts,
            base=await controller.update_context(self.config.site_position),
        )

    @sk.task_handler
    async def sensor_recover(self, task: sk.RecoverTask):
        """Reconnect to all devices and stop any in-progress motion."""
        await self.sensor.recover()

    @sk.task_handler
    async def sensor_shutdown(self, task: sk.ShutdownTask):
        """Shut down the sensor."""
        await self.sensor.shutdown()


sk.declare_config_section(
    "sensors",
    list[SensorConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def sensor_control_service(service: sk.Service):
    await service.register()

    try:
        # Read configuration.
        config = await service.context.kv_get_model(SensorConfig)
    except sk.KVError as e:
        logger.error(f"Service {service.name} could not get SensorConfig ({e})")
        return

    match config.implementation:
        case Implementation.LEGACY:
            control = LegacySensor(config=config)
        case Implementation.WORKFLOW:
            control = StandardSensor(config=config)

    service.include(
        control,
        name=config.controller_name,
    )

    await service.run()
