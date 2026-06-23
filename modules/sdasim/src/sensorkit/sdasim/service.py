from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.sdasim.camera import SdasimCameraConfig


class SdasimConfig(BaseModel):
    """Per-service-instance sdasim configuration.

    Maps entity names to simulated cameras, e.g.::

        sdasim:
          - id: Sdasim
            devices:
              SimulatedCamera:
                sdasim_config: /opt/sk/scenes/dao01.yaml
                mount_entity: SimulatedMount
                rotator_entity: SimulatedRotator
    """

    devices: dict[str, SdasimCameraConfig] = Field(default_factory=dict)


sk.declare_config_section(
    "sdasim",
    list[SdasimConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def sdasim_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(SdasimConfig)

    for entity, device_config in config.devices.items():
        service.include(device_config.create_device(), name=entity)

    await service.run()
