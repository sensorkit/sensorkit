from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.sdasim.camera import SdasimCameraConfig

# Only the camera device type exists today. Kept as a named alias so a
# discriminated union can be introduced here if more device types are added.
type SdasimDeviceConfigs = SdasimCameraConfig


class SdasimConfig(BaseModel):
    """Per-service-instance sdasim configuration.

    Maps entity names to simulated devices, e.g.::

        sdasim:
          - id: Sdasim
            devices:
              SimulatedCamera:
                device_type: camera
                sdasim_config: /opt/sk/scenes/dao01.yaml
                mount_entity: SimulatedMount
                rotator_entity: SimulatedRotator
    """

    devices: dict[str, SdasimDeviceConfigs] = Field(default_factory=dict)


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
