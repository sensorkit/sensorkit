# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field, model_validator

import sensorkit.api as sk
from sensorkit.indigo.weather import IndigoWeatherConfig

# When more device types are added, use a Discriminator union here:
#   type IndigoDeviceConfigs = Annotated[
#       IndigoWeatherConfig | IndigoMountConfig | ...,
#       Discriminator("device_type"),
#   ]
type IndigoDeviceConfigs = IndigoWeatherConfig


class IndigoServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 7624
    client_name: str = "SensorKit"
    devices: dict[str, IndigoDeviceConfigs] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _propagate_connection(cls, data):
        if isinstance(data, dict):
            if devices := data.get("devices"):
                for device in devices.values():
                    device.setdefault("host", data.get("host", "localhost"))
                    device.setdefault("port", data.get("port", 7624))
                    device.setdefault("client_name", data.get("client_name", "SensorKit"))
        return data


class IndigoConfig(BaseModel):
    endpoints: list[IndigoServerConfig] = []


sk.declare_config_section(
    "indigo",
    list[IndigoConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def indigo_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(IndigoConfig)

    for server in config.endpoints:
        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(), name=entity)

    await service.run()
