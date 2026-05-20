from typing import Annotated

from pydantic import BaseModel, Discriminator, Field, model_validator

import sensorkit.api as sk
from sensorkit.pwi4.cover import PWI4CoverConfig
from sensorkit.pwi4.device import PWI4Client
from sensorkit.pwi4.focuser import PWI4FocuserConfig
from sensorkit.pwi4.mount import PWI4MountConfig
from sensorkit.pwi4.rotator import PWI4RotatorConfig

type PWI4DeviceConfigs = Annotated[
    PWI4MountConfig | PWI4FocuserConfig | PWI4RotatorConfig | PWI4CoverConfig,
    Discriminator("device_type"),
]


class PWI4ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 8220
    timeout: float = 60.0
    devices: dict[str, PWI4DeviceConfigs] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _propagate_connection(cls, data):
        if isinstance(data, dict):
            if devices := data.get("devices"):
                for device in devices.values():
                    device.setdefault("host", data.get("host", "localhost"))
                    device.setdefault("port", data.get("port", 8220))
        return data


class PWI4Config(BaseModel):
    endpoints: list[PWI4ServerConfig] = []


@sk.service_entrypoint(version=sk.VERSION)
async def pwi4_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(PWI4Config)
    clients: list[PWI4Client] = []

    for server in config.endpoints:
        client = PWI4Client(
            host=server.host,
            port=server.port,
            timeout=server.timeout,
        )
        clients.append(client)

        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(client), name=entity)

    await service.run()
