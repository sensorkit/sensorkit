from typing import Annotated

from pydantic import BaseModel, Discriminator, Field, model_validator

import sensorkit.api as sk
from sensorkit.thesky.camera import TheSkyCameraConfig
from sensorkit.thesky.dome import TheSkyDomeConfig
from sensorkit.thesky.filter_wheel import TheSkyFilterWheelConfig
from sensorkit.thesky.focuser import TheSkyFocuserConfig
from sensorkit.thesky.ota import TheSkyOTAConfig
from sensorkit.thesky.telescope import TheSkyTelescopeConfig
from sensorkit.thesky.rotator import TheSkyRotatorConfig
from sensorkit.thesky.weather import TheSkyWeatherConfig

type TheSkyDeviceConfigs = Annotated[
    TheSkyCameraConfig
    | TheSkyDomeConfig
    | TheSkyFocuserConfig
    | TheSkyFilterWheelConfig
    | TheSkyOTAConfig
    | TheSkyTelescopeConfig
    | TheSkyRotatorConfig
    | TheSkyWeatherConfig,
    Discriminator("device_type"),
]


class TheSkyServerConfig(BaseModel):
    host: str
    port: int = 3040
    devices: dict[str, TheSkyDeviceConfigs] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _propagate_host_port(cls, data):
        if isinstance(data, dict):
            if devices := data.get("devices"):
                for device in devices.values():
                    assert "host" not in device and "port" not in device
                    device["host"] = data.get("host")
                    device["port"] = data.get("port")

        return data


class TheSkyConfig(BaseModel):
    endpoints: list[TheSkyServerConfig] = []


@sk.service_entrypoint(version=sk.VERSION)
async def thesky_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(TheSkyConfig)

    for server in config.endpoints:
        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(), name=entity)

    await service.run()
