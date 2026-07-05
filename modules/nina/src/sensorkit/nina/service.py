# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from pydantic import BaseModel, Discriminator, Field, model_validator

import sensorkit.api as sk
from sensorkit.nina.camera import NinaCameraConfig
from sensorkit.nina.dome import NinaDomeConfig
from sensorkit.nina.filter_wheel import NinaFilterWheelConfig
from sensorkit.nina.focuser import NinaFocuserConfig
from sensorkit.nina.guider import NinaGuiderConfig
from sensorkit.nina.mount import NinaMountConfig
from sensorkit.nina.rotator import NinaRotatorConfig
from sensorkit.nina.safety_monitor import NinaSafetyMonitorConfig
from sensorkit.nina.switch import NinaSwitchConfig
from sensorkit.nina.weather import NinaWeatherConfig

type NinaDeviceConfigs = Annotated[
    NinaCameraConfig
    | NinaDomeConfig
    | NinaFilterWheelConfig
    | NinaFocuserConfig
    | NinaGuiderConfig
    | NinaMountConfig
    | NinaRotatorConfig
    | NinaSafetyMonitorConfig
    | NinaSwitchConfig
    | NinaWeatherConfig,
    Discriminator("device_type"),
]


class NinaServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 1888
    devices: dict[str, NinaDeviceConfigs] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _propagate_connection(cls, data):
        if isinstance(data, dict):
            if devices := data.get("devices"):
                for device in devices.values():
                    device.setdefault("host", data.get("host", "localhost"))
                    device.setdefault("port", data.get("port", 1888))
        return data


class NinaConfig(BaseModel):
    endpoints: list[NinaServerConfig] = []


@sk.service_entrypoint(version=sk.VERSION)
async def nina_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(NinaConfig)

    for server in config.endpoints:
        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(), name=entity)

    await service.run()
