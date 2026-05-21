from typing import Annotated

from pydantic import BaseModel, Discriminator, Field, model_validator

import sensorkit.api as sk
from sensorkit.alpaca.camera import AlpacaCameraConfig
from sensorkit.alpaca.cover_calibrator import AlpacaCoverCalibratorConfig
from sensorkit.alpaca.dome import AlpacaDomeConfig
from sensorkit.alpaca.filter_wheel import AlpacaFilterWheelConfig
from sensorkit.alpaca.focuser import AlpacaFocuserConfig
from sensorkit.alpaca.observing_conditions import AlpacaObservingConditionsConfig
from sensorkit.alpaca.rotator import AlpacaRotatorConfig
from sensorkit.alpaca.safety_monitor import AlpacaSafetyMonitorConfig
from sensorkit.alpaca.switch import AlpacaSwitchConfig
from sensorkit.alpaca.telescope import AlpacaTelescopeConfig

type AlpacaDeviceConfigs = Annotated[
    AlpacaCameraConfig
    | AlpacaCoverCalibratorConfig
    | AlpacaDomeConfig
    | AlpacaFilterWheelConfig
    | AlpacaFocuserConfig
    | AlpacaObservingConditionsConfig
    | AlpacaRotatorConfig
    | AlpacaSafetyMonitorConfig
    | AlpacaSwitchConfig
    | AlpacaTelescopeConfig,
    Discriminator("device_type"),
]


class AlpacaServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 11111
    protocol: str = "http"
    devices: dict[str, AlpacaDeviceConfigs] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _propagate_connection(cls, data):
        if isinstance(data, dict):
            if devices := data.get("devices"):
                for device in devices.values():
                    device.setdefault("host", data.get("host", "localhost"))
                    device.setdefault("port", data.get("port", 11111))
                    device.setdefault("protocol", data.get("protocol", "http"))
        return data


class AlpacaConfig(BaseModel):
    endpoints: list[AlpacaServerConfig] = []


sk.declare_config_section(
    "alpaca",
    list[AlpacaConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def alpaca_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(AlpacaConfig)

    for server in config.endpoints:
        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(), name=entity)

    await service.run()
