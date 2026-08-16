# SPDX-License-Identifier: Apache-2.0
"""Config section + service entrypoint for the Autoslew module.

One Autoslew Alpaca server (host:port) exposes several devices; each is configured
under a ``devices`` mapping and shares the server's connection details, which are
propagated down by the validator below (mirrors the ``alpaca`` module).
"""

from typing import Annotated

from pydantic import BaseModel, Discriminator, Field, model_validator

import sensorkit.api as sk
from sensorkit.autoslew.cover_calibrator import AutoslewCoverCalibratorConfig
from sensorkit.autoslew.dome import AutoslewDomeConfig
from sensorkit.autoslew.focuser import AutoslewFocuserConfig
from sensorkit.autoslew.rotator import AutoslewRotatorConfig
from sensorkit.autoslew.telescope import AutoslewTelescopeConfig
from sensorkit.autoslew.tertiary import AutoslewTertiaryConfig

type AutoslewDeviceConfigs = Annotated[
    AutoslewCoverCalibratorConfig
    | AutoslewDomeConfig
    | AutoslewFocuserConfig
    | AutoslewRotatorConfig
    | AutoslewTelescopeConfig
    | AutoslewTertiaryConfig,
    Discriminator("device_type"),
]


class AutoslewServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 11111
    protocol: str = "http"
    devices: dict[str, AutoslewDeviceConfigs] = Field(default_factory=dict)

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


class AutoslewConfig(BaseModel):
    endpoints: list[AutoslewServerConfig] = []


sk.declare_config_section(
    "autoslew",
    list[AutoslewConfig],
    id_source="by_subkey",
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def autoslew_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(AutoslewConfig)

    for server in config.endpoints:
        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(), name=entity)

    await service.run()
