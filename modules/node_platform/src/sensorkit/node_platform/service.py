from typing import Annotated

from pydantic import BaseModel, Discriminator, Field, model_validator

import sensorkit.api as sk
from sensorkit.node_platform._version import VERSION
from sensorkit.node_platform.enclosure import NodePlatformEnclosureConfig
from sensorkit.node_platform.focuser import NodePlatformFocuserConfig
from sensorkit.node_platform.m3 import NodePlatformM3Config
from sensorkit.node_platform.mirror_cover import NodePlatformMirrorCoverConfig
from sensorkit.node_platform.mount import NodePlatformMountConfig
from sensorkit.node_platform.rotator import NodePlatformRotatorConfig
from sensorkit.node_platform.weather import NodePlatformWeatherConfig

# Type alias for all supported device configs
type NodePlatformDeviceConfigs = Annotated[
    NodePlatformEnclosureConfig
    | NodePlatformFocuserConfig
    | NodePlatformM3Config
    | NodePlatformMirrorCoverConfig
    | NodePlatformMountConfig
    | NodePlatformRotatorConfig
    | NodePlatformWeatherConfig,
    Discriminator("device_type"),
]


class NodePlatformServerConfig(BaseModel):
    host: str
    port: int = 9080
    api_key: str | None = None
    lineage_id: str | None = None
    request_timeout: float = 30.0
    devices: dict[str, NodePlatformDeviceConfigs] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _propagate_connection_settings(cls, data):
        if isinstance(data, dict):
            if devices := data.get("devices"):
                for device in devices.values():
                    assert "host" not in device and "port" not in device
                    assert "api_key" not in device and "lineage_id" not in device
                    assert "request_timeout" not in device
                    device["host"] = data.get("host")
                    device["port"] = data.get("port", 9080)
                    device["api_key"] = data.get("api_key")
                    device["lineage_id"] = data.get("lineage_id")
                    device["request_timeout"] = data.get("request_timeout", 30.0)
        return data


class NodePlatformConfig(BaseModel):
    endpoints: list[NodePlatformServerConfig] = []


@sk.service_entrypoint(version=VERSION)
async def node_platform_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(NodePlatformConfig)

    for server in config.endpoints:
        for entity, device_config in server.devices.items():
            service.include(device_config.create_device(), name=entity)

    await service.run()