from typing import Annotated, Union

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.models.devices import Capabilities
from sensorkit.pwi4.api import PWI4, PWI4Connection
from sensorkit.pwi4.focuser import FocuserConfig, FocuserDevice
from sensorkit.pwi4.mirrorcover import MirrorCoverConfig, MirrorCoverDevice
from sensorkit.pwi4.mount import MountConfig, MountDevice
from sensorkit.pwi4.rotator import RotatorConfig, RotatorDevice

# Discriminated union of all device configs
PWI4DeviceConfig = Annotated[
    Union[MountConfig, FocuserConfig, RotatorConfig, MirrorCoverConfig],
    Field(discriminator="device_type")
]

device_map = {
    "mount": MountDevice,
    "focuser": FocuserDevice,
    "rotator": RotatorDevice,
    "mirrorcover": MirrorCoverDevice,
}

class PWI4ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 8220
    devices: list[PWI4DeviceConfig] = Field(default_factory=list)

class PWI4Config(BaseModel):
    endpoints: list[PWI4ServerConfig]

@sk.service_entrypoint(version=sk.VERSION)
async def pwi4_service(service: sk.Service):
    await service.register()

    objs = []

    # Retrieve config from kv
    config = await service.context.kv_get_model(PWI4Config)
    logger.debug(f"Retrieved config for {service.name}: {config=}")

    for server in config.endpoints:
        pwi_conn = PWI4Connection(server.host, server.port, request_timeout=10)
        pwi = PWI4(connection=pwi_conn)
        for device in server.devices:
            cls = device_map[device.device_type]
            instance = cls(pwi, config=device)
            service.include(
                instance,
                name=device.entity,
            )
            objs.append(instance)

    await service.start()

    # TODO: Remove in favor of built-in DeviceInfo once UI is updated.
    for obj in objs:
        decl: sk.DeclaredDevice = sk.decl_for_instance(obj)
        await decl.kv_put_model(
            Capabilities(
                device_type=obj.config.device_type,
                commands=[name for name in decl.impl._handlers.keys()],
            )
        )

    await service.shutdown
