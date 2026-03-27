from enum import StrEnum
from typing import Annotated, Literal, Union

from loguru import logger
from pydantic import BaseModel, Field

import sensorkit.api as sk
from sensorkit.ascom.camera import CameraService
from sensorkit.ascom.dome import DomeService
from sensorkit.ascom.filter_wheel import FilterWheelService
from sensorkit.ascom.focuser import FocuserService
from sensorkit.ascom.mount import MountService
from sensorkit.ascom.observing_conditions import ObservingConditionsService
from sensorkit.ascom.rotator import RotatorService
from sensorkit.models.devices import Capabilities


class AscomDeviceType(StrEnum):
    CAMERA = "camera"
    COVER_CALIBRATOR = "covercalibrator"
    DOME = "dome"
    FILTER_WHEEL = "filterwheel"
    FOCUSER = "focuser"
    OBSERVING_CONDITIONS = "observingconditions"
    ROTATOR = "rotator"
    SAFETY_MONITOR = "safetymonitor"
    SWITCH = "switch"
    TELESCOPE = "telescope"


# Per-device configuration models
class CameraConfig(BaseModel):
    """Configuration specific to camera devices."""
    device_type: Literal["camera"] = "camera"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0
    timeout: float | None = None

    cooler_on: bool = True
    ccd_temperature_setpoint: float | None = None
    readout_mode: int | str | None = None


class DomeConfig(BaseModel):
    """Configuration specific to dome devices."""
    device_type: Literal["dome"] = "dome"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0


class FilterWheelConfig(BaseModel):
    """Configuration specific to filter wheel devices."""
    device_type: Literal["filterwheel"] = "filterwheel"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0


class FocuserConfig(BaseModel):
    """Configuration specific to focuser devices."""
    device_type: Literal["focuser"] = "focuser"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0


class ObservingConditionsConfig(BaseModel):
    """Configuration specific to observing conditions devices."""
    device_type: Literal["observingconditions"] = "observingconditions"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0


class RotatorConfig(BaseModel):
    """Configuration specific to rotator devices."""
    device_type: Literal["rotator"] = "rotator"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0


class TelescopeConfig(BaseModel):
    """Configuration specific to telescope/mount devices."""
    device_type: Literal["telescope"] = "telescope"
    entity: str
    device_number: int = 0
    status_frequency: float = 1.0


# Discriminated union of all device configs
AscomDeviceConfig = Annotated[
    Union[
        CameraConfig,
        DomeConfig,
        FilterWheelConfig,
        FocuserConfig,
        ObservingConditionsConfig,
        RotatorConfig,
        TelescopeConfig,
    ],
    Field(discriminator="device_type")
]


device_map = {
    "camera": CameraService,
    "dome": DomeService,
    "filterwheel": FilterWheelService,
    "focuser": FocuserService,
    "observingconditions": ObservingConditionsService,
    "rotator": RotatorService,
    "telescope": MountService,
}


class AscomServerConfig(BaseModel):
    host: str
    port: int = 32323
    protocol: Literal["http", "https"] = "http"
    devices: list[AscomDeviceConfig] = Field(default_factory=list)


class AscomConfig(BaseModel):
    endpoints: list[AscomServerConfig]


@sk.service_entrypoint(version=sk.VERSION)
async def ascom_service(service: sk.Service):
    await service.register()

    objs = []

    # Retrieve config from kv
    config = await service.context.kv_get_model(AscomConfig)
    logger.debug(f"Retrieved config for {service.name}: {config=}")

    # Build service from input config.
    for srv in config.endpoints:
        addr = f"{srv.host}:{srv.port}"

        for dev in srv.devices:
            cls = device_map[dev.device_type]
            obj = await cls.create(
                device_url=addr,
                config=dev,
            )
            service.include(
                obj,
                name=dev.entity,
            )
            objs.append((obj, dev.device_type))

    # Run service.
    await service.start()

    # TODO: Remove in favor of built-in DeviceInfo once UI is updated.
    for obj, device_type in objs:
        decl: sk.DeclaredDevice = sk.decl_for_instance(obj)
        await decl.kv_put_model(
            Capabilities(
                device_type=device_type,
                commands=[name for name in decl.impl._handlers.keys()],
            )
        )

    await service.shutdown
