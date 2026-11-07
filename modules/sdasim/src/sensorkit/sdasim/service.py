# SPDX-License-Identifier: Apache-2.0
import sensorkit.api as sk
from sensorkit.sdasim.camera import SdasimCameraConfig

# Each entry in the `sdasim:` config section is a single camera, registered as
# the service's entity. Run separate service processes to isolate CPU-bound
# rendering; `sensorkit go` runs services in threads within one process.
#
#     sdasim:
#       - id: sdasimCameraAlpaca
#         sdasim_config: /opt/sk/scenes/dao01.yaml
#         mount_entity: OmniSimTelescope
#         rotator_entity: OmniSimRotator
#       - id: sdasimCameraPWI4
#         sdasim_config: /opt/sk/scenes/dao01.yaml
#         mount_entity: PWI4Telescope

sk.declare_config_section(
    "sdasim",
    list[SdasimCameraConfig],
    id_source="by_subkey",
    service_path=__name__,
)


@sk.service_entrypoint(version=sk.VERSION)
async def sdasim_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(SdasimCameraConfig)
    # No name -> the camera becomes the service's delegate entity (it shares the
    # service name and lease), so there is no separate child entity.
    service.include(config.create_device())

    await service.run()
