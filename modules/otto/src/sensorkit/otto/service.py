import sensorkit.api as sk
from sensorkit.otto.models import OttoConfig
from sensorkit.otto.program import OttoProgram


@sk.service_entrypoint(version=sk.VERSION)
async def otto_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(OttoConfig)

    service.include(OttoProgram(config), name=config.entity)
    await service.run()
