import sensorkit.api as sk
from sensorkit.udl.models import UDLConfig
from sensorkit.udl.program import UDLProgram


@sk.service_entrypoint(version=sk.VERSION)
async def udl_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(UDLConfig)

    service.include(UDLProgram(config))
    await service.run()
