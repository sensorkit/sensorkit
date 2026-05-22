import sensorkit.api as sk
from sensorkit.otto.models import OttoConfig
from sensorkit.otto.program import OttoProgram


@sk.service_entrypoint(version=sk.VERSION)
async def otto_service(service: sk.Service):
    await service.register()

    service.include(OttoProgram())
    await service.run()
