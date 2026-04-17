import sensorkit.api as sk
from sensorkit.otto.program import OttoProgram


@sk.service_entrypoint(version=sk.VERSION)
async def otto_service(service: sk.Service):
    service.include(OttoProgram(), name="otto_program")
    await service.run()
