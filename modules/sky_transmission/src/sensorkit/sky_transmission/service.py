import sensorkit.api as sk
from sensorkit.sky_transmission.analyzer import SkyTransmissionAnalyzer
from sensorkit.sky_transmission.models import SkyTransmissionConfig


@sk.service_entrypoint(version=sk.VERSION)
async def sky_transmission_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(SkyTransmissionConfig)

    service.include(SkyTransmissionAnalyzer(config), name=config.entity)

    await service.run()
