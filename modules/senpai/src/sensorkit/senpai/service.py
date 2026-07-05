# SPDX-License-Identifier: Apache-2.0
import sensorkit.api as sk
from sensorkit.senpai.analyzer import SenpaiAnalyzer
from sensorkit.senpai.models import SenpaiConfig


@sk.service_entrypoint(version=sk.VERSION)
async def senpai_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(SenpaiConfig)

    service.include(SenpaiAnalyzer(config))

    await service.run()
