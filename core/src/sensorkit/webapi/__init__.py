# SPDX-License-Identifier: Apache-2.0
import sensorkit.api as sk
from sensorkit.backend.base import KeyNotFound

from .fastapi import WebAPI, WebAPIConfig

sk.declare_config_section(
    "webapi",
    WebAPIConfig,
    entity_mapper=lambda raw: raw.pop("id", "webapi"),
    service_path=__name__,
)


@sk.declare_entity
class WebAPIService:
    @sk.on_attach
    async def startup(self):
        entity = sk.entity()

        try:
            config = await entity.kv_get_model(WebAPIConfig)
        except KeyNotFound:
            config = WebAPIConfig()

        self.webapi = WebAPI(entity.sensorkit(), config)
        entity.task_group.create_task(self.webapi.serve(task_group=entity.task_group))

    @sk.on_detach
    async def shutdown(self):
        await self.webapi.shutdown()


@sk.service_entrypoint(version=sk.VERSION)
async def webapi_service(service: sk.Service):
    service.include(WebAPIService)
    await service.run()
