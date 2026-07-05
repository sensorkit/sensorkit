# SPDX-License-Identifier: Apache-2.0
import sensorkit.api as sk
from sensorkit.udl.program import UDLProgram


@sk.service_entrypoint(version=sk.VERSION)
async def udl_service(service: sk.Service):
    await service.register()

    service.include(UDLProgram())
    await service.run()
