import sensorkit.api as sk
from sensorkit.autofocus.analyzer import AutofocusAnalyzer
from sensorkit.autofocus.models import AutofocusConfig
from sensorkit.autofocus.program import AutofocusProgram


@sk.service_entrypoint(version=sk.VERSION)
async def autofocus_service(service: sk.Service):
    await service.register()

    config = await service.context.kv_get_model(AutofocusConfig)

    analyzer = AutofocusAnalyzer(config, service.client)

    program = AutofocusProgram(config, analyzer)
    analyzer._program = program

    service.include(analyzer, name=config.entity)
    service.include(program, name="autofocus_program")

    await service.run()
