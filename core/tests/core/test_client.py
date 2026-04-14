from __future__ import annotations

import asyncio

import pytest

from sensorkit.core.entity import EntityInfo


@pytest.mark.asyncio
async def test_register_service(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        services = await kit.list_services()
        assert len(services) == 1
        assert sc.entity in services
        assert services[sc.entity].service.info.name == "testservice"
        assert services[sc.entity].service.info.version == "0.1.0"
        assert services[sc.entity].online

        await asyncio.wait_for(sc.shutdown(), 1.0)

        services = await kit.list_services()
        assert len(services) == 1
        assert sc.entity in services
        assert not services[sc.entity].online


@pytest.mark.asyncio
async def test_service_abnormal_shutdown(kit):
    class MyError(Exception):
        pass

    async def service_task():
        raise MyError()

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")

        with pytest.raises(MyError):
            await sc.task_group.create_task(service_task())

        try:
            await sc.join()
        except* MyError:
            pass
        except* BaseException:
            pytest.fail("unexpected exception")
        else:
            pytest.fail("expected exception")


@pytest.mark.asyncio
async def test_list_entities(kit, service_context):
    # Register a couple of entities under the running service context
    async with asyncio.timeout(2.0):
        gen = await service_context.register_entity("generic")
        dev = await service_context.register_device("device")

        # Publish EntityInfo for all so they are fully visible
        await gen.publish_entity_info()
        await dev.publish_entity_info()
        await service_context.publish_entity_info()

        entities = await kit.list_entities()

        # Should at least include the service itself and our two registered entities
        assert gen.entity in entities
        assert dev.entity in entities
        assert service_context.entity in entities

        # Lease values are bytes->str JSON of ServiceInfo; sanity check it's a string
        assert isinstance(entities[gen.entity], EntityInfo)
        assert isinstance(entities[dev.entity], EntityInfo)
