from __future__ import annotations

import asyncio

import pytest


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

        # Publish EntityInfo for both so they are fully visible (not strictly required for leases)
        await gen.publish_entity_info()
        await dev.publish_entity_info()

        entities = await kit.list_entities()

        # Should at least include the service itself and our two registered entities
        assert gen.entity in entities
        assert dev.entity in entities
        assert service_context.entity in entities

        # Lease values are bytes->str JSON of ServiceInfo; sanity check it's a string
        assert isinstance(entities[gen.entity], str)
        assert isinstance(entities[dev.entity], str)


@pytest.mark.asyncio
async def test_list_devices(kit, service_context):
    async with asyncio.timeout(2.0):
        # Register one device and one generic entity
        dev = await service_context.register_device("dev1")
        gen = await service_context.register_entity("gen1")

        # Publish EntityInfo so list_devices can discover from KV
        await dev.publish_entity_info()
        await gen.publish_entity_info()

        listings = await kit.list_devices()

        # Only the device should be listed
        assert any(l.entity == dev.entity for l in listings)
        assert all(l.entity != gen.entity for l in listings)

        # Validate key fields on the found listing
        listing = next(l for l in listings if l.entity == dev.entity)
        assert listing.name == str(dev.entity)
        # With no registered traits, expect empty traits and no archetype
        assert listing.traits == []
        assert listing.archetype is None
