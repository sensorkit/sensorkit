# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest

from sensorkit.core.entity import EntityInfo
from sensorkit.core.impl.entity import EntityImpl


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
async def test_entity_detach_on_shutdown(kit):
    """Entities registered on a service context are detached when the context shuts down."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        ent = await sc.register_entity("e1")

    detached = asyncio.Event()

    @ent.on_detach
    async def on_detach():
        # Detach hooks run within the entity's execution context.
        assert EntityImpl.current.get() is ent
        detached.set()

    async with asyncio.timeout(1.0):
        await sc.shutdown()
        await detached.wait()


@pytest.mark.asyncio
async def test_detach_resilient_to_hook_errors(kit):
    """A failing detach hook on one entity must not prevent others from detaching, and the
    service must still shut down cleanly."""
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        first = await sc.register_entity("first")
        second = await sc.register_entity("second")

    first_detached = asyncio.Event()
    second_detached = asyncio.Event()

    @first.on_detach
    async def first_detach():
        first_detached.set()
        raise RuntimeError("detach boom")

    @second.on_detach
    async def second_detach():
        second_detached.set()

    async with asyncio.timeout(1.0):
        # The failing hook is logged, not propagated, so shutdown does not raise.
        await sc.shutdown()
        await first_detached.wait()
        await second_detached.wait()

    # Intentional shutdown completes without error despite the failing hook.
    await sc.join()


@pytest.mark.asyncio
async def test_detach_continues_after_internal_failure(kit):
    """If one entity's internal detach (detach_impl) raises, the service context logs it and
    still detaches the remaining entities, shutting down cleanly."""

    class FailingDetachImpl(EntityImpl):
        async def detach_impl(self):
            raise RuntimeError("detach_impl boom")

    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        await sc.register_impl(FailingDetachImpl.for_service_context(sc, "failing"))
        ok = await sc.register_entity("ok")

    ok_detached = asyncio.Event()

    @ok.on_detach
    async def ok_detach():
        ok_detached.set()

    async with asyncio.timeout(1.0):
        await sc.shutdown()
        await ok_detached.wait()

    # The fatal detach_impl error is caught by _run_detach; shutdown stays clean.
    await sc.join()


@pytest.mark.asyncio
async def test_list_entities(kit, service_context):
    # Register a couple of entities under the running service context
    async with asyncio.timeout(2.0):
        gen = await service_context.register_entity("generic")
        dev = await service_context.register_device("device")
        entities = await kit.list_entities()

        # Should at least include the service itself and our two registered entities
        assert gen.entity in entities
        assert dev.entity in entities
        assert service_context.entity in entities
        assert isinstance(entities[gen.entity], EntityInfo)
        assert entities[gen.entity].entity_type == "generic"
        assert isinstance(entities[dev.entity], EntityInfo)
        assert entities[dev.entity].entity_type == "device"
        assert isinstance(entities[service_context.entity], EntityInfo)
        assert entities[service_context.entity].entity_type == "generic"
