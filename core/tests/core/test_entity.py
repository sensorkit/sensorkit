# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from sensorkit.common.keyword import UnknownKeyword, declare_keyword
from sensorkit.core.entity import EntityRef
from sensorkit.backend.event import Event


def test_entity_ref_unset_and_unresolved_behavior():
    # Unset reference returns None and require() raises
    ref = EntityRef()
    assert ref.get() is None
    assert ref() is None

    with pytest.raises(RuntimeError, match="Required entity reference was not defined"):
        _ = ref.require()

    # Named but not resolved yet: get() should raise
    ref2 = EntityRef("my-entity")
    with pytest.raises(RuntimeError, match="Entity reference not resolved"):
        _ = ref2.get()


def test_entity_ref_resolve_and_cache(kit):
    # Given a named reference
    name = "test-entity"
    ref = EntityRef(name)

    # Before resolve, calling get() should raise
    with pytest.raises(RuntimeError):
        _ = ref.get()

    # Resolving binds it to a client
    ref.resolve(kit)
    client1 = ref.get()
    assert client1 is not None

    # Cached on subsequent calls and __call__ proxy works
    client2 = ref()
    assert client2 is client1
    assert ref.require() is client1


def test_entity_ref_subclass_serialization():
    class SubRef(EntityRef):
        pass

    class Holder(BaseModel):
        e: EntityRef
        d: SubRef

    model = Holder(e=EntityRef("e1"), d=SubRef("d1"))

    # Ensure fields are instances of their ref types and names preserved
    assert isinstance(model.e, EntityRef) and model.e.name == "e1"
    assert isinstance(model.d, SubRef) and model.d.name == "d1"

    data = model.model_dump()
    assert data == {"e": "e1", "d": "d1"}

    # JSON roundtrip
    json_data = model.model_dump_json()
    restored = Holder.model_validate_json(json_data)

    # Compare by dumped representation to avoid instance identity differences
    assert restored.model_dump() == data
    assert isinstance(restored.e, EntityRef) and restored.e.name == "e1"
    assert isinstance(restored.d, SubRef) and restored.d.name == "d1"


@pytest.mark.asyncio
async def test_entity_stream(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        ent = await sc.register_entity("myentity")

    barrier = asyncio.Barrier(3)
    good = False
    done = asyncio.Event()

    @declare_keyword
    class MyData(BaseModel):
        my_key: str

    @declare_keyword
    class MyOtherData(BaseModel):
        my_other_key: int

    class UnregisteredData(BaseModel):
        foo: str

    cli = kit.entity("myentity")

    async def monitor_single():
        monitor = await cli.monitor(MyData)

        async with barrier:
            async for subject, data in monitor:
                assert subject.prop == "MyData"
                assert data.my_key == "foobar"

                nonlocal good
                good = True
                break

    async def monitor_all():
        monitor = await cli.monitor_all()

        async with barrier:
            count = 0

            async for subject, data in monitor:
                match data:
                    case MyData():
                        data: MyData
                        assert subject.prop == "MyData"
                        assert data.my_key == "foobar"
                        count += 1
                    case MyOtherData():
                        data: MyOtherData
                        assert subject.prop == "MyOtherData"
                        assert data.my_other_key == 42
                        count += 1
                    case UnknownKeyword():
                        data: UnregisteredData
                        assert data.foo == "bar"
                        count += 1
                    case _:
                        pytest.fail(f"unexpected type {type(data).__name__}")

                if count == 3:
                    break

        done.set()

    async with asyncio.timeout(1.0):
        t1 = asyncio.create_task(monitor_single())
        t2 = asyncio.create_task(monitor_all())

        async with barrier:
            await ent.publish(MyData(my_key="foobar"))
            await ent.publish(MyOtherData(my_other_key=42))
            await ent.publish(UnregisteredData(foo="bar"))
            await t1
            await t2
            await done.wait()

    if not good:
        pytest.fail("not all monitor messages received")


@pytest.mark.asyncio
async def test_entity_event(kit):
    async with asyncio.timeout(1.0):
        sc = await kit.register_service("testservice", "0.1.0")
        ent = await sc.register_entity("myentity")

    ready = asyncio.Event()
    done = asyncio.Event()

    class MyEvent(Event):
        my_key: str

    async def my_monitor():
        monitor = await kit.entity("myentity").monitor_event(MyEvent)
        ready.set()

        async for event in monitor:
            assert event.my_key == "foobar"
            done.set()

    async with asyncio.timeout(1.0):
        _t = asyncio.create_task(my_monitor())
        await ready.wait()

        await ent.emit_event(MyEvent(my_key="foobar"))
        await done.wait()
