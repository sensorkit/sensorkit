# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

import sensorkit.api as sk
from sensorkit.backend.event import Event
from sensorkit.backend.request import HandlerError, declare_request
from sensorkit.common.keyword import UnknownKeyword, declare_keyword
from sensorkit.core.entity import (
    ControllerDetails,
    DeviceDetails,
    EntityDetails,
    EntityInfo,
    EntityRef,
)
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl


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

    assert restored == model
    assert isinstance(restored.e, EntityRef) and restored.e.name == "e1"
    assert isinstance(restored.d, SubRef) and restored.d.name == "d1"


def test_entity_ref_equality(kit):
    class SubRef(EntityRef):
        pass

    assert EntityRef("e1") == EntityRef("e1")
    assert EntityRef("e1") != EntityRef("e2")
    assert EntityRef() == EntityRef()
    assert EntityRef("e1") != "e1"

    # The refs name the same entity, so the serialized form cannot tell them apart.
    assert EntityRef("e1") == SubRef("e1")

    # Resolving caches a client, which says nothing about the entity named.
    resolved = EntityRef("e1")
    resolved.resolve(kit)
    assert resolved == EntityRef("e1")

    assert len({EntityRef("e1"), SubRef("e1"), resolved}) == 1


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
@pytest.mark.parametrize("name", ["entity", ""])
def test_entity_ref_name_round_trip(ref_type, name):
    adapter = TypeAdapter(ref_type)
    ref = adapter.validate_python(name)
    restored = adapter.validate_json(adapter.dump_json(ref))

    assert type(ref) is ref_type
    assert ref.name == name
    assert type(restored) is ref_type
    assert restored == ref


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
def test_entity_ref_validation_preserves_resolved_instance(ref_type, kit):
    ref = ref_type("entity")
    ref.resolve(kit)
    client = ref.require()

    validated = TypeAdapter(ref_type).validate_python(ref)

    assert validated is ref
    assert validated.require() is client


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
def test_entity_ref_unset_round_trip(ref_type):
    adapter = TypeAdapter(ref_type)
    unset = ref_type()

    assert adapter.validate_python(unset) is unset
    assert adapter.dump_json(unset) == b"null"

    restored = adapter.validate_json(b"null")
    assert type(restored) is ref_type
    assert restored.name is None
    assert restored.get() is None

    validated = adapter.validate_python(None)
    assert type(validated) is ref_type
    assert validated.name is None


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
def test_optional_entity_ref_preserves_none(ref_type):
    adapter = TypeAdapter(ref_type | None)

    assert adapter.validate_python(None) is None
    assert adapter.validate_json(adapter.dump_json(None)) is None


def test_entity_ref_accepts_subclasses():
    class SensorControllerRef(sk.ControllerRef):
        pass

    controller = SensorControllerRef("controller")

    assert TypeAdapter(EntityRef).validate_python(controller) is controller
    assert TypeAdapter(sk.ControllerRef).validate_python(controller) is controller


@pytest.mark.parametrize("ref_type", [sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
def test_entity_ref_rejects_other_reference_types(ref_type):
    adapter = TypeAdapter(ref_type)

    for other_type in (EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef):
        if other_type is ref_type:
            continue

        with pytest.raises(ValidationError, match=ref_type.__name__):
            adapter.validate_python(other_type("entity"))


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
@pytest.mark.parametrize("value", [5, True, 1.5, b"entity", {}, [], object()])
def test_entity_ref_rejects_invalid_python_values(ref_type, value):
    with pytest.raises(ValidationError, match=ref_type.__name__):
        TypeAdapter(ref_type).validate_python(value)


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
@pytest.mark.parametrize("value", [b"5", b"true", b"{}", b"[]"])
def test_entity_ref_rejects_invalid_json_values(ref_type, value):
    with pytest.raises(ValidationError, match=ref_type.__name__):
        TypeAdapter(ref_type).validate_json(value)


@pytest.mark.parametrize("ref_type", [EntityRef, sk.DeviceRef, sk.ControllerRef, sk.ProgramRef])
@pytest.mark.parametrize("mode", ["validation", "serialization"])
def test_entity_ref_json_schema(ref_type, mode):
    schema = TypeAdapter(ref_type).json_schema(mode=mode)

    assert {variant["type"] for variant in schema["anyOf"]} == {"string", "null"}


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


@pytest.mark.asyncio
async def test_entity_duplicate_request_handler(entity_impl):
    """An entity cannot bind two handlers to one request name."""
    class Message(BaseModel):
        foo: str

    class Response(BaseModel):
        val: int

    request = declare_request("do_thing", message=Message, response=Response)
    other = declare_request("do_thing", message=Message)

    async def handle_thing(msg: Message) -> Response:
        return Response(val=1)

    async def handle_other(msg: Message) -> None:
        pass

    await entity_impl.handle_request(request, handle_thing)

    # The name is what binds the backend subject, so a different declaration still collides.
    with pytest.raises(HandlerError, match="already handles request"):
        await entity_impl.handle_request(other, handle_other)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "impl_type, details_type",
    [
        (EntityImpl, EntityDetails),
        (DeviceImpl, DeviceDetails),
        (ControllerImpl, ControllerDetails),
        (ProgramImpl, EntityDetails),
    ],
)
async def test_attachment_advertises_keywords(service_context, kit, impl_type, details_type):
    impl = impl_type.for_service_context(service_context, "provider")

    def declare_outputs():
        impl.declare_published_keyword("ResultA")
        impl.declare_published_keyword("ResultB")
        impl.declare_published_keyword("ResultA")

    impl.on_attach(declare_outputs)
    await service_context.register_impl(impl)
    info = await kit.entity("provider").kv_get_model(EntityInfo)

    assert type(info.details) is details_type
    assert info.details.published_keywords == {"ResultA", "ResultB"}


@pytest.mark.asyncio
async def test_late_keyword_declaration_needs_republish(service_context, kit):
    """Late declarations appear after entity information is republished."""
    declaration = sk.declare_entity(name="provider")
    await declaration.register(kit, service_context)
    client = kit.entity("provider")
    declaration.declare_published_keyword("ResultA")

    info = await client.kv_get_model(EntityInfo)
    assert info.details.published_keywords == frozenset()

    await declaration.publish_entity_info()

    info = await client.kv_get_model(EntityInfo)
    assert info.details.published_keywords == {"ResultA"}


@pytest.mark.parametrize(
    "info",
    [
        EntityInfo(entity_type="generic", details=EntityDetails(published_keywords={"KwA"})),
        EntityInfo(entity_type="program", details=EntityDetails()),
        EntityInfo(
            entity_type="device",
            details=DeviceDetails(supported_commands={"Stop"}, published_keywords={"KwA"}),
        ),
        EntityInfo(
            entity_type="device",
            details=DeviceDetails(supported_commands={"Stop"}),
        ),
        EntityInfo(
            entity_type="controller",
            details=ControllerDetails(
                supported_tasks=["Observe"],
                controlled_devices=["camera"],
                published_keywords={"KwA"},
            ),
        ),
        EntityInfo(
            entity_type="controller",
            details=ControllerDetails(supported_tasks=[], controlled_devices=[]),
        ),
    ],
)
def test_entity_info_round_trip_preserves_details_type(info):
    restored = EntityInfo.model_validate_json(info.model_dump_json())

    assert type(restored.details) is type(info.details)
    assert restored == info


@pytest.mark.parametrize(
    "data, details",
    [
        (
            '{"entity_type": "controller", "details": {"supported_tasks": ["Observe"], "controlled_devices": []}}',
            ControllerDetails(supported_tasks=["Observe"], controlled_devices=[]),
        ),
        ('{"entity_type": "generic", "details": null}', None),
        ('{"entity_type": "program", "details": null}', None),
    ],
)
def test_entity_info_reads_historical_records(data, details):
    info = EntityInfo.model_validate_json(data)

    assert type(info.details) is type(details)
    assert info.details == details
