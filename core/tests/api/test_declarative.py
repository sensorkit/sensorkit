import asyncio
import os
import uuid
from typing import Any, Literal, override

import pytest

from sensorkit.api.declarative import (
    AUTO_ENTITY_ATTR,
    DeclarationError,
    DeclaredController,
    DeclaredDevice,
    DeclaredEntity,
    DeclaredProgram,
    Service,
    auto_create_decl,
    command_handler,
    declare_controller,
    declare_device,
    declare_entity,
    declare_program,
    introspect_param_type,
    on_attach,
    on_detach,
    on_disable,
    on_enable,
    task_factory,
    task_handler,
)
from sensorkit.core.device import DeviceCommand
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.core.task import ControllerTask
from sensorkit.core.trait import declare_archetype, declare_trait


def test_introspect_param_type():
    def my_func(a: int):
        pass

    assert introspect_param_type(my_func) is int

    class MyClass:
        def my_method(self, b: str):
            pass

    # Testing bound method
    assert introspect_param_type(MyClass().my_method) is str

    # Testing unbound method
    # Unbound method does NOT have 'self' in type hints if 'self' is not hinted.
    assert introspect_param_type(MyClass.my_method) is str

    # If we add hint to self, it might fail if get_type_hints can't resolve the forward reference
    # or it will have 2 hints.
    class MyClassHinted:
        def my_method(self: "Any", b: str):
            pass

    # This should have 2 hints: self and b.
    with pytest.raises(DeclarationError):
        introspect_param_type(MyClassHinted.my_method)

    def no_hint_func(a):
        pass

    # get_type_hints will return {} for this if no hints at all.
    with pytest.raises(DeclarationError):
        introspect_param_type(no_hint_func)

    class MyClass:
        @staticmethod
        def my_static_method(c: float):
            pass

    assert introspect_param_type(MyClass.my_static_method) is float

    class MyClass:
        @classmethod
        def my_class_method(cls, d: list):
            pass

    assert introspect_param_type(MyClass.my_class_method) is list

    def multi_func(a: int, b: str):
        pass

    with pytest.raises(DeclarationError):
        introspect_param_type(multi_func)

    def no_param_func():
        pass

    with pytest.raises(DeclarationError):
        introspect_param_type(no_param_func)

    def func_with_return(a: int) -> str:
        pass

    assert introspect_param_type(func_with_return) is int


@pytest.mark.asyncio
async def test_service():
    svc_decl = Service("test", "0.1.0")

    async with asyncio.timeout(2.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()
        await svc_decl.stop()


def test_entity_declaration():
    # Statement form.
    decl = declare_entity()
    assert isinstance(decl, DeclaredEntity)
    assert decl.name is None

    decl = declare_entity(name="hardcoded_name")
    assert decl.name == "hardcoded_name"

    # Class form.
    @declare_entity
    class MyEntity:
        pass

    obj = auto_create_decl(MyEntity())
    assert hasattr(obj, AUTO_ENTITY_ATTR)
    decl = getattr(obj, AUTO_ENTITY_ATTR)
    assert isinstance(decl, DeclaredEntity)
    assert decl.name is None

    @declare_device
    class MyDevice:
        pass

    assert isinstance(getattr(auto_create_decl(MyDevice()), AUTO_ENTITY_ATTR), DeclaredDevice)

    @declare_controller
    class MyController:
        pass

    assert isinstance(getattr(auto_create_decl(MyController()), AUTO_ENTITY_ATTR), DeclaredController)

    @declare_program
    class MyProgram:
        pass

    assert isinstance(getattr(auto_create_decl(MyProgram()), AUTO_ENTITY_ATTR), DeclaredProgram)


@pytest.mark.asyncio
async def test_entity_class_include():
    done = asyncio.Event()

    @declare_entity
    class MyEntity:
        @on_attach
        def on_init(self):
            done.set()

    instance = MyEntity()
    svc_decl = Service("test", "0.1.0")
    svc_decl.include(instance, name="my_entity")

    decl: DeclaredEntity = getattr(instance, AUTO_ENTITY_ATTR)
    assert decl.name == "my_entity"
    assert instance.on_init in [func for kind, func in decl._associated_callbacks]

    async with asyncio.timeout(1.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()
        await done.wait()
        await svc_decl.stop()


@pytest.mark.asyncio
async def test_entity_class_inheritance():
    done = asyncio.Event()

    @declare_entity
    class MyEntity:
        @on_attach
        def on_init(self):
            done.set()

    @declare_entity
    class MyExtendedEntity(MyEntity):
        pass

    override_init = asyncio.Event()
    override_deinit = asyncio.Event()

    @declare_entity
    class OverrideEntity(MyEntity):
        @override
        def on_init(self):
            # This never happens because the superclass decorator doesn't apply to overrides.
            override_init.set()

        @on_detach
        def on_deinit(self):
            override_deinit.set()

    instance = MyExtendedEntity()
    instance2 = OverrideEntity()

    svc_decl = Service("test", "0.1.0")
    svc_decl.include(instance, name="my_entity")
    svc_decl.include(instance2, name="override_test")

    decl: DeclaredEntity = getattr(instance, AUTO_ENTITY_ATTR)
    assert decl.name == "my_entity"
    assert instance.on_init in [func for kind, func in decl._associated_callbacks]

    async with asyncio.timeout(1.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()

        await done.wait()
        await svc_decl.stop()
        await override_deinit.wait()
        assert not override_init.is_set()


@pytest.mark.asyncio
async def test_declared_entity():
    ent = declare_entity(name="test_entity")
    init_done = asyncio.Event()
    deinit_done = asyncio.Event()

    @on_attach(ent)
    async def on_init():
        assert EntityImpl.current.get() is ent.impl
        init_done.set()

    @on_detach(ent)
    async def on_deinit():
        assert EntityImpl.current.get() is ent.impl
        deinit_done.set()

    svc_decl = Service("test", "0.1.0")
    svc_decl.add(ent)

    async with asyncio.timeout(1.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()

        await init_done.wait()
        assert not deinit_done.is_set()
        await svc_decl.stop()
        await deinit_done.wait()


class DevCmd(DeviceCommand):
    command_id: Literal["test_declared_device"] = "test_declared_device"
    val: int


@pytest.mark.asyncio
async def test_declared_device():
    dev = declare_device(name="test_device")
    done = asyncio.Event()

    @on_attach(dev)
    async def on_init():
        assert DeviceImpl.current.get() is dev.impl

    @on_detach(dev)
    async def on_deinit():
        assert DeviceImpl.current.get() is dev.impl

    @command_handler(dev)
    async def handle(cmd: DevCmd):
        assert cmd.val == 42
        done.set()

    svc_decl = Service("test", "0.1.0")
    svc_decl.add(dev)

    async with asyncio.timeout(1.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()

        cli = svc_decl.client.device("test_device")
        await cli.command(DevCmd(val=42))
        await done.wait()
        await svc_decl.stop()


@pytest.mark.asyncio
async def test_device_declared_device_traits(service):
    """Test that advisory trait mismatches produce a warning at publish time."""
    missing_trait = declare_trait("missing_trait", required_commands=(DevCmd,))

    @declare_device(traits=[missing_trait])
    class TestDevice:
        pass

    service.include(TestDevice(), name="test_device")

    with pytest.raises(DeclarationError):
        await service.run()


@pytest.mark.asyncio
async def test_declare_device_type_param(service):
    """Test that the type= parameter on declare_device works like traits=."""
    arch = declare_archetype("test_arch_type_param", required_commands=(DevCmd,))

    @declare_device(type=arch)
    class TestDevice:
        pass

    service.include(TestDevice(), name="test_device")

    with pytest.raises(DeclarationError):
        await service.run()


@pytest.mark.asyncio
async def test_declare_device_type_and_traits(service):
    """Test that type= and traits= are both validated when used together."""
    arch = declare_archetype("test_arch_combined", required_commands=(DevCmd,))
    extra_trait = declare_trait("extra_trait_combined", required_commands=(DevCmd,))

    @declare_device(type=arch, traits=[extra_trait])
    class TestDevice:
        pass

    service.include(TestDevice(), name="test_device")

    with pytest.raises(DeclarationError):
        await service.run()


@pytest.mark.asyncio
async def test_declared_controller():
    ctrl = declare_controller(name="test_controller")
    done = asyncio.Event()

    @on_attach(ctrl)
    async def on_init():
        assert ControllerImpl.current.get() is ctrl.impl

    @on_detach(ctrl)
    async def on_deinit():
        assert ControllerImpl.current.get() is ctrl.impl

    class TestTask(ControllerTask):
        task_type: Literal["test_task"] = "test_task"
        my_value: int

    @task_handler(ctrl)
    async def handle(task: TestTask):
        assert task.my_value == 42
        done.set()

    svc_decl = Service("test", "0.1.0")
    svc_decl.add(ctrl)

    async with asyncio.timeout(1.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()

        cli = svc_decl.client.controller("test_controller")
        await cli.enable()
        await cli.execute_task(
            TestTask(task_id=uuid.uuid1(), controller_id="test_controller", my_value=42)
        )
        await done.wait()
        await svc_decl.stop()


@pytest.mark.asyncio
async def test_declared_program():
    ctrl = declare_controller(name="test_controller")
    prog = declare_program(name="test_program")
    enable_done = asyncio.Event()
    disable_done = asyncio.Event()
    task_factory_called = asyncio.Event()

    @on_attach(prog)
    async def on_init():
        assert ProgramImpl.current.get() is prog.impl

    @on_detach(prog)
    async def on_deinit():
        assert ProgramImpl.current.get() is prog.impl

    @on_enable(prog)
    async def on_program_enable():
        enable_done.set()

    @on_disable(prog)
    async def on_program_disable():
        disable_done.set()

    @task_factory(prog)
    async def program_task_factory():
        task_factory_called.set()

    svc_decl = Service("test", "0.1.0")
    svc_decl.add(ctrl)
    svc_decl.add(prog)

    async with asyncio.timeout(1.0):
        os.environ["SENSORKIT_BACKEND"] = "sensorkit.backend.fake"
        await svc_decl.start()

        cli = svc_decl.client.program("test_program")
        await cli.enable("test_controller")
        await enable_done.wait()
        assert not task_factory_called.is_set() and not disable_done.is_set()
        await cli.start_tasking()
        await task_factory_called.wait()
        assert not disable_done.is_set()
        await cli.disable()
        await disable_done.wait()
        await svc_decl.stop()
