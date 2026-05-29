from __future__ import annotations

import asyncio
import functools
import inspect
import warnings
from collections.abc import Callable, Coroutine
from enum import StrEnum, auto
from typing import Any, final, get_type_hints, override

from loguru import logger

from sensorkit.api.bootstrap import connect
from sensorkit.common.importutil import get_caller_module
from sensorkit.core.client import SensorKit, ServiceContext
from sensorkit.core.controller import TaskHandlerCallback
from sensorkit.core.delegate import (
    ControllerDelegate,
    DeviceDelegate,
    EntityDelegate,
    ProgramDelegate,
)
from sensorkit.core.device import CommandHandlerCallback, DeviceCommand
from sensorkit.core.entity import DeviceDetails
from sensorkit.core.executor import TaskFactoryFunc
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.core.task import ControllerTask
from sensorkit.core.trait import Archetype, Trait

type AnyEntityDecl = DeclaredEntity | DeclaredDevice | DeclaredController | DeclaredProgram
type InitDeinitCallback = Callable[[], Coroutine[Any, Any, None] | None]

AUTO_ENTITY_ATTR = "__sk_entity__"
DECL_MARK_ATTR = "__sk_decl__"
CALLBACK_MARK_ATTR = "__sk_callback__"
TRAIT_ANNOTATION_ATTR = "__sk_traits__"


def _mark_decl_type(cls: type, decl_type: type[DeclaredEntity]):
    setattr(cls, DECL_MARK_ATTR, decl_type)
    return cls


def auto_create_decl(instance):
    if decl_type := getattr(instance, DECL_MARK_ATTR, None):
        decl = decl_type(None)
        setattr(instance, AUTO_ENTITY_ATTR, decl)

        # Pick up advisory trait annotations for devices.
        if isinstance(decl, DeclaredDevice):
            if traits := getattr(instance, TRAIT_ANNOTATION_ATTR, None):
                decl._declared_traits = list(traits)

    return instance


def decl_for_instance(instance) -> AnyEntityDecl:
    return getattr(instance, AUTO_ENTITY_ATTR, None)


def entity_for_instance(instance):
    return decl.impl if (decl := decl_for_instance(instance)) else None


def _mark_callback(func: Callable, kind: CallbackKind):
    """Mark a function as an unassociated callback."""
    if inspect.ismethod(func):
        # If we're working with a method, we need to unwrap it to get the underlying function. This
        # is crucial because function objects are mutable (unlike methods) and are shared across
        # subclass relationships.
        func = func.__func__

    setattr(func, CALLBACK_MARK_ATTR, kind)


def is_callback(func: Callable):
    """Return True if the given function is declared as a callback."""
    return hasattr(func, CALLBACK_MARK_ATTR)


def get_callback_kind(func: Callable):
    """Return the type code of the callback function."""
    return getattr(func, CALLBACK_MARK_ATTR)


def introspect_param_type(func: Callable):
    """Return the parameter type hint of the given single-parameter function."""
    type_hints = get_type_hints(func)
    type_hints.pop("return", None)

    if len(type_hints) != 1:
        raise DeclarationError("Callback must have exactly one typed parameter")

    return next(iter(type_hints.values()))


def introspect_decls(obj):
    """Discover declared entities and callbacks within the given object."""
    decls: list[DeclaredEntity] = []
    callbacks: list[tuple[Callable, CallbackKind]] = []

    for symbol in dir(obj):
        if symbol.startswith("__") and symbol != AUTO_ENTITY_ATTR:
            continue

        # We use getattr_static here to avoid triggering property evaluations. This is quite slow,
        # but with expected usage this performance should be acceptable.
        attr = inspect.getattr_static(obj, symbol)

        match attr:
            case DeclaredEntity():
                decls.append(attr)
            case func if is_callback(func):
                # Since getattr_static returns the function object, we need to do another getattr
                # to get the method.
                func = getattr(obj, symbol)
                callbacks.append((func, get_callback_kind(func)))

    return decls, callbacks


class CallbackKind(StrEnum):
    ENTITY_INIT = auto()
    ENTITY_DEINIT = auto()
    COMMAND_HANDLER = auto()
    TASK_HANDLER = auto()
    TASK_FACTORY = auto()
    ENABLE = auto()
    DISABLE = auto()


class DeclaredEntity[T: EntityImpl = EntityImpl](EntityDelegate):
    """Represents an eventual entity registration, collecting callbacks before the service starts."""

    def __init__(self, name: str | None):
        self.name = name
        self._associated_callbacks: list[tuple[CallbackKind, Callable]] = []
        self._init_callbacks: list[InitDeinitCallback] = []
        self._deinit_callbacks: list[InitDeinitCallback] = []

        # These are set when the service is registered.
        self.client: SensorKit | None = None  # TODO: Remove when the impl itself provides this.
        self.service: ServiceContext | None = None
        self.impl: T | None = None

    @property
    def delegate_target(self):
        return self.impl

    @property
    def binding(self):
        warnings.warn("Deprecated impl access from declarative object", stacklevel=2)
        return self.impl

    @final
    def associate(self, func: Callable, kind: CallbackKind):
        """Queue a callback to be registered with the implementation after it is created."""
        self._associated_callbacks.append((kind, func))

    @final
    async def register(
        self,
        client: SensorKit,
        service: ServiceContext,
        *,
        acquire_lease: bool = True,
    ):
        """Register the declared entity with the SensorKit backend.

        Args:
            client: the SensorKit client instance for interacting with the backend
            service: the service context that manages this entity's lifecycle
            acquire_lease: whether to acquire a lease on the entity during registration

        Returns:
            a Future that will complete when the service exits

        Raises:
            DeclarationError: if the entity does not have a name assigned
            Exception: any exception raised by an initialization callback is propagated
        """
        if not self.name:
            raise DeclarationError("Entity must have a name")

        # Create our entity implementation and register all associated callbacks. Nothing hits the
        # wire at this point.
        self.impl = self.create_impl(service)

        for kind, func in self._associated_callbacks:
            self.register_callback(kind, func)

        # Register with the backend. If lease acquisition fails, we raise here and the service
        # will exit.
        await service.register_impl(self.impl, acquire_lease=acquire_lease)

        # Store references for use by the caller.
        self.client = client
        self.service = service

        # Run init callbacks.
        with self.impl.enter_context():
            for init_callback in self._init_callbacks:
                try:
                    aw = init_callback()

                    if asyncio.iscoroutine(aw):
                        await aw
                except Exception as e:
                    logger.warning(f"Error during {self.name} initialization ({type(e).__name__})")
                    raise

        # Run post-initialization hook.
        await self.post_decl_init()

        async def run_deinit_callbacks():
            try:
                # Wait for the service task to end.
                await self.service.join()
            except asyncio.CancelledError:
                # Propagate only direct cancellation.
                if asyncio.current_task().cancelling():
                    raise
            except Exception:
                # Defer propagation of exceptions until after our deinit callbacks have been run.
                pass

            # Run deinit callbacks.
            with self.impl.enter_context():
                for deinit_callback in self._deinit_callbacks:
                    try:
                        aw = deinit_callback()

                        if asyncio.iscoroutine(aw):
                            await aw
                    except asyncio.CancelledError:
                        # Propagate only direct cancellation.
                        if asyncio.current_task().cancelling():
                            raise

                        logger.warning(f"Cancelled during cleanup of {self.name}")
                    except Exception as e:
                        logger.warning(f"Error cleaning up {self.name} ({type(e).__name__})")
                        logger.opt(exception=e).debug("deinit callback raised")

            # Propagate exception, if any.
            await self.service.join()

        self._deinit_task = asyncio.create_task(run_deinit_callbacks())
        return self._deinit_task

    def create_impl(self, service: ServiceContext):
        """Instantiate the implementation object bound to *service*."""
        return EntityImpl.for_service_context(service, self.name)

    async def post_decl_init(self):
        """Hook called after initialization callbacks."""
        await self.impl.publish_entity_info()

    def register_callback(self, kind: CallbackKind, func: Callable):
        """Route a callback to the appropriate registration method on the implementation."""
        match kind:
            case CallbackKind.ENTITY_INIT:
                self._init_callbacks.append(func)
            case CallbackKind.ENTITY_DEINIT:
                self._deinit_callbacks.append(func)


class DeclaredDevice(DeclaredEntity[DeviceImpl], DeviceDelegate):
    """Represents an eventual device registration."""

    def __init__(self, name: str | None):
        super().__init__(name)
        self._declared_traits: list[Trait] = []

    @override
    def create_impl(self, svc: ServiceContext):
        return DeviceImpl.for_service_context(svc, self.name)

    @override
    async def post_decl_init(self):
        # FIXME: For now we trust the device to publish the keywords required by its traits.
        #        This should be removed in favor of API that allows the device implementation
        #        to explicitly declare the keywords it publishes, which can then be used to
        #        validate whether its traits are satisfied.
        for trait in self._declared_traits:
            for kw_id in trait.effective_keyword_ids():
                self.impl.declare_published_keyword(kw_id)

        info = await self.impl.publish_entity_info()
        details = info.details

        if not isinstance(details, DeviceDetails):
            raise RuntimeError("Device did not publish its details")

        # Validate declared traits.
        for trait in self._declared_traits:
            if not trait.match(info.details):
                missing = []

                if commands := trait.effective_command_ids() - details.supported_commands:
                    missing.append(" does not implement " + ", ".join(sorted(commands)))

                if keywords := trait.effective_keyword_ids() - info.details.published_keywords:
                    missing.append(" does not publish " + ", ".join(sorted(keywords)))

                raise DeclarationError(
                    f"Device declares trait '{trait.name}' but {'; '.join(missing)}"
                )

    @override
    def register_callback(self, kind: CallbackKind, func: Callable):
        super().register_callback(kind, func)

        match kind:
            case CallbackKind.COMMAND_HANDLER:
                command_type = introspect_param_type(func)

                if not issubclass(command_type, DeviceCommand):
                    raise DeclarationError("Command handler parameter has incorrect type")

                self.impl.command_handler(command_type)(func)


class DeclaredController(DeclaredEntity[ControllerImpl], ControllerDelegate):
    """Represents an eventual controller registration."""

    @override
    def create_impl(self, svc: ServiceContext):
        return ControllerImpl.for_service_context(svc, self.name)

    @override
    async def post_decl_init(self):
        await self.impl.publish_entity_info()

        # Insert subscription stop as the first deinit callback so it runs
        # before user-defined on_detach handlers.
        self._deinit_callbacks.insert(0, self.impl.stop_device_subscriptions)

        await self.impl.start_device_subscriptions()

    @override
    def register_callback(self, kind: CallbackKind, func: Callable):
        super().register_callback(kind, func)

        match kind:
            case CallbackKind.TASK_HANDLER:
                task_type = introspect_param_type(func)

                if not issubclass(task_type, ControllerTask):
                    raise DeclarationError("Task handler parameter has incorrect type")

                self.impl.task_handler(task_type)(func)


class DeclaredProgram(DeclaredEntity[ProgramImpl], ProgramDelegate):
    """Represents an eventual program registration."""

    @override
    def create_impl(self, svc: ServiceContext):
        return ProgramImpl.for_service_context(svc, self.name)

    @override
    def register_callback(self, kind: CallbackKind, func: Callable):
        super().register_callback(kind, func)

        match kind:
            case CallbackKind.ENABLE:
                self.impl.on_enable(func)
            case CallbackKind.DISABLE:
                self.impl.on_disable(func)
            case CallbackKind.TASK_FACTORY:
                self.impl.task_factory(func)


class Service:
    """Runs a single SensorKit service instance given a set of declared entity objects."""

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version
        self.declarations: set[DeclaredEntity] = set()
        self.context: ServiceContext | None = None
        self.client: SensorKit | None = None
        self._delegate_entity: DeclaredEntity | None = None
        self._register_lock = asyncio.Lock()
        self._deinit_tasks: tuple[asyncio.Task, ...] = ()
        self._started = False
        loop = asyncio.get_running_loop()
        self.running = loop.create_future()
        self.shutdown = loop.create_future()

    def add(self, declared: DeclaredEntity, name: str | None = None):
        """Add an entity declaration to the service.

            >>> my_entity = declare_entity(name="my_entity")
            >>> service.add(my_entity)

        If `name` is given and the declaration already includes a static name assignment, an error
        is raised.

        If no `name` is given and the declaration has no static name assigned, the entity will
        "share" the name of the containing service. This can only apply to the first such nameless
        declaration. Adding a second declaration without a name will raise an error.
        """
        if name:
            if declared.name:
                raise DeclarationError("Cannot reassign entity name")

            declared.name = name
        elif not declared.name:
            if self._delegate_entity:
                raise DeclarationError("Cannot determine entity name")

            # Set the name of this entity to the name of the service. This allows single-entity
            # services to avoid having to name both the service and the entity.
            self._delegate_entity = declared
            declared.name = self.name

        self.declarations.add(declared)

    def include(self, obj: Any, *, name: str | None = None):
        """Add all entity declarations contained or marked in the given object.

        Typically, the target object will be the instance of a class that has been decorated by,
        e.g., `declare_entity`. In this case, the marks applied by that decorator are found and
        a single entity declaration is created automatically.

            >>> @declare_entity
            >>> class MyEntity:
            >>>     ...
            >>> service.include(MyEntity(), name="my_entity")

        If the input object is a type, the type will be instantiated assuming a no-parameter
        constructor. The instance object will then be searched for marks and entity declarations
        as above.

            >>> service.include(MyEntity, name="my_entity")

        If the input object is a module, marks do not apply. It is simply searched for entity
        declarations.

            >>> import myorg.devices
            >>> service.include(myorg.devices)

        In all cases, the `name` argument is applied if and only if a single entity declaration is
        resolved. If more than one is resolved (e.g., in the module case), an error is raised to
        indicate the name assignment ambiguity.
        """
        if isinstance(obj, type):
            cls = obj

            try:
                obj = cls()
            except Exception as e:
                raise DeclarationError(f"Failed to instantiate class {cls.__name__}") from e

        typename = type(obj).__name__

        # If we have an instance of a class marked for automatic entity creation, make it so.
        auto_create_decl(obj)

        # Get the declarations contained in the input object.
        decls, callbacks = introspect_decls(obj)

        if not decls:
            raise DeclarationError(f"No entity declarations found in {typename}")

        if len(decls) == 1:
            # Exactly one declaration in this namespace. This will typically be the case for
            # classes that implement an entity.
            decl = decls[0]

            # Automatically associate all floating callbacks and then add the declaration.
            for func, kind in callbacks:
                decl.associate(func, kind)

            self.add(decl, name)
        else:
            # Multiple declarations. This will generally be a module include. In this case, we
            # treat the existence of any floating callbacks as an error condition rather than make
            # assumptions about which declaration is intended to be associated with which callback.
            if callbacks:
                raise DeclarationError(
                    f"Floating callbacks alongside multiple declarations in {typename}"
                )

            # Similarly, we can't be sure about name assignment either. The module include use case
            # demands hardcoded naming.
            if name is not None:
                raise DeclarationError(
                    f"Name assignment is ambiguous with multiple declarations in {typename}"
                )

            # Add all declarations.
            for decl in decls:
                self.add(decl)

        return decls

    def include_module(self, **kwargs):
        """Add all entity declarations found in the calling module.

        See the ``include`` method.
        """
        # Include decls from the module of the user code call site. We must check for None here
        # because in certain contexts there may be no calling module.
        if mod := get_caller_module(depth=1):
            self.include(mod, **kwargs)

    async def register(self):
        """Idempotent method to connect to the backend and register this service."""
        async with self._register_lock:
            if self.client is None:
                self.client = await connect()

            if self.context is None:
                self.context = await self.client.register_service(self.name, self.version)

    async def start(self):
        """Start the service."""
        if self._started:
            raise RuntimeError("Service was already started")

        self._started = True

        try:
            # Register as a service.
            await self.register()

            # Register all declared entities.
            self._deinit_tasks = await asyncio.gather(
                *(
                    decl.register(
                        self.client, self.context, acquire_lease=decl is not self._delegate_entity
                    )
                    for decl in self.declarations
                )
            )

            self.running.set_result(True)
        except BaseException as e:
            logger.debug(f"service error propagated to declarative API: {type(e).__name__}: {e}")
            self.running.set_exception(e)
            self.shutdown.set_exception(e)

            if self.context is not None:
                await self.context.shutdown()

            raise

        async def wait_for_shutdown():
            try:
                await self.context.join()
                self.shutdown.set_result(True)
            except asyncio.CancelledError:
                self.shutdown.cancel()
                raise
            except BaseException as e:
                if not self.shutdown.done():
                    self.shutdown.set_exception(e)

                if not isinstance(e, Exception):
                    # Make sure to re-raise BaseException.
                    raise

        self._shutdown_waiter = asyncio.create_task(wait_for_shutdown())

    async def run(self):
        """Run the service."""
        await self.start()

        try:
            await self.shutdown
        finally:
            await self.stop()

    async def cleanup(self):
        """Wait for all deinit tasks to complete."""
        await asyncio.gather(*self._deinit_tasks, return_exceptions=True)

    async def stop(self):
        """Stop the service."""
        await self.context.shutdown()
        await self.cleanup()
        await self.shutdown


class DeclarationError(Exception):
    """Raised when a declaration or its usage is invalid."""


def _decorator(decl: DeclaredEntity | None, kind: CallbackKind, func: Callable):
    if decl:
        # Associate the callback with the declaration.
        decl.associate(func, kind)
    else:
        # Mark as floating, for later automatic association.
        _mark_callback(func, kind)

    return func


def on_attach(arg: DeclaredEntity | InitDeinitCallback):
    """Register a callback to be executed during entity initialization."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.ENTITY_INIT)
        case _:
            return _decorator(None, CallbackKind.ENTITY_INIT, arg)


def on_detach(arg: DeclaredEntity | InitDeinitCallback):
    """Register a callback to be executed during entity deinitialization."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.ENTITY_DEINIT)
        case _:
            return _decorator(None, CallbackKind.ENTITY_DEINIT, arg)


def command_handler(arg: DeclaredEntity | CommandHandlerCallback):
    """Declare a command handler."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.COMMAND_HANDLER)
        case _:
            return _decorator(None, CallbackKind.COMMAND_HANDLER, arg)


def task_handler(arg: DeclaredEntity | TaskHandlerCallback):
    """Declare a task handler for a controller."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.TASK_HANDLER)
        case _:
            return _decorator(None, CallbackKind.TASK_HANDLER, arg)


def task_factory(arg: DeclaredEntity | TaskFactoryFunc):
    """Declare the task factory function for a program."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.TASK_FACTORY)
        case _:
            return _decorator(None, CallbackKind.TASK_FACTORY, arg)


def on_enable(arg: DeclaredEntity | Callable):
    """Register a callback to be called when a program or device is enabled."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.ENABLE)
        case _:
            return _decorator(None, CallbackKind.ENABLE, arg)


def on_disable(arg: DeclaredEntity | Callable):
    """Register a callback to be called when a program or device is disabled."""
    match arg:
        case DeclaredEntity():
            return functools.partial(_decorator, arg, CallbackKind.DISABLE)
        case _:
            return _decorator(None, CallbackKind.DISABLE, arg)


def declare_entity(cls: type | None = None, *, name: str | None = None):
    """Declare a generic entity to be implemented by a local service."""
    if cls:
        return _mark_decl_type(cls, DeclaredEntity)
    else:
        return DeclaredEntity(name)


def declare_device(
    cls: type | None = None,
    *,
    name: str | None = None,
    type: Archetype | None = None,
    traits: list[Trait] | None = None,
):
    """Declare a Device to be implemented by a local service.

    Can be used as a bare class decorator (``@sk.declare_device``), a keyword
    decorator (``@sk.declare_device(type=..., traits=[...])``), or an explicit declaration
    (``my_device = sk.declare_device(name="foo", type=..., traits=[...])``)
    """
    all_traits: list[Trait] | None = None
    if type is not None or traits:
        all_traits = ([type] if type is not None else []) + list(traits or [])

    if cls:
        # Bare decorator: @sk.declare_device
        if all_traits:
            setattr(cls, TRAIT_ANNOTATION_ATTR, all_traits)
        return _mark_decl_type(cls, DeclaredDevice)
    elif name is not None:
        # Explicit declaration: sk.declare_device(name="foo")
        decl = DeclaredDevice(name)
        if all_traits:
            decl._declared_traits = all_traits
        return decl
    elif all_traits is not None:
        # Keyword decorator: @sk.declare_device(type=..., traits=[...])
        def decorator(decorated_cls):
            setattr(decorated_cls, TRAIT_ANNOTATION_ATTR, all_traits)
            return _mark_decl_type(decorated_cls, DeclaredDevice)
        return decorator
    else:
        return DeclaredDevice(None)


def declare_controller(cls: type | None = None, *, name: str | None = None):
    """Declare a Controller to be implemented by a local service."""
    if cls:
        return _mark_decl_type(cls, DeclaredController)
    else:
        return DeclaredController(name)


def declare_program(cls: type | None = None, *, name: str | None = None):
    """Declare a Program to be implemented by a local service."""
    if cls:
        return _mark_decl_type(cls, DeclaredProgram)
    else:
        return DeclaredProgram(name)
