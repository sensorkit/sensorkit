import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Self, overload, override

from loguru import logger
from pydantic import BaseModel

from sensorkit.backend.base import Backend, BackendError, BackendImpl, Entity, ServiceInfo
from sensorkit.backend.lease import LeaseGroup
from sensorkit.common.aio import PerpetualGroup
from sensorkit.core.controller import ControllerClient
from sensorkit.core.device import DeviceClient
from sensorkit.core.entity import DeviceDetails, EntityClient, EntityInfo
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl
from sensorkit.core.program import ProgramClient
from sensorkit.core.trait import Trait


class SensorKit:
    """The main SensorKit API entrypoint."""

    def __init__(self, backend: Backend | BackendImpl):
        if isinstance(backend, BackendImpl):
            backend = Backend(impl=backend)

        self.backend: Backend = backend
        self._clients: dict[Entity, EntityClient] = {}
        self._service_registered = False

    async def register_service(self, name: str, version: str):
        """Register this SensorKit instance as a named service and return a ServiceContext."""
        if self._service_registered:
            raise RuntimeError("Service already registered")

        self._service_registered = True
        return await ServiceContext.register(
            self,
            ServiceInfo(name=name, version=version),
        )

    async def list_services(self):
        """Return a mapping of entity to ServiceStatus for all services visible in the KV store."""
        records: dict[Entity, ServiceRecord] = {}
        online: set[Entity] = set()

        for entry in await self.backend.key_value().get_all(deep=True):
            match entry.key.prop:
                case "ServiceRecord":
                    records[entry.key.entity()] = ServiceRecord.model_validate_json(entry.value)
                case "EntityLease":
                    online.add(entry.key.entity())

        return {
            entity: ServiceStatus(service=record, online=entity in online)
            for entity, record in records.items()
        }

    async def list_entities(self):
        """Return a mapping of entity to lease-value string for all currently online entities."""
        return {
            entry.key.entity(): EntityInfo.model_validate_json(entry.value)
            for entry in await self.backend.key_value().get_all(deep=True)
            if entry.key.prop == "EntityInfo"
        }

    async def find_devices(self, *, match_trait: Trait) -> list[DeviceClient]:
        """Return DeviceClients for all online devices matching the given trait."""
        results: list[DeviceClient] = []

        for entry in await self.backend.key_value().get_all(deep=True):
            if entry.key.prop != "EntityInfo":
                continue

            info = EntityInfo.model_validate_json(entry.value)
            if not isinstance(info.details, DeviceDetails):
                continue

            if match_trait.match(info.details):
                results.append(self.device(entry.key.entity()))

        return results

    @overload
    def entity(self, entity: Entity) -> EntityClient: ...

    @overload
    def entity(self, *path: str) -> EntityClient: ...

    def entity(self, *args: Any):
        """Return (or create) an EntityClient for the given entity path or Entity object."""
        entity = args[0] if args and isinstance(args[0], Entity) else Entity(args)

        if entity in self._clients:
            return self._clients[entity]

        return self._clients.setdefault(entity, EntityClient(self, entity))

    @overload
    def device(self, entity: Entity) -> DeviceClient: ...

    @overload
    def device(self, *path: str) -> DeviceClient: ...

    def device(self, *args: Any):
        """Return (or create) a DeviceClient for the given entity path or Entity object."""
        entity = args[0] if args and isinstance(args[0], Entity) else Entity(args)

        if entity in self._clients:
            return self._clients[entity]

        return self._clients.setdefault(entity, DeviceClient(self, entity))

    @overload
    def controller(self, entity: Entity) -> ControllerClient: ...

    @overload
    def controller(self, *path: str) -> ControllerClient: ...

    def controller(self, *args: Any):
        """Return (or create) a ControllerClient for the given entity path or Entity object."""
        entity = args[0] if args and isinstance(args[0], Entity) else Entity(args)

        if entity in self._clients:
            return self._clients[entity]

        return self._clients.setdefault(entity, ControllerClient(self, entity))

    @overload
    def program(self, entity: Entity) -> ProgramClient: ...

    @overload
    def program(self, *path: str) -> ProgramClient: ...

    def program(self, *args: Any):
        """Return (or create) a ProgramClient for the given entity path or Entity object."""
        entity = args[0] if args and isinstance(args[0], Entity) else Entity(args)

        if entity in self._clients:
            return self._clients[entity]

        return self._clients.setdefault(entity, ProgramClient(self, entity))


class ServiceRecord(BaseModel):
    """Information about a service entity."""

    info: ServiceInfo
    last_registered: datetime


@dataclass
class ServiceStatus:
    """Status of a service."""

    service: ServiceRecord
    online: bool


class ServiceContext(EntityImpl):
    """An object that exposes SensorKit service API."""

    ENTITY_LEASE_TTL = 60.0
    SERVICE_LEASE_TTL = 60.0

    @classmethod
    async def register(cls, kit: SensorKit, info: ServiceInfo) -> Self:
        """Create a ServiceContext and register itself as an entity."""
        instance = cls(kit, info)
        return await instance.register_impl(instance, lease_ttl=cls.SERVICE_LEASE_TTL)

    def __init__(self, sensorkit: SensorKit, info: ServiceInfo):
        # Perpetual background tasks (constraint monitors, lifecycle drivers, etc.) should be
        # spawned on perpetual_group so they get force-cancelled at shutdown rather than blocking
        # task_group.__aexit__ on their natural completion. See PerpetualGroup docstring.
        super().__init__(
            sensorkit=sensorkit,
            entity=Entity.at(info.name),
            task_group=asyncio.TaskGroup(),
            perpetual_group=PerpetualGroup(),
        )

        self.info = info
        self._lease_group = LeaseGroup()
        self._shutdown = asyncio.get_running_loop().create_future()

    async def register_impl[T: EntityImpl](
        self,
        impl: T,
        *,
        acquire_lease: bool = True,
        lease_ttl: float = ENTITY_LEASE_TTL,
    ):
        """Register an entity implementation on the backend."""
        if acquire_lease:
            await self._lease_group.acquire(
                impl._kv,
                key="EntityLease",
                ttl=lease_ttl,
                record=self.info,
            )

        await impl.init_impl()
        return impl

    @override
    async def init_impl(self):
        """Start the main service task."""
        # Publish information about this service.
        await self.kv_put_model(ServiceRecord(info=self.info, last_registered=datetime.now()))

        # Notify the backend implementation that we are a service.
        with contextlib.suppress(Exception):
            await self.backend.register_service(self.info)

        # Start the service task.
        ready = asyncio.Event()
        self._task = asyncio.create_task(
            self._service_task(ready),
            name=f"service-{self.info.name}",
        )
        await ready.wait()

    async def _service_task(self, ready: asyncio.Event):
        logger.debug(f"Service {self.info.name} startup")

        try:
            async with self.task_group:
                async with self.perpetual_group:
                    # Signal the outer coroutine that the TaskGroup is active.
                    ready.set()

                    try:
                        # Start lease maintenance.
                        await self._lease_group.refresh_loop()
                    finally:
                        # Force-cancel perpetual-group children so they don't hold up
                        # task_group's __aexit__ draining bounded work.
                        self.perpetual_group.cancel_all()
        except asyncio.CancelledError:
            logger.error(f"Service {self.info.name} abnormal shutdown (cancelled)")
            self._shutdown.cancel()
        except BaseExceptionGroup as eg:
            logger.error(f"Service {self.info.name} abnormal shutdown")
            errors = [e for e in eg.exceptions if not isinstance(e, asyncio.CancelledError)]

            for i, e in enumerate(errors, 1):
                log = logger.opt(exception=e) if not isinstance(e, GeneratorExit) else logger
                log.debug(f"{e.__class__.__name__} (error {i} of {len(errors)})")

            if errors:
                self._shutdown.set_exception(BaseExceptionGroup("ServiceContext errors", errors))
            else:
                self._shutdown.cancel()
        else:
            logger.debug(f"Service {self.info.name} normal shutdown")
            self._shutdown.set_result(True)
        finally:
            # Shut down request listeners.
            with contextlib.suppress(Exception):
                async with asyncio.timeout(5.0):
                    await self.backend.shutdown_request_listeners()

    async def shutdown(self):
        """Shut down this service context, making it no longer usable."""
        # Expire our leases to trigger shutdown.
        try:
            await self._lease_group.expire()
        except* BackendError:
            # If the backend is unavailable, the task group is in the process of tearing down
            # anyway, so we continue to await the shutdown future.
            pass

        # The caller has requested an intentional shutdown, so we won't bother propagating an error
        # if the service context happened to simultaneously blow up.
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._shutdown

    async def join(self):
        """Wait until the service shuts down.

        If the service exited abnormally, the exception that caused it to exit will be raised.
        """
        await self._shutdown

    async def register_entity(self, entity: str):
        """Register a generic entity implementation on the backend."""
        return await self.register_impl(EntityImpl.for_service_context(self, entity))

    async def register_device(self, entity: str):
        """Register a Device implementation on the backend."""
        return await self.register_impl(DeviceImpl.for_service_context(self, entity))

    async def register_controller(self, entity: str):
        """Register a Controller implementation on the backend."""
        return await self.register_impl(ControllerImpl.for_service_context(self, entity))

    async def register_program(self, entity: str):
        """Register a Program implementation on the backend."""
        return await self.register_impl(ProgramImpl.for_service_context(self, entity))
