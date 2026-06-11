from __future__ import annotations

import uuid
from abc import abstractmethod
from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from pydantic import BaseModel

from sensorkit.backend.event import Event
from sensorkit.backend.request import Call, ExtendedResponse, Request
from sensorkit.common.model import ModelRegistry, RegistryBaseModel
from sensorkit.core.entity import (
    DeviceDetails,
    EntityClient,
    EntityInfo,
    EntityInterface,
    EntityRef,
)
from sensorkit.core.state import EventSourcedState
from sensorkit.core.trait import Trait, match_archetype, match_traits

if TYPE_CHECKING:
    from sensorkit.core.client import SensorKit

type CommandHandlerCallback = Callable[[DeviceCommand], Coroutine[Any, Any, BaseModel | None]]


class DeviceCommand(RegistryBaseModel):
    """Base Device Command model."""
    command_id: str = None

    registry: ClassVar[ModelRegistry[DeviceCommand]] = ModelRegistry(discriminator="command_id")

    @classmethod
    def model_registry(cls):
        return cls.registry


class Abort(DeviceCommand):
    """Built-in abort command."""


class CommandStarted(Event):
    """Event indicating a Command has been accepted and execution has begun."""
    command_id: str
    call_id: uuid.UUID


class CommandDone(Event):
    """Event indicating a Command has finished execution."""
    command_id: str
    call_id: uuid.UUID
    success: bool


class DeviceEnableState(Event):
    """Event indicating the Device enable state has changed."""
    enabled: bool


class DeviceState(EventSourcedState):
    """Device state."""
    enable_state: DeviceEnableState


class DeviceEnableStateRequest(BaseModel):
    """Request that a Device enable or disable its command handlers."""
    enable: bool


set_enable_state_request = Request.define(
    "set_enable_state",
    payload=DeviceEnableStateRequest,
)


class CommandRequestMessage(BaseModel):
    """Message model representing a Command execution request."""
    command: DeviceCommand


class CommandResult(BaseModel):
    """Message model representing a response to a Command execution request."""
    data: Any


run_command_request = Request.define(
    "command",
    payload=CommandRequestMessage,
    result=CommandResult,
)


class DeviceClient(EntityClient):
    """Object that exposes client-side functionality of a Device."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._details: DeviceDetails | None = None

    async def enable(self):
        """Enable device command handlers."""
        return await self.call(
            set_enable_state_request,
            DeviceEnableStateRequest(enable=True)
        )

    async def disable(self):
        """Disable device command handlers."""
        return await self.call(
            set_enable_state_request,
            DeviceEnableStateRequest(enable=False)
        )

    def command(self, command: DeviceCommand) -> Call[ExtendedResponse, CommandResult]:
        """Send a command to the device and return a Call tracking the result."""
        return self.call(
            run_command_request,
            CommandRequestMessage(command=command),
        )

    async def get_details(self) -> DeviceDetails:
        """Fetch and cache the device's supported command details.

        The result is cached because a device's supported commands do not change at
        runtime.
        """
        if self._details is None:
            info = await self.kv_get_model(EntityInfo)
            if not isinstance(info.details, DeviceDetails):
                raise ValueError(f"Entity is not a device: {self.entity}")
            self._details = info.details
        return self._details

    async def has_trait(self, trait: Trait) -> bool:
        """Return True if this device satisfies the given trait."""
        details = await self.get_details()
        return trait.match(details)

    async def get_traits(self, traits: Iterable[Trait]) -> list[Trait]:
        """Return all matching traits from the given iterable."""
        details = await self.get_details()
        return match_traits(details, traits)

    async def get_archetype(self) -> Trait | None:
        """Return the matching archetype for this device, if any."""
        details = await self.get_details()
        return match_archetype(details)


class DeviceRef(EntityRef[DeviceClient]):
    """A serializable reference to a device client."""

    def _get_client(self, kit: SensorKit) -> DeviceClient:
        return kit.device(self.name)


class DeviceInterface(EntityInterface):
    """Interface describing an implementation of a device."""

    @abstractmethod
    def on_enable(self, func: Callable[[], None]):
        """Register a callback to invoke when the device is enabled."""
        ...

    @abstractmethod
    def on_disable(self, func: Callable[[], None]):
        """Register a callback to invoke when the device is disabled."""
        ...

    @abstractmethod
    def command_handler(
        self,
        command_type: type[DeviceCommand],
    ) -> Callable[..., CommandHandlerCallback]:
        """Register a handler for the given command type and return a decorator."""
        ...
