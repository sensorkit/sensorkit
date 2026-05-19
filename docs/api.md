# API reference — `sensorkit.api`

All public symbols are importable from `sensorkit.api`:

```python
import sensorkit.api as sk
```

Symbols are organized below by role. The source of truth for each symbol is indicated as `(module → sensorkit.api)`.

---

## Service entry

### `service_entrypoint`

```python
@sk.service_entrypoint(*, version: str)
async def main(service: sk.Service) -> None: ...
```

Decorator that marks an `async` function as a service entrypoint. The function receives a `Service` instance, adds entity declarations to it via `service.include(...)`, and then calls `await service.run()` to start the service loop.

```python
@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(MyDevice, name="my-device")
    await service.run()
```

The decorated function cannot be called directly; it becomes a `ServiceEntrypoint` object that the CLI runner invokes.

---

### `Service`

```python
class Service:
    name: str
    version: str
    context: ServiceContext | None
    client: SensorKit | None
```

Represents a running service instance. Passed to the service entrypoint function. Manages entity lifecycle and connects to the backend.

**Methods:**

- `include(obj, *, name: str | None = None)` — Discover and register entity declarations from an object (class instance, class, or module). If a name is given it is assigned to the resolved declaration.
- `add(declared: DeclaredEntity, name: str | None = None)` — Register a declaration object directly.
- `run()` — Connect to the backend, register all entities, and run until shutdown.

---

### `connect`

```python
async def connect(backend: str | BackendImpl | None = None) -> SensorKit
```

Create and connect a `SensorKit` client without starting a full service. Useful for scripts, tests, and one-off CLI interactions.

`(sensorkit.api.bootstrap → sensorkit.api)`

---

## Entity declarations

The `declare_*` functions are used as class decorators or to create class-level descriptors that bind callbacks to an entity registration.

---

### `declare_device`

```python
def declare_device(name: str | None = None) -> DeclaredDevice
```

Used as a class-level descriptor to declare a device entity, or as a class decorator.

**As a descriptor (explicit):**
```python
class MyMount:
    mount = sk.declare_device()

    @mount.command_handler(sk.Home)
    async def go_home(self, cmd: sk.Home): ...
```

**As a class decorator:**
```python
@sk.declare_device
class MyMount:
    @sk.command_handler(sk.Home)
    async def go_home(self, cmd: sk.Home): ...
```

---

### `DeclaredDevice`

```python
class DeclaredDevice(DeclaredEntity[DeviceImpl])
```

Returned by `declare_device`. Holds command handler and lifecycle registrations until the service starts. Validates that declared traits are actually satisfied by the registered handlers.

`(sensorkit.api.declarative → sensorkit.api)`

---

### `declare_controller`

```python
def declare_controller(name: str | None = None) -> DeclaredController
```

Used as a class-level descriptor or class decorator to declare a controller entity.

```python
class MyController:
    ctrl = sk.declare_controller()

    @ctrl.task_handler(sk.InitTask)
    async def on_init(self, task: sk.InitTask): ...
```

---

### `DeclaredController`

```python
class DeclaredController(DeclaredEntity[ControllerImpl])
```

Returned by `declare_controller`. Collects task handler registrations. After init, automatically starts and stops device keyword subscriptions.

`(sensorkit.api.declarative → sensorkit.api)`

---

### `declare_program`

```python
def declare_program(name: str | None = None) -> DeclaredProgram
```

Used as a class-level descriptor or class decorator to declare a program entity.

```python
@sk.declare_program
class MyProgram:
    @sk.on_attach
    async def startup(self): ...

    @sk.task_factory
    async def next_task(self): ...
```

---

### `DeclaredProgram`

```python
class DeclaredProgram(DeclaredEntity[ProgramImpl])
```

Returned by `declare_program`. Collects `on_enable`, `on_disable`, and `task_factory` registrations.

`(sensorkit.api.declarative → sensorkit.api)`

---

### `declare_entity`

```python
def declare_entity(name: str | None = None) -> DeclaredEntity
```

Declares a generic entity (not a device, controller, or program). For advanced use cases where the built-in entity types do not apply.

---

### `DeclaredEntity`

```python
class DeclaredEntity[T: EntityImpl = EntityImpl]
```

Base class for all declaration objects. Holds the entity name and queued callbacks; registers them with the backend when the service starts.

`(sensorkit.api.declarative → sensorkit.api)`

---

### `decl_for_instance`

```python
def decl_for_instance(instance) -> DeclaredEntity | None
```

Return the `DeclaredEntity` attached to a service class instance, if any.

---

### `entity_for_instance`

```python
def entity_for_instance(instance) -> EntityImpl | None
```

Return the running `EntityImpl` for a given service class instance, if registered.

---

## Lifecycle callbacks

These decorators register methods on a service class as callbacks invoked at specific points in the entity lifecycle. They work either on a named descriptor (e.g. `@mount.on_attach`) or as standalone decorators (e.g. `@sk.on_attach`) when used with a class-level `@declare_*` decorator.

---

### `on_attach`

```python
@sk.on_attach
async def startup(self): ...
```

Called after the entity is registered with the backend and ready to operate. Equivalent to "after init."

---

### `on_detach`

```python
@sk.on_detach
async def teardown(self): ...
```

Called when the service is shutting down, after the main service loop ends.

---

### `on_enable`

```python
@sk.on_enable
async def handle_enable(self): ...
```

Called when the entity transitions to the enabled state. Applies to devices and programs.

---

### `on_disable`

```python
@sk.on_disable
async def handle_disable(self): ...
```

Called when the entity transitions to the disabled state. Applies to devices and programs.

---

### `command_handler`

```python
@decl.command_handler(CommandType)
async def handle_cmd(self, cmd: CommandType) -> BaseModel | None: ...
```

Registers a method as the handler for a given `DeviceCommand` subclass. The parameter type annotation must match the declared command type. Only one handler per command type is allowed per device.

---

### `task_handler`

```python
@decl.task_handler(TaskType)
async def handle_task(self, task: TaskType) -> None: ...
```

Registers a method as the handler for a given `ControllerTask` subclass on a controller entity. The parameter type is used for dispatch.

---

### `task_factory`

```python
@sk.task_factory
async def make_task(self) -> ControllerTask: ...
```

Registers the method that the program calls to produce the next `ControllerTask` when activated by the agent. Only one task factory can be registered per program.

---

## Context accessors

These functions return the `*Impl` object for the entity currently executing a handler, allowing service code to publish keywords, emit events, and access backend facilities without holding a direct reference.

### `entity`

```python
def entity(obj=None) -> EntityImpl | None
```

Return the entity implementation for the given object, or for the current execution context if `obj` is `None`.

### `device`

```python
def device(obj=None) -> DeviceImpl | None
```

Return the `DeviceImpl` for the given object or current context.

### `controller`

```python
def controller(obj=None) -> ControllerImpl | None
```

Return the `ControllerImpl` for the given object or current context.

### `program`

```python
def program(obj=None) -> ProgramImpl | None
```

Return the `ProgramImpl` for the given object or current context. Commonly used inside `on_attach` to publish initial offer windows:

```python
@sk.on_attach
async def startup(self):
    now = datetime.now(UTC)
    sk.program().add_offer(now, now + timedelta(days=1))
    await sk.program().publish_offers()
```

---

## Tasks

Tasks are dispatched by the agent or the CLI to a controller, which routes them to the appropriate `task_handler`.

### `ControllerTask`

```python
class ControllerTask(RegistryBaseModel):
    task_type: str
    task_id: uuid.UUID
    controller_id: str
    context: KeywordDict | None
    end_time: datetime | None
```

Base class for all controller tasks. Subclasses set a `task_type` literal used for dispatch.

**Methods:**
- `target_state() -> InternalControllerState | None` — Return the operating-state transition implied by this task, if any.
- `set_context(context: KeywordDict)` — Associate keyword context with the task (called by the agent).
- `get_context() -> KeywordDict` — Return the associated context, or an empty dict.

`(sensorkit.core.task → sensorkit.api)`

---

### `InitTask`

```python
class InitTask(ControllerTask):
    task_type: Literal["init"] = "init"
```

Signals the controller to connect devices, initialize hardware, and transition to the `OPERATE` state.

---

### `StandbyTask`

```python
class StandbyTask(ControllerTask):
    task_type: Literal["standby"] = "standby"
```

Signals the controller to enter a warm, ready-but-not-collecting `STANDBY` state.

---

### `ShutdownTask`

```python
class ShutdownTask(ControllerTask):
    task_type: Literal["shutdown"] = "shutdown"
```

Signals the controller to close hardware and disconnect devices, transitioning to `SHUTDOWN`.

---

### `CollectTask`

```python
class CollectTask(ControllerTask):
    task_type: Literal["collect"] = "collect"
```

Base class for data-collection tasks. Subclassed by `StandardCollectTask` (in `sensorkit.std.collect`) which adds target, camera parameters, and timing fields.

---

### `CalibrateTask`

```python
class CalibrateTask(ControllerTask):
    task_type: Literal["calibrate"] = "calibrate"
```

Signals the controller to perform a calibration routine.

---

### `RecoverTask`

```python
class RecoverTask(ControllerTask):
    task_type: Literal["recover"] = "recover"
```

Signals the controller to reconnect devices and clear in-progress motion after a fault.

---

## Device commands

Commands are sent to devices via `DeviceClient.command(cmd)` or issued internally by the sensor controller.

### `DeviceCommand`

```python
class DeviceCommand(RegistryBaseModel):
    command_id: str
```

Base class for all device commands. Subclasses define a `command_id` literal that is used as a registry key and dispatch discriminator.

`(sensorkit.core.device → sensorkit.api)`

---

### `Abort`

```python
class Abort(DeviceCommand):
    command_id: Literal["Abort"] = "Abort"
```

Built-in command requesting immediate halt of in-progress motion or operation.

---

### `CommandStarted`

```python
class CommandStarted(Event):
    command_id: str
    call_id: uuid.UUID
```

Event emitted by a device when it begins executing a command.

---

### `CommandDone`

```python
class CommandDone(Event):
    command_id: str
    call_id: uuid.UUID
    success: bool
```

Event emitted by a device when a command finishes. `success` is `False` if the command raised an error.

---

### Standard device commands

The following `DeviceCommand` subclasses are defined in `sensorkit.models.devices` and re-exported from `sensorkit.api`. Each corresponds to a standard device operation.

| Command | Description |
|---|---|
| `Init` | Initialize (connect and prepare) the device |
| `Deinit` | Deinitialize (disconnect and clean up) the device |
| `Connect` | Establish a connection to the hardware driver |
| `Disconnect` | Close the connection to the hardware driver |
| `Enable` | Enable command handling |
| `Disable` | Disable command handling |
| `Home` | Move the device to its home position |
| `MoveToPark` | Move the device to its park position |
| `SetParkPosition` | Define the current position as the park position |
| `FollowTarget` | Begin tracking or slewing to a target |
| `Stop` | Stop all motion |
| `Open` | Open the device (dome shutter, mirror cover, etc.) |
| `Close` | Close the device |
| `SetFilter` | Select a filter by name or index |
| `SetBinning` | Set camera binning |
| `ChangeFocusPosition` | Move focuser to an absolute or relative position |
| `ChangeRotatorPosition` | Move rotator to an absolute position |
| `SetSyncEnabled` | Enable or disable mount synchronization |
| `SetTemperature` | Set a temperature setpoint |
| `EnableAxis` | Enable a mount axis |
| `DisableAxis` | Disable a mount axis |
| `CameraCapture` | Trigger a camera exposure |

---

## Device status models

These Pydantic models are published as keywords to the entity stream by device implementations. Subscribe to them via `EntityClient.monitor(ModelType)`.

| Model | Description |
|---|---|
| `Connected` | Whether the device is connected to its hardware driver |
| `Enabled` | Whether command handling is enabled |
| `Opened` | Whether the device (dome, mirror cover) is currently open |
| `Target` | Current slew or tracking target |
| `SitePosition` | Observatory location (`latitude_degrees`, `longitude_degrees`, `altitude_km`) |
| `Filter` | Currently selected filter |
| `FocusPosition` | Current focuser position |
| `RotatorPosition` | Current rotator angle |
| `Binning` | Current camera binning (`x`, `y`) |
| `Temperature` | Current temperature reading |
| `TemperatureUnit` | Unit of the temperature reading |
| `MountAxis` | State of a single mount axis |
| `AxisTargetDistance` | Angular distance between current and target axis positions |
| `CameraSensorSize` | Physical sensor dimensions |
| `BasicWeather` | Weather snapshot (`humidity`, `wind_speed`, `rain_rate`, `sky_temperature`, etc.) |

`(sensorkit.models.devices, sensorkit.std.weather → sensorkit.api)`

---

## Client types

Client objects are used to interact with remote entities from outside the service that hosts them. Obtain them via `SensorKit.device(name)`, `.controller(name)`, or `.program(name)`.

### `EntityClient`

```python
class EntityClient(EntityBase)
```

Base client for any entity. Provides KV access, event monitoring, and keyword streaming.

**Key methods:**
- `kv_get_model(model_type: type[M]) -> M` — Read a model from the entity's KV namespace.
- `kv_put_model(model: BaseModel)` — Write a model to the entity's KV namespace.
- `monitor(model_type: type[M])` — Async generator yielding `(Subject, M)` as the entity publishes keyword updates.
- `monitor_event(event_type: type[M])` — Async generator yielding events of the specified type.
- `monitor_all_events()` — Async generator yielding all events from the entity's stream.
- `observe_online_state()` — Async generator yielding `True`/`False` as the entity comes online or goes offline.
- `call(request, data)` — Invoke a request and return a `Call` tracking the response.

`(sensorkit.core.entity → sensorkit.api)`

---

### `DeviceClient`

```python
class DeviceClient(EntityClient)
```

Client for device entities.

**Additional methods:**
- `enable()` — Enable the device's command handlers.
- `disable()` — Disable the device's command handlers.
- `command(cmd: DeviceCommand) -> Call` — Send a command and track its result.
- `get_details() -> DeviceDetails` — Fetch and cache the device's supported-command list.
- `has_trait(trait: Trait) -> bool` — Return `True` if this device satisfies the given trait.
- `get_traits(traits) -> list[Trait]` — Return all matching traits from an iterable.
- `get_archetype() -> Trait | None` — Return the device's matching archetype, if any.

`(sensorkit.core.device → sensorkit.api)`

---

### `ControllerClient`

```python
class ControllerClient(EntityClient)
```

Client for controller entities.

**Additional methods:**
- `enable()` — Enable task handling.
- `disable()` — Disable task handling.
- `execute_task(task: ControllerTask, interrupt=False) -> Call` — Send a task and track completion.
- `abort_task(task_id: UUID | None = None) -> Call` — Request abort of the current (or specified) task.
- `wait_for_task(task_id: UUID | None = None)` — Await until no task is executing.

`(sensorkit.core.controller → sensorkit.api)`

---

### `ProgramClient`

```python
class ProgramClient(EntityClient)
```

Client for program entities.

**Additional methods:**
- `enable(target_controller: str)` — Enable task sourcing for the given controller.
- `disable()` — Disable task sourcing.
- `start_tasking(contexts: TaskContexts | None = None)` — Begin the tasking loop.
- `stop_tasking()` — Stop the tasking loop after the current task completes.
- `abort_tasking()` — Stop the tasking loop and abort the current task immediately.
- `wait_until_tasking_stops()` — Await until the tasking loop ends.
- `tasking_change_events()` — Async generator yielding `ProgramActiveState` events.

`(sensorkit.core.program → sensorkit.api)`

---

## Implementation types

These are the server-side objects available inside service code — typically via the context accessors (`sk.device()`, `sk.controller()`, etc.) or directly from `DeclaredEntity.impl`.

### `EntityImpl`

```python
class EntityImpl
```

Server-side implementation of a generic entity. Provides publishing, event emission, KV write access, request handler registration, and a `DataGraph` for the data pipeline.

**Key methods:**
- `publish(model: Keyword)` — Publish a keyword model to the entity's stream.
- `emit_event(event: Event)` — Emit an event to the entity's event stream.
- `kv_put_model(model: BaseModel)` — Write a model to KV.
- `kv_get_model(model_type)` — Read a model from KV.
- `data_graph() -> DataGraph` — Return the entity's data pipeline graph.

`(sensorkit.core.impl.entity → sensorkit.api)`

---

### `DeviceImpl`

```python
class DeviceImpl(EntityImpl)
```

Server-side implementation of a device. Adds command handler registration and enable/disable state management.

`(sensorkit.core.impl.device → sensorkit.api)`

---

### `ControllerImpl`

```python
class ControllerImpl(EntityImpl)
```

Server-side implementation of a controller. Adds task handler registration, device dependency management, and context building from live device keyword state.

**Key methods:**
- `use_device(name: str, *, subscribe: list[type] | None = None)` — Declare a device dependency and optionally subscribe to keyword types.
- `get_device(name: str) -> ControllerDevice` — Return the attached device and its cached keyword state.
- `update_context(base=None, **kwargs) -> Context` — Build and update task context from the current device state.
- `add_offer(start, end)` / `publish_offers()` — Manage and publish program offer windows.

`(sensorkit.core.impl.controller → sensorkit.api)`

---

### `ProgramImpl`

```python
class ProgramImpl(EntityImpl)
```

Server-side implementation of a program. Manages offer windows and drives the tasking loop.

**Key methods:**
- `add_offer(start: datetime, end: datetime, obj=None)` — Add an offer window.
- `remove_offer(start, end, obj=None)` — Remove an offer window.
- `clear_offers()` — Remove all offer windows.
- `publish_offers()` — Publish the current offer set to the keyword stream.

`(sensorkit.core.impl.program → sensorkit.api)`

---

## SensorKit client

### `SensorKit`

```python
class SensorKit:
    backend: Backend
```

Main API entrypoint for interacting with a running SensorKit system. Usually obtained via `connect()` or from `Service.client`.

**Methods:**
- `register_service(name: str, version: str) -> ServiceContext` — Register this instance as a named service.
- `list_services() -> dict[Entity, ServiceStatus]` — Return all visible services and their online status.
- `list_entities() -> dict[Entity, str]` — Return all currently online entities.
- `list_devices() -> list[DeviceListing]` — Return all online devices with resolved traits and archetypes.
- `device(name: str) -> DeviceClient` — Return a client for the named device.
- `controller(name: str) -> ControllerClient` — Return a client for the named controller.
- `program(name: str) -> ProgramClient` — Return a client for the named program.
- `entity(name: str) -> EntityClient` — Return a client for any named entity.

`(sensorkit.core.client → sensorkit.api)`

---

### `ServiceContext`

```python
class ServiceContext
```

Manages a service's backend registration, entity lifecycle, and lease maintenance. Returned by `SensorKit.register_service()`. Held by `Service.context`.

`(sensorkit.core.client → sensorkit.api)`

---

### `ServiceRecord`

```python
class ServiceRecord(BaseModel)
```

Persistent metadata for a registered service (name, version, registration timestamp).

`(sensorkit.core.client → sensorkit.api)`

---

### `ServiceStatus`

```python
class ServiceStatus
    service: ServiceRecord
    online: bool
```

Combines a `ServiceRecord` with a live online flag, as returned by `SensorKit.list_services()`.

`(sensorkit.core.client → sensorkit.api)`

---

### `DeviceListing`

```python
@dataclass
class DeviceListing:
    name: str
    entity: Entity
    archetype: Trait | None
    traits: list[Trait]
```

Describes a discovered device, including its resolved archetype and traits.

`(sensorkit.core.device → sensorkit.api)`

---

## State models

### `EventSourcedState`

```python
class EventSourcedState(BaseModel)
```

Base class for state objects whose fields are populated from an event stream. Used by `ControllerState` and `DeviceState`.

**Key methods:**
- `update(entity, *events, publish_state=True)` — Apply events, emit them on the stream, and optionally write the updated state to KV.
- `recover(entity) -> Self` — Reconstruct state from KV and replay the event stream to ensure currency.
- `recover_or_init(entity, **kwargs) -> Self` — Recover from KV, or create and publish a new instance if none exists.
- `event_stream(entity, event_type) -> AsyncGenerator` — Yield a continuous stream of events of the given type, seeded from the current stored state.

`(sensorkit.core.state → sensorkit.api)`

---

### `ControllerState`

```python
class ControllerState(EventSourcedState):
    enable_state: ControllerEnableState
    operating_state: ControllerOperatingState
    execution_state: TaskExecutionState
```

Snapshot of a controller's complete state, stored in KV and updated as events are emitted.

`(sensorkit.core.controller → sensorkit.api)`

---

### `ProgramState`

```python
class ProgramState(BaseModel):
    enable_state: ProgramEnableState
    active_state: ProgramActiveState
    tasking_status: ProgramTaskingStatus
```

Snapshot of a program's complete state.

`(sensorkit.core.program → sensorkit.api)`

---

## Program types

### `OfferInterval`

```python
class OfferInterval(Interval):
    begin: datetime
    end: datetime
    data: Any
```

A time interval representing one window during which a program can supply tasks. Stored in an `IntervalTree` by `ProgramImpl`.

`(sensorkit.core.program → sensorkit.api)`

---

### `ProgramOffering`

```python
@declare_keyword
class ProgramOffering(BaseModel):
    offer_windows: list[OfferInterval]
```

Keyword published by a program to advertise its current offer windows.

`(sensorkit.core.program → sensorkit.api)`

---

### `ProgramOffers`

```python
class ProgramOffers
```

Internal helper class that manages and serialises a program's offer window set. Not typically used directly.

`(sensorkit.core.impl.program → sensorkit.api)`

---

## Traits and archetypes

Traits describe device capabilities structurally: a device satisfies a trait if it implements all required commands. Archetypes extend traits with optional commands and required sub-traits.

### `Trait`

```python
@dataclass(frozen=True)
class Trait:
    name: str
    required_commands: tuple[type[DeviceCommand], ...]
```

A named set of required commands. A device satisfies a trait if it registers handlers for every command in `required_commands`.

**Methods:**
- `match(details: DeviceDetails) -> bool` — Return `True` if the device satisfies this trait.
- `effective_command_ids() -> frozenset[str]` — Return all required command IDs.

`(sensorkit.core.trait → sensorkit.api)`

---

### `Archetype`

```python
@dataclass(frozen=True)
class Archetype(Trait):
    required_traits: tuple[Trait, ...]
    optional_commands: tuple[type[DeviceCommand], ...]
```

A trait archetype. A device should match at most one archetype. Extends `Trait` with required sub-traits (which must be plain `Trait` instances, not other archetypes) and optional commands that do not affect matching.

`(sensorkit.core.trait → sensorkit.api)`

---

### `declare_trait`

```python
def declare_trait(
    name: str,
    *,
    required_commands: tuple[type[DeviceCommand], ...] = (),
) -> Trait
```

Create a `Trait` and add it to the global trait registry.

---

### `declare_archetype`

```python
def declare_archetype(
    name: str,
    *,
    required_commands: tuple[type[DeviceCommand], ...] = (),
    required_traits: tuple[Trait, ...] = (),
    optional_commands: tuple[type[DeviceCommand], ...] = (),
) -> Archetype
```

Create an `Archetype` and add it to the global trait and archetype registries.

---

## Keywords

Keywords are typed Pydantic models published to entity data streams. The keyword system handles serialization, routing by key, and stream multiplexing.

### `Keyword`

```python
Keyword = Annotated[object, ...]
```

A type annotation matching any registered keyword type. Used in generic stream consumers.

`(sensorkit.common.keyword → sensorkit.api)`

---

### `KeywordDict`

```python
class KeywordDict(dict)
```

A dictionary mapping keyword keys to their values, used for task context and controller state snapshots.

`(sensorkit.common.keyword → sensorkit.api)`

---

### `declare_keyword`

```python
@declare_keyword
class MyModel(BaseModel): ...

# Or with options:
@declare_keyword(key="MyKey", kind="state")
class MyModel(BaseModel): ...
```

Decorator that registers a Pydantic model as a keyword type, enabling it to be published and consumed via the entity stream system.

**Parameters:**
- `key: str | None` — Stream key; defaults to the class name.
- `ns: str | None` — Optional namespace prefix.
- `kind: "stream" | "state" | "config"` — Keyword variant; defaults to `"stream"`.

`(sensorkit.common.keyword → sensorkit.api)`

---

## Data pipeline

### `Context`

```python
class Context
```

A named, typed container for keyword values collected from device state. Built by `ControllerImpl.update_context()` and passed along with tasks for use in data graph pipelines (e.g., populating FITS headers).

`(sensorkit.data.context → sensorkit.api)`

---

### `ContextSubscription`

```python
class ContextSubscription
```

Maintains a live cache of keyword values for a specific entity, updated as new values arrive on the stream. Used by `ControllerImpl` to build contexts from current device state.

`(sensorkit.data.context → sensorkit.api)`

---

### `DataGraph`

```python
class DataGraph
```

A composable pipeline for routing and transforming data (typically image arrays) as they flow from a device through processing stages to a sink. Configured via the entity's `DataGraph` KV key.

Stages are defined in YAML (see [Configuration](configuration.md)) and include built-in operations such as `app_source`, `array_to_fits`, `write_file`, and `app_sink`.

`(sensorkit.data.graph → sensorkit.api)`

---

## Backend

These types provide the low-level NATS abstraction. Most service code does not use them directly.

### `Backend`

```python
class Backend
```

Wraps a `BackendImpl` and exposes `key_value()`, `stream()`, and `request()` context factories.

---

### `BackendImpl`

```python
class BackendImpl(ABC)
```

Abstract base for backend implementations. `NATSBackendImpl` is used in production; `FakeBackendImpl` is used in tests.

---

### `Entity`

```python
class Entity
```

An identifier for a named entity on the bus. Wraps a string name and provides subject-construction helpers.

---

### `Subject`

```python
class Subject
```

A fully-qualified message subject on the bus, composed of an entity name and a property name.

---

### `KVEntry`

```python
class KVEntry
```

A key-value store entry, with `key`, `value`, and metadata fields. Returned by KV read operations.

---

### `KVError`

```python
class KVError(Exception)
```

Raised when a KV operation fails (e.g., key not found, revision conflict).

---

### `RevisionError`

```python
class RevisionError(KVError)
```

Raised when a KV write is rejected due to a revision mismatch (optimistic concurrency failure).

---

### `SpecialProperty`

```python
class SpecialProperty(str, Enum)
```

Enumeration of reserved property names used internally by the backend (e.g., `EntityLease`, `ServiceRecord`).

---

### `Event`

```python
class Event(BaseModel)
```

Base class for all event types published to entity event streams. Events carry a timestamp and event ID.

`(sensorkit.backend.event → sensorkit.api)`

---

### `UnknownEvent`

```python
class UnknownEvent(Event)
```

Placeholder event produced when a received event type is not recognized.

`(sensorkit.backend.event → sensorkit.api)`

---

### `Request`

```python
class Request[P, R, V]
```

Typed descriptor for a request/response interaction on the bus. Created via `Request.define(name, payload=..., response=..., result=...)`.

`(sensorkit.backend.request → sensorkit.api)`

---

### `CallContext`

```python
class CallContext
```

Context object available to request handler functions, providing access to the caller's identity and the ability to send intermediate responses.

`(sensorkit.backend.request → sensorkit.api)`

---

### `CallError`

```python
class CallError(Exception)
```

Raised when a request call results in an error response from the remote handler.

`(sensorkit.backend.request → sensorkit.api)`

---

### `ExtendedResponse`

```python
class ExtendedResponse(BaseModel)
```

Base class for extended-response messages, used when a request handler may send an initial acknowledgement before the final result (e.g., task execution).

`(sensorkit.backend.request → sensorkit.api)`
