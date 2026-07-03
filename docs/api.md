# API reference — `sensorkit.api`

Everything you need to write devices, controllers, and programs is exported from one module:

```python
import sensorkit.api as sk
```

The API is small by design: you declare entities with decorators, attach handlers, and hand the class to a `Service`. This page covers the public surface, grouped by role. SensorKit is in beta — this surface is stable in shape but still evolving in detail.

---

## Service entry

### `service_entrypoint`

```python
@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(MyProgram)
    await service.run()
```

Marks an `async` function as the entrypoint of a service. The CLI (`sensorkit service run`, `sensorkit go`) finds this object in your module or `.py` file and invokes it with a fresh `Service` named after the service definition. The decorated function is not directly callable.

### `Service`

Represents the running service instance passed to your entrypoint.

- `include(obj, *, name=None)` — Register the entity declarations found in a class, instance, or module. A single-entity service may omit `name`, in which case the entity shares the service's name.
- `add(declared, name=None)` — Register a declaration object directly.
- `include_module()` — Register all declarations found in the calling module.
- `run()` — Connect to the backend, register everything, and run until shutdown. (`start()`/`stop()` are available for finer control.)
- Attributes: `name`, `version`, `client` (a `SensorKit`), `context` (a `ServiceContext`).

Entity registration acquires an **exclusive lease** per entity, so accidentally starting a second copy of a device service fails fast instead of double-driving hardware.

### `connect`

```python
kit = await sk.connect()                    # backend from env/config; NATS by default
```

Create a standalone `SensorKit` client without running a service — for scripts, notebooks, and tests. Pass `backend="fake"` for the in-memory test backend.

### `load_config` / `import_modules` / `set_config_location`

Bootstrap helpers: `set_config_location(path)` points at a unified config file (else `$SENSORKIT_CONFIG` / `./sensorkit.yaml`), `load_config()` reads and fully resolves it (importing configured modules), and `import_modules()` performs just the imports — needed before deserializing types that modules define.

---

## Declaring entities

Three entity kinds cover almost everything: **devices** (wrap hardware), **controllers** (sequence devices to execute tasks), and **programs** (produce tasks). Each has a `declare_*` function usable as a class decorator or an explicit descriptor:

```python
@sk.declare_device
class MyMount: ...

# or, with capability annotations checked at registration:
@sk.declare_device(type=MOUNT_ARCHETYPE, traits=[TRACKING])
class MyMount: ...

# or explicitly, for multiple entities in one class:
class Bundle:
    mount = sk.declare_device(name="mount-a")
    wheel = sk.declare_device(name="wheel-a")
```

- `declare_device(cls=None, *, name=None, type=None, traits=None)` — declare a device; `type` (an `Archetype`) and `traits` are validated against the registered command handlers when the service starts, so a device claiming "mount" that forgot its `Stop` handler fails at startup, not mid-slew.
- `declare_controller(cls=None, *, name=None)` — declare a controller.
- `declare_program(cls=None, *, name=None)` — declare a program.
- `declare_entity(cls=None, *, name=None)` — a generic entity, for anything that just publishes/consumes data.

The returned `DeclaredDevice` / `DeclaredController` / `DeclaredProgram` / `DeclaredEntity` objects collect callbacks until the service starts, then create and register the corresponding implementation (`impl`). Helpers `decl_for_instance(obj)` and `entity_for_instance(obj)` recover the declaration or running impl from your class instance.

## Callbacks

Attach behavior with decorators — bare (`@sk.on_attach`, auto-associated with the class's declaration) or scoped to a descriptor (`@mount.on_attach`):

| Decorator          | Applies to  | Fires                                                        |
|--------------------|-------------|--------------------------------------------------------------|
| `on_attach`        | all         | after the entity is registered and ready                    |
| `on_detach`        | all         | at service shutdown                                          |
| `on_enable`        | device, program | when enabled (programs receive the target controller)   |
| `on_disable`       | device, program | when disabled                                            |
| `command_handler`  | device      | when the matching `DeviceCommand` arrives                    |
| `task_handler`     | controller  | when the matching `Task` is dispatched                       |
| `task_factory`     | program     | when the tasking loop wants the next task                    |

Handler dispatch is by parameter type annotation:

```python
@sk.command_handler
async def go_home(self, cmd: Home): ...          # handles Home commands

@sk.task_handler
async def do_collect(self, task: StandardCollectTask): ...
```

One handler per command/task type per entity; only one task factory per program.

## Context accessors

Inside any handler, these return the implementation object for the entity currently executing (or, given an argument, for that instance):

```python
sk.entity()      # -> EntityImpl | None
sk.device()      # -> DeviceImpl | None
sk.controller()  # -> ControllerImpl | None
sk.program()     # -> ProgramImpl | None
```

Typical use — publishing offers from a program hook:

```python
@sk.on_attach
async def startup(self):
    sk.program().add_offer(start, end)
    await sk.program().publish_offers()
```

---

## Tasks

### `Task`

```python
class Task(RegistryBaseModel):
    task_type: str          # Literal discriminator set by each subclass
```

The base of the task system, and the user-extensible part: subclass it, set a `task_type` literal, add your fields, and it registers itself for dispatch and serialization. Execution-envelope data (IDs, context, expiry) lives on a separate `TaskExecution` minted by the controller at submission time; inside a task handler it's available as `task.execution`.

- `submit(*, context=None, expiry_time=None) -> TaskSubmission` — bundle execution parameters, for use in task factories.
- `default_expiry() -> datetime | timedelta` — deadline used when no explicit expiry is supplied (default 300 s from dispatch); override for domain-specific deadlines.
- `target_state()` — for lifecycle tasks, the controller state this task drives toward.

**Built-in lifecycle tasks:** `InitTask`, `StandbyTask`, `ShutdownTask`, `RecoverTask`, plus the `CollectTask` and `CalibrateTask` bases. `StandardCollectTask` (in `sensorkit.std.collect`) is the fully-featured observation task — see [Observing programs](programs.md#standardcollecttask).

### `TaskSubmission`, `TaskExecution`, `TaskInfo`

`TaskSubmission` pairs a task with optional context and expiry. `TaskExecution` is the controller-minted envelope (`task_id`, `controller_id`, context, expiry) that tracks a dispatched task; awaiting it yields the execution result. `TaskInfo` is the keyword snapshot of task identity published into data-pipeline context (e.g. for FITS `TASKID` headers).

---

## Device commands

### `DeviceCommand`

```python
class DeviceCommand(RegistryBaseModel):
    command_id: str          # Literal discriminator per subclass
```

Commands are typed Pydantic models sent to devices via `DeviceClient.command(...)`. Devices emit `CommandStarted` and `CommandDone` events around execution (`CommandDone.success` is `False` on error). `Abort` is the built-in halt-everything command.

### Standard commands

`sensorkit.models.devices` defines the shared vocabulary (re-exported from `sk`): `Init`, `Deinit`, `Connect`, `Disconnect`, `Enable`, `Disable`, `Home`, `MoveToPark`, `SetParkPosition`, `FollowTarget`, `Stop`, `Open`, `Close`, `SetFilter`, `SetBinning`, `ChangeFocusPosition`, `ChangeRotatorPosition`, `SetSyncEnabled`, `SetTemperature`, `EnableAxis`, `DisableAxis`, `CameraCapture`.

Devices implement whichever subset applies; traits and archetypes (below) describe the result.

---

## Traits and archetypes

Devices are matched **structurally**: a device satisfies a `Trait` if it implements all of the trait's required commands (and publishes its required keywords). This is what lets controllers and tooling work against capabilities rather than specific driver classes.

```python
TRACKING = sk.declare_trait("tracking", required_commands=(FollowTarget, Stop))
```

- `Trait(name, required_commands, required_keywords)` — `match(details)` tests a device; declared via `declare_trait(...)`.
- `Archetype` — a trait a device should match at most one of (mount, camera, …), adding `required_traits` and non-matching `optional_commands`; declared via `declare_archetype(...)`.

Declared traits/archetypes join global registries used for discovery (`SensorKit.list_devices()` reports each device's resolved traits and archetype) and for validation at device registration when passed to `declare_device(type=..., traits=[...])`.

---

## Keywords

Keywords are the typed telemetry system: Pydantic models published to an entity's stream and consumed by name-and-type anywhere else.

```python
@sk.declare_keyword
class ShutterState(BaseModel):
    open_fraction: float
```

`declare_keyword(key=None, ns=None, kind="stream")` registers the model (`kind` may be `"stream"`, `"state"`, or `"config"`). `Keyword` is the annotation matching any registered keyword; `KeywordDict` maps keyword keys to values and is the currency of task contexts.

Publish from inside a service with `sk.entity().publish(model)`; consume remotely with `EntityClient.monitor(ModelType)`.

---

## Clients

`SensorKit` is the client-side entrypoint — obtained from `sk.connect()` or `Service.client`:

- `device(name)` / `controller(name)` / `program(name)` / `entity(name)` — typed clients for remote entities.
- `list_services()`, `list_entities()`, `list_devices()` — discovery with online status; `list_devices()` includes resolved traits/archetypes.
- `register_service(name, version)` — join the bus as a named service.

### `EntityClient`

Base client for any remote entity:

- `kv_get_model(ModelType)` / `kv_put_model(model)` — typed KV access in the entity's namespace.
- `monitor(ModelType)` — async generator of keyword updates.
- `monitor_event(EventType)` / `monitor_all_events()` — async generators of events.
- `observe_online_state()` — async generator of `True`/`False` liveness transitions.
- `call(request, payload)` — invoke a typed request/response.

### `DeviceClient`

Adds: `command(cmd) -> Call`, `enable()` / `disable()`, `get_details()`, and trait queries (`has_trait`, `get_traits`, `get_archetype`).

### `ControllerClient`

Adds:

- `execute_task(task, *, context=None, expiry_time=None, interrupt=False) -> Call` — submit a task; the returned `Call` tracks acknowledgement and completion.
- `abort_task(task_id=None)` — abort the current (or a specific) task.
- `wait_for_task(task_id=None)` — wait until nothing (or the given task) is executing.
- `enable()` / `disable()` — gate task handling.

### `ProgramClient`

Adds: `enable(target_controller)` / `disable()`, `start_tasking(contexts=None)`, `stop_tasking()` (graceful), `abort_tasking()` (immediate), `wait_until_tasking_stops()`, and `tasking_change_events()`. The agent is the usual caller of these; the CLI and your own tooling can use them too.

---

## Implementation objects

The server-side counterparts, reached via the context accessors inside your service code:

### `EntityImpl`

- `publish(keyword_model)` — publish telemetry to the entity's stream.
- `emit_event(event)` — emit to the event stream.
- `kv_get_model(ModelType)` / `kv_put_model(model)` — the entity's KV namespace.
- `data_graph()` — the entity's configured [data pipeline](configuration.md#data-flow).

### `DeviceImpl`

Adds command-handler registration and enable/disable state (normally driven for you by the declarative layer).

### `ControllerImpl`

- `use_device(name, *, subscribe=[...])` — declare a device dependency, optionally subscribing to keyword types; returns a handle for sending commands and reading cached state.
- `get_device(name)` / `all_devices()` — access attached devices.
- `update_context(...)` — build a `Context` snapshot from live device keyword state (feeds FITS headers and pipeline expressions).

### `ProgramImpl`

- `add_offer(start, end)` / `remove_offer(start, end)` / `clear_offers()` — manage the offer set.
- `publish_offers()` — publish it (as the `ProgramOffering` keyword, a list of `OfferInterval`s).

---

## State

### `EventSourcedState`

Base for state models that are rebuilt from an event stream — the mechanism behind crash-safe controller/device state:

- `update(entity, *events)` — apply and emit events, persisting the new state.
- `recover(entity)` / `recover_or_init(entity, **defaults)` — reconstruct from KV plus event replay.
- `event_stream(entity, EventType)` — continuous typed event stream, seeded from stored state.

`ControllerState` (enable / operating / task-execution state) and `ProgramState` (enable / active / tasking status) are the primary instances.

---

## Backend

The low-level bus abstraction. Most service code never touches it, but it's public for advanced integrations and tests:

- `Backend` / `BackendImpl` — `NATS` in production, `fake` (in-memory) for tests; the entire test suite runs against either.
- `Entity`, `Subject` — addressable names on the bus.
- `KVEntry`, `KVError`, `RevisionError` — KV records and failures (`RevisionError` signals an optimistic-concurrency conflict).
- `Event`, `UnknownEvent` — event stream base types.
- `Request`, `CallContext`, `CallError`, `ExtendedResponse` — the typed request/response layer used by commands and tasks.
- `SpecialProperty` — reserved subject tokens used internally.

---

## Configuration sections

### `declare_config_section`

```python
sk.declare_config_section(
    "mysection",
    list[MySectionConfig],
    entity_mapper=lambda raw: (elem.pop("id") for elem in raw),
    model_mapper=iter,
    service_path="myorg.myservice",
)
```

Registers a new top-level section for the [unified config file](configuration.md). This is how SensorKit's own modules plug in (`alpaca:`, `automation:`, …), and your packages can do the same: the section's records are validated against your Pydantic model, written to the owning entities' KV namespaces, and — if `service_path` is given — a service is registered automatically for `sensorkit go` to launch.
