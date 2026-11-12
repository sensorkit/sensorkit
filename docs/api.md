# API reference - `sensorkit.api`

Everything you need to write devices, controllers, and programs is exported from one module:

```python
import sensorkit.api as sk
```

The API is small by design: you declare entities with decorators, attach handlers, and hand the class to a `Service`. This page covers the public surface, grouped by role.

!!! note "The API is still settling"

    SensorKit is in beta.
    Public names, signatures, and configuration formats may change.

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
async def do_init(self, task: sk.InitTask): ...

@sk.task_handler
async def do_collect(self, execution: sk.TaskExecution[StandardCollectTask]): ...
```

A task handler receives either the task or its execution envelope, which adds the task ID and context.
`sk.task_handler` selects the input from the parameter annotation.
`ControllerImpl.add_task_handler` registers the callback explicitly through `task_callback` or `execution_callback`.
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

`Task` defines the work to perform.
Subclasses set a `task_type` literal and add domain-specific fields.
They register automatically for dispatch and serialization.
The controller creates a separate `TaskExecution` with identity, context, and expiry when a task is submitted.
A declared handler annotated with `T` receives the task.
A declared handler annotated with `TaskExecution[T]` receives the envelope and accesses its task through `.task`.

- `submit(*, context=None, expiry_time=None) -> TaskSubmission` — bundle execution parameters, for use in task factories.
- `default_expiry() -> datetime | timedelta` — deadline used when no explicit expiry is supplied (default 300 s from dispatch); override for domain-specific deadlines.
- `target_state()` — for lifecycle tasks, the controller state this task drives toward.

Built-in tasks include `InitTask`, `StandbyTask`, `ShutdownTask`, `RecoverTask`, `CollectTask`, and `CalibrateTask`.
The standard observation task is `StandardCollectTask` in `sensorkit.std.collect`.
See [Observing programs](programs.md#standardcollecttask).

### `TaskSubmission`, `TaskExecution`, `TaskInfo`

`TaskSubmission` pairs a task with optional context and expiry.
`TaskExecution[T]` carries the controller-minted identity, execution parameters, and a task of type `T`.
When `submitted_context` is omitted or `None`, it defaults to a deep copy of `context`.
An explicitly supplied mapping is retained without copying.
Controller-created executions capture submitted context before adding task identity.
When reading an envelope without a snapshot, the fallback captures its stored working context.
The working `context` includes task identity and can change during execution.
Executions returned by `ControllerClient.start_task` can be awaited for the final result.
`TaskInfo` records task identity in data-pipeline context under the keyword key `Task`.
Configuration expressions use `Task.task_id`; Python code can access the keyword through `context[TaskInfo]`.

---

## Device commands

### `DeviceCommand`

```python
class DeviceCommand(RegistryBaseModel):
    command_id: str          # Literal discriminator per subclass
```

Commands are typed Pydantic models sent to devices via `DeviceClient.command(...)`. Devices emit `CommandStarted` and `CommandDone` events around execution (`CommandDone.success` is `False` on error). `Abort` is the built-in halt-everything command.

### Standard commands

`sensorkit.std` defines the shared vocabulary. Generic commands live in `sensorkit.std.traits`: `Init`, `Deinit`, `Connect`, `Disconnect`, `Enable`, `Disable`, `Home`, `Stop`, `MoveToPark`, `SetParkPosition`. Family-specific modules add the rest, e.g. `sensorkit.std.mount` (`FollowTarget`, `ApplyOffset`, `EnableAxis`, `DisableAxis`, pointing-model commands), `sensorkit.std.optics` (`SetFilter`, `ChangeFocusPosition`, `OpenMirrorCover`, `CloseMirrorCover`), `sensorkit.std.instrument` (`CameraCapture`, `ConfigureCameraSensor`, `ChangeRotatorPosition`), and `sensorkit.std.enclosure` (`OpenEnclosure`, `CloseEnclosure`, `MoveEnclosure`). Everything is re-exported from `sensorkit.std`.

Devices implement whichever subset applies; traits and archetypes (below) describe the result.

---

## Traits and archetypes

Traits match advertised capabilities.
A trait with required commands matches a device that implements all of those commands.
For these traits, required keyword publications are trusted rather than checked.
A trait with no required commands matches its required keyword publications.
Controllers can therefore select devices by capability.

```python
TRACKING = sk.declare_trait("tracking", required_commands=(FollowTarget, Stop))
```

- `Trait(name, required_commands, required_keywords)` — `match(details)` tests a device; declared via `declare_trait(...)`.
  A device that implements a trait's required commands is currently assumed to publish its required keywords.
  A trait with no required commands still needs its keywords declared.
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

- `start_task(task, *, context=None, expiry_time=None, interrupt=False) -> TaskExecution` - Await task acceptance and return the execution envelope, which can be awaited for its final result.
- `execute_task(task, *, context=None, expiry_time=None, interrupt=False) -> Call` — submit a task; the returned `Call` tracks acknowledgement and completion.
- `abort_task(task_id=None)` — abort the current (or a specific) task.
- `wait_for_task(task_id=None)` — wait until nothing (or the given task) is executing.
- `enable()` / `disable()` — gate task handling.

### `ProgramClient`

Adds: `enable(target_controller)` / `disable()`, `start_tasking(contexts=None)`, `stop_tasking(session_id=None)` (graceful), `abort_tasking(session_id=None)` (immediate), `wait_until_tasking_stops()`, and `tasking_change_events()`. The agent is the usual caller of these; the CLI and your own tooling can use them too.

`start_tasking()` creates a `ProgramActivation` without sending a request.
Await it to send the request and receive a `ProgramTaskingMonitor` after startup is confirmed.
If the requested contexts match active tasking, activation returns a monitor for that existing session.

```python
monitor = await program.start_tasking(contexts)
```

To check demand immediately before dispatch, finish preparation before calling `send()`.
Once prepared, `send()` sends the start request before its first suspension.

```python
activation = program.start_tasking(contexts)
await activation.prepare()
check_demand()
monitor = await activation.send()
```

Repeated calls to `send()` and repeated awaits use the same start request.
Canceling a waiter leaves that request running.
If startup fails after acceptance, `activation.monitor` retains the accepted session's monitor.

Close the monitor or activation when observation ends, or use the monitor as a context manager.
Closing either releases the event subscription without stopping tasking.
The monitor retains the session's first fault and the event that marks it inactive.
Use `monitor.events()` to iterate its session's `ProgramTaskingState` events in order.
Iteration ends when a fault or inactive state is observed.
The initial snapshot is available through the monitor's properties and is not yielded.
`monitor.read_events()` processes buffered events immediately and returns the selected session's tasking states.
Use one iterator or waiter at a time, and drain buffered events after that consumer stops.
Cancellation leaves unread events available for cleanup.

Constructing `ProgramTaskingMonitor(program)` directly starts buffering events.
Call `bind(session_id, initial=None)` before consuming them.
Activation performs construction and binding automatically.

Pass `monitor.session_id` to `stop_tasking()` or `abort_tasking()` to stop that session.
A request for an ended session succeeds without stopping a later session.

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
- `register_instrument(name, *, devices=())` - add a name and optional device associations to controller metadata. See [Instruments](#instruments).
- `instrument_context(name)` - return a new `Context` holding a registered instrument's metadata. See [Instruments](#instruments).
- `get_device(name)` / `all_devices()` — access attached devices.
- `update_context(...)` — build a `Context` snapshot from live device keyword state (feeds FITS headers and pipeline expressions).
- `report_state(state, context)` — report the observed operating state and its lifecycle input.

A controller reports UNKNOWN when its service starts.
The controller reports TRANSITION while a lifecycle task, including recovery, is pending or running.
While it reports TRANSITION, it rejects other tasks even with `interrupt`.
Only another lifecycle task or an abort can take over.

UNKNOWN and ERROR latch until a lifecycle task succeeds.
A failed lifecycle task latches ERROR, and one aborted after it starts latches UNKNOWN.
Controllers can report UNKNOWN or ERROR to latch a fault without reporting known states.
Reporting UNKNOWN or ERROR after a lifecycle handler returns and before settlement fails that task.
A latch keeps the recorded lifecycle input for diagnosis.
Reporting another state does not clear a latch.
Restarted controllers begin at UNKNOWN.
A persisted failure or interrupted transition keeps UNKNOWN latched until a lifecycle task succeeds.
After a restart from a known state, an attach hook can report a known state for the agent to adopt.

#### Controllers that report no known states

A successful lifecycle task establishes its target state, with its submitted context as lifecycle input.
The recorded input describes how that state was established.
After the controller leaves that state, the input is retained only for diagnosis.
These controllers rely on their handlers raising when a lifecycle task fails.

#### Controllers that report known states

A known state is reported with its lifecycle input, which replaces the state and input established by lifecycle tasks.
A lifecycle task succeeds when its handler returns successfully and the latest reported state and input match its target and submitted context.
If they do not yet match, the controller waits up to `confirmation_timeout` seconds after the handler returns.
The default is five seconds.
A controller that reports known states keeps them current, for example by polling the hardware or by reporting the target from its lifecycle handler.
An execution handler can pass `execution.submitted_context` to `report_state` while changing its working `execution.context`.

The agent skips a lifecycle task when the reported state and input already match demand.
A controller must therefore be able to reconstruct the input it reports.
Each input keyword must be read back from the hardware or be authoritative in software, for example held in memory by the controller.
A controller that cannot reconstruct its input should do one of the following.

- Report no known states.
- Report UNKNOWN at startup.
- Report a known state only if the controller has no input options for that state.

#### Instruments

Controllers publish instrument names and device associations in `EntityInfo.details.instruments`.
Names are case-sensitive and unique within a controller.
Register instruments in an attach hook to include them in the initial publication.
Later registrations require a call to `publish_entity_info()`.

```python
@sk.on_attach
def register_instruments(self):
    sk.controller().register_instrument("imager")
```

Supply device IDs or device clients through `devices`.
The published description contains opaque IDs whose order has no meaning.
Instruments can share device associations or have none.
Call `use_device()` separately for device dependencies.
A TCS integration can expose instruments without SensorKit devices.

Read the controller's `EntityInfo` to discover its instruments.

```python
info = await kit.controller("MySensor").kv_get_model(sk.EntityInfo)

if isinstance(info.details, sk.ControllerDetails):
    for instrument in info.details.instruments:
        print(instrument.name, instrument.devices)
```

Set `CameraParameterSet.instrument` to select an instrument for the entire sequence.
Collect handlers must honor the selection or reject the task.
The controller chooses an instrument when the field is omitted.

Use `instrument_context(name)` inside a controller to read its local registration without a backend request.
It returns a fresh `Context` containing the registered `InstrumentInfo` under the key `Instrument`.
Changing the context or its metadata leaves the registration unchanged.
Unknown names raise `KeyError`.
Custom controllers can use the accessor for instruments with no device associations.

The sensor controller resolves instrument context for each capture, including automatic selections.
Capture context merges task values, instrument values, then device snapshots from root to camera.
Planned per-frame `CollectInfo` metadata belongs to the task values under the keyword key `Collect`.
Python code imports `CollectInfo` from `sensorkit.std.collect` and accesses it through `context[CollectInfo]`.
Configuration expressions use `Collect.frame_number`.
Later values replace earlier values.
DataGraph expressions can read `Instrument.name` and `Instrument.devices`.
For example, `=Instrument.name` can supply the FITS `UNITID` value.

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

- `Backend` / `BackendImpl` - Backend interface and implementation; `nats` uses NATS, and `fake` provides in-memory messaging and storage for tests.
  Backend tests exercise both implementations when a NATS server is available.
- `Entity`, `Subject` — addressable names on the bus.
- `KVEntry`, `KVError`, `RevisionError` — KV records and failures (`RevisionError` signals an optimistic-concurrency conflict).
- `Event`, `UnknownEvent` — event stream base types.
- `Request`, `LongRequest`, `CallContext`, `CallError`, `HandlerError`, `declare_request`, `declare_long_request` — the typed request/response layer used by commands and tasks.
- `SpecialProperty` — reserved subject tokens used internally.

---

## Configuration sections

### `declare_config_section`

```python
sk.declare_config_section(
    "mysection",
    list[MySectionConfig],
    id_source="by_subkey",
    service_path="myorg.myservice",
)
```

Registers a new top-level section for the [unified config file](configuration.md). This is how SensorKit's own modules plug in (`alpaca:`, `automation:`, …), and your packages can do the same: the section's records are validated against your Pydantic model, written to the owning entities' KV namespaces, and — if `service_path` is given — a service is registered automatically for `sensorkit go` to launch.

Every record belongs to an entity, and `id_source` says where the section gets that entity's ID:

| Value | The section is | The ID comes from |
|-------|----------------|---------------------|
| `by_subkey` | a list of entries, one entity each | `id_key` in each entry |
| `by_key` | one entry configuring one entity | `id_key` in the entry |
| `mapping_key` | a mapping of ID to entry | the mapping's own keys |
| `default` | one entry for an entity named here | `id_default` |

`id_key` defaults to `id`. `id_default` supplies an ID for when the file does not. Given with `by_key` it makes the key optional, which is how the singleton services work — name your `webapi:` entity if you care what it is called, or leave the key out and get `webapi`:

```python
sk.declare_config_section("webapi", WebAPIConfig, id_source="by_key", id_default="webapi")
```

With `default` the ID is fixed at declaration, and the section takes no key at all:

```python
sk.declare_config_section(
    "mysection", MySectionConfig, id_source="default", id_default="myservice"
)
```

Where an entry configures more than one KV model, pass a `model_mapper` yielding the models and an `id_mapper` yielding the ID each one belongs to. The two are matched pairwise, so they must be the same length. `id_source` still describes the file's layout for the generated JSON Schema.

### `config_json_schema`

```python
await sk.import_modules()
schema = sk.config_json_schema()
```

Returns a JSON Schema for a whole config file: the fixed top-level keys, plus every section registered at the time of the call, including the entity naming key each section declares. Sections come from modules, so import them first. `sensorkit config schema` is the command-line form.
