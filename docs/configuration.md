# Configuration

A SensorKit system is described by **one YAML file** — conventionally `sensorkit.yaml` — that declares everything: which modules to load, your devices, sensors, automation rules, data flow, and services. You load this file into SensorKit's configuration store (backed by the NATS key-value store), and services read their piece of it when they start.

This page covers the file format, how to load and update configuration, and the lower-level KV tools.

## The unified config file

A minimal but realistic example:

```yaml
# yaml-language-server: $schema=https://sensorkit.github.io/main/config-schema.json
version: 1

sensorkit:
  imports:
    - sensorkit.alpaca.service
    - sensorkit.pwi4.service

# Device services (one section per driver module)
alpaca:
  - id: Alpaca
    endpoints:
      - host: localhost
        port: 32323
        devices:
          MyCamera:
            device_type: camera
          MyDome:
            device_type: dome
          MyWeather:
            device_type: observing_conditions

pwi4:
  - id: PlaneWave
    endpoints:
      - host: localhost
        port: 8220
        devices:
          MyMount:
            device_type: mount

# The sensor controller: devices composed into one instrument
sensors:
  - id: MySensor
    controller_name: MySensor
    devices:
      mount: MyMount
      camera: MyCamera
      dome: MyDome
    site_position:
      latitude_degrees: 34.05
      longitude_degrees: -118.25
      altitude_km: 0.3

# The automation agent
automation:
  controllers:
    MySensor:
      modes:
        - name: nighttime
          state: operate
          criteria:
            - when: time_range
              start: sunset
              end: sunrise
      constraints:
        - kind: weather
          provider: MyWeather
          humidity_max: 85.0
          wind_max: 15.0
          rain_max: 0.0
      tasking:
        - program: MyProgram
          priority: 5

# Where camera data goes
data_flow:
  - entity: MyCamera
    producer:
      simple:
        - op: app_source
        - op: array_to_fits
        - op: write_file
          directory: /data/mysensor

# Your own services
services:
  - id: MyProgram
    python_module: programs/my_program.py
```

Each top-level section is handled by the component that owns it — `sensors` by the sensor controller, `automation` by the agent, `alpaca` by the Alpaca module, and so on. Modules register their own sections, so the set of valid sections grows with the modules you import. An unknown section name is an error at load time, which catches typos early.

### Core sections

| Section      | Owner                                | Purpose                                                             |
|--------------|--------------------------------------|---------------------------------------------------------------------|
| `sensorkit`  | bootstrap                            | Global settings: `imports` (modules to load), `backend`             |
| `sensors`    | [sensor controller](sensor.md)       | Device composition, site position, operating policies              |
| `automation` | [agent](agent.md)                    | Modes, constraints, scheduling                                     |
| `data_flow`  | data pipeline (below)                | Per-device data processing graphs                                  |
| `webapi`     | web API service                      | HTTP API port and served data products                             |
| `services`   | `sensorkit go` / `service run`       | Your own program/service definitions                               |
| `config`     | —                                    | Free-form per-entity keyword values (escape hatch)                 |

### Module sections

Installed modules add their own: `alpaca`, `pwi4`, `thesky`, `node_platform`, `indigo`, `otto`, `udl`, `slack`, `sky_transmission`, and others. These are documented in [Device services](devices.md). For a module's section to be recognized, the module must appear in `sensorkit.imports`.

### Services, implicit and explicit

Entries under `services` name a service and point at your Python code:

```yaml
services:
  - id: MyProgram
    python_module: programs/my_program.py   # a file path or dotted module path
```

Many sections *imply* a service automatically — declaring an `alpaca` section registers an Alpaca device service under its `id`, `sensors` entries register sensor controller services, `automation` registers the agent. You don't list those under `services`; `sensorkit go` finds and launches all of them.

## Loading configuration

```bash
sensorkit config load sensorkit.yaml
```

This validates the whole file against each section's schema, compares against what's already stored, and writes only the keys that changed:

```bash
sensorkit config load sensorkit.yaml -n     # dry run: validate and diff, write nothing
sensorkit config load sensorkit.yaml -v     # show each entity and key with its status
sensorkit config load sensorkit.yaml -f     # write everything, even unchanged keys
```

!!! warning "Config changes need a service restart"

    Presently, all services read their configuration **once at startup**. `config load` updates the store, not running services — after changing a value, load and then restart the affected service. (In a future release, some configuration will become hot-reloadable for some services.)

`sensorkit go -l` performs the load automatically before starting services, which is the usual workflow during setup:

```bash
sensorkit go -c sensorkit.yaml -l
```

## Editor support

SensorKit publishes a JSON Schema covering the core sections and every built-in module. Reference it from the top of your config file to get completion and inline validation as you edit. In VS Code, with the YAML extension installed:

```yaml
# yaml-language-server: $schema=https://sensorkit.github.io/main/config-schema.json
version: 1
```

Sites running their own plugins can generate a schema that also covers the sections those plugins register:

```bash
sensorkit config schema -c sensorkit.yaml -o sensorkit.schema.json
```

`sensorkit config load -n` remains the full check, covering the cross-field rules the models enforce.

## Data flow

The `data_flow` section defines what happens to data a device produces — most commonly, turning camera frames into FITS files on disk. Each entry attaches a pipeline of operations to an entity:

```yaml
data_flow:
  - entity: MyCamera
    producer:
      simple:
        - op: app_source              # frames enter from the camera driver
        - op: fits_header             # add/override FITS header cards
          define:
            TASKID:   =str(TaskInfo.task_id)
            FRAMENUM: =Collect.frame_number
            CENTAZ:   =AltAzPointing.azimuth_degrees
            CENTALT:  =AltAzPointing.altitude_degrees
            RADEG:    =RADecPointing.right_ascension_hours * 15
            DECDEG:   =RADecPointing.declination_degrees
        - op: array_to_fits           # encode the raw array as FITS
        - op: write_file
          directory: /data/mysensor
```

Values beginning with `=` are expressions evaluated against the task's **context** — a typed snapshot of live system state (current pointing, site position, task identity, frame number) that the sensor controller assembles and passes along with every frame. This is how FITS headers get accurate per-frame telescope state without the camera driver knowing anything about the mount.

Built-in operations include:

| Op                  | Purpose                                                        |
|---------------------|----------------------------------------------------------------|
| `app_source` / `app_sink` | Enter/exit point connecting the pipeline to the device code |
| `fits_header`       | Define or override FITS header cards, with `=` context expressions |
| `array_to_fits`     | Encode a raw image array as a FITS file                        |
| `compress_fits`     | FITS tile compression                                          |
| `apply_dark`        | Subtract a dark frame                                          |
| `context_from_fits` | Populate context from an existing FITS header                  |
| `write_file` / `read_file` | Write to / read from disk                               |
| `watch_directory`   | Source files appearing in a directory                          |

Output naming is controlled by the `FileNameTemplate` context value, typically set under `automation.contexts` so it applies to every collect task:

```yaml
automation:
  contexts:
    standard_collect:
      FileNameTemplate:
        template: f"{datetime.now(UTC):%Y%m%dT%H%M%S}-f{frame_num}.fits"
```

Analysis modules extend the same pipeline mechanism — e.g. focus estimation ops (`analyze_focus_stars`, `analyze_focus_fft`) or handing frames to astrometry/photometry services.

## Web API

The `webapi` section enables an HTTP/JSON service that exposes entities, device details, controller and agent state, live event streams (server-sent events), and data products such as FITS files with JPEG previews:

```yaml
webapi:
  id: WebAPI
  port: 8000
  serve_data_products:
    - kind: local_fits
      root_directory: /data/mysensor
      controller_id: from_path
```

This is the integration point for dashboards and remote monitoring.

### Reaching the service

`host` defaults to `127.0.0.1`, so the service answers only on the machine running it. A containerized deployment has to bind every interface, since the port is published from outside the container:

```yaml
webapi:
  host: 0.0.0.0
```

If the web API is exposed on non-loopback interfaces and no authentication is configured, the service will log a warning at startup.

### TLS

```yaml
webapi:
  tls:
    certfile: /etc/sensorkit/tls/server.crt
    keyfile: /etc/sensorkit/tls/server.key
    keyfile_password_env: SENSORKIT_TLS_PASSWORD   # optional, for an encrypted key
    minimum_version: "1.2"                         # or "1.3"
```

Certificates are read when the service starts, so renewing one takes a restart.

### Authentication

Bearer token auth applies to every endpoint. Set `allow_anonymous_read` to leave `GET` requests, including the event streams, open to a dashboard while still guarding the endpoints that act:

```yaml
webapi:
  auth:
    kind: token
    token_env: SENSORKIT_API_TOKEN   # or token_file: /etc/sensorkit/api-token
    allow_anonymous_read: false
```

Clients present it as `Authorization: Bearer <token>`. Anonymous read does not extend to `/openapi.json`, `/docs`, or `/redoc`.

### Browser access

Cross-origin requests are refused unless the origin is listed. A dashboard served from somewhere other than the API's own origin needs:

```yaml
webapi:
  cors:
    allow_origins:
      - https://dashboard.example.org
```

### Limits

`max_stream_clients` (default 32) caps concurrent event-stream subscribers, and `stream_queue_size` (default 4096) bounds each subscriber's backlog. `expose_docs: true` serves the browser documentation pages at `/docs` and `/redoc`, which are off by default. `/openapi.json` is always served to a caller the authentication settings admit.

## Under the hood: the KV store

Everything `config load` writes lands in the NATS key-value store, namespaced by entity. Each entity (a device, sensor, program, or the agent) has its own keyspace, and each key holds one typed record — `SensorConfig` for a sensor, `AgentConfig` for the agent, and so on. Runtime state (controller state, agent decisions, offers) lives in the same store, which is what makes the system inspectable:

```bash
# Everything, or one entity's keys
sensorkit kv ls
sensorkit kv ls -e MySensor

# Read one key
sensorkit kv get -e MySensor SensorConfig

# Write or delete directly (surgical changes; prefer `config load` for config)
sensorkit kv put -e MySensor SomeKey '{"field": "value"}'
sensorkit kv delete -e MySensor SomeKey
sensorkit kv delete -e MySensor            # all keys for the entity
```

`sensorkit kv load` also accepts raw record files in `entity`/`key`/`value` form for low-level seeding — useful in scripts, but the unified file plus `config load` is the recommended path.
