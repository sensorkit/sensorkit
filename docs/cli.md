# CLI reference

```
sensorkit <command> [options] [args]
```

---

## service

Manage running services.

```bash
# Start a service
sensorkit service run <module> <name> [-r]

# List all registered services
sensorkit service ls
```

`<module>` is a dotted Python import path (e.g. `sensorkit.std.sensor`) or a path to a `.py` file. If the module has multiple entrypoints, append the function name: `module:function`.

`-r` / `--restart` — restart the service automatically if it exits.

**Examples:**

```bash
sensorkit service run sensorkit.ascom.service ascom-service
sensorkit service run sensorkit.std.sensor my-sensor -r
sensorkit service run sensorkit.auto.agent my-agent -r
```

---

## go

Launch and supervise multiple services from a config file.

```bash
sensorkit go [-c FILE] [-l] [-r] [--log-file FILE] [--log-level LEVEL]
             [--add-service name:module[:entrypoint] ...]
             [--shutdown-timeout SECONDS]
```

`sensorkit go` looks for `sensorkit.yaml` (unified format), then `services.yaml`, in the current directory.

**Config file formats:**

*Unified config (`sensorkit.yaml`)* — holds service definitions alongside all other configuration (sensors, devices, automation, data flow). Use `-l` to automatically load configuration into NATS before starting:

```bash
sensorkit go -c sensorkit.yaml -l
```

*Services manifest (`services.yaml`)* — lists only service definitions:

```yaml
services:
  - name: alpaca-service
    module: sensorkit.alpaca.service
  - name: pwi4-service
    module: sensorkit.pwi4.service
  - name: my-sensor
    module: sensorkit.std.sensor
  - name: my-agent
    module: sensorkit.auto.agent
    func: agent_service        # optional: explicit entrypoint function
```

**Options:**

| Flag | Description |
|---|---|
| `-c FILE` | Path to config file (default: `sensorkit.yaml`, then `services.yaml`) |
| `-l` / `--load-config` | Automatically load configuration before starting (unified format only) |
| `-r` / `--restart` | Restart services automatically on exit |
| `--log-file FILE` | Write combined log output to a file |
| `--log-file-append` | Append to log file instead of overwriting (default: true) |
| `--log-level LEVEL` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`) |
| `--add-service name:module` | Add a service without editing the config file |
| `--shutdown-timeout SECONDS` | Max seconds per service for graceful shutdown (default: `180`) |

---

## kv

Manage the NATS key-value store.

```bash
# Load config records from YAML files
sensorkit kv load [--no-clobber] [FILES...]

# List all KV entries
sensorkit kv ls [-e ENTITY]

# Read a single key
sensorkit kv get -e ENTITY KEY

# Write a value
sensorkit kv put -e ENTITY KEY VALUE

# Delete a key, or all keys for an entity
sensorkit kv delete -e ENTITY [KEY]
```

`FILES` accepts glob patterns. If no files are given, reads from stdin.

`--no-clobber` / `-n` — skip keys that already have a value.

---

## controller

Send tasks and commands to a sensor controller.

```bash
# Initialize (connect devices and bring the sensor to ready state)
sensorkit controller init -e ENTITY [-f]

# Shut down (close everything down)
sensorkit controller shutdown -e ENTITY [-f]

# Abort the currently running task
sensorkit controller abort -e ENTITY

# Run a collect task
sensorkit controller collect -e ENTITY \
    -t TARGET_JSON \
    [-i INTEGRATION_SECONDS] \
    [-c FRAME_COUNT] \
    [-b BINNING]
```

`-f` / `--force` — interrupt the current task before running the new one.

**Target JSON for `collect`:**

```bash
# ICRS (fixed celestial target)
-t '{"target_type": "icrs", "right_ascension_hours": 5.58, "declination_degrees": -5.39}'

# Alt/Az (fixed horizon position)
-t '{"target_type": "altaz", "azimuth_degrees": 180.0, "altitude_degrees": 45.0}'

# TLE (satellite)
-t '{"target_type": "tle", "line0": "ISS", "line1": "1 25544U ...", "line2": "2 25544 ..."}'
```

---

## device

Send commands directly to a device.

```bash
sensorkit device connect    -e ENTITY
sensorkit device disconnect -e ENTITY
sensorkit device abort      -e ENTITY
```

---

## agent

Control the automation agent.

```bash
# Enable or disable global autonomous control
sensorkit agent global-control {on|off} [-e AGENT]

# Enable or disable control for one controller
sensorkit agent control CONTROLLER {on|off} [-e AGENT]

# Force a controller up, down, or let the election decide
sensorkit agent override CONTROLLER {up|down|none} [-e AGENT]

# Enable or disable the scheduler
sensorkit agent scheduling {on|off} [-e AGENT]

# Add or remove a program from scheduling
sensorkit agent include PROGRAM [-e AGENT]
sensorkit agent exclude PROGRAM [-e AGENT]

# View current status
sensorkit agent status [-e AGENT]
```

`-e` defaults to `agent` if omitted.
