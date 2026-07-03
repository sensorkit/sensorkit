# CLI reference

```
sensorkit <group> <command> [options] [args]
```

The CLI talks to a running system over NATS (`NATS_URL`, default `nats://127.0.0.1:4222`). A `.env` file in the working directory is loaded automatically, so it's a good place for `NATS_URL` and `SENSORKIT_CONFIG`.

Every command supports `--help`.

---

## `config` — unified configuration

```bash
sensorkit config load FILE [-n] [-f] [-v]
```

Validate a unified config file and write it to the configuration store. Only changed keys are written by default.

| Flag              | Description                                        |
|-------------------|----------------------------------------------------|
| `-n, --dry-run`   | Validate and show the diff; write nothing          |
| `-f, --force`     | Write all keys, even unchanged ones                |
| `-v`              | Verbose: list each entity/key and its status (`-vv` includes values) |

---

## `go` — run everything

```bash
sensorkit go [-c FILE] [-l] [--log-file FILE] [--log-level LEVEL] [--shutdown-timeout SECONDS]
```

Launch and supervise **all** services defined in the unified config — both explicit `services:` entries and those implied by config sections (device services, sensors, the agent, the web API). Streams combined, color-coded log output; services restart automatically on failure.

| Flag                 | Description                                                             |
|----------------------|-------------------------------------------------------------------------|
| `-c FILE`            | Config file (default: `$SENSORKIT_CONFIG`, then `./sensorkit.yaml`)     |
| `-l, --load-config`  | Load the configuration before starting services                         |
| `--log-file FILE`    | Where the debug log goes (a default location is used otherwise)         |
| `--log-file-append`  | Append to the log file (default) rather than overwrite                  |
| `--log-level LEVEL`  | Console log level: `DEBUG`, `INFO` (default), `WARNING`, `ERROR`        |
| `--shutdown-timeout` | Max seconds per service for graceful shutdown (default 180). Set it to comfortably cover your slowest hardware deinit — dome close, mount park. |

---

## `service` — individual services

```bash
# Run one service by name; implementation resolved from config
sensorkit service run NAME [python_module[:entrypoint]] [-c FILE] [-r]

# List registered services and their online status
sensorkit service ls
```

If a config file is available, `NAME` alone is enough — the module is looked up from configuration. Without config, supply the module explicitly:

```bash
sensorkit service run MySensor                              # from config
sensorkit service run MySensor sensorkit.std.sensor         # explicit
sensorkit service run MyProgram programs/my_program.py -r   # a .py file works too
```

| Flag             | Description                                            |
|------------------|--------------------------------------------------------|
| `-c FILE`        | Config file to resolve the implementation from         |
| `-r, --restart`  | Restart the service automatically if it exits          |

---

## `controller` — drive a sensor

```bash
sensorkit controller init     -e ENTITY [-f]
sensorkit controller shutdown -e ENTITY [-f]
sensorkit controller abort    -e ENTITY
sensorkit controller collect  -e ENTITY -t TARGET_JSON [-i SECONDS] [-c COUNT] [-b BINNING]
```

`-f / --force` interrupts the currently running task first.

**Target JSON for `collect`:**

```bash
# Fixed alt/az (degrees)
-t '{"target_type": "fixed", "frame": "altaz", "coords": {"az": 180.0, "alt": 60.0}}'

# Fixed ICRS (RA/Dec in degrees)
-t '{"target_type": "fixed", "frame": "icrf", "coords": {"ra": 83.82, "dec": -5.39}}'

# Satellite TLE
-t '{"target_type": "tle", "tle": {"line0": "ISS", "line1": "1 25544U ...", "line2": "2 25544 ..."}}'
```

| Flag                              | Default | Description                 |
|-----------------------------------|---------|-----------------------------|
| `-i, --integration-time-seconds`  | 1.0     | Exposure time per frame     |
| `-c, --frame-count`               | 1       | Number of frames            |
| `-b, --binning`                   | 1       | Camera binning (n×n)        |

---

## `device` — poke hardware directly

```bash
sensorkit device connect    -e ENTITY
sensorkit device disconnect -e ENTITY
sensorkit device abort      -e ENTITY     # stop in-progress motion
```

---

## `agent` — autonomous control

```bash
# Master switch for all autonomous action
sensorkit agent global-control {on|off} [-e AGENT]

# Per-controller control gate
sensorkit agent control CONTROLLER {on|off} [-e AGENT]

# Force a controller up/down; 'none' returns it to the election
sensorkit agent override CONTROLLER {up|down|none} [-e AGENT]

# Scheduler on/off
sensorkit agent scheduling {on|off} [-e AGENT]

# Exclude/re-include a program from scheduling
sensorkit agent exclude PROGRAM [-e AGENT]
sensorkit agent include PROGRAM [-e AGENT]

# Full status: switches, votes, elected state, upcoming schedule
sensorkit agent status [-e AGENT]
```

`-e` defaults to `agent`.

---

## `kv` — the raw key-value store

```bash
sensorkit kv ls [-e ENTITY]                  # list entries (all, or one entity)
sensorkit kv get -e ENTITY KEY               # read one key (pretty-printed JSON)
sensorkit kv put -e ENTITY KEY VALUE         # write a raw value
sensorkit kv delete -e ENTITY [KEY]          # delete a key, or ALL keys for the entity
sensorkit kv load [-e ENTITY] [-n] [FILES...]  # load entity/key/value records from YAML or stdin
```

`kv load` accepts glob patterns and multi-document YAML in the low-level record format (`entity:` / `key:` / `value:`); `-n / --no-clobber` skips keys whose stored value already matches. For normal configuration work, prefer `sensorkit config load`.
