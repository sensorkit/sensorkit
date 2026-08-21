# Installation

## Requirements

- **Python 3.13 or newer**
- **A NATS server with JetStream enabled** — this is the message bus and configuration store that every SensorKit service connects to. It's a single small binary (or container) and takes under a minute to set up.

## Install SensorKit

```bash
pip install sensorkit
```

Hardware and feature support ship as optional extras, so you only install what your site uses:

```bash
pip install "sensorkit[alpaca]"          # ASCOM Alpaca devices
pip install "sensorkit[pwi4]"            # PlaneWave PWI4
pip install "sensorkit[alpaca,pwi4]"     # both
```

| Extra              | Provides                                                        |
|--------------------|-----------------------------------------------------------------|
| `alpaca`           | ASCOM Alpaca devices                                            |
| `autoslew`         | ASA Autoslew mounts over Alpaca                                 |
| `burr`             | Sensor characterization and calibration tasking                 |
| `indigo`           | INDIGO and INDI devices                                         |
| `nina`             | N.I.N.A. equipment through the Advanced API plugin              |
| `node-platform`    | Observable Space Node Platform                                  |
| `otto`             | Otto, a standalone satellite observation program                |
| `pwi4`             | PlaneWave PWI4 mount, focuser, rotator, and cover               |
| `sdasim`           | Synthetic SDA scene renderer that acts as an ordinary camera    |
| `senpai`           | Astrometry and photometry analysis                              |
| `sky-transmission` | All-sky camera analysis                                         |
| `slack`            | Slack notifications                                             |
| `systemd`          | Debug logging to the systemd journal on Linux                   |
| `thesky`           | Software Bisque TheSky                                          |
| `thesky-simulator` | In-process TheSky simulator, no TheSky install needed           |
| `udl`              | Unified Data Library observing program                          |

To work from a clone of the repository instead, [uv](https://docs.astral.sh/uv/) sets up everything at once:

```bash
git clone https://github.com/sensorkit/sensorkit.git && cd sensorkit
uv sync --all-extras
uv run sensorkit --help
```

### PyTorch

The `sdasim` extra pulls in PyTorch, which defaults to the CUDA build on Linux (presently around 2.5 GB with its NVIDIA runtime packages).

Using pip, install PyTorch from its CPU index first, and SensorKit will use it:

```bash
pip install torch --index-url https://download.pytorch.org/whl/cpu
pip install "sensorkit[sdasim]"
```

Using uv, select the CPU build instead with a dependency group:

```bash
uv sync --all-extras --group torch-cpu     # CPU
uv sync --all-extras --group torch-cu128   # CUDA, pinned to the cu128 index
```

## Start NATS

The simplest way to run NATS locally is Docker:

```bash
docker run -d --name nats -p 4222:4222 nats:alpine -js
```

Or in a `docker-compose.yml`:

```yaml
services:
  nats:
    image: nats:alpine
    ports:
      - "4222:4222"
    command: "-js"
    restart: unless-stopped
```

!!! warning "JetStream is required"

    The `-js` flag enables JetStream. Without it NATS runs fine, but SensorKit can't create its streams or KV store — nothing will work.

## Point SensorKit at NATS

SensorKit reads the server address from the `NATS_URL` environment variable:

```bash
export NATS_URL=nats://localhost:4222
```

If unset, it defaults to `nats://127.0.0.1:4222`. SensorKit also reads a `.env` file from the working directory, which is a convenient place to keep this alongside your config.

Verify the connection:

```bash
sensorkit kv ls
```

On a fresh system this prints an empty listing — which confirms the connection worked.

## Write a configuration

SensorKit is driven by a single unified YAML file, conventionally named `sensorkit.yaml`, that declares your devices, sensors, automation rules, and data flow. The [Configuration](configuration.md) page covers the format; `deploy/simulated/sensorkit.yaml` in the repository is a complete working example.

Load it into the system:

```bash
sensorkit config load sensorkit.yaml
```

`config load` validates the file, shows what changed, and only writes keys whose values differ — so it's safe to re-run after every edit.

## Run services

### Everything at once: `sensorkit go`

For evaluation and development, `sensorkit go` launches every service defined in your config in one supervised process, with combined color-coded logs:

```bash
sensorkit go -c sensorkit.yaml -l
```

The `-l` flag loads the configuration first, so this single command takes you from YAML file to running observatory. Logs also stream to a debug log file (`--log-file` to choose where; `--log-level DEBUG` for more console detail).

### One service at a time: `sensorkit service run`

In production you'll typically run each service under its own supervisor (Docker/Podman containers or systemd units). Each service is started by name; the implementation is resolved from your configuration:

```bash
sensorkit service run my-sensor
sensorkit service run agent
```

If no config file is available to the process, supply the Python module explicitly:

```bash
sensorkit service run my-sensor sensorkit.std.sensor
sensorkit service run agent sensorkit.auto.agent -r
```

`-r` restarts the service automatically if it exits.

### Check what's running

```bash
sensorkit service ls
```

Lists every registered service and whether it is currently online.

## Environment variables

| Variable            | Purpose                                                              | Default                  |
|---------------------|----------------------------------------------------------------------|--------------------------|
| `NATS_URL`          | NATS server address                                                  | `nats://127.0.0.1:4222`  |
| `SENSORKIT_CONFIG`  | Path to the unified config file                                      | `./sensorkit.yaml`       |
| `SENSORKIT_IMPORTS` | Comma-separated extra modules to import (e.g. `sensorkit.alpaca.service`) | —                   |
| `SENSORKIT_BACKEND` | Backend implementation (`nats` or `fake` for tests)                  | `nats`                   |

Module imports are usually configured in the `sensorkit.imports` section of your config file rather than the environment — see [Configuration](configuration.md).

## Trying it against simulators

You can run SensorKit natively on your machine against the containerized hardware simulators — useful when developing:

```bash
# 1. Start just NATS and the simulators
docker compose -f deploy/simulated/docker-compose.yml up --build -d nats planewave-sim ascom-sim

# 2. Copy deploy/simulated/sensorkit.yaml and adjust paths for your machine
#    (in particular, the output directory in the data_flow section)

# 3. Load config and launch everything
sensorkit go -c sensorkit.yaml -l --log-level DEBUG
```
