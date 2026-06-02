# Installation

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- A running NATS server with JetStream enabled

## Install SensorKit

```bash
pip install sensorkit
```

Modules are optional extras. To include specific hardware support:

```bash
pip install "sensorkit[alpaca]"
pip install "sensorkit[pwi4]"
pip install "sensorkit[alpaca,pwi4]"
```

## Start NATS

SensorKit requires NATS JetStream. The simplest way to run it locally is with Docker:

```bash
docker run -d --name nats \
  -p 4222:4222 \
  nats:alpine -js
```

Or with Docker Compose, add this to your `docker-compose.yml`:

```yaml
services:
  nats:
    image: nats:alpine
    ports:
      - "4222:4222"
    command: "-js"
    restart: unless-stopped
```

## Connect to NATS

SensorKit reads the NATS URL from the `NATS_URL` environment variable:

```bash
export NATS_URL=nats://localhost:4222
```

For remote NATS servers, substitute the hostname or IP. If this variable is not set, SensorKit defaults to `nats://127.0.0.1:4222`.

## Verify the connection

```bash
sensorkit kv ls
```

This lists all KV entries. On a fresh install the output will be empty, which confirms that the connection succeeded.

## Load configuration

Before starting any services, their configuration must be loaded into the KV store. Write a YAML config file (see [Configuration](configuration.md)) and load it:

```bash
sensorkit kv load config.yaml
```

## Start services

### Single service

```bash
sensorkit service run <module> <name>
```

For example:

```bash
sensorkit service run sensorkit.ascom.service ascom-service
sensorkit service run sensorkit.std.sensor my-sensor
sensorkit service run sensorkit.auto.agent my-agent
```

Add `-r` to restart the service automatically if it exits:

```bash
sensorkit service run sensorkit.std.sensor my-sensor -r
```

### Multiple services from a config file

`sensorkit go` can launch all services defined in a config file. It accepts two formats:

**Unified config (`sensorkit.yaml`)** — the preferred format. A single file that holds both service definitions and all other configuration (sensors, devices, automation, data flow). Pass `-l` to have `sensorkit go` automatically load the configuration into NATS before starting services:

```bash
sensorkit go -c sensorkit.yaml -l
```

**Services manifest (`services.yaml`)** — a minimal file listing only service definitions:

```yaml
services:
  - name: ascom-service
    module: sensorkit.alpaca.service
  - name: my-sensor
    module: sensorkit.std.sensor
  - name: my-agent
    module: sensorkit.auto.agent
```

By default, `sensorkit go` looks for `sensorkit.yaml`, then `services.yaml`, in the current directory. Pass `-c` to use a different file:

```bash
sensorkit go -c /etc/sensorkit/sensorkit.yaml -l -r
```

The `go` command streams combined log output from all services. Add `--log-file path/to/file.log` to also write logs to disk.

## Test Deployment

To run SensorKit locally against simulated hardware (without the full Docker stack):

1. Start only the simulators and NATS:

```bash
docker compose -f deploy/simulated/docker-compose.yml up --build -d nats planewave-sim ascom-sim
```

2. Copy `deploy/simulated/sensorkit.yaml` and edit it for your environment. In particular, check the image output path in the `data_flow` section.

3. Launch SensorKit directly:

```bash
sensorkit go -c deploy/simulated/sensorkit.yaml -l --log-level DEBUG
```

The `-l` flag loads configuration into NATS automatically before starting services.

## Check running services

```bash
sensorkit service ls
```
