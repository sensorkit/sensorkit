# Installation

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- A running NATS server with JetStream enabled

## Install SensorKit

```bash
uv pip install sensorkit
```

To include a specific hardware module (ASCOM, PWI4, etc.):

```bash
uv pip install "sensorkit[ascom]"
uv pip install "sensorkit[pwi4]"
uv pip install "sensorkit[ascom,pwi4]"
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

SensorKit reads the NATS URL from the `SENSORKIT_BACKEND_ARG` environment variable:

```bash
export SENSORKIT_BACKEND_ARG=nats://localhost:4222
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

### Multiple services from a manifest

Create a `services.yaml` file:

```yaml
services:
  - name: ascom-service
    module: sensorkit.ascom.service
  - name: my-sensor
    module: sensorkit.std.sensor
  - name: my-agent
    module: sensorkit.auto.agent
```

Then launch all services in one command:

```bash
sensorkit go
```

By default, `sensorkit go` looks for `services.yaml` in the current directory. Pass `-c` to use a different file:

```bash
sensorkit go -c /etc/sensorkit/services.yaml -r
```

The `go` command streams combined log output from all services. Add `--log-file path/to/file.log` to also write logs to disk.

## Check running services

```bash
sensorkit service ls
```
