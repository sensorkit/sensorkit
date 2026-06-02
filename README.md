# SensorKit

SensorKit is a high-level, distributed command and control system for autonomous operation of sensor systems and astronomical observatories. It connects hardware drivers to a central automation layer over a NATS message bus, coordinating devices, sensors, and observing programs into a single managed system.

The automation layer handles operating modes, weather constraints, priority-based scheduling, and safety interlocks. All components are independently deployable and communicate exclusively over NATS — services can run on separate machines, restart without disrupting each other, and be replaced without modifying anything else in the system.

## Modules

SensorKit ships support for several external interfaces and features through its module system.

Device drivers:

 - ASCOM Alpaca
 - PlaneWave PWI4
 - Observable Space Node Platform
 - INDI
 - NINA
 - INDIGO
 - Software Bisque TheSky

Observing programs:

 - UDL -- Unified Data Library
 - Otto -- Standalone satellite observation scheduler

Analysis services:

 - Sky Transmission -- All sky camera data analysis
 - SENPAI -- Astrometry and photometry analysis

Other:

 - Slack Integration

## Try it

The fastest way to see SensorKit running is the simulated demo, which spins up a full observatory stack — simulated mount, camera, dome, and weather — using Docker Compose:

```bash
git clone <repo-url>
cd sensorkit/deploy/simulated
docker compose up --build
```

Open [http://localhost:6080](http://localhost:6080) to see the mount simulator. See the [quick start guide](./docs/index.md) for how to interact with the running system.

## Documentation

Documentation is located at [`docs/`](./docs/index.md). To build and serve it locally:

```bash
uv run zensical build
uv run zensical serve
```

## Installation

```bash
pip install sensorkit
```

Modules are optional extras:

| Extra              | Import                       | Feature                                |
|--------------------|------------------------------|----------------------------------------|
| `alpaca`           | `sensorkit.alpaca`           | ASCOM Alpaca devices                   |
| `indi`             | `sensorkit.indi`             | INDI devices                           |
| `indigo`           | `sensorkit.indigo`           | INDIGO devices                         |
| `nina`             | `sensorkit.nina`             | NINA devices                           |
| `node-platform`    | `sensorkit.node_platform`    | Node Platform devices                  |
| `otto`             | `sensorkit.otto`             | Otto observing program                 |
| `pwi4`             | `sensorkit.pwi4`             | PlaneWave PWI4 devices                 |
| `senpai`           | `sensorkit.senpai`           | Astrometry and photometry analysis     |
| `sky-transmission` | `sensorkit.sky_transmission` | All sky camera data analysis           |
| `slack`            | `sensorkit.slack`            | Slack integration                      |
| `thesky`           | `sensorkit.thesky`           | Software Bisque TheSky devices         |
| `udl`              | `sensorkit.udl`              | Unified Data Library observing program |

Install one or more:

```bash
pip install sensorkit[alpaca,pwi4]
```

SensorKit requires a running NATS server with JetStream enabled. The simplest way to run one is:

```bash
docker run -d --name nats -p 4222:4222 nats:alpine -js
```

Point SensorKit at it:

```bash
export NATS_URL=nats://localhost:4222
```

## Configuration

1. Configure site imports. These are the modules that will be loaded by each service. This should correspond to the extras you installed, shown in the table above. Presently this is done by setting a comma-delimited list in the `SENSORKIT_IMPORTS` environment variable.
2. Configure sensors, devices, data flow, and other services. See `deploy/simulated/sensorkit.yaml` for an example.
3. Configure your execution environment. You can use `sensorkit go` for testing and evaluation. Docker/podman or systemd services are recommended for production.

## Development

**Requirements:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
git clone <repo-url>
cd sensorkit
uv sync --all-extras
```

### Common tasks

```bash
# Run the test suite
uv run pytest core/tests

# Run tests against a live NATS server (requires Docker)
SK_TEST_BACKEND=nats ENV=local uv run pytest core/tests

# Lint
uv run ruff check core modules

# Check import-layer contracts
uv run lint-imports
```

### Running the CLI in development

```bash
uv run sensorkit --help
```

### Test Deployment

1. Spin up simulated dependencies via: `docker compose -f deploy/simulated/docker-compose.yml up --build -d nats planewave-sim ascom-sim` 
2. Copy and/or edit your test configuration at deploy/simulated/sensorkit.yaml.
  - Make sure the test image output path in the `data_flow` section is what you want / correct for your platform
3. Use the `sensorkit go` command to directly run the system specified by your configuration.

```bash
sensorkit go -c deploy/simulated/sensorkit.yaml -l --log-level DEBUG
```

Run `sensorkit go --help` for a description of options.

## Contributing

*TBD* — contribution guidelines, PR process, and coding standards will be documented here.

Please open an issue before starting work on a significant change.

## License

*TBD*
