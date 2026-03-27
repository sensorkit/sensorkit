# SensorKit

SensorKit is a high-level, distributed command and control system for autonomous operation of sensor systems and astronomical observatories. It connects hardware drivers to a central automation layer over a NATS message bus, coordinating devices, sensors, and observing programs into a single managed system.

Hardware integrations are provided for ASCOM Alpaca, PlaneWave PWI4, Software Bisque TheSky, and Node Platform. The automation layer handles operating modes, weather constraints, priority-based scheduling, and safety interlocks. All components are independently deployable and communicate exclusively over NATS — services can run on separate machines, restart without disrupting each other, and be replaced without modifying anything else in the system.

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

Hardware modules are optional extras:

| Extra           | Hardware                                                                    |
|-----------------|-----------------------------------------------------------------------------|
| `ascom`         | ASCOM Alpaca (camera, mount, dome, focuser, rotator, filter wheel, weather) |
| `pwi4`          | PlaneWave PWI4 (mount, focuser, rotator, mirror cover)                      |
| `thesky`        | Software Bisque TheSky / SkyX                                               |
| `node-platform` | Node Platform observatory controller                                        |
| `otto`          | *TBD*                                                                       |

Install one or more:

```bash
pip install "sensorkit[ascom,pwi4]"
```

SensorKit requires a running NATS server with JetStream enabled. The simplest way to run one is:

```bash
docker run -d --name nats -p 4222:4222 nats:alpine -js
```

Point SensorKit at it:

```bash
export SENSORKIT_BACKEND_ARG=nats://localhost:4222
```

## Project structure

```
core/              Core framework and APIs
  src/sensorkit/
    api/           Declarative service API and entrypoint
    backend/       NATS and FakeBackend abstraction
    common/        Shared utilities and keyword system
    core/          Device, Controller, Program base types
    data/          Data graph and context pipeline
    std/           Standard sensor controller and collect routines
    astro/         Observers, targets, and trajectory types
    auto/          Automation agent
    cli/           Command-line interface

modules/           Hardware and service integrations
  ascom/           ASCOM Alpaca
  pwi4/            PlaneWave PWI4
  thesky/          Software Bisque TheSky
  node_platform/   Node Platform
  otto/            Otto

deploy/
  simulated/       Docker Compose demo with simulated hardware

docs/              User documentation
```

Module source trees are assembled into the `sensorkit` package at build time by the Hatch build hook. In development, `core/src` and each `modules/*/src` directory are all on the Python path simultaneously (configured via `dev-mode-dirs` in `pyproject.toml`).

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

# Run a single test file
uv run pytest core/tests/path/to/test_file.py

# Run tests against a live NATS server (requires Docker)
SK_TEST_BACKEND=nats ENV=local uv run pytest core/tests

# Lint
uv run ruff check core modules

# Format
uv run ruff format core modules

# Check import-layer contracts
uv run lint-imports
```

### Architecture rules

Import-linter enforces a strict layering contract. The dependency order from bottom to top is:

```
sensorkit.common
sensorkit.backend
sensorkit.core / .data
sensorkit.api
sensorkit.std
sensorkit.astro
sensorkit.auto
sensorkit.cli
modules (ascom, pwi4, …)
```

No layer may import from a layer above it. Modules may not import from other modules.

### Running the CLI in development

```bash
uv run sensorkit --help
```

## Contributing

*TBD* — contribution guidelines, PR process, and coding standards will be documented here.

Please open an issue before starting work on a significant change.

## License

*TBD*
