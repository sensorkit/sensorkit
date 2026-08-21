<div align="center">
  <p>
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sensorkit/sensorkit/main/docs/assets/sensorkit-horizontal-dark.svg">
      <img src="https://raw.githubusercontent.com/sensorkit/sensorkit/main/docs/assets/sensorkit-horizontal-light.svg" alt="SensorKit" width="460">
    </picture>
  </p>

[![CI](https://img.shields.io/github/actions/workflow/status/sensorkit/sensorkit/ci.yml?branch=main&style=flat-square&logo=github&label=CI)](https://github.com/sensorkit/sensorkit/actions/workflows/ci.yml?query=branch%3Amain)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fsensorkit.github.io%2Fsensorkit%2Fcoverage%2Fbadge.json&style=flat-square)](https://sensorkit.github.io/sensorkit/coverage/)
[![Docs](https://img.shields.io/badge/docs-zensical-blue)](https://sensorkit.github.io/sensorkit/)
[![PyPI](https://img.shields.io/pypi/v/sensorkit?style=flat-square&logo=pypi&logoColor=white&color=131c4b)](https://pypi.org/project/sensorkit/)
[![Python](https://img.shields.io/pypi/pyversions/sensorkit?style=flat-square&logo=python&logoColor=white&color=131c4b)](https://pypi.org/project/sensorkit/)
[![License](https://img.shields.io/badge/license-Apache--2.0-131c4b?style=flat-square)](https://github.com/sensorkit/sensorkit/blob/main/LICENSE)

</div>

Open-source orchestration for autonomous sensor systems.
Built for **space domain awareness**, fully capable for **time-domain astronomy**.

**SensorKit** is a Python framework for edge-distributed control systems, plus a complete, extensible ground-based sensor stack built on it.
It drives your hardware, manages multiple tasking sources, and runs the full operating lifecycle, from scheduling observations to standing down for weather.
Configurable from a single YAML file.

## Highlights

- **Autonomous operation.** An agent service evaluates operating modes, weather and safety constraints, and program schedules, then starts up, tasks, and shuts down each sensor on its own.
  A single command hands control back to a human.
- **Speaks your hardware's language.** ASCOM Alpaca, PlaneWave PWI4, Software Bisque TheSky, and [more](#modules), all included.
- **Satellite tracking as a first-class citizen.** Targets can be fixed alt/az or ICRS positions, TLEs, state vectors, or precomputed ephemerides.
  SensorKit propagates orbits and drives mounts in rate-tracking modes for fast-moving objects.
- **Observing programs in a few dozen lines of Python.** A program advertises *when* it has work and produces the *next task* when asked.
  Scheduling, priorities, device sequencing, and FITS writing are handled for you.
- **Configuration-defined data flow.** Camera frames move through a pipeline you declare in YAML:
  FITS headers populated from live telescope state, compression, disk, or hand-off to analysis services.
- **Built to be pulled apart.** Every device driver, sensor controller, program, and the agent itself is an independent service communicating over [NATS JetStream](https://nats.io).
  Run them on different machines, restart them independently, swap them freely.

[SensorView](https://github.com/sensorkit/sensorview) is the graphical interface to a running system.

## Quick start

The repository ships a complete simulated observatory: mount, camera, dome, and weather simulators with the full automation stack on top.
No hardware, just Docker:

```bash
git clone https://github.com/sensorkit/sensorkit.git
cd sensorkit/deploy/simulated
docker compose up --build
```

Open [http://localhost:6080](http://localhost:6080) to watch the simulated mount slew to targets.
The [quick start guide](docs/quickstart.md) walks through the rest:
exploring the system with the CLI, connecting SensorView, and reading the ~35-line observing program that drives it.

[//]: # (For a demo, watch [this]&#40;https://youtube.com/@SensorKit/live&#41; 24/7 YouTube stream of SensorKit orchestrating:)

[//]: # ()
[//]: # ( - An **AlpacaSensor**, tasked by Otto &#40;4 random, visible GEOs&#41;)

[//]: # ( - A **PWI4Sensor**, listening to the UDL & being tasked &#40;via bot&#41; every 2 min in SensorView)

[//]: # ( - A **TheSkySensor**, listening to NEOCP alerts &#40;via NASA/JPL Scout&#41;)

[//]: # ()
[//]: # (All applications are being run on and streamed from a low-power x86 mini-PC.)

## Installation

```bash
pip install sensorkit
```

Hardware and feature support ships as optional extras.
Install only what your site uses:

```bash
pip install "sensorkit[alpaca,pwi4]"
```

SensorKit requires Python 3.13+ and a NATS server with JetStream enabled:

```bash
docker run -d --name nats -p 4222:4222 nats:alpine -js
export NATS_URL=nats://localhost:4222
```

The [modules table](#modules) below lists every extra.
See [Installation](docs/installation.md) for how to go from an empty config to running services.

## Modules

Every module below ships in the box.
Each one is an install extra named after the module, with underscores written as dashes, so `node_platform` becomes `node-platform`.

| Module | Kind | What it does |
| --- | --- | --- |
| [alpaca](modules/alpaca/README.md) | Hardware | **ASCOM Alpaca** cameras, mounts, domes, focusers, filter wheels, rotators, switches, weather, and safety monitors |
| [autoslew](modules/autoslew/README.md) | Hardware | **ASA Autoslew** mounts over **Alpaca**, including ASA satellite tracking, pointing model, and Nasmyth selection |
| [indigo](modules/indigo/README.md) | Hardware | **INDIGO** (and **INDI**) devices over the INDIGO WebSocket protocol |
| [nina](modules/nina/README.md) | Hardware | **N.I.N.A.** equipment through the Advanced API plugin |
| [node_platform](modules/node_platform/README.md) | Hardware | **Observable Space Node Platform** mounts, enclosures, optics, and weather |
| [pwi4](modules/pwi4/README.md) | Hardware | **PlaneWave Instruments** mounts, focusers, rotators, and covers over the **PWI4** HTTP API |
| [thesky](modules/thesky/README.md) | Hardware | **Software Bisque TheSky** over its JavaScript-over-TCP scripting interface |
| [otto](modules/otto/README.md) | Program | Autonomous satellite collection from NORAD IDs, orbit regimes, or **Horizons** names, across a camera parameter grid |
| [udl](modules/udl/README.md) | Program | **Unified Data Library** tasking. Polls CollectRequests, reports progress, and delivers products back |
| [burr](modules/burr/README.md) | Program | Sensor characterization and calibration tasking |
| [senpai](modules/senpai/README.md) | Analysis | Per-frame astrometry and photometry, with results published back into the system |
| [sky_transmission](modules/sky_transmission/README.md) | Analysis | All-sky star matching for live cloud and sky-transmission telemetry |
| [sdasim](modules/sdasim/README.md) | Simulation | Synthetic SDA scene renderer that presents itself as an ordinary camera device |
| [slack](modules/slack/README.md) | Notification | Real-time **Slack** alerts and daily observatory summaries |

## Documentation

Full documentation lives in [`docs/`](docs/index.md):
quick start, installation, configuration, guides for devices, sensors, programs, and the agent, plus CLI and API references.
To build and serve it locally:

```bash
uv run zensical serve
```

## Development

**Requirements:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
git clone https://github.com/sensorkit/sensorkit.git
cd sensorkit
uv sync --all-extras

# Run the test suite. Backend tests are parametrized over the fake backend and a
# real NATS server in a container; the latter is skipped when Docker is absent.
uv run pytest core/tests

# Force every test onto a live NATS server instead of the fake backend
SK_TEST_BACKEND=nats uv run pytest core/tests

# Also run tests needing site-local hardware or services
ENV=local uv run pytest core/tests

# Lint and check import-layer contracts
uv run ruff check core modules
uv run lint-imports
```

To run the stack natively against the containerized simulators during development, see [Trying it against simulators](docs/installation.md#trying-it-against-simulators).

## Status

SensorKit is in **beta** and under active development.
It runs real telescopes nightly, but APIs and configuration formats are still evolving, and some corners are unfinished.
Feedback and issues are very welcome.

## Contributing

Contribution guidelines, PR process, and coding standards are still being documented.
In the meantime, please open an issue before starting work on a significant change.

## License

Licensed under the [Apache License 2.0](LICENSE).
