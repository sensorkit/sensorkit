# SensorKit

**An open-source control system for autonomous observatories**, built for space domain awareness and time-domain astronomy. SensorKit connects your telescope hardware to an automation layer that decides — continuously and safely — when to open the dome, what to observe, and when to shut down, whether that's for a passing satellite, a transient follow-up, or an ordinary night of survey work.

You describe your site in a single YAML file: which mount, camera, and dome you have, what weather limits you trust, and which observing programs are allowed to request time. SensorKit runs the night.

## Highlights

- **Autonomous operation.** An agent service evaluates operating modes, weather and safety constraints, and program schedules — then starts up, tasks, and shuts down each sensor on its own. A single command hands control back to a human.
- **Speaks your hardware's language.** Modules for ASA Autoslew, ASCOM Alpaca, INDIGO (+INDI), NINA, the Observable Space Node Platform, PlaneWave PWI4, and Software Bisque TheSky are included.
- **Satellite tracking as a first-class citizen.** Targets can be fixed alt/az or ICRS positions, TLEs, state vectors, or precomputed ephemerides. SensorKit propagates orbits and drives mounts in rate-tracking modes for fast-moving objects.
- **Observing programs in a few dozen lines of Python.** A program advertises *when* it has work and produces the *next task* when asked. Scheduling, priorities, device sequencing, and FITS writing are handled for you.
- **Configuration-defined data flow.** Camera frames move through a pipeline you declare in YAML: FITS headers populated from live telescope state, compression, disk, or hand-off to analysis services.
- **Built to be pulled apart.** Every device driver, sensor controller, program, and the agent itself is an independent service communicating over [NATS JetStream](https://nats.io) — run them on different machines, restart them independently, swap them freely.

Batteries included: **Otto** (standalone satellite observation scheduling), **UDL** (Unified Data Library tasking), and **Burr** (sensor characterization tasking) observing programs, plus analysis modules for astrometry and photometry (**SENPAI**) and all-sky transmission, and **sdasim** — a synthetic SDA scene renderer. [SensorView](https://github.com/sensorkit/sensorview) is the graphical interface to a running system.

## Quick start

The repository ships a complete simulated observatory — mount, camera, dome, and weather simulators with the full automation stack on top. No hardware, just Docker:

```bash
git clone <repo-url>
cd sensorkit/deploy/simulated
docker compose up --build
```

Open [http://localhost:6080](http://localhost:6080) to watch the simulated mount slew to targets. The [quick start guide](docs/quickstart.md) walks through the rest: exploring the system with the CLI, connecting SensorView, and reading the 35-line observing program that drives it.

For a demo, watch [this](https://youtube.com/@SensorKit/live) 24/7 YouTube stream of SensorKit orchestrating:

 - An **AlpacaSensor**, tasked by Otto (4 random, visible GEOs)
 - A **PWI4Sensor**, listening to the UDL & being tasked (via bot) every 2 min in SensorView
 - A **TheSkySensor**, listening to NEOCP alerts (via NASA/JPL Scout)

All applications are being run on and streamed from a low-power x86 mini-PC.

## Installation

```bash
pip install sensorkit
```

Hardware and feature support ships as optional extras — install only what your site uses:

```bash
pip install "sensorkit[alpaca,pwi4]"
```

SensorKit requires Python 3.13+ and a NATS server with JetStream enabled:

```bash
docker run -d --name nats -p 4222:4222 nats:alpine -js
export NATS_URL=nats://localhost:4222
```

See [Installation](docs/installation.md) for the full list of extras and how to go from an empty config to running services.

## Documentation

Full documentation lives in [`docs/`](docs/index.md) — quick start, installation, configuration, guides for devices, sensors, programs, and the agent, plus CLI and API references. To build and serve it locally:

```bash
uv run zensical serve
```

## Development

**Requirements:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
git clone <repo-url>
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

SensorKit is in **beta** and under active development. It runs real telescopes nightly, but APIs and configuration formats are still evolving, and some corners are unfinished. Feedback and issues are very welcome.

## Contributing

Contribution guidelines, PR process, and coding standards are still being documented. In the meantime, please open an issue before starting work on a significant change.

## License

Licensed under the [Apache License 2.0](LICENSE).
