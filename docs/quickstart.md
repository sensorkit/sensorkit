# Quick start — a simulated observatory

The fastest way to understand SensorKit is to run one. The repository ships a complete simulated observatory in `deploy/simulated`: a PlaneWave PWI4 mount simulator (with a graphical interface you can watch), ASCOM Alpaca simulators for camera, dome, filter wheel, focuser, rotator, and weather, and the full SensorKit automation stack on top.

No hardware, no site config — just Docker.

**Prerequisites:** Docker with the Compose plugin, and Git.

## 1. Clone and start the stack

```bash
git clone <sensorkit-repo-url>
cd sensorkit/deploy/simulated
docker compose up --build
```

The first run builds the SensorKit image and pulls the simulators, which takes a few minutes. The services then start in dependency order:

| Service         | What it does                                                                           |
|-----------------|----------------------------------------------------------------------------------------|
| `nats`          | NATS JetStream — the message bus everything communicates over                          |
| `planewave-sim` | PWI4 mount simulator, with a browser-accessible desktop                                |
| `ascom-sim`     | ASCOM Alpaca simulators (camera, dome, filter wheel, focuser, rotator, weather)        |
| `reload-config` | Loads `sensorkit.yaml` into the configuration store, then exits                        |
| `sensor`        | The sensor controller — coordinates all devices as one instrument (`SimulatedSensor`)  |
| `planewave`     | SensorKit's PWI4 device service, connected to the mount simulator                      |
| `alpaca`        | SensorKit's Alpaca device service, connected to the ASCOM simulators                   |
| `agent`         | The automation agent — schedules and supervises the controller                         |
| `program`       | A demo observing program that generates collect tasks at random alt/az positions       |
| `otto`          | Otto, a satellite observation program (tasked with a real NORAD ID)                    |
| `webapi`        | HTTP/JSON view into the running system                                                 |

!!! note "`reload-config` is temporary"

    The explicit `reload-config` container is a requirement that's going away: in an upcoming release, each service loads its own configuration at startup, so the stack won't need a separate seeding step.

## 2. Watch it run

Open [http://localhost:6080](http://localhost:6080) to see the PWI4 mount simulator's screen, and [http://localhost:30000](http://localhost:30000) for the ASCOM Alpaca simulator dashboard.

The simulated deployment starts with automation **enabled** (`first_run.operate_all: true` in its config), so within a minute or two you should see the agent bring the sensor up and the mount begin slewing to targets from the demo program.

## 3. Talk to it with the CLI

Install the CLI on your host machine. Either install from PyPI into any Python ≥ 3.13 environment:

```bash
pip install sensorkit
export NATS_URL=nats://localhost:4222
```

or, from the cloned repository, use [uv](https://docs.astral.sh/uv/) and prefix commands with `uv run`:

```bash
uv sync --all-extras
export NATS_URL=nats://localhost:4222
uv run sensorkit service ls
```

Now explore:

```bash
# See every registered service and whether it is online
sensorkit service ls

# What is the agent doing right now?
sensorkit agent status

# Watch the raw configuration and state store
sensorkit kv ls -e SimulatedSensor
```

## 4. Open SensorView

[SensorView](https://github.com/sensorkit/sensorview) is the primary graphical interface for SensorKit: a live view of your sensors, devices, and the agent's decisions, with manual control when you want to take over. It's a separate project — clone it and follow its README to build and install:

```bash
git clone https://github.com/sensorkit/sensorview
cd sensorview
# then follow the build and run instructions in its README
```

Point it at the simulated stack's web API at [http://localhost:8000](http://localhost:8000).

!!! tip "Prefer the terminal?"

    Everything SensorView does is also available from the CLI — see [Sensor controller → Manual operation](sensor.md#manual-operation) and [The agent → CLI control](agent.md#cli-control) for driving the system by hand.

## 5. Read the program

The entire demo observing program is 35 lines — `deploy/simulated/program.py`:

```python
import random
from datetime import UTC, datetime, timedelta

import sensorkit.api as sk
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask


@sk.declare_program
class SimProgram:

    @sk.on_attach
    async def startup(self):
        now = datetime.now(UTC)
        sk.program().add_offer(now, now + timedelta(days=1))
        await sk.program().publish_offers()

    @sk.task_factory
    async def task_factory(self):
        return StandardCollectTask(
            target=AltAzTarget(coords=Horizontal(random.randint(0, 30), 85)),
            camera_params=CameraParameterSet(
                integration_time_seconds=5.0,
                frame_count=3,
            ),
        )


@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(SimProgram)
    await service.run()
```

Two ideas carry all the weight here:

- **Offer windows** tell the agent *when* the program has work available. This one advertises a full day starting at launch.
- The **task factory** produces the next task each time the agent activates the program — here, a standard collect at a random azimuth near the zenith.

Swap `AltAzTarget` for a `TLETarget` and you have a satellite tracker. That, plus scheduling and priorities, is covered in [Observing programs](programs.md).

!!! note "Less boilerplate ahead"

    The explicit `@sk.service_entrypoint` block at the bottom is another requirement on its way out: for single-entity services like this one, declaring the program will soon be enough on its own.

## Where to next

- [Installation](installation.md) — run SensorKit against your own hardware and NATS server
- [Configuration](configuration.md) — everything that was in that `sensorkit.yaml`
- [The agent](agent.md) — modes, constraints, and what "global control" actually gates
