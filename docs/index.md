# SensorKit

**SensorKit**: A Python framework for building autonomous sensor and astronomical observatory systems.

Features:

- Autonomous SDA operation out of the box
- Controls ASCOM, Planewave, and Software Bisque devices
- State Vector propagation
- Configuration-defined data and metadata flow
- Analysis infrastructure for calibration and edge data processing
- Designed for extensibility, with discovery-based extension points
- Loosely coupled distributed architecture for failover and scalability at the edge

SensorKit supports NATS JetStream as its messaging backbone, as well as an in-memory implementation for testing.
 
---

## Quick start — simulated observatory

The `deploy/simulated` directory contains a complete Docker Compose stack with simulated hardware: a PlaneWave mount simulator, ASCOM Alpaca device simulators (camera, dome, filter wheel, focuser, rotator, weather monitor), and a full SensorKit automation stack on top.

**Prerequisites:** Docker Compose and Git.

### 1. Clone

```bash
git clone <sensorkit-repo-url>
cd sensorkit/deploy/simulated
```

### 2. Start the stack

```bash
docker compose up --build
```

The `--build` flag compiles the SensorKit image and pulls the simulator images on first run (takes a few minutes). The services start in dependency order:

| Service         | What it does                                                                          |
|-----------------|---------------------------------------------------------------------------------------|
| `nats`          | NATS JetStream message bus                                                            |
| `planewave-sim` | PWI4 mount simulator with a browser-accessible noVNC UI                               |
| `ascom-sim`     | ASCOM Alpaca simulators for camera, dome, filter wheel, focuser, rotator, and weather |
| `reload-config` | Loads all config from `sensorkit.yaml` into the NATS KV store, then exits            |
| `sensor`        | SensorKit sensor controller — connects and coordinates all devices                    |
| `planewave`     | SensorKit PWI4 service — connects to the mount simulator                              |
| `alpaca`        | SensorKit ASCOM Alpaca service — connects to the Alpaca simulators                    |
| `agent`         | Automation agent — manages the controller and schedules the program                   |
| `program`       | Demo observing program — generates collect tasks with random alt/az targets           |
| `otto`          | Otto observing program service                                                        |
| `webapi`        | SensorKit web API                                                                     |

### 3. Run the SensorKit CLI

To interact with your new running instance:

- install via `pip` as outlined in (Installation)[installation.md]
- run `uv sync --all-extras` to install in a venv associated with your cloned repository
- run `docker exec -it <container-name> bash` to gain access via one of the running containers

```bash
uv sync --all-extras 
```

Alternatively, you can run the CLI directly from a service container.

When the Agent starts for the first time, automation is disabled for safety reasons. You can inspect and control the system manually:

```bash
# See all registered services
sensorkit service ls

# Check the agent status
sensorkit agent status

# Manually initialize the sensor (connect and ready all devices)
sensorkit controller init -e sim_sensor

# Run a standard collect task manually
sensorkit controller collect -e sim_sensor \
    -t '{"target_type": "altaz", "azimuth_degrees": 180.0, "altitude_degrees": 60.0}' \
    -i 5.0 -c 3

# Shut the sensor down
sensorkit controller shutdown -e sim_sensor
```

To instruct the Agent to begin autonomous operation, run:

```bash
sensorkit agent global-control on
```

### 4. Watch the simulators

Open [http://localhost:6080](http://localhost:6080) in a browser to see the PWI4 noVNC interface showing the simulated mount.

Similarly, open [http://localhost:30000](http://localhost:30000) in a browser to see the ASCOM Alpaca simulators.

### 5. Inspect the code

`deploy/simulated/program.py` is the complete source for the simulated observing program — a good first look at the SensorKit API:

```python
import sensorkit.api as sk
from sensorkit.astro.coords import Horizontal
from sensorkit.astro.target import AltAzTarget
from sensorkit.std import CameraParameterSet, StandardCollectTask


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
            end_time=datetime.now(UTC) + timedelta(minutes=2),
        )


@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(SimProgram)
    await service.run()
```

Programs publish **offer windows** that tell the agent when they have work available. When the agent decides it is time to observe, it activates the program to begin pulling tasks from the **task factory** and dispatching them to the program's associated controller.

In this example, the program advertises that it can do work for 1 day after it starts up, and, when activated, generates standard collect tasks with random alt/az targets.

---

## What's in this documentation

|                                   |                                                                   |
|-----------------------------------|-------------------------------------------------------------------|
| [Installation](installation.md)   | Install SensorKit, start NATS, load config, and run services      |
| [Configuration](configuration.md) | KV store structure, loading config, inspecting and editing values |
| [Device services](devices.md)     | ASCOM, PWI4, TheSky, Node Platform — config reference             |
| [Sensor controller](sensor.md)    | Configure the controller, task lifecycle, and manual operation    |
| [Agent](agent.md)                 | Modes, criteria, constraints, scheduling, and CLI control         |
| [CLI reference](cli.md)           | Full reference for all `sensorkit` commands                       |
| [API reference](api.md)           | All symbols exported from `sensorkit.api`                         |
