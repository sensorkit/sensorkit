# Device services

Device services are the bridge between SensorKit and your hardware. Each service connects to a hardware driver (an Alpaca server, a PWI4 instance, TheSky, …) and exposes its devices on the message bus, where the sensor controller and CLI address them by name.

Each driver module contributes its own section to the unified config file. Declaring a section both configures the devices **and** registers the service — `sensorkit go` will launch it automatically, or you can run it individually:

```bash
sensorkit service run <Id>       # the section's `id` is the service name
```

!!! tip "Don't forget the import"

    A module's config section is only recognized if the module appears in `sensorkit.imports` — otherwise `config load` rejects the section as unknown:

    ```yaml
    sensorkit:
      imports:
        - sensorkit.alpaca.service
        - sensorkit.pwi4.service
    ```

Device names (the keys under `devices:`) are the entity names you'll use everywhere else — in the sensor's `model.components` topology, in constraints (`provider:`), and on the CLI (`-e MyMount`).

---

## ASCOM Alpaca

**Module:** `sensorkit.alpaca.service` · **Extra:** `pip install "sensorkit[alpaca]"`

Connects to any ASCOM Alpaca server — including the ASCOM Remote adapter, so classic ASCOM drivers work too.

```yaml
alpaca:
  - id: Alpaca
    endpoints:
      - host: 192.168.1.10
        port: 11111              # default: 11111 (the Alpaca standard port)
        protocol: http           # http or https
        devices:
          MyCamera:
            device_type: camera
            device_number: 0     # ASCOM device index; default 0
            status_frequency: 1.0  # telemetry poll interval, seconds
          MyMount:
            device_type: telescope
          MyDome:
            device_type: dome
          MyWheel:
            device_type: filter_wheel
          MyFocuser:
            device_type: focuser
          MyRotator:
            device_type: rotator
          MyWeather:
            device_type: observing_conditions
          MySafety:
            device_type: safety_monitor
```

**Supported `device_type` values:** `camera`, `telescope`, `dome`, `filter_wheel`, `focuser`, `rotator`, `observing_conditions`, `safety_monitor`, `cover_calibrator`, `switch`

**Camera options:**

| Field                      | Default | Description                    |
|----------------------------|---------|--------------------------------|
| `temperature`             | `-10.0` | Target sensor temperature in degrees C |

---

## PlaneWave PWI4

**Module:** `sensorkit.pwi4.service` · **Extra:** `pip install "sensorkit[pwi4]"`

Connects to PlaneWave's PWI4 software over its HTTP API: mount, focuser, rotator, and mirror cover.

```yaml
pwi4:
  - id: PlaneWave
    endpoints:
      - host: localhost
        port: 8220               # default: 8220
        devices:
          MyMount:
            device_type: mount
            status_frequency_slow: 1.0
          MyFocuser:
            device_type: focuser
          MyRotator:
            device_type: rotator
          MyCover:
            device_type: cover
```

**Supported `device_type` values:** `mount`, `focuser`, `rotator`, `cover`

---

## Software Bisque TheSky

**Module:** `sensorkit.thesky.service` · **Extra:** `pip install "sensorkit[thesky]"`

Connects to TheSky/SkyX via its TCP scripting interface.

```yaml
thesky:
  - id: TheSky
    endpoints:
      - host: 192.168.1.20
        port: 3040
        devices:
          MyMount:
            device_type: telescope
          MyCamera:
            device_type: camera
          MyDome:
            device_type: dome
```

**Supported `device_type` values:** `telescope`, `camera`, `dome`, `filter_wheel`, `focuser`, `rotator`, `weather`, `ota`

---

## Observable Space Node Platform

**Module:** `sensorkit.node_platform.service` · **Extra:** `pip install "sensorkit[node-platform]"`

Connects to a Node Platform observatory controller.

```yaml
node_platform:
  - id: NodePlatform
    endpoints:
      - host: 192.168.1.30
        port: 9080               # default: 9080
        env_file: .env           # NODE_PLATFORM_API_KEY (and optional NODE_PLATFORM_LINEAGE_ID)
        request_timeout: 30.0
        operation_mode: assisted # or: manual
        devices:
          MyMount:
            device_type: mount
          MyEnclosure:
            device_type: enclosure
          MyWeather:
            device_type: weather
```

Node Platform does not provide a camera device through this module.
Configure a camera through another device service.

Other modules (INDIGO, NINA) follow the same pattern; their sections are named after the module. Module maturity varies — Alpaca and PWI4 are the most heavily exercised.

---

## Multiple endpoints and hosts

A single service can talk to several driver endpoints, and you can declare several services against different hosts. Connection settings on the endpoint propagate down to its devices:

```yaml
alpaca:
  - id: Alpaca
    endpoints:
      - host: camera-pc
        devices:
          MyCamera:
            device_type: camera
      - host: dome-pc
        devices:
          MyDome:
            device_type: dome
```

## How devices appear on the bus

Once connected, each device publishes:

- **Details** — the commands it supports, from which SensorKit derives its *traits* (structural capabilities like "can slew", "can capture") and *archetype* (mount, camera, …). Controllers match devices by capability, not by driver.
- **Keywords** - Typed telemetry, such as pointing, temperature, and weather readings.
  Polling intervals depend on the driver; INDIGO publishes property changes as they arrive.
- **State** - Framework enablement and any driver-specific persisted state.
  Connection status is established by the driver when it connects.

You can command a device directly from the CLI:

```bash
sensorkit device connect    -e MyMount
sensorkit device disconnect -e MyMount
sensorkit device abort      -e MyMount     # stop motion immediately
```

## Writing your own device

If your hardware speaks something SensorKit doesn't yet, a device is just a Python class with command handlers — see the [API reference](api.md):

```python
import sensorkit.api as sk
from sensorkit.std import Connect, Stop

@sk.declare_device
class MyMount:
    @sk.command_handler
    async def connect(self, cmd: Connect):
        ...

    @sk.command_handler
    async def stop(self, cmd: Stop):
        ...

@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(MyMount, name="my-mount")
    await service.run()
```
