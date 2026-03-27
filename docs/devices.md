# Device services

Device services are the bridge between SensorKit and your hardware. Each service connects to a hardware driver and exposes one or more devices (mount, camera, dome, etc.) on the NATS bus. The sensor controller addresses them by name.

All device services are started with:

```bash
sensorkit service run <module> <service-name>
```

---

## ASCOM Alpaca

**Module**: `sensorkit.ascom.service`

Connects to any ASCOM Alpaca server. Supports cameras, mounts (telescopes), domes, filter wheels, focusers, rotators, and observing conditions monitors.

### Config

```yaml
entity: ascom-service
key: AscomConfig
value:
  endpoints:
    - host: 192.168.1.10      # Alpaca server hostname or IP
      port: 32323             # default: 32323
      protocol: http          # http or https; default: http
      devices:
        - entity: my-camera
          device_type: camera
          device_number: 0    # ASCOM device index; default: 0
          status_frequency: 1.0  # status poll interval in seconds

        - entity: my-mount
          device_type: telescope
          device_number: 0
          status_frequency: 1.0

        - entity: my-dome
          device_type: dome

        - entity: my-wheel
          device_type: filterwheel

        - entity: my-focuser
          device_type: focuser

        - entity: my-rotator
          device_type: rotator

        - entity: my-weather
          device_type: observingconditions
```

**Camera-specific fields:**

| Field                      | Type          | Default | Description                   |
|----------------------------|---------------|---------|-------------------------------|
| `cooler_on`                | bool          | `true`  | Turn the cooler on at connect |
| `ccd_temperature_setpoint` | float         | —       | Target CCD temperature (°C)   |
| `readout_mode`             | int or string | —       | Readout mode index or name    |

**Supported `device_type` values:** `camera`, `telescope`, `dome`, `filterwheel`, `focuser`, `rotator`, `observingconditions`

### Launch

```bash
sensorkit service run sensorkit.ascom.service ascom-service
```

---

## PlaneWave PWI4

**Module**: `sensorkit.pwi4.service`

Connects to a PlaneWave mount, focuser, rotator, and mirror cover via the PWI4 HTTP API.

### Config

```yaml
entity: pwi4-service
key: PWI4Config
value:
  endpoints:
    - host: localhost          # PWI4 hostname or IP
      port: 8220               # default: 8220
      devices:
        - entity: my-mount
          device_type: mount
          status_frequency: 1.0
          park_absolute: false          # use absolute axis positions for park
          park_axis0_degrees: 0.0       # axis 0 park position (if park_absolute)
          park_axis1_degrees: 0.0       # axis 1 park position (if park_absolute)
          disable_axis_on_deinit: false # disable motors on shutdown

        - entity: my-focuser
          device_type: focuser
          status_frequency: 1.0
          disable_on_deinit: false

        - entity: my-rotator
          device_type: rotator
          status_frequency: 1.0

        - entity: my-mirror
          device_type: mirrorcover
```

**Supported `device_type` values:** `mount`, `focuser`, `rotator`, `mirrorcover`

### Launch

```bash
sensorkit service run sensorkit.pwi4.service pwi4-service
```

---

## Software Bisque TheSky

**Module**: `sensorkit.thesky.service`

Connects to TheSky/SkyX via its TCP scripting interface. Supports mounts, cameras, domes, filter wheels, focusers, rotators, mirror covers, and weather monitors.

### Config

```yaml
entity: thesky-service
key: TheSkyConfig
value:
  endpoints:
    - host: 192.168.1.20
      port: 3040             # default: 3040
      devices:
        my-mount:
          device_type: mount
        my-camera:
          device_type: camera
        my-dome:
          device_type: dome
```

### Launch

```bash
sensorkit service run sensorkit.thesky.service thesky-service
```

---

## Node Platform

**Module**: `sensorkit.node_platform.service`

Connects to a Node Platform observatory controller.

### Config

```yaml
entity: node-service
key: NodePlatformConfig
value:
  endpoints:
    - host: 192.168.1.30
      port: 9080               # default: 9080
      api_key: your-api-key    # optional
      lineage_id: your-id      # optional
      request_timeout: 30.0    # seconds; default: 30.0
      devices:
        my-mount:
          device_type: mount
        my-camera:
          device_type: camera
```

### Launch

```bash
sensorkit service run sensorkit.node_platform.service node-service
```

---

## Connecting multiple devices

You can have multiple endpoints in a single service config, and multiple service configs pointing to different hosts. Each device gets a unique `entity` name that the sensor controller uses to reference it:

```yaml
entity: ascom-service
key: AscomConfig
value:
  endpoints:
    - host: camera-pc
      port: 32323
      devices:
        - entity: my-camera
          device_type: camera
    - host: dome-pc
      port: 32323
      devices:
        - entity: my-dome
          device_type: dome
```
