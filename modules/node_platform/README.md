# Node Platform Module

The `node_platform` module provides integration for the Observable Space Node Platform via the `ourskyai_node_platform_api` package.

## Supported Devices
- Cover (mirror cover)
- Enclosure
- Focuser
- M3
- Mount
- Rotator
- Weather

> **Note:** The `weather` device currently doubles as a safety provider. Alongside
> `BasicWeather`, it publishes the standard `BasicSafety` go/no-go keyword (plus
> an extended `Safety` breakdown of the contributing sources), so the same entity
> can back a `kind: safety` constraint as well as a `kind: weather` one.

## Supported Tracking Modes

The `mount` device handles `FollowTarget` commands. The Node Platform exposes a native path-follow primitive (streamed ENU unit-vector
samples) plus native TLE following, so moving targets are tracked along their
full path rather than collapsed to a single offset rate. The following target
types are currently supported:

| Target            | Frame        | Behavior                                                                                                          |
| ----------------- | ------------ |-------------------------------------------------------------------------------------------------------------------|
| `ICRSTarget`      | ICRF         | Slew to fixed RA/Dec and enable sidereal tracking.                                                                |
| `AltAzTarget`     | ALTAZ        | Slew to fixed Alt/Az.                                                                                             |
| `TLETarget`       | TEME         | Native TLE following via the Node Platform.                                                                       |
| `RateTarget`      | CIRF         | Constant-rate path sampled to ENU unit vectors and tracked via the path-follow primitive.                         |
| `EphemerisTarget` | ICRF         | Precomputed path, resolved to ENU unit vectors and tracked; supports through the pole / RA=0 seam.                |
| `FrameTarget`     | ICRF / ALTAZ | Set tracking at current pointing: ICRF enables sidereal tracking, ALTAZ disables tracking (stop / hold in place). |

Propagatable inputs (e.g. a state vector) are normalized to an ICRF
`EphemerisTarget` by `adapt()` before being followed.

## Example Config

Connection settings live on the endpoint and apply to every device on it:
`host`, `port`, `request_timeout`, and `env_file` are propagated to each device,
so setting them per-device has no effect. Credentials live only in `env_file`
(default `.env`): the API key is read as `NODE_PLATFORM_API_KEY`, and the lineage ID — optional, since the Node Platform
can scope it server-side from the API key — as `NODE_PLATFORM_LINEAGE_ID` (each
also falling back to the process environment). `status_frequency` and `timeout` are optional overrides for each
device (except `mount`).

```yaml
entity: node_platform
key: NodePlatformConfig
value:
  endpoints:
    - host: nodeplatformhost
      port: 9080
      request_timeout: 30.0           # HTTP request timeout (applies to all devices)
      env_file: .env                  # NODE_PLATFORM_API_KEY (and optional NODE_PLATFORM_LINEAGE_ID)
      operation_mode: assisted        # assisted = Node Platform controls shutter; manual = SensorKit controls shutter
      devices:
        weather1:
          device_type: weather
          metric_lookback_seconds: 300.0   # window of platform metrics to average
        enclosure1:
          device_type: enclosure
        cover1:
          device_type: cover
        mount1:
          device_type: mount
          fans: false                  # optical-tube fans
          heater_power:
            M1: 1.0                    # percent
            M2: 1.0
            M3: 0.0
          status_frequency_slow: 1.0   # status rate while idle/tracking
          status_frequency_fast: 0.1   # increased publishing rate during tracking
        m3_1:
          device_type: m3
        rotator1:
          device_type: rotator
          derotate: false
        focuser1:
          device_type: focuser
```

## Example: Weather Constraint

A weather constraint pauses a controller (agent) when conditions exceed the
configured thresholds. The constraint's `provider` is the entity name of any
device that publishes the `BasicWeather` keyword — that can be this module's
`weather` device.

First, expose the weather device as an entity (here named `NodeWeather`):

```yaml
entity: node_platform
key: NodePlatformConfig
value:
  endpoints:
    - host: nodeplatformhost
      port: 9080
      env_file: .env
      devices:
        NodeWeather:
          device_type: weather
          metric_lookback_seconds: 300.0
          status_frequency: 30.0
          timeout: 30.0
```

Then reference it as the `provider` of a weather constraint on the controller:

```yaml
automation:
  controllers:
    MySensor:
      constraints:
        - kind: weather
          provider: NodeWeather   # entity publishing BasicWeather
          hold: 300.0             # seconds to stay constrained after clearing
          ttl: 30.0               # constrain if no data within this window
          optional: false         # fail-closed: missing provider => constrained
          humidity_max: 85.0
          humidity_deadband: 2.0
          wind_max: 30.0
          wind_deadband: 2.0
```

Each `*_max` threshold is optional — omit a field to skip that check. The
matching `*_deadband` widens the clear-side threshold (hysteresis) so the
constraint doesn't chatter around the limit.

## Example: Safety Constraint

The same `weather` device also publishes `BasicSafety`, a simple go/no-go flag
reflecting the Node Platform's own safety assessment. A `kind: safety`
constraint pauses the controller whenever that flag reports unsafe — the
cleanest way to gate on the platform's verdict, since it needs no thresholds:

```yaml
automation:
  controllers:
    MySensor:
      constraints:
        - kind: safety
          provider: NodeWeather   # entity publishing BasicSafety
          hold: 300.0             # seconds to stay constrained after clearing
          ttl: 30.0               # constrain if no data within this window
          optional: false         # fail-closed: unsafe or missing => constrained
```

Unlike the weather constraint, this carries no thresholds — it simply mirrors
the provider's `is_safe` flag, constraining while unsafe and clearing when safe.

## Usage
```sh
sensorkit service run node_platform_service sensorkit.node_platform.service
```
