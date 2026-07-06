# NINA Module

The `nina` module provides integration for N.I.N.A. (Nighttime Imaging 'N' Astronomy) via the NINA Advanced API plugin.

## Supported Devices
- Camera
- Dome (enclosure)
- FilterWheel
- Focuser
- Guider
- Mount
- Rotator
- SafetyMonitor
- Switch
- Weather

## Supported Tracking Modes

The `mount` device handles `FollowTarget` commands. NINA's Advanced API exposes
only sidereal tracking on/off — there is no non-sidereal offset-rate primitive —
so moving targets can only be slewed to their current position, not rate-tracked.
The following target types are currently supported:

| Target        | Frame        | Behavior                                                                                                          |
| ------------- | ------------ |-------------------------------------------------------------------------------------------------------------------|
| `ICRSTarget`  | ICRF         | Slew to fixed RA/Dec and enable sidereal tracking.                                                                |
| `AltAzTarget` | ALTAZ        | Slew to fixed Alt/Az.                                                                                             |
| `TLETarget`   | TEME         | Best-effort: adapted to ICRF and slewed to the satellite's current position; no offset rates, so it does not track the motion. |
| `FrameTarget` | ICRF / ALTAZ | Set tracking at current pointing: ICRF enables sidereal tracking, ALTAZ disables tracking (stop / hold in place). |

Any other target type (e.g. `RateTarget`, `EphemerisTarget`) is logged and ignored.

Note: the Advanced API plugin is open-source on GitHub, and so adding support for custom rates would open use of this module for satellite tracking.

## Example Config

`host` and `port` set on the endpoint propagate to every device; override them
per-device when a device lives on a different NINA instance. `status_frequency` and `timeout` are optional overrides for each device (except
`mount`). Each device also reads optional Advanced API credentials from `env_file` (default `.env`), which may
define `NINA_USERNAME` and `NINA_PASSWORD` for HMAC auth — set it per device, or
rely on the `.env` default; omit auth entirely if your NINA API is unsecured.

```yaml
entity: nina_service
key: NinaConfig
value:
  endpoints:
    - host: localhost
      port: 1888
      devices:
        safety_monitor1:
          device_type: safety_monitor
        weather1:
          device_type: weather
        switch1:
          device_type: switch
        dome1:
          device_type: dome
        mount1:
          device_type: mount
          status_frequency_slow: 1.0   # status rate while idle/tracking
          status_frequency_fast: 0.1   # status rate while slewing (seconds)
        guider1:
          device_type: guider
        rotator1:
          device_type: rotator
        focuser1:
          device_type: focuser
        filter_wheel1:
          device_type: filter_wheel
        camera1:
          device_type: camera
          temperature: -10.0           # target sensor setpoint (C)
          env_file: .env               # optional; NINA_USERNAME / NINA_PASSWORD for HMAC auth
```

## Example: Weather Constraint

A weather constraint pauses a controller (agent) when conditions exceed the
configured thresholds. The constraint's `provider` is the entity name of any
device that publishes the `BasicWeather` keyword — that can be this module's
`weather` device.

First, expose the weather device as an entity (here named `NinaWeather`):

```yaml
entity: nina_service
key: NinaConfig
value:
  endpoints:
    - host: localhost
      port: 1888
      devices:
        NinaWeather:
          device_type: weather
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
          provider: NinaWeather   # entity publishing BasicWeather
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

## Usage
```sh
sensorkit service run nina_service sensorkit.nina.service
```
