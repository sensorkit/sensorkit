# PWI4 Module

The `pwi4` module provides integration for PlaneWave Instruments devices via the PWI4 HTTP API.

## Supported Devices
- Cover (mirror cover)
- Focuser
- Mount
- Rotator

## Supported Tracking Modes

The `mount` device handles `FollowTarget` commands. PWI4 exposes a native RA/Dec
path-follow primitive plus native TLE following and constant offset rates, so
moving targets are tracked along their full path rather than collapsed to a
single offset rate. The following target types are currently supported:

| Target            | Frame        | Behavior                                                                                                          |
| ----------------- | ------------ |-------------------------------------------------------------------------------------------------------------------|
| `ICRSTarget`      | ICRF         | Slew to fixed RA/Dec and enable sidereal tracking.                                                                |
| `AltAzTarget`     | ALTAZ        | Slew to fixed Alt/Az.                                                                                             |
| `TLETarget`       | TEME         | Native TLE following via PWI4.                                                                                    |
| `RateTarget`      | ICRF         | Slew to an initial RA/Dec, then apply constant RA/Dec offset rates.                                               |
| `EphemerisTarget` | ICRF         | Precomputed RA/Dec path streamed to PWI4's path-follow primitive (`radecpath`).                                  |
| `FrameTarget`     | ICRF / ALTAZ | Set tracking at current pointing: ICRF enables sidereal tracking, ALTAZ disables tracking (stop / hold in place). |

Propagatable inputs (e.g. a state vector) are normalized to an ICRF
`EphemerisTarget` by `adapt()` before being followed.

## Example Config

`host` and `port` set on the endpoint propagate to every device; override them
per-device when a device lives on a different PWI4 instance. The endpoint-level
`request_timeout` is the HTTP request timeout for the shared PWI4 client
(transport), distinct from the per-device `timeout`, which bounds device
operations. `status_frequency` and `timeout` are optional overrides for each
device (the `mount` splits status into slow/fast rates).

```yaml
entity: pwi4_service
key: PWI4Config
value:
  endpoints:
    - host: localhost
      port: 8220
      request_timeout: 60.0          # HTTP request timeout for the shared PWI4 client
      devices:
        cover1:
          device_type: cover
        mount1:
          device_type: mount
          wrap_autocenter: false       # keep the azimuth cable wrap centered on long tracks
          wrap_interval: 60.0          # seconds between wrap-center checks
          wrap_deadband_deg: 10.0      # only recenter when beyond this from center
          heaters: {}                  # {role: power_percent} for mirror/optics heaters
          fans: []                     # fan roles to turn on
          status_frequency_slow: 1.0   # status rate while idle/tracking
          status_frequency_fast: 0.1   # increased publishing rate during tracking
        rotator1:
          device_type: rotator
        focuser1:
          device_type: focuser
```

## Usage
```sh
sensorkit service run pwi4_service sensorkit.pwi4.service
```
