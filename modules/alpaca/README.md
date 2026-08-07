# Alpaca Module

The `alpaca` module provides integration for ASCOM Alpaca devices via the `alpyca` package.

## Supported Devices
- Camera
- CoverCalibrator (mirror cover)
- Dome (enclosure)
- FilterWheel
- Focuser
- ObservingConditions (weather)
- Rotator
- SafetyMonitor
- Switch
- Telescope (mount)

## Supported Tracking Modes

The `telescope` device handles `FollowTarget` commands. ASCOM/Alpaca has no
path-follow primitive — only slew plus a constant offset rate — so moving
targets are collapsed to an initial position plus a best-effort offset rate that
drifts over time. The following target types are currently supported:

| Target            | Frame        | Behavior                                                                                                          |
| ----------------- | ------------ |-------------------------------------------------------------------------------------------------------------------|
| `ICRSTarget`      | ICRF         | Slew to fixed RA/Dec and enable sidereal tracking.                                                                |
| `AltAzTarget`     | ALTAZ        | Slew to fixed Alt/Az.                                                                                             |
| `TLETarget`       | TEME         | Skyfield-propagated satellite position + offset rates, with re-acquisition after the initial slew.                |
| `RateTarget`      | ICRF         | Slew to an initial RA/Dec, then apply constant RA/Dec offset rates.                                               |
| `EphemerisTarget` | ICRF         | Slew to the precomputed sample nearest *now* and apply a finite-difference offset rate.                           |
| `FrameTarget`     | ICRF / ALTAZ | Set tracking at current pointing: ICRF enables sidereal tracking, ALTAZ disables tracking (stop / hold in place). |

Propagatable inputs (e.g. a state vector) are normalized to an ICRF
`EphemerisTarget` by `adapt()` before being followed. Any other target type
raises `NotImplementedError`.

Note: both `RateTarget` and `EphemerisTarget` cases will employ closed-loop tracking with the eventual inclusion of PID controls within SensorKit.

## Example Config

`host`, `port`, and `protocol` set on the endpoint propagate to every device;
override them per-device when a device lives on a different server. Each device
is addressed by its `device_number` on that server. `status_frequency` and `timeout` are optional overrides for each device (except `telescope`).

```yaml
entity: alpaca_service
key: AlpacaConfig
value:
  endpoints:
    - host: localhost
      port: 11111
      protocol: http
      devices:
        safety_monitor1:
          device_type: safety_monitor
          device_number: 0
        weather1:
          device_type: observing_conditions
          device_number: 0
          average_period: null         # optional; hours of averaging requested from the device
        switch1:
          device_type: switch
          device_number: 0
        dome1:
          device_type: dome
          device_number: 0
        cover_calibrator1:
          device_type: cover_calibrator
          device_number: 0
        telescope1:
          device_type: telescope
          device_number: 0
          status_frequency: 1.0        # status rate while idle (seconds)
          status_frequency_fast: 0.1   # status rate while slewing or tracking
        rotator1:
          device_type: rotator
          device_number: 0
        focuser1:
          device_type: focuser
          device_number: 0
        filter_wheel1:
          device_type: filter_wheel
          device_number: 0
        camera1:
          device_type: camera
          device_number: 0
          temperature: -10.0           # C
```

## Example: Weather Constraint

A weather constraint pauses a controller (agent) when conditions exceed the
configured thresholds. The constraint's `provider` is the entity name of any
device that publishes the `BasicWeather` keyword — that can be this module's
`observing_conditions` device.

First, expose the weather device as an entity (here named `AlpacaWeather`):

```yaml
entity: alpaca_service
key: AlpacaConfig
value:
  endpoints:
    - host: localhost
      port: 11111
      protocol: http
      devices:
        AlpacaWeather:
          device_type: weather
          status_frequency: 5.0
          timeout: 60.0
```

Then reference it as the `provider` of a weather constraint on the controller:

```yaml
automation:
  controllers:
    MySensor:
      constraints:
        - kind: weather
          provider: AlpacaWeather   # entity publishing BasicWeather
          hold: 300.0               # seconds to stay constrained after clearing
          ttl: 30.0                 # constrain if no data within this window
          optional: false           # fail-closed: missing provider => constrained
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
sensorkit service run alpaca_service sensorkit.alpaca.service
```
