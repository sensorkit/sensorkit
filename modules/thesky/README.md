# TheSky Module

The `thesky` module provides integration for the Software Bisque TheSky application via its JavaScript-over-TCP scripting interface.

## Supported Devices
- Camera
- Dome (enclosure)
- FilterWheel
- Focuser
- Telescope (mount)
- OTA (mirror cover)
- Rotator
- Weather

> **Note:** TheSky's `weather` device is a go/no-go **safety** provider — it
> publishes the standard `BasicSafety` keyword (derived from TheSky's
> `WeatherUtil.goodToGo`), not numeric `BasicWeather`. So it backs a
> `kind: safety` constraint rather than a `kind: weather` one (see below).

## Supported Tracking Modes

The `telescope` device handles `FollowTarget` commands. TheSky
supports custom offset-rate tracking and native satellite (TLE) following via its
Raven3 plugin, but has no path-follow primitive — so a precomputed ephemeris is
collapsed to an initial position plus a single constant offset rate. The
following target types are currently supported:

| Target            | Frame        | Behavior                                                                                                    |
| ----------------- | ------------ |-------------------------------------------------------------------------------------------------------------|
| `ICRSTarget`      | ICRF         | Slew to fixed RA/Dec and enable sidereal tracking.                                                          |
| `AltAzTarget`     | ALTAZ        | Slew to fixed Alt/Az.                                                                                       |
| `TLETarget`       | TEME         | Native satellite tracking via TheSky's Raven3 plugin.                                                       |
| `RateTarget`      | ICRF         | Slew to an initial RA/Dec, then apply constant RA/Dec offset rates.                                         |
| `EphemerisTarget` | ICRF         | Slew to the precomputed sample nearest *now* and apply a finite-difference offset rate (best-effort; drifts). |
| `FrameTarget`     | ICRF / ALTAZ | Set tracking at current pointing: ICRF enables sidereal tracking, ALTAZ disables tracking (stop / hold in place). |

Propagatable inputs (e.g. a state vector) are normalized to an ICRF
`EphemerisTarget` by `adapt()` before being followed.

Note: TLE following drives TheSky's Raven3 plugin — it requires the *TheSky Satellites* module to be
installed and is not available in TheSky's bundled mount simulator.

## Example Config

`host` and `port` set on the endpoint propagate to every device; override the `host`
per-device when a device lives on a different TheSky instance. Devices talk to
TheSky over its JavaScript-over-TCP interface (port 3040), serialized
behind a shared per-connection lock. `status_frequency` and `timeout` are optional overrides for each device (except `telescope`).

```yaml
entity: thesky_service
key: TheSkyConfig
value:
  endpoints:
    - host: localhost
      port: 3040
      devices:
        weather1:
          device_type: weather           # go/no-go safety provider (publishes BasicSafety)
        dome1:
          device_type: dome
        mirror_cover1:
          device_type: ota
        mount1:
          device_type: telescope
          status_frequency_slow: 1.0     # status rate while idle/tracking
          status_frequency_fast: 0.1     # increased publishing rate during tracking
        rotator1:
          device_type: rotator
        focuser1:
          device_type: focuser
        filter_wheel1:
          device_type: filter_wheel
          filters:                       # name -> slot index
            Filter 1: 0
            Filter 2: 1
            Filter 3: 2
        camera1:
          device_type: camera
          temperature: -10.0             # C
```

## Example: Safety Constraint

TheSky's `weather` device doesn't report numeric conditions — it publishes
`BasicSafety`, a single go/no-go flag derived from TheSky's `WeatherUtil.goodToGo`.
A `kind: safety` constraint pauses the controller (agent) whenever that flag
reports unsafe — the cleanest way to gate on TheSky's verdict, since it needs no
thresholds.

First, expose the weather device as an entity (here named `TheSkyWeather`):

```yaml
entity: thesky_service
key: TheSkyConfig
value:
  endpoints:
    - host: localhost
      port: 3040
      devices:
        TheSkyWeather:
          device_type: weather
          status_frequency: 5.0
          timeout: 60.0
```

Then reference it as the `provider` of a safety constraint on the controller:

```yaml
automation:
  controllers:
    MySensor:
      constraints:
        - kind: safety
          provider: TheSkyWeather   # entity publishing BasicSafety
          hold: 300.0               # seconds to stay constrained after clearing
          ttl: 30.0                 # constrain if no data within this window
          optional: false           # fail-closed: unsafe or missing => constrained
```

It carries no thresholds — it simply mirrors the provider's `is_safe` flag,
constraining while unsafe and clearing when safe.

## Usage
```sh
sensorkit service run thesky sensorkit.thesky.service
```
