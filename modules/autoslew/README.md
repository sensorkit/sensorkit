# Autoslew Module

The `autoslew` module provides integration for ASA Autoslew mounts via ASCOM Alpaca (using the `alpyca` package).

## Supported Devices
- Cover Calibrator (mirror cover)
- Dome (enclosure; shutter only)
- Focuser
- Rotator
- Telescope (mount)
- Tertiary

> **Note:** ASA's telescope-specific extensions (satellite tracking, refraction,
> pointing model, dome shutter, Nasmyth selection) all ride the ASCOM
> `Action` / `CommandString` / `CommandBool` mechanisms on the **Telescope**
> device. So the `dome` and `tertiary` devices — which have no ASCOM device of
> their own — drive their verbs through a shared Telescope "backbone", and the
> `focuser`/`rotator` use it for their ASA extras (`focuser:homefind`,
> `rotator:setslewoption`, …) on top of their own standard ASCOM device. Autoslew
> exposes no ASCOM Dome device, so most deployments run the enclosure as its own
> module and use `dome` here only when Autoslew drives its own shutter.

## Supported Tracking Modes

The `telescope` device handles `FollowTarget` commands. ASCOM/Alpaca has no
generic path-follow primitive, but Autoslew runs the satellite ephemeris on the
controller, so `TLETarget`s are followed natively (via ASA's `sat:*` flow) rather
than collapsed to an offset rate; other moving targets slew to an initial position
and apply a constant RA/Dec offset rate. Autoslew's ASCOM interface is JNow /
of-date on both read and command, so RA/Dec is converted at the boundary — status
is published as ICRF, and gotos are sent as JNow. The following target types are
currently supported:

| Target            | Frame        | Behavior                                                                 |
| ----------------- | ------------ |-------------------------------------------------------------------------|
| `ICRSTarget`      | ICRF         | Slew to fixed RA/Dec (converted ICRS→JNow) and enable sidereal tracking. |
| `AltAzTarget`     | ALTAZ        | Slew to fixed Alt/Az.                                                   |
| `TLETarget`       | TEME         | Native satellite following via Autoslew (`sat:*`).                      |
| `RateTarget`      | ICRF         | Slew to an initial RA/Dec (JNow), then apply constant RA/Dec offset rates. |
| `EphemerisTarget` | ICRF         | Reduced to the first sample's position plus a constant offset rate (Alpaca has no path primitive). |
| `FrameTarget`     | ICRF / ALTAZ | Set tracking at current pointing: ICRF enables sidereal tracking, ALTAZ disables tracking (stop / hold in place). |

Propagatable inputs (e.g. a state vector) are normalized to an ICRF
`EphemerisTarget` by `adapt()` before being followed.

## Example Config

`host` and `port` set on the endpoint propagate to every device; override them
per-device when a device lives on a different Autoslew instance (Autoslew's Alpaca
server is always on port `11111`). `status_frequency` and `timeout` are optional
overrides for each device (the `telescope` splits status into slow/fast rates).

```yaml
entity: autoslew_service
key: AutoslewConfig
value:
  endpoints:
    - host: localhost
      port: 11111                      # Autoslew's Alpaca server is always 11111
      devices:
        Telescope:
          device_type: telescope
          min_altitude_degrees: 20.0   # floor for the satellite start altitude (sat:startalt)
          status_frequency: 1.0        # status rate while idle (seconds)
          status_frequency_fast: 0.1   # status rate while slewing or tracking
        Focuser:
          device_type: focuser
        Rotator:
          device_type: rotator
          slew_option: 0               # 0=track (de-rotate), 1=North, 2=SmartNorthSouth
        CoverCalibrator:
          device_type: cover_calibrator
        Dome:
          device_type: dome            # only when Autoslew drives its own shutter
        Tertiary:
          device_type: tertiary
```

## Usage
```sh
sensorkit service run autoslew_service sensorkit.autoslew.service
```
