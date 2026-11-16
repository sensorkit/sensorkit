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

The `telescope` device handles `FollowTarget` commands.
TLE targets use ASA's native `sat:*` satellite tracking.
Other moving targets use an initial slew and constant RA/Dec tracking offsets.

Commanded positions and tracking offsets are converted from ICRS into the coordinate system returned by Autoslew's `EquatorialSystem` property.
Reported pointing is converted back to ICRS and published as ICRF.
Topocentric coordinates use the observation time and site (TETE/JNow).
An unspecified system uses the shared J2000/ICRS fallback.
Initialization warns when the reported system differs from the expected Topocentric system and continues using the reported value.
The following target types are currently supported:

| Target            | Frame        | Behavior                                                                 |
| ----------------- | ------------ |-------------------------------------------------------------------------|
| `ICRSTarget`      | ICRF         | Convert RA/Dec to the reported equatorial system, slew, and enable sidereal tracking. |
| `AltAzTarget`     | ALTAZ        | Slew to fixed Alt/Az.                                                   |
| `TLETarget`       | TEME         | Native satellite following via Autoslew (`sat:*`).                      |
| `RateTarget`      | ICRF         | Slew to initial coordinates and apply constant tracking offsets in the reported equatorial system. |
| `EphemerisTarget` | ICRF         | Slew to the first sample and apply constant tracking offsets estimated from the first two samples. |
| `FrameTarget`     | ICRF / ALTAZ | At the current pointing, ICRF enables sidereal tracking and ALTAZ disables it. |

Propagatable inputs (e.g. a state vector) are normalized to an ICRF
`EphemerisTarget` by `adapt()` before being followed.

## Example Config

The YAML below uses low-level `entity`/`key`/`value` records.
Save all documents in a file and load them with `sensorkit kv load FILE` before starting the service.
For deployment-wide configuration, see the [unified configuration guide](../../docs/configuration.md).

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
          status_frequency: 1.0        # poll interval while idle (seconds)
          status_frequency_fast: 0.1   # poll interval while slewing or tracking (seconds)
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
