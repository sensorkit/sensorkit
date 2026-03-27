# Sensor controller

The sensor controller (`sensorkit.std.sensor`) is the built-in controller for a standard observatory-like sensor. It manages a mount and camera, with optional support for a dome, focuser, rotator, filter wheel, and mirror cover.

## Launch

```bash
sensorkit service run sensorkit.std.sensor my-sensor
```

The service name (`my-sensor` here) must match the `entity` in `SensorConfig`.

## Config

```yaml
entity: my-sensor
key: SensorConfig
value:
  controller_name: my-sensor

  devices:
    mount: my-mount           # required
    camera: my-camera         # required
    dome: my-dome             # optional
    filter_wheel: my-wheel    # optional
    focuser: my-focuser       # optional
    rotator: my-rotator       # optional
    mirror_cover: my-mirror   # optional

  site_position:
    latitude_degrees: 34.0522
    longitude_degrees: -118.2437
    altitude_km: 0.086

  policies:
    mount_init_timeout: 30.0
    mount_home_timeout: 300.0
    dome_open_close_timeout: 120.0
    mirror_cover_open_close_timeout: 60.0
    concurrent_dome_and_mount_init: false
    concurrent_dome_and_mount_deinit: false
    concurrent_mount_and_mirror_cover_init: false
    minimum_target_altitude_degrees: 20.0
    sun_separation_degrees: 45.0
    moon_separation_degrees: 5.0
```

### `devices`

Each value is the entity name of a running device service. The names must match the `entity` fields you configured in your device service.

### `site_position`

Used to calculate rise/set times, target altitude checks, and FITS header keywords.

| Field               | Type  | Description                           |
|---------------------|-------|---------------------------------------|
| `latitude_degrees`  | float | Observatory latitude, positive north  |
| `longitude_degrees` | float | Observatory longitude, positive east  |
| `altitude_km`       | float | Observatory elevation above sea level |

### `policies`

All policy fields are optional. Values shown above are the defaults.

| Field                                    | Default | Description                                   |
|------------------------------------------|---------|-----------------------------------------------|
| `mount_init_timeout`                     | 30.0    | Seconds to wait for mount init to complete    |
| `mount_home_timeout`                     | 300.0   | Seconds to wait for mount home sequence       |
| `dome_open_close_timeout`                | 120.0   | Seconds to wait for dome open or close        |
| `mirror_cover_open_close_timeout`        | 60.0    | Seconds to wait for mirror cover              |
| `concurrent_dome_and_mount_init`         | false   | Open dome and init mount at the same time     |
| `concurrent_dome_and_mount_deinit`       | false   | Close dome and deinit mount at the same time  |
| `concurrent_mount_and_mirror_cover_init` | false   | Open mirror cover while mount is initializing |
| `minimum_target_altitude_degrees`        | —       | Reject targets below this altitude            |
| `sun_separation_degrees`                 | —       | Reject targets within this angle of the sun   |
| `moon_separation_degrees`                | —       | Reject targets within this angle of the moon  |

## Task lifecycle

The controller responds to tasks sent by the agent or triggered manually via the CLI.

| Task         | What happens                                                           |
|--------------|------------------------------------------------------------------------|
| **Init**     | Connect devices, initialize mount, open mirror cover, open dome        |
| **Standby**  | Same as Init (brings the sensor to a ready-to-observe state)           |
| **Collect**  | Slew to target, set filter and binning, capture frames, stop mount     |
| **Recover**  | Reconnect all devices, stop any in-progress motion                     |
| **Shutdown** | Close mirror cover, deinitialize mount, close dome, disconnect devices |

## Manual operation

Use the CLI to drive the controller directly:

```bash
# Initialize (connect and prepare the sensor)
sensorkit controller init -e my-sensor

# Abort whatever is currently running
sensorkit controller abort -e my-sensor

# Run a collect task manually
sensorkit controller collect -e my-sensor \
    -t '{"target_type": "icrs", "right_ascension_hours": 5.58, "declination_degrees": -5.39}' \
    -i 30.0 \
    -c 10

# Shut down
sensorkit controller shutdown -e my-sensor
```

### Collect target types

Pass a JSON object to `-t` with a `target_type` field:

| `target_type` | Required fields                                | Description                                     |
|---------------|------------------------------------------------|-------------------------------------------------|
| `icrs`        | `right_ascension_hours`, `declination_degrees` | Fixed celestial object; mount tracks sidereally |
| `altaz`       | `azimuth_degrees`, `altitude_degrees`          | Fixed horizon position                          |
| `tle`         | `line0`, `line1`, `line2`                      | Earth-orbiting satellite; mount tracks from TLE |

Additional collect options:

| Flag                              | Description                       |
|-----------------------------------|-----------------------------------|
| `-i / --integration-time-seconds` | Exposure time per frame           |
| `-c / --frame-count`              | Number of frames to capture       |
| `-b / --binning`                  | Camera binning (e.g. `2` for 2×2) |

## Device commands

Individual devices can also be controlled directly:

```bash
sensorkit device connect -e my-mount
sensorkit device disconnect -e my-mount
sensorkit device abort -e my-mount
```
