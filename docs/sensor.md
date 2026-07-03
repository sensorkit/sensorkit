# Sensor controller

The sensor controller (`sensorkit.std.sensor`) turns a collection of devices into one logical instrument. It owns a mount and camera — plus, optionally, a dome, focuser, rotator, filter wheel, and mirror cover — and knows how to bring them up, point them, collect frames, and shut them down in the right order.

You rarely command devices individually during operations; you send *tasks* to the controller, and it sequences the hardware.

## Configuration

Declare each sensor in the `sensors` section of the unified config:

```yaml
sensors:
  - id: MySensor                # entity/service name
    controller_name: MySensor

    devices:
      mount: MyMount            # required
      camera: MyCamera          # required
      dome: MyDome              # optional
      filter_wheel: MyWheel     # optional
      focuser: MyFocuser        # optional
      rotator: MyRotator        # optional
      mirror_cover: MyCover     # optional

    site_position:
      latitude_degrees: 34.0522   # positive north
      longitude_degrees: -118.2437  # positive east
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

Each value under `devices` is the entity name of a device declared in a [device service](devices.md). The `site_position` drives sunrise/sunset calculation, target altitude checks, orbit propagation, and FITS metadata.

### Policies

All policies are optional; timeouts show their defaults.

| Policy                                    | Default | Meaning                                          |
|-------------------------------------------|---------|--------------------------------------------------|
| `mount_init_timeout`                      | 30.0    | Seconds allowed for mount power-up/axis enable   |
| `mount_home_timeout`                      | 300.0   | Seconds allowed for the homing sequence          |
| `dome_open_close_timeout`                 | 120.0   | Seconds allowed for dome open/close              |
| `mirror_cover_open_close_timeout`         | 60.0    | Seconds allowed for the mirror cover             |
| `concurrent_dome_and_mount_init`          | false   | Open dome while the mount initializes            |
| `concurrent_dome_and_mount_deinit`        | false   | Close dome while the mount deinitializes         |
| `concurrent_mount_and_mirror_cover_init`  | false   | Open mirror cover during mount init              |
| `minimum_target_altitude_degrees`         | off     | Refuse to track targets below this altitude      |
| `sun_separation_degrees`                  | off     | Refuse targets within this angle of the Sun      |
| `moon_separation_degrees`                 | off     | Refuse targets within this angle of the Moon     |

The pointing-safety policies are enforced per frame during collection, so a satellite pass that drifts too close to the Sun is cut off mid-task, not just checked at the start.

## Tasks

The controller responds to tasks — from the agent during autonomous operation, or from you via the CLI:

| Task         | What happens                                                                    |
|--------------|----------------------------------------------------------------------------------|
| **Init**     | Connect devices, initialize and home the mount, open the mirror cover and dome  |
| **Standby**  | Bring the sensor to a warm, ready-to-observe state                              |
| **Collect**  | Slew/track a target, set filter and binning, capture frames, stop the mount     |
| **Recover**  | Reconnect all devices and stop any in-progress motion after a fault             |
| **Shutdown** | Close the mirror cover, deinitialize the mount, close the dome                  |

During a collect, the controller adapts the target to what the mount supports (e.g. propagating a TLE into an ephemeris or rate stream), and before each frame it snapshots live pointing and task state into the frame's *context* — which is how downstream FITS files get accurate per-frame metadata (see [Configuration → Data flow](configuration.md#data-flow)).

## Manual operation

```bash
# Bring the sensor up
sensorkit controller init -e MySensor

# Abort whatever is currently running
sensorkit controller abort -e MySensor

# Collect: 10 × 30 s on a fixed ICRS position
sensorkit controller collect -e MySensor \
    -t '{"target_type": "fixed", "frame": "icrf", "coords": {"ra": 83.82, "dec": -5.39}}' \
    -i 30.0 -c 10

# Shut down
sensorkit controller shutdown -e MySensor
```

`-f` on `init`/`shutdown` interrupts a running task first. If the agent is managing this controller, disable its control first (`sensorkit agent global-control off`) so you aren't fighting it.

### Targets

The `-t` argument takes a JSON object discriminated on `target_type`:

```bash
# Fixed alt/az position (degrees)
-t '{"target_type": "fixed", "frame": "altaz", "coords": {"az": 180.0, "alt": 60.0}}'

# Fixed ICRS position (RA/Dec in degrees)
-t '{"target_type": "fixed", "frame": "icrf", "coords": {"ra": 83.82, "dec": -5.39}}'

# Satellite from a TLE
-t '{"target_type": "tle", "tle": {"line0": "ISS (ZARYA)", "line1": "1 25544U ...", "line2": "2 25544 ..."}}'
```

The full target family — including state vectors and precomputed ephemerides — is described in [Observing programs](programs.md#targets).

### Collect options

| Flag                              | Description                          |
|-----------------------------------|--------------------------------------|
| `-i / --integration-time-seconds` | Exposure time per frame (default 1.0)|
| `-c / --frame-count`              | Number of frames (default 1)         |
| `-b / --binning`                  | Camera binning, e.g. `2` for 2×2     |

## Custom controllers

The standard sensor covers the common observatory shape. If your instrument doesn't fit it — different hardware roles, different sequencing — you can write your own controller with the same task interface, and the agent and CLI will drive it identically. See `declare_controller` and `task_handler` in the [API reference](api.md).
