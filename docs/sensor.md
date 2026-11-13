# Sensor controller

The sensor controller coordinates a device topology containing one or more instruments.
A typical sensor has a shared mount and dome, with a camera, focuser, and filter wheel on each instrument branch.
Lifecycle workflows initialize and shut down the devices; collect workflows point the mount and capture frames.

You rarely command devices individually during operations; you send *tasks* to the controller, and it sequences the hardware.

Scripts can use `Sensor.connect(definition, sensorkit)` to bind device capabilities,
then plan and execute lifecycle or collect workflows.
The controller service uses the same interface and generates lifecycle tables
and deadline rules from site policies.

## Configuration

Declare each sensor in the `sensors` section of the unified config:

```yaml
sensors:
  - id: MySensor
    model:
      components:
        - device: MyMount
        - device: MyDome
        - device: MyCover
        - unit: science
          components:
            - device: MyWheel
            - device: MyFocuser
            - device: MyCamera
              instrument: true
    policies: {}

config:
  MySensor:
    SitePosition:
      latitude_degrees: 34.0522
      longitude_degrees: -118.2437
      altitude_km: 0.086
```

Each `device` names an entity declared in a [device service](devices.md).
Root devices belong to every instrument's chain.
Units group devices belonging to a branch; mark each collection target with
`instrument: true`.
For several cameras, declare separate units with their cameras and any local
filter wheels or focusers.

The device key of each collection target is its instrument name.
The controller publishes each name and its chain's device IDs in `EntityInfo.details.instruments`
(see [Instruments](api.md#instruments)).
Names stay the same across restarts and layout changes that preserve device keys.
Changing a target's device key changes its instrument name.

Configure policy overrides within the sensor's `policies` block:

```yaml
    policies:
      mount_init_timeout: 30.0
      mount_home_timeout: 300.0
      dome_open_close_timeout: 120.0
      mirror_cover_open_close_timeout: 60.0
      concurrent_dome_and_mount_init: false
      concurrent_dome_and_mount_deinit: false
      concurrent_mount_and_mirror_cover_init: false
```

`SitePosition` supplies the location included in each frame's metadata.

### Policies

All policies are optional; timeouts show their defaults.

| Policy                                    | Default | Meaning                                          |
|-------------------------------------------|---------|--------------------------------------------------|
| `mount_init_timeout`                      | 30.0    | Seconds allowed for mount power-up/axis enable   |
| `mount_home_timeout`                      | 300.0   | Seconds allowed for the homing sequence          |
| `mount_deinit_timeout`                    | 60.0    | Seconds allowed for mount deinitialization       |
| `connect_timeout`                         | 60.0    | Seconds allowed to connect a device              |
| `stop_timeout`                            | 30.0    | Seconds allowed to stop a device                 |
| `follow_target_timeout`                   | 300.0   | Seconds allowed to slew to and track a target    |
| `filter_change_timeout`                   | 30.0    | Seconds allowed to change the filter             |
| `camera_configure_timeout`                | 30.0    | Seconds allowed to configure a camera sensor     |
| `focus_change_timeout`                    | 30.0    | Seconds allowed to move the focuser              |
| `default_timeout`                         | 300.0   | Seconds allowed for any other command            |
| `dome_open_close_timeout`                 | 120.0   | Seconds allowed for dome open/close              |
| `mirror_cover_open_close_timeout`         | 60.0    | Seconds allowed for the mirror cover             |
| `concurrent_dome_and_mount_init`          | false   | Open dome while the mount initializes            |
| `concurrent_dome_and_mount_deinit`        | false   | Close dome while the mount deinitializes         |
| `concurrent_mount_and_mirror_cover_init`  | false   | Open mirror cover during mount init              |

Unknown policy fields are rejected.
A recovery attempt is aborted after 120 seconds.
It connects every device before stopping any,
so keep `connect_timeout` and `stop_timeout` together well under that limit.
Altitude and Sun/Moon admission limits belong in tasking and automation;
the sensor's policies control sequencing and command deadlines.

## Tasks

The controller responds to tasks — from the agent during autonomous operation, or from you via the CLI:

| Task         | What happens                                                                    |
|--------------|----------------------------------------------------------------------------------|
| **Init**     | Initialize the dome and mount, open the dome and mirror cover                  |
| **Standby**  | Run the same device sequence as Init and establish STANDBY                              |
| **Collect**  | Slew/track a target, set filter and binning, capture frames, stop the mount     |
| **AutoFocus** | Sweep an instrument's focuser, then move it to the analyzed best focus (beta) |
| **Recover**  | Reconnect all devices and stop any in-progress motion after a fault             |
| **Shutdown** | Close the mirror cover, deinitialize the mount, close the dome                  |

During a collect, the controller commands the target and camera settings.
Set a sequence's `instrument` field to select a collection target.
Planning rejects unknown names, ordinary device IDs, and targets incompatible with the requested settings before sending commands.
Before each frame, it samples subscribed device keywords from that instrument's
chain into a fresh context, together with task context, site position and numbered
`CollectInfo` metadata under the `Collect` key.
Requested parameters remain separate from reported device values.
Downstream FITS files use this context for per-frame metadata
(see [Configuration → Data flow](configuration.md#data-flow)).

An autofocus sweep takes one light frame at each of `count` positions on the named instrument.
Positions are spaced `step` apart and visited from lowest to highest.
The sweep is centered on the focuser's reported starting position unless the task gives a `center`.
The instrument's chain must contain exactly one focuser with a reported starting position.
Pointing and tracking are preserved, and requested camera and filter settings remain applied.
Each frame carries `FocusSweep` context with the task ID as its sweep ID.
The controller's `FocusAnalysisSource` configuration names an online entity advertising the `focus_provider` trait.
After the last frame, the controller waits up to `default_timeout` for matching `FocusSweepAnalysis`, bounded by task expiry.

The task succeeds after moving to an estimated best focus within the sampled range.
If best focus lies beyond the sampled range, the controller moves to the nearer sampled boundary and fails the task.
A later sweep can start from that position.
A failed analysis, analysis timeout, or failed move triggers an attempt to restore starting focus and fails the task.
Any received analysis is retained in the task context.
A failed restoration is included in the task's failure message.
Cancellation, expiry, controller disable, and service shutdown cancel active work without a return move.
For file output, configure a `FileNameTemplate` for the `autofocus` task type and make `Collect`-dependent header cards optional.

## Manual operation

```bash
# Bring the sensor up
sensorkit controller init -e MySensor

# Abort whatever is currently running
sensorkit controller abort -e MySensor

# Capture 10 frames of 30 seconds each at a fixed ICRS position
sensorkit controller collect -e MySensor \
    -t '{"target_type": "fixed", "frame": "icrf", "coords": {"ra": 83.82, "dec": -5.39}}' \
    -i 30.0 -c 10

# Sweep a focuser through seven positions 200 steps apart around its current position,
# one 2-second frame at each
sensorkit controller autofocus -e MySensor -n MyCamera -s 200 -c 7 -i 2.0

# Shut down
sensorkit controller shutdown -e MySensor
```

`-f` on `init`/`shutdown` interrupts a running task first.
`-x` on `autofocus` adds task context as a JSON object, for values the agent would otherwise supply, such as those a file output path needs.

!!! warning "Stand the agent down first"

    If the agent is managing this controller, disable its control before driving the sensor manually (`sensorkit agent global-control off`, or per-controller with `sensorkit agent control MySensor off`) — otherwise the two of you will fight over the hardware.

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
