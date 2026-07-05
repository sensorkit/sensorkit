# sensorkit.sdasim

A **native SensorKit camera** backed by [sdasim](https://github.com/zgazak/sdasim),
Zach Gazak's speed-optimized differentiable satellite-scene simulator.

The device implements the `StandardCamera` archetype (`CameraCapture`): on each
capture it renders a frame with sdasim and delivers the pixels through the
**DataGraph**, exactly like the `nina`, `thesky`, and `alpaca` camera modules.
That makes it a drop-in simulated camera that senpai (streak detection),
autofocus, `array_to_fits` → `write_file`, etc. consume with no special casing.

It optionally subscribes to a mount and rotator entity for **live pointing**:
the renderer follows the mount's RA/Dec and axis rates, choosing sidereal
tracking (sharp stars, streaked targets) or rate-tracking (streaked stars,
stationary target) automatically from the rates.

> This replaces the older GitLab `sdasim` tool, which stood up a standalone
> ASCOM Alpaca HTTP camera *server*. The latest SensorKit has no Alpaca server
> (its `alpaca` module is a *client*), so the simulator is now a first-class
> SensorKit device instead.

## Layout

```
modules/sdasim/src/sensorkit/sdasim/
  service.py   # declare_config_section("sdasim", ...) + service entrypoint
  device.py    # SdasimDevice base (require_connected, status-loop helpers)
  camera.py    # SdasimCamera: StandardCamera device, capture -> DataGraph
  engine.py    # SdasimEngine (render) + SensorKitBridge (mount/rotator telemetry)
```

## Install

`sdasim` (and its `torch` dependency) are an **optional extra** — they are not
pulled into a default SensorKit install:

```sh
uv sync --extra sdasim
```

> The extra is currently sourced from a local editable path (`/opt/sdasim`) via
> `[tool.uv.sources]`, because the orbital API the module uses isn't yet on a
> public branch. Revert to a git pin once it's pushed.

## Configure

```yaml
sdasim:
  - id: Sdasim
    devices:
      SimulatedCamera:
        device_type: camera
        sdasim_config: /path/to/scene.yaml  # your sdasim SceneConfig YAML
        mount_entity: SimulatedMount       # optional: live RA/Dec + rates
        rotator_entity: SimulatedRotator   # optional: rotator position
        device: cpu                        # torch device (cpu | cuda | mps | auto)
        temperature: -10.0                 # simulated cooler setpoint
        binning: 1
```

Satellite rendering, the star catalog, and the observer `site` are configured in
the **sdasim scene YAML** (`sdasim_config`), not here — enable `catalog` there to
have sdasim discover and streak field satellites for the live pointing. The
module supplies pointing, inertial mount rate, and `obs_time` per exposure, and
rebuilds the Scene only when the pointing drifts or the exposure changes.

Add `sensorkit.sdasim.service` to your `SENSORKIT_IMPORTS` / `sensorkit.imports`
so the config section registers, then run via `sensorkit go` (or
`sensorkit service run Sdasim`).

## Notes / limitations

- **Full-frame only.** The SensorKit camera archetype has no ROI; binning is
  supported via `ConfigureCameraSensor` (symmetric NxN only).
- **CCD vs CMOS binning physics** is preserved from the original: CCD defers
  read noise until charge is summed on-chip; CMOS bins post read-noise.
- **Rotator** position is subscribed and reported but **not yet applied** —
  sdasim does not model field rotation during rendering.
- The `sdasim.Scene` (and its satellite catalog) is built once and reused across
  exposures; it is rebuilt only when the pointing drifts past
  `rebuild_threshold_deg` or the exposure changes (the star field and exposure
  are fixed at construction).
- **Mount rate convention:** the camera passes the mount's `AxisRates` straight
  to sdasim as the inertial rate, which requires an **ICRF** AxisRates producer
  (sidereal → 0). Only the `alpaca` mount adapter is ICRF so far — nina / pwi4 /
  thesky / node_platform still publish sidereal-inclusive RA rates and need the
  same fix before they'll drive sdasim correctly.
