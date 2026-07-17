# sdasim Module

The `sdasim` module runs [sdasim](https://github.com/zgazak/sdasim) — a
satellite scene simulator — as a native SensorKit camera device. It renders
synthetic frames from the live mount state and delivers them through the
camera DataGraph exactly like real camera hardware, so a full deployment
(controllers, collects, downstream processing) runs end-to-end with no
hardware (see sdasim for details):

- **Scene rendering** — a star field plus satellites (optionally the full
  Space-Track catalog: every sunlit in-FOV satellite streaks at its apparent
  rate) over a configurable sensor model (FOV, zeropoint, PSF, noise), driven
  per exposure by the live mount pointing and rate — sidereal frames get
  sharp stars and streaking satellites, rate-tracked frames the reverse.
- **Native camera device** — a standard SensorKit camera device: connect, binning,
  cooler, capture, and status telemetry, with frames delivered through the
  camera DataGraph like any real camera module.

## How It Works

1. Each configured camera runs as its own service process (rendering is
   CPU-bound; run multiple cameras as separate services)
2. The camera subscribes to the configured mount (and rotator) entities for
   live pointing and inertial (ICRF) axis rates;
   sdasim renders apparent motion as object rate minus mount rate, so
   sidereal and rate tracking need no special casing
3. On capture, the frame is rendered off the event loop, held to the
   commanded integration time, then pushed with its context into the device's
   DataGraph — `array_to_fits` / `write_file` produce FITS as for real
   hardware
4. Every frame is rendered for its own time and pointing: satellites are
   propagated to the frame's timestamp, and the stars are those the catalog
   holds at the commanded pointing

## Example Config

```yaml
entity: sdasimCamera
key: SdasimCameraConfig
value:
  sdasim_config: /path/to/scene.yaml   # sdasim SceneConfig: sensor model, star field, satellites, site
  mount_entity: OmniSimTelescope       # optional: live pointing + rates drive the render
  rotator_entity: OmniSimRotator       # optional: subscribed, not yet rendered (see TODO)
  device: cpu                          # torch device: cpu | cuda | mps | auto
  temperature: -10.0                   # simulated cooler setpoint (°C)
```

Satellite rendering, the star field, and the observer site live in the sdasim
scene YAML (`sdasim_config`), not here (but see `TODO`). Wire the camera into a sensor like
any other camera device (`devices: camera: sdasimCamera`) and give it the
standard camera producer DataGraph (`app_source → array_to_fits →
write_file`); its `write_file` directory is where the synthetic FITS land.

## Usage

The simulator (`sdasim` on PyPI) and its `torch` dependency are an optional
extra — install it, then run the service:

```sh
pip install "sensorkit[sdasim]"
sensorkit service run sdasimCamera sensorkit.sdasim.service
```

## TODO

- **Config** — integrate sdasim scene config into `sdasim` module. 
- **Rotator** — the rotator position is subscribed and reported, but field
  rotation is not yet applied to the render.
- **Filters** — no filter wheel is simulated, and no filter context/header
  key is set on captured frames.
- **Calibration frames** — every capture is a light frame; dark/bias frame
  types are not simulated beyond the sensor noise model.
- **Full-frame only** — no ROI support; binning is symmetric N×N.
