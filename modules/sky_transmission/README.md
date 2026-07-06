# Sky Transmission Module

The `sky_transmission` module runs [allclear](https://github.com/zgazak/allclear)
— an all-sky star-matching cloud analyzer — against a site's all-sky camera
and publishes live sky-transmission telemetry (see allclear for details):

- **Cloud detection** — each all-sky frame is matched against the star field
  expected for the camera's instrument model; the matched fraction becomes
  `clear_fraction`, plus a per-region alt/az transmission map. This `clear_fraction`
  can be used as an agent constraint, i.e. a binary "too cloudy" constraint (but
  see `TODO`).
- **Telemetry and monitoring** — results are published as `SkyTransmission` /
  `SkyTransmissionMap` keywords (the former is what automation weather
  constraints consume, e.g. to hold tasking when `clear_fraction` drops), and
  an annotated live view is served as an MJPEG stream (consumed by
  SensorView's Streams tab).

## How It Works

1. Frames arrive in `watch_path` — either an external all-sky program drops
   them there (`watch_directory` mode), or the module triggers a capture on a
   SensorKit camera entity every `interval_seconds` and that camera's
   DataGraph writes into the same directory (`alpaca` mode)
2. Each frame runs through allclear: detected stars are matched against the
   instrument model's expected field, yielding `clear_fraction`
   (`n_matched / n_expected`), a clear/cloudy status against
   `clear_threshold`, and an alt/az transmission map
3. `SkyTransmission` and `SkyTransmissionMap` are published per frame; the
   annotated frame — status, timestamp, and the live pointing of each
   configured controller's mount — feeds the MJPEG `/stream` and a rolling
   `latest.mp4` in `output.directory`
4. Mounts are discovered at runtime from each listed controller's device map,
   so only controllers are named in the config

## Example Config

```yaml
entity: allsky
key: SkyTransmissionConfig
value:
  controllers: [controller1]           # overlay these controllers' mount pointings on the frames
  acquisition:
    mode: watch_directory              # watch_directory | alpaca
    watch_path: /path/to/allsky        # frames appear here (both modes)
    watch_pattern: "*.fits"
    # camera: AllSkyCam                # alpaca mode: SensorKit camera entity to trigger...
    # exposure_time_seconds: 10.0      #   ...with this exposure...
    # interval_seconds: 60.0           #   ...every this many seconds
  allclear:
    instrument_model_path: /path/to/instrument_model   # allclear model for this camera
    clear_threshold: 0.7               # clear/cloudy status cutoff on clear_fraction
  output:
    directory: /path/to/data/allsky    # annotated frames + rolling latest.mp4
    movie_lookback_hours: 24.0
    retention_hours: 48.0
  server:
    port: 8200                         # MJPEG stream at http://<host>:8200/stream
```

Each config entry is one all-sky camera with its own analyzer service. The
published `SkyTransmission` keyword is the automation hook — e.g. a
conditional constraint on `clear_fraction` to stand down under clouds.

## Usage

allclear (`allclear` on PyPI) is an optional dependency — install the
`sky-transmission` extra, then run the service:

```sh
pip install "sensorkit[sky-transmission]"
sensorkit service run allsky sensorkit.sky_transmission.service
```

## TODO

- **Alpaca free-run metadata** — `alpaca`-mode captures are issued without
  tasking context, so the camera's producer graph must supply frame metadata
  (`DATE-OBS` / `EXPTIME`) itself; to be verified on hardware.
- **Transparency optimization** — i.e. make use of alt/az-dependent transmission
  measurements, for SNR-optimization, for feedback to schedulers, etc.
