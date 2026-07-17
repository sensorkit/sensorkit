# SENPAI Module

The `senpai` module runs [SENPAI](https://github.com/zgazak/senpai) — an
astronomical image-processing engine — as a live, per-frame analyzer inside
SensorKit. Frames are consumed from a DataGraph as they land on disk, run
through SENPAI's collect pipeline, and the results are published back into
SensorKit as telemetry (see SENPAI for details):

- **Per-frame analysis** — plate solution (WCS), FWHM / seeing, zero point and
  limiting magnitude, point-source and streak detections, with annotated
  plots written per frame.
- **Sequence processing** (on by default) — frames sharing a collect are batched
  and run through SENPAI as one multi-frame collect: the WCS is anchored from
  sidereal frames and propagated to rate frames (the `rate_sidereal` collect
  shape), and streaks are confirmed across frames.
- **Telemetry** — each frame's results are published as a `SenpaiResult`
  keyword for downstream consumers (e.g. monitoring, dashboards, the `udl`
  module's EOObservation publisher).

## How It Works

1. On startup the module loads the SENPAI engine config (`senpai_config`) and
   starts consuming FITS frames from its DataGraph
2. With `process_sequence` on (the default), frames carrying a collect identity (`task_id` +
   `frame_count` in the DataGraph context) are accumulated per `task_id`; the
   batch runs once `frame_count` frames have arrived. A batch with nothing new
   for its exposure time plus a fixed margin is assumed incomplete (e.g. a
   dropped exposure) and is processed partially. Frames without a collect
   identity — and everything, when `process_sequence` is off — process one at
   a time
3. Each batch runs through SENPAI's collect pipeline — pre-processing, plate
   solution, detection, photometry — with plots written to `senpai_output_dir`
4. A `SenpaiResult` (track mode, solve status, FWHM, zero point, limiting
   magnitude, detections tagged with their `kind`, collect identity, and
   `from_sequence` for multi-frame batches) is published per frame

## Example Config

```yaml
entity: senpai
key: SenpaiConfig
value:
  senpai_config: /path/to/senpai.yaml                 # SENPAI engine config (astrometry, calibrations, ...)
  senpai_output_dir: /path/to/data/processed/senpai   # per-frame plots land here
  # process_sequence: false                           # disable multi-frame sequence processing

---

entity: senpai
key: DataGraph
value:
  nodes:
    source:
      op: watch_directory
      directory: /path/to/data/raw                    # the camera graph's write_file directory
      match: "*.fits"
      recursive: true
      output:
        - read_fits_file
    read_fits_file:
      op: read_file
      output:
        - read_context
    read_context:
      op: context_from_fits
      keyword_map:                # required for sequence batching / correlation;
        task_id: TASKID           # the right side must mirror whatever the camera
        frame_num: FRAMENUM       # graph's array_to_fits `header:` map wrote
        frame_count: NFRAMES      # (the collect context provides these keys to
        exptime: EXPTIME          # that map); exptime sizes the stalled-
      output:                     # sequence limit
        - sink
    sink:
      op: app_sink
```

Both blocks live on the same entity — the analyzer shares the service name
(`-n senpai` below). Point `watch_directory` at the camera graph's
`write_file` directory; every FITS file that appears there is analyzed,
regardless of which program collected it. The `keyword_map` is only needed for
sequence batching and for downstream consumers that correlate results to
tasking (e.g. EOObservations); without it, every frame processes individually
and `SenpaiResult.task_id` stays null.

## Usage

The SENPAI application (`astro-senpai`) is an optional dependency — install the `senpai`
extra, then run the service:

```sh
pip install "sensorkit[senpai]"
sensorkit service run senpai sensorkit.senpai.service
```

## TODO

- **Large results** — a frame with very many streak candidates can produce a
  `SenpaiResult` that exceeds the NATS max payload when published.
