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

The YAML below uses low-level `entity`/`key`/`value` records.
Save all documents in a file and load them with `sensorkit kv load FILE` before starting the service.
For deployment-wide configuration, see the [unified configuration guide](../../docs/configuration.md).

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
        frame_count: NFRAMES      # frame count from the camera header
        exptime: EXPTIME          # exposure time sets the stalled-sequence limit
      output:
        - sink
    sink:
      op: app_sink
```

Both blocks live on the same entity — the analyzer shares the service name
(`senpai` below). Point `watch_directory` at the camera graph's
`write_file` directory; every FITS file that appears there is analyzed,
regardless of which program collected it. The `keyword_map` is only needed for
sequence batching and for downstream consumers that correlate results to
tasking (e.g. EOObservations); without it, every frame processes individually
and `SenpaiResult.task_id` stays null.
For the example above, the camera's `array_to_fits.header` mapping can use
`TASKID: =str(Task.task_id)`, `FRAMENUM: =Collect.frame_number`,
`NFRAMES: =Collect.params.frame_count`, and `EXPTIME: =Exposure.exposure_time`.

## Focus Sweeps

The analyzer satisfies the standard `focus_provider` trait.
It publishes `FocusAnalysis` for each sweep frame and `FocusSweepAnalysis` for each completed or failed sweep.
A successful fit estimates best focus or its direction beyond the sampled range.
The controller handles focuser positioning.

A typed `FocusSweepContext` or a `FOCSWEEP` FITS card identifies a focus sweep frame.
Typed sweep context takes precedence over FITS metadata.
Frames marked by `FOCSWEEP` with missing or invalid metadata are rejected before science analysis.
Each sweep frame needs the following capture metadata.

| Context keyword | FITS cards | Required | Use |
| --- | --- | --- | --- |
| `FocusSweep` | `FOCSWEEP`, `FOCSAMP`, `FOCCOUNT`, `FOCCMD` | yes | Sweep identity, sample index and count, commanded position |
| `Instrument` | `INSTRUME` | yes | Controller instrument name |
| `Task` or `controller_name` | `SKCTRL` | yes | Controller identity |
| `Exposure` | `DATE-OBS`, `EXPTIME`, and optional exposure cards | yes | Observed exposure metadata and the stall limit |
| `FocusPosition` | `FOCPOS` | no | Reported focuser position |

A camera graph writes these cards with a `fits_header` operation configured with `include: all`.
The analyzer reads them from the first header carrying `FOCSWEEP`, so tile-compressed files also work.

Sweep frames skip calibration filtering and sequence batching.
Each frame is measured alone with source detection only, without a plate solution.
The measurement is the engine's median point-source FWHM in image pixels.
In detection-only mode, the engine supplies one aggregate FWHM measurement.
The `measurement_count` field records this aggregate count.
A `FocusAnalysis` is published for each frame, including frames with no usable measurement.

A sweep is identified by controller, instrument, and sweep ID.
It closes when every sample index has been measured, in any arrival order.
A repeated delivery of a measured sample is ignored unless its sample count or commanded position conflicts.
One `FocusSweepAnalysis` is then published with the samples ordered by index.
Later frames of a closed sweep are ignored.

A quadratic is fitted to the median FWHM at each commanded position.
The fit requires five distinct positions with usable measurements.

| Fit | Result |
| --- | --- |
| Minimum inside the sampled positions | `disposition: equals` at the estimated best focus |
| Widths only fall or only rise across the sweep | `greater_than` the highest position or `less_than` the lowest |
| Fitted width change no greater than twice the RMS residual | Failure |
| Fitted width change within numerical rounding tolerance | Failure |
| Width peak inside the sampled positions | Failure |
| Nonpositive fitted minimum inside the sampled positions | Failure |
| Fewer than five usable positions | Failure |

A sweep also fails in these cases:

- A sample changes the expected sample count.
- A repeated sample index changes its commanded position.
- A frame lacks exposure metadata.
- No new sample arrives within the exposure duration plus 60 seconds.

A failure retains the measurements received so far.

The analyzer rejects the engine's default summary of exactly 4 pixels with a count of one.
This also discards any genuine measurement with that exact pair.

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
