# SENPAI Module

The `senpai` module runs [SENPAI](https://github.com/zgazak/senpai) — an
astronomical image-processing engine — as a live, per-frame analyzer inside
SensorKit. Frames are consumed from a DataGraph as they land on disk, run
through SENPAI's collect pipeline, and the results are published back into
SensorKit as telemetry (see SENPAI for details):

- **Per-frame analysis** — plate solution (WCS), FWHM / seeing, zero point and
  limiting magnitude, point-source and streak detections, with annotated
  plots written per frame.
- **Telemetry** — each frame's results are published as a `SenpaiResult`
  keyword for downstream consumers (e.g. monitoring, dashboards).

## How It Works

1. On startup the module loads the SENPAI engine config (`senpai_config`) and
   starts consuming FITS frames from its DataGraph
2. Each frame runs through SENPAI's collect pipeline — pre-processing, plate
   solution, detection, photometry — with plots written to `senpai_output_dir`
3. A `SenpaiResult` (track mode, solve status, FWHM, zero point, limiting
   magnitude, detections) is published per frame

## Example Config

```yaml
entity: senpai
key: SenpaiConfig
value:
  senpai_config: /path/to/senpai.yaml                 # SENPAI engine config (astrometry, calibrations, ...)
  senpai_output_dir: /path/to/data/processed/senpai   # per-frame plots land here

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
        - sink
    sink:
      op: app_sink
```

Both blocks live on the same entity — the analyzer shares the service name
(`-n senpai` below). Point `watch_directory` at the camera graph's
`write_file` directory; every FITS file that appears there is analyzed,
regardless of which program collected it.

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
