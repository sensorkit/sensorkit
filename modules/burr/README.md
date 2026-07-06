# Burr Module

The `burr` module runs [Burr](https://github.com/zgazak/burr) — a sensor
characterization and calibration tasker and processor — as a SensorKit
scheduling program. The module advertises the observing windows as offers and
translates each observation request into `StandardCollectTask`s for a
controller to execute. Some supported features (see Burr for details):

- **Collections** — twilight flats, lunar sky brightness, photometric
  standards (e.g. for instrument zeropoint), sky coverage for search rate /
  survey speed, and calibration satellites for streak characterization and
  astrometric performance assessment, each individually
  enabled and tuned under `schedule`.
- **Processing** — routines for analyzing a night's worth of collections,
  producing, e.g. extinction curves, instrument sensitivity plots, etc.
  These routines run off-line from SensorKit (see `TODO`).

## How It Works

1. On enable, the module creates (or resumes) Burr's per-night run — computing
   the lighting schedule and then building the task
   sources enabled in `schedule`
2. When the agent requests a task, burr's scheduler ranks the sources suitable
   for the *current* lighting condition and asks the best one to generate an
   observation request; a source with nothing to produce defers to the next
3. Each request (a set of exposure times × a per-frame tracking-mode sequence)
   becomes one or more `StandardCollectTask`s per exposure: uniform
   sidereal/rate maps to a single task, a rate→sidereal sequence uses one task
   with a mid-task `sidereal_frames` switch, and sidereal→rate splits into two
   tasks
4. Task completion (or failure) feeds Burr's scheduling statistics and the
   source's completion hook, which appends an executed-command entry
   (observation time, per-frame tracking modes, target metadata) to
   `run_state.json` — the sidecar SENPAI uses to attribute frames to collects

## Example Config

```yaml
entity: burr
key: BurrConfig
value:
  controller: controller1              # named controller for the agent to task
  minimum_altitude_degrees: 20         # site-wide altitude floor
  runtime:
    output_dir: /path/to/data/processed/burr   # burr run dirs (run_state.json, plots) — not the FITS dir
  plotting:
    review: true                       # write review plots into the run dir
  schedule:                            # `windows` values: day | civil_twilight |
    twilight_flats:                    #   nautical_twilight | astronomical_twilight | night
      collect: true
      windows: [nautical_twilight]
      n_frames: 50
      minimum_exposure_seconds: 0.1
      min_counts: 40000                # exposure is stepped to keep the median
      max_counts: 50000                #   frame counts inside this band
      scheduling: {type: one_shot}     # `scheduling` types: one_shot | time_share | interval
    lunar_background:
      collect: true
      windows: [night]
      minimum_moon_altitude_degrees: 40
      minimum_moon_phase: 0.2
      moon_separation_degrees: [1.0, 2.0, 4.0, 8.0]
      n_exposures: 3
      scheduling: {type: one_shot}
    photometric_standards:
      collect: true
      windows: [night]
      minimum_exposure_seconds: 1
      maximum_exposure_seconds: 10
      n_exposures: 5
      scheduling: {type: time_share, target_percentage: 10}
    coverage:
      collect: true
      windows: [nautical_twilight, astronomical_twilight, night]
      points_per_map: 12
      n_exposures: 5
      scheduling: {type: time_share, target_percentage: 20}
    calsats:
      collect: true
      windows: [civil_twilight, nautical_twilight, astronomical_twilight, night]
      minimum_exposure_seconds: 1
      maximum_exposure_seconds: 5
      tle_provider: spacebook          # spacebook | spacetrack
      scheduling: {type: interval, minimum_interval_minutes: 3, fill_gaps: true}
    streaks:
      collect: true                    # append streak frames to the groups above
      streaks_per_collect: 2
      minimum_length_pixel: 10
      maximum_length_pixel: 100
```

The module itself has no DataGraph: FITS files
are written by the *camera* entity's DataGraph (`app_source →
array_to_fits → write_file`), which Burr feeds only through the task
context — `write_file` renders the module's filename template and
`array_to_fits` merges the stamped `BURR*` cards automatically. Everything
else downstream processing reads must come from the camera graph's
`array_to_fits` `header:` map: most importantly `TRKMODE` (from
`track_mode`; required by SENPAI), plus `DATE-OBS`, `EXPTIME`, `FILTER`, `RA` / `DEC` (degrees),
`RA_RATE` / `DEC_RATE` (deg/s), and `FRAMENUM` / `FRAMECNT`. The camera
graph's `write_file` `directory:` decides where the night's frames land —
it is the `--data-dir` handed to SENPAI below.

## Processing a Night (SENPAI)

Collection is automated; and while SENPAI (see the `senpai` module) can process frames -- e.g. for WCS, FWHM,
zero point, limiting magnitude, detections -- as they arrive, the sensor
characterization outputs are currently produced via an offline, manual CLI:

```sh
python -m senpai.cli.burr night /data/processed/burr/<Site>_<YYYYMMDD> \
    --data-dir /data/raw/burr/<controller> --seq-key BURRSEQ -o processed/
python -m senpai.cli.burr calibrate processed/<Site>_<YYYYMMDD>
```

`night` groups the night's frames into per-collect batches — `--seq-key
BURRSEQ` batches on the stamped sequence id; without it attribution falls back
to `run_state.json` command timestamps and filename heuristics — and runs the
full pipeline (calibration, astrometry, photometry, streak detection) on each,
writing a manifest plus per-batch results and review plots. `calibrate` then
aggregates every batch into the night-level products: `night_calibration.json`
and the combined plots — atmospheric extinction curves (per-filter zero point
vs airmass), zero point drift over the night, limiting magnitude histograms,
alt/az coverage, detector gain, sky surface brightness, and PSF profiles.
Related subcommands: `flats` (build a master flat), `plots` (re-render),
`build-dataset` (COCO streak training sets), and `nights-summary`.

## Usage

The Burr application (`astro-burr` on PyPI) is an optional dependency —
install the `burr` extra, then run the service:

```sh
pip install "sensorkit[burr]"
sensorkit service run burr sensorkit.burr.program
```

## TODO

- **Flat-field exposure feedback** — burr's flats manager can read back the
  latest flat from the FITS directory to close its exposure-adjustment loop,
  but the module does not currently wire the data directory through
  (`sk_data_root`), so flats step through the night blind.
- **Rate target frame** — `RateTarget`s are emitted in ICRF (what PWI4-backed
  controllers accept); the `cirf` alternative (node_platform-backed
  controllers) exists in burr's config but is not yet exposed through
  `BurrConfig`.
- **Live night-level processing** — `senpai.cli.burr live` is a stub, and
  analysis results are not yet written back into the run's
  `AnalysisResults` container; today the live tier is per-frame telemetry
  only, and the combined products require the post-night batch commands.
