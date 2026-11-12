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

The YAML below uses low-level `entity`/`key`/`value` records.
Save all documents in a file and load them with `sensorkit kv load FILE` before starting the service.
For deployment-wide configuration, see the [unified configuration guide](../../docs/configuration.md).

```yaml
entity: burr
key: BurrConfig
value:
  controller: controller1              # named controller for the agent to task
  minimum_altitude_degrees: 20         # site-wide altitude floor
  raw_data_root: /path/to/data/raw/burr   # flat FITS directory for exposure adjustment
  collect:
    gain: 100
    binning: 1
    filter_name: r
    instruments:
      cam-east: {}
      cam-west:
        filter_name: g
        gain: 200
  runtime:
    output_dir: /path/to/data/processed/burr   # burr run dirs (run_state.json, plots) — not the FITS dir
  plotting:
    debug: false
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

Set shared `filter_name`, `readout_mode`, `gain`, and `binning` values under `collect`.
Use `collect.instruments` to select instruments and override their camera settings.
Each entry inherits omitted settings from `collect`.
An explicit null clears an inherited value and leaves that setting unspecified.
In the example, cam-east uses filter r and gain 100, while cam-west uses filter g and gain 200.

Every enabled Burr source uses these instrument selections.
Each task includes one sequence per configured instrument.
Burr's exposure duration, target, tracking pattern, and frame count apply to all instruments.

For one instrument, the `collect.instrument` shorthand uses the shared settings directly.
Omit both selectors for automatic instrument selection.
The selectors cannot be combined, and `collect.instruments` must contain at least one entry.

Instrument names are case-sensitive, and whitespace is preserved.
A named selection requires the program's assigned controller to match `controller`.
Before each task, the program checks the assignment and instrument metadata.
An invalid selection ends tasking with a program fault before another task is dispatched.

The camera entity's data graph writes FITS files through `app_source`, `array_to_fits`, and `write_file`.
Burr supplies sequence identity in the task's `BurrContext` keyword under the key `Burr`.
Configure the camera graph's `array_to_fits.header` mapping to write `BURRSEQ: =Burr.seq`.
Other useful mappings include `TRKMODE: =Collect.track_mode`, `FRAMENUM: =Collect.frame_number`, and `FRAMECNT: =Collect.params.frame_count`.
Include exposure timing, filter, pointing, and angular rates when required by downstream analysis.
The `write_file.directory` value is the input directory passed to SENPAI's `--data-dir` option.
Every filename includes the SensorKit task ID, the assigned instrument name encoded as hexadecimal, and the frame number.
The encoding distinguishes names that differ only by case and prevents path separators from creating subdirectories.
Capture context must include `Task`, `Instrument`, and `Collect` to resolve these filenames.
`SensorController` supplies the assigned instrument's context automatically.
Custom controllers merge `controller.instrument_context(name)` into each capture's context after selecting an instrument.

Set `raw_data_root` to the FITS directory to enable flat exposure adjustment from measured counts.
Burr 0.3.4 requires `IMAGETYP=FLAT` and nonempty `UNITID` and `FILTER` values in each flat's image header.
Use `UNITID: =Instrument.name` to identify the selected instrument.
Burr tracks exposure adjustment by that value.
For a configured filter, `FILTER: =Collect.params.filter_name` writes the commanded name.
Use device telemetry instead if the header needs the measured filter position.

## Processing a Night (SENPAI)

Collection is automated; and while SENPAI (see the `senpai` module) can process frames -- e.g. for WCS, FWHM,
zero point, limiting magnitude, detections -- as they arrive, the sensor
characterization outputs are currently produced via an offline, manual CLI:

```sh
python -m senpai.cli.burr night /data/processed/burr/<Site>_<YYYYMMDD> \
    --data-dir /data/raw/burr/<controller> --seq-key BURRSEQ -o processed/
python -m senpai.cli.burr calibrate processed/<Site>_<YYYYMMDD>
```

Use `--seq-key BURRSEQ` to group frames from the same requested exposure.
SENPAI's filename parser includes the task and instrument suffixes in its target label.
Without sequence headers, those suffixes also affect its fallback grouping.

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

- **Rate target frame** — `RateTarget`s are emitted in ICRF (what PWI4-backed
  controllers accept); the `cirf` alternative (node_platform-backed
  controllers) exists in burr's config but is not yet exposed through
  `BurrConfig`.
- **Live night-level processing** — `senpai.cli.burr live` is a stub, and
  analysis results are not yet written back into the run's
  `AnalysisResults` container; today the live tier is per-frame telemetry
  only, and the combined products require the post-night batch commands.
