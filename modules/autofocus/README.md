# Autofocus Module

The `autofocus` module keeps a sensor in focus. It runs two entities that
share one config block:

- **`V-Curve`** — a program that sweeps the focuser through a range of
  positions, captures a frame at each, and fits a V-curve to find best focus.
- **`Autofocus`** — an analyzer that watches every `SenpaiResult` for stellar 
  PSFs (FWHMs) and optionally adjusts the focus residual between frames.

Neither entity commands the focuser directly. Both write a single **residual**
into the focuser's KV, and the controller applies it at every capture (see
[Focus Model](#focus-model)), supporting:

- **V-curve calibration** — a sweep of `num_steps` frames around the current
  focus; a parabola fit in FWHM² gives best position, best FWHM, and slope. The
  fit folds into the residual and also *learns* the defocus-sign convention
  (below).
- **Passive correction** — each in-focus science frame's FWHM is compared to
  the calibrated best. Small drift is corrected in place; large excursions
  trigger a fresh V-curve; a deadband absorbs noise.
- **Persisted state** — calibration (slope, best FWHM, sign convention) and the
  enable flag survive restarts via the entity KV.

## How It Works

1. The **`V-Curve` program** queues a sweep — `num_steps` `StandardCollectTask`s,
   one per focuser position, centered on the current focus and clamped to the
   focuser's limits. It publishes offers, so the agent schedules the sweep like
   any program (give it a lower `priority` number than science programs so a
   queued sweep preempts). A sweep is queued on the `run_vcurve` request, on a
   `schedule`, or by the analyzer's recalibrate path.
2. Every sweep frame carries the sweep ID as the FITS card **`AFID`** and its
   requested SENPAI mode as **`AFMODE`** (see [Pipeline Mode](#pipeline-mode)).
3. The **`Autofocus` analyzer** subscribes to `SenpaiResult` and routes each
   frame by its header:
   - **`AFID` present** → a sweep frame: accumulate, and once the last step
     arrives, fit. The fit publishes a `VCurveResult`, folds `best − sweep
     center` into the residual, and learns the sign convention from the frames'
     measured defocus signs.
   - **Sidereal science frame** → evaluate the FWHM departure from best:
     within `min_arcsec` → skip (deadband); within `max_arcsec` → correct the
     residual in place; beyond `max_arcsec` → too far for a single-frame
     estimate, queue a V-curve instead. Currently only supported for sidereal
     frames, i.e. rate-tracked frames are ignored (see [TODO](#todo)).
4. A passive correction needs a **direction**. It comes from the star field's
   radial ellipticity (a quadrupole moment): intra-focal stars elongate
   radially, extra-focal tangentially. The measured per-frame sign is mapped to
   a direction of focuser travel by a convention the analyzer learns during each
   V-curve. If the sign can't be measured, or the convention isn't yet learned,
   the frame is skipped.

## Focus Model

At every capture the controller drives the focuser to:

```
base_position  +  active filter's focus_offset  +  FocusCorrection residual
```

The **residual** is what this module owns (posted to the focuser KV; zero until
something corrects). `base_position` and the per-filter `focus_offset` come
from the focuser and filter-wheel devices. This module reads none of that — it
only shifts the residual.

Manual vs. managed is decided by the collect task: a task that **states a
`focus_position`** is driven there verbatim (nothing else applied) — a manual
capture holding the operator's focus, or a V-curve sweep step. A task with **no
`focus_position`** is managed and gets the full expression above. The enable
flag gates only the *analyzer*; base + filter offset (+ the standing residual)
always drive.

## Pipeline Mode

Sweep frames are processed in `vcurve.pipeline_mode` (default `detect`),
carried to SENPAI on the `AFMODE` card. `detect` skips the plate solve, catalog
and photometry (~5 s/frame vs ~40 s on representative large frame tests). Science
frames carry no `AFMODE` and keep SENPAI's configured mode (normally `full`).

`detect` never solves, so it reports no plate scale — set `pixel_scale_arcsec`
(the native, 1×1 scale; the binning math is applied for you) so `VCurveResult`
can report arcsec, or use `detect_solve`, which adds a few more seconds per frame. **Known trade-off:** calibrating in a reduced mode while science runs
`full` mixes two FWHM scales (catalog-refined vs. detection-stage), leaving a
standing focus offset. Accepted for fast sweeps; set `pipeline_mode: full` to
avoid it at the cost of time.

## Requests

The `Autofocus` entity registers two entity-level Requests (the same mechanism
the agent uses). Invoke them over HTTP with
`POST /entity/Autofocus/request/{name}`, or from code with
`kit.entity("Autofocus").request(name, payload)`.

- **`run_vcurve`** `{ra?, dec?}` — queue a sweep; omit coordinates to
  auto-select a bright target (preferring the galactic plane). Returns as soon
  as the sweep is handed off; watch the log and `VCurveResult` for the outcome.
- **`set_enabled`** `{enabled}` — turn the analyzer's passive corrections on or
  off. Read the current state from the `AutofocusState` keyword
  (`GET /data/snapshot/Autofocus`).

## Example Config

```yaml
autofocus:
  - entity: Autofocus
    controller: MyController
    senpai_entity: SENPAI
    focuser:
      entity: MyFocuser
      min_position: 20000            # focuser hard limits [steps]; the sweep is clamped to these
      max_position: 30000
    vcurve:
      num_steps: 9
      step_size: 35                  # spacing [steps]; blur [binned pixels] = step_size · µm_per_step / (f_number × pixel_pitch_µm × binning)
      exposure_time: 2.0
      binning: 2
      pipeline_mode: detect          # detect | detect_solve | full  (see Pipeline Mode)
      # schedule: sunset+30m         # optional daily time — sunset/sunrise ±delta, or a clock time ("2am utc"); on-demand otherwise
    correction:
      min_arcsec: 0.5                # deadband [arcsec FWHM from best]
      max_arcsec: 4.0                # above this, recalibrate instead of correcting
      defocus_sign_threshold: 0.02   # min |mean radial ellipticity| to call a direction
    pixel_scale_arcsec: 0.887        # native (1x1) arcsec/px; optional, only for arcsec reporting when pipeline_mode = detect
    catalog_path: /path/to/sstrc7    # SSTR7 catalog dir; REQUIRED for target auto-selection
    min_altitude: 15
    min_magnitude: 2.0               # reject targets brighter than this (saturation)
```

`priority` for the `V-Curve` program is set in the agent's `tasking:` block,
not here — give it a lower number than science so a queued sweep preempts.

## Usage

Run the service (registers both entities):

```sh
sensorkit service run autofocus sensorkit.autofocus.program
```

Requires a `senpai` entity producing `SenpaiResult`, a focuser publishing
`FocusPosition` (with a base position), and — for target auto-selection — an
SSTRC7 catalog.

## TODO

- **Rate-tracked** — only sidereal frames are analyzed for passive correction
  purposes. To support rate-tracked frames, testing against real-world streaked
  FWHM measurements is required.
