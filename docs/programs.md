# Observing programs

An observing program is the piece of SensorKit you're most likely to write yourself: the Python code that decides **what to observe next**. Everything else — whether it's safe to open, which program gets the telescope, how the mount is driven, where the FITS files go — is handled by the agent, the controller, and the data pipeline.

A program has exactly two responsibilities:

1. **Advertise when it has work**, by publishing *offer windows* (time intervals).
2. **Produce the next task** when the agent activates it, via a *task factory*.

## Example program

```python
from datetime import UTC, datetime, timedelta

import sensorkit.api as sk
from sensorkit.astro.common import TLE
from sensorkit.astro.target import TLETarget
from sensorkit.std.collect import CameraParameterSet, StandardCollectTask

# Replace the abbreviated element lines with a current, complete TLE.
ISS = TLE(
    line0="ISS (ZARYA)",
    line1="1 25544U 98067A   ...",
    line2="2 25544  51.6416 ...",
)


@sk.declare_program
class ISSChaser:

    @sk.on_attach
    async def startup(self):
        # Offer the whole night; the agent intersects this with modes,
        # weather, and other programs' priorities.
        now = datetime.now(UTC)
        sk.program().add_offer(now, now + timedelta(hours=12))
        await sk.program().publish_offers()

    @sk.task_factory
    async def next_task(self):
        return StandardCollectTask(
            target=TLETarget(tle=ISS),
            camera_params=CameraParameterSet(
                integration_time_seconds=2.0,
                frame_count=10,
            ),
        )


@sk.service_entrypoint(version="1.0")
async def main(service: sk.Service):
    service.include(ISSChaser)
    await service.run()
```

Register it in your config and connect it to a controller:

```yaml
services:
  - id: ISSChaser
    python_module: programs/iss_chaser.py

automation:
  controllers:
    MySensor:
      tasking:
        - program: ISSChaser
          priority: 5
```

Run it standalone with `sensorkit service run ISSChaser`, or let `sensorkit go` launch it with everything else. The program runs as its own service — it can live on a different machine from the telescope, restart independently, and crash without taking the night down.

## Offer windows

Offers are how a program negotiates for telescope time without owning the schedule. Publish them whenever your plans change — at startup, when a new pass is computed, when your queue drains:

```python
sk.program().add_offer(start, end)         # datetimes (UTC)
sk.program().remove_offer(start, end)
sk.program().clear_offers()
await sk.program().publish_offers()        # make the current set visible
```

The agent merges offers from all programs with each controller's modes and constraints. If your program is the reason the sensor should be up at all, pair it with a `tasking_available` mode criterion — the dome opens when you have work and closes when you don't (see [The agent](agent.md#tasking_available)).

## The task factory

When your program is active, the tasking loop repeatedly calls the factory for the next task. The factory can:

- **return a task** — dispatched immediately with default execution parameters;
- **return `None`** — no work right now (the loop backs off and asks again);
- **return `task.submit(expiry_time=..., context=...)`** — attach execution parameters, such as a hard deadline after which a queued task is discarded (important for satellite passes: a task that missed its window shouldn't run late);
- **be an async generator** — `yield` the task, then optionally resume to observe its execution:

```python
@sk.task_factory
async def next_task(self):
    task = self.plan_next_pass()
    if task is None:
        return  # nothing visible right now

    execution = yield task.submit(expiry_time=task.pass_end)
    result = await execution        # wait for the outcome
    self.record(result)
```

Lifecycle hooks round out the picture: `@sk.on_enable` / `@sk.on_disable` fire when the agent activates or deactivates your program (with the target controller), and `@sk.on_attach` / `@sk.on_detach` on service start/stop.

## `StandardCollectTask`

The standard collect task drives the whole observation — slew, track, filter, binning, N frames, stop:

```python
StandardCollectTask(
    target=...,                     # any Target (below)
    camera_params=CameraParameterSet(
        integration_time_seconds=5.0,
        frame_count=3,
        binning_x=2, binning_y=2,   # optional
        gain=100,                   # optional
        filter_name="r",            # optional
        frame_type=FrameType.LIGHT, # light | bias | dark | flat
    ),
    target_id="25544",              # optional label for metadata; inferred for TLEs
    sidereal_frames=[0],            # frames to expose in sidereal mode (e.g. astrometric anchor)
)
```

`sidereal_frames` is a small but practical SDA feature: within one rate-tracked satellite collect, chosen frame numbers switch the mount to sidereal tracking — giving you fixed-star frames for astrometric calibration in the same sequence.

`camera_params` also takes a **list** of parameter sets, each describing a sequence of frames:

```python
StandardCollectTask(
    target=...,
    camera_params=[
        CameraParameterSet(integration_time_seconds=5.0, frame_count=3, filter_name="r"),
        CameraParameterSet(integration_time_seconds=30.0, frame_count=1, filter_name="g"),
    ],
)
```

The sensor matches each frame sequence to an instrument.
One camera takes the sequences in turn; several cameras can run them concurrently when their shared devices allow it.
Shared filter wheels and focusers add dependencies between captures and configuration changes.
The planner splits incompatible requests into separate configuration epochs while preserving request order.
Requests that cannot run even on their own are rejected.
`sidereal_frames` counts from zero within each sequence.
Instruments capturing concurrently switch tracking mode together.

Choose an instrument for each sequence with `CameraParameterSet.instrument`.

```python
StandardCollectTask(
    target=...,
    camera_params=[
        CameraParameterSet(
            instrument="cam-east",
            integration_time_seconds=5.0,
            frame_count=3,
            filter_name="r",
        ),
        CameraParameterSet(
            instrument="cam-west",
            integration_time_seconds=30.0,
            frame_count=1,
            filter_name="r",
        ),
        CameraParameterSet(integration_time_seconds=10.0, frame_count=2),
    ],
)
```

The selection applies to every frame, including frames that use sidereal tracking.
Omitting `instrument` lets the controller choose.
The controller rejects unknown names and instruments that cannot satisfy the requested settings.
Read `EntityInfo.details.instruments` for the available names (see [Instruments](api.md#instruments)).

You can also define your own task types (subclass `sk.Task`) and handle them in a custom controller — the scheduling machinery is the same.

## Targets

All target types live in `sensorkit.astro.target` and share a discriminated model, so they serialize cleanly to JSON/YAML (the same forms work with `sensorkit controller collect -t`):

| Type                | What it describes                                | Construction |
|---------------------|--------------------------------------------------|--------------|
| `AltAzTarget`       | Fixed horizon position                           | `AltAzTarget(coords=Horizontal(az, alt))` |
| `ICRSTarget`        | Fixed celestial position (sidereal track)        | `ICRSTarget(coords=Equatorial(ra, dec))` — degrees |
| `TLETarget`         | Satellite from a Two-Line Element set            | `TLETarget(tle=TLE(line0, line1, line2))` |
| `StateVectorTarget` | Satellite from a position/velocity state vector  | `StateVectorTarget(frame=..., sv=StateVector(...))` — meters, m/s |
| `EphemerisTarget`   | Precomputed time-tagged path                     | `EphemerisTarget(frame=..., jds=..., points=...)` |
| `RateTarget`        | Fixed angular rates from an initial position     | `RateTarget(rates=..., initial_coords=..., ...)` |

You hand the controller *what* the target is, not *how* to track it. At execution time the target is **adapted** to the mount's actual capabilities — propagated into an ephemeris or a rate stream (via `satkit`/`astropy`, with atmospheric refraction applied from live weather data when available) if the mount can't consume the native form directly. The same program works across mounts with different tracking interfaces.

## Programs that ship with SensorKit

Before writing your own, check the built-in ones:

- **Otto** (`sensorkit[otto]`) — a standalone satellite observation scheduler: give it NORAD IDs and collection parameters, and it maintains TLEs, computes passes, and publishes offers/tasks accordingly.
- **UDL** (`sensorkit[udl]`) — tasking from and publication to the Unified Data Library.

Both are configured through their own sections of the unified config, and are useful as reference implementations of full-featured programs.
