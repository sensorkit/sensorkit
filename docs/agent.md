# The agent

The agent (`sensorkit.auto.agent`) is what makes a SensorKit site autonomous. It continuously answers one question per controller — *should this be operating right now?* — and acts on the answer: initializing the sensor when a scheduled window opens, feeding it tasks from observing programs, and shutting it down when the weather turns or the night ends.

## How it decides

On every evaluation cycle, per controller, the agent:

1. Evaluates the controller's **modes** to find the time windows in which it should operate or stand by.
2. Intersects those windows with the **offer windows** published by observing programs, applying per-program **priorities**, to build a schedule.
3. Runs **constraints** (weather, safety monitors, arbitrary telemetry conditions) as continuous background monitors. An active constraint vetoes operation.
4. Applies any manual **overrides** you've set from the CLI.
5. Holds an *election* over all of these votes and drives the controller toward the winner — sending Init, Standby, or Shutdown tasks and starting or stopping program tasking as needed.

The agent's decisions and your control settings are persisted (in the KV store) and its state is event-sourced, so an agent restart resumes with the same overrides, exclusions, and control settings — no surprises after a power cycle.

The current election, per-source votes, and the schedule are all observable:

```bash
sensorkit agent status
```

## Configuration

The agent is configured in the `automation` section of the unified config:

```yaml
automation:
  # State assumed the very first time the agent runs, before any persisted state exists.
  first_run:
    operate_all: false        # default: don't start operating controllers immediately
    enable_scheduling: true   # default: scheduling is on

  controllers:
    MySensor:
      # Other controllers that must be up before this one starts
      depends_on: []

      # Used for schedule lookahead so the sensor is ready when a window opens
      estimated_startup_time: 60.0

      # Seconds a lifecycle task may disagree with reported state before it is a failure
      transition_grace: 10.0

      modes:
        - name: nighttime
          state: operate
          criteria:
            - when: time_range
              start: sunset
              end: sunrise

        - name: daytime-standby
          state: standby
          criteria:
            - when: time_range
              start: sunrise
              end: sunset

      constraints:
        - kind: weather
          provider: MyWeather
          humidity_max: 85.0
          humidity_deadband: 5.0
          wind_max: 15.0
          wind_deadband: 2.0
          rain_max: 0.0
          hold: 300.0

        - kind: safety
          provider: MySafetyMonitor
          ttl: 30.0

      tasking:
        - program: MyProgram
          priority: 5
        - program: BackupSurvey
          priority: 3
```

`transition_grace` bounds how long the controller may report an unexpected state while a lifecycle task sent by the agent runs.
It defaults to 10 seconds.
See [Controller state and tasking](#controller-state-and-tasking) for details.

---

## Modes

A **mode** names a target state (`operate` or `standby`) and the criteria under which it applies. All criteria in a mode must hold simultaneously; when multiple modes are active, `operate` wins over `standby`.

### `time_range`

A repeating daily window. `start` and `end` accept clock times (`"21:00"`) or the tokens `sunrise` and `sunset`, resolved from the controller's site position:

```yaml
- when: time_range
  start: sunset
  end: sunrise
```

### `tasking_available`

Active whenever a program has published offer windows — so the sensor only runs when there is actually work to do. Optionally restricted to one program:

```yaml
- when: tasking_available
  from_program: MyProgram    # optional
```

### `after_activity`

Active for a duration after the last offer window ends — e.g. to keep the sensor warm through short gaps rather than cycling the dome:

```yaml
- when: after_activity
  duration: "30m"
```

---

## Constraints

Constraints are continuous safety monitors. While one is **active**, the agent votes the controller down; when it clears, operation can resume. All constraints share three fields:

| Field      | Default | Meaning                                                                    |
|------------|---------|-----------------------------------------------------------------------------|
| `ttl`      | 30.0    | Seconds without fresh data before the constraint is considered stale       |
| `hold`     | 0.0     | Seconds the constraint stays active *after* conditions recover (anti-flap)  |
| `optional` | false   | If `true`, missing/stale data does **not** trip the constraint             |

!!! info "No data means unsafe"

    By default a constraint with no fresh data is treated as active — a dead weather station keeps the dome closed. Mark a constraint `optional` only if you're comfortable operating without its input.

### `weather`

Monitors a weather device's humidity, wind, and rain. Each threshold has a deadband: the value must fall back below `max − deadband` before the constraint clears, preventing rapid open/close cycling around a threshold.

```yaml
- kind: weather
  provider: MyWeather       # entity name of the weather device
  humidity_max: 85.0        # percent
  humidity_deadband: 5.0
  wind_max: 15.0            # m/s
  wind_deadband: 2.0
  rain_max: 0.0             # any rain trips it
  hold: 300.0               # stay closed 5 min after recovery
```

Omit any threshold you don't want checked.

### `safety`

Monitors a safety-monitor device (e.g. an Alpaca `safety_monitor`). Trips when the device reports unsafe, or when no report arrives within `ttl` seconds:

```yaml
- kind: safety
  provider: MySafetyMonitor
  ttl: 30.0
```

### `conditional`

A general-purpose constraint over **any telemetry keyword from any entity** — mount axis state, cooler temperature, a custom sensor you publish yourself. Point it at an entity, keyword, and (optionally) a field within it, then attach a condition:

```yaml
- kind: conditional
  entity: MyMount
  keyword: BasicWeather        # any published keyword model
  field: humidity              # dot-path into the model (optional)
  condition:
    kind: above                # above | below | equals | becomes | changes
    threshold: 75.0
    deadband: 2.0
  ttl: 30.0
```

`above`/`below` take a numeric `threshold` with optional hysteresis `deadband`; `equals`/`becomes` compare against a number, string, or boolean; `changes` fires on any value change.

---

## Programs and scheduling

The `tasking` list connects programs to the controller. When the schedule activates a program, the agent enables it against the controller and the program's task factory starts producing work. Higher `priority` wins when offer windows overlap; `interrupt: true` lets a program cut off a lower-priority task already in flight.

Programs can also be excluded from scheduling at runtime without touching config:

```bash
sensorkit agent exclude MyProgram
sensorkit agent include MyProgram
```

## Controller state and tasking

The agent compares the controller's reported state with the state chosen by the election.
It also checks the lifecycle context, the keyword values attached to the Init, Standby, or Shutdown task.
If both match and no transition is pending, the agent skips the lifecycle task.
After a controller failure, a matching observation alone is not enough, and the agent sends the lifecycle task again.
If a running lifecycle task is already transitioning to the desired state with the matching context, the agent waits up to five minutes or until that task expires, whichever comes first.
When the agent sends a lifecycle task, it interrupts any running controller task.

The agent keeps the selected program's existing tasking session when all of the following hold:

- The session is running on the desired controller with matching task contexts.
- The controller reports the desired state and lifecycle context, with no transition pending.
- The session has no stop pending.

Otherwise, the agent aborts that program's tasking before sending lifecycle tasks.
Before starting a new tasking session, the agent aborts any running controller task.
The agent manages tasking sessions it starts or adopts, but does not stop other programs' sessions.
Tasks from those sessions can still be interrupted on the controller.

### Expected state

The agent tracks its expected state separately from observed controller state.
On startup, the first settled observation sets the expectation, allowing the agent to adopt a controller that is already operating.

When the agent sends a lifecycle task, it expects TRANSITION.
The controller may keep reporting its previous state for up to `transition_grace`, for example while it aborts an interrupted task.
Observing TRANSITION clears the mismatch deadline, and the task may then run as long as its own timeout allows.
If the controller reports the target state before the task result arrives, a new `transition_grace` period starts.
The agent expects the target state once the task succeeds and a fresh state read confirms it.
The agent does not need to observe TRANSITION for a fast task.

Outside a lifecycle task, a controller that leaves the expected state has failed.
A state mismatch that lasts longer than `transition_grace` during a lifecycle task is also a failure.
Either case counts one controller failure, aborts the current procedure, and sets the local expectation to UNKNOWN.
A matching observation alone does not restore that expectation.
A program fault reported for the same incident does not count again.

A changed lifecycle context alone is not a failure.
The agent stops its program tasking gracefully and restores the desired context without delay.

Recovery runs with an UNKNOWN expectation and may leave the controller in any state.
The next successful lifecycle task establishes a settled expectation.
Shutdown bypasses the retry delay after a failure.

Two failures trigger recovery even if a lifecycle task succeeded between them.
The failure count starts over once a confirmed state has held for 15 minutes.

## CLI control

Three independent switches gate autonomy, all persisted across restarts:

```bash
# Master switch: may the agent act at all?
sensorkit agent global-control on|off

# Per-controller: may the agent manage this controller?
sensorkit agent control MySensor on|off

# Scheduler: should program tasking be scheduled?
sensorkit agent scheduling on|off
```

And for direct intervention:

```bash
# Force a controller up or down regardless of the election; 'none' clears it
sensorkit agent override MySensor up
sensorkit agent override MySensor down
sensorkit agent override MySensor none

# Inspect everything: switches, votes, elections, and the near-term schedule
sensorkit agent status
```

All commands accept `-e <name>` if your agent entity isn't called `agent` (the default).

## A typical bring-up

```bash
# 1. Load config (defines the agent + controllers)
sensorkit config load sensorkit.yaml

# 2. Start services (or `sensorkit go -c sensorkit.yaml -l` for everything at once)
sensorkit service run agent

# 3. Sanity-check the schedule and votes before arming it
sensorkit agent status

# 4. Arm
sensorkit agent global-control on
sensorkit agent scheduling on
```
