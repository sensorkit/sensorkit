# Agent

The agent (`sensorkit.auto.agent`) automates observatory operations. It monitors controllers, evaluates operating modes and safety constraints, and starts or stops observations according to a schedule.

## Launch

```bash
sensorkit service run sensorkit.auto.agent my-agent
```

## How it works

Every few seconds the agent:

1. Evaluates each controller's **modes** to determine when it should be operating
2. Intersects those intervals with **program offer windows** to build a schedule
3. Checks **constraints** (weather, safety) — if any activate, the controller shuts down
4. Applies any manual **overrides** set via the CLI
5. Sends the result to each controller: bring up or shut down

The agent persists its control state in the KV store, so settings survive restarts.

## Config

```yaml
entity: my-agent
key: AgentConfig
value:

  # Initial state the very first time the agent starts (before any persisted state exists)
  first_run:
    operate_all: false        # don't begin operating controllers immediately
    enable_scheduling: false  # scheduling stays off until manually enabled

  controllers:
    my-sensor:

      # Other controllers this one waits for before starting
      depends_on: []

      # Estimated seconds the controller needs to reach operating state
      # (used for schedule lookahead)
      estimated_startup_time: 60.0

      # Operating modes — evaluated in priority order (operate beats standby)
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

      # Safety constraints — if any activate, the controller shuts down
      constraints:
        - kind: weather
          provider: my-weather
          humidity_max: 85.0
          humidity_deadband: 5.0
          wind_max: 15.0
          wind_deadband: 2.0
          rain_max: 0.0
          hold_duration: 300.0

        - kind: safety
          provider: my-safety-monitor
          time_to_live: 30.0

      # Programs to schedule against this controller
      tasking:
        - program: my-program
          priority: 5
          interrupt: false
```

---

## Modes and criteria

A **mode** has a name, a target state (`operate` or `standby`), and one or more criteria that define when it is active. All criteria in a mode must be satisfied simultaneously. `operate` modes take priority over `standby` modes.

### `time_range`

Active during a repeating daily window. Start and end can be clock times (`HH:MM`) or the special tokens `sunrise` and `sunset`, which are resolved using the controller's configured site position.

```yaml
- when: time_range
  start: "21:00"
  end: "05:00"

# Or using solar events:
- when: time_range
  start: sunset
  end: sunrise
```

### `tasking_available`

Active when a program has published offer windows. Optionally restricted to a specific program.

```yaml
- when: tasking_available

# Or restrict to one program:
- when: tasking_available
  from_program: my-program
```

### `after_activity`

Active for a fixed duration after the last program offer window ends. Useful for keeping the sensor in standby for a warm-down period at the end of a night.

```yaml
- when: after_activity
  duration: "30m"
```

---

## Constraints

Constraints run continuously. When one activates, the agent votes to shut the controller down; when it clears, the agent votes to bring it back up.

### `weather`

Monitors a weather device for humidity, wind speed, and rain rate. A deadband prevents rapid toggling on threshold crossings, and `hold_duration` keeps the constraint active for a period after conditions recover.

```yaml
- kind: weather
  provider: my-weather        # entity name of the weather device
  humidity_max: 85.0          # percent
  humidity_deadband: 5.0
  wind_max: 15.0              # m/s
  wind_deadband: 2.0
  rain_max: 0.0               # mm/hr; 0.0 = any rain trips the constraint
  hold_duration: 300.0        # seconds to stay tripped after conditions recover
```

### `safety`

Monitors a safety monitor device. Activates when `is_safe` is `false`, or when no data has been received within `time_to_live` seconds.

```yaml
- kind: safety
  provider: my-safety-monitor
  time_to_live: 30.0
```

### `conditional`

Monitors any keyword field from any entity, with a configurable threshold condition.

```yaml
- kind: conditional
  entity: my-mount
  keyword: AxisEnabled
  field: right_ascension.enabled
  condition:
    type: threshold
    below: 1
  time_to_live: 30.0
```

---

## CLI commands

```bash
# Enable or disable global autonomous control
sensorkit agent global-control on  [-e my-agent]
sensorkit agent global-control off [-e my-agent]

# Enable or disable control for a single controller
sensorkit agent control my-sensor on  [-e my-agent]
sensorkit agent control my-sensor off [-e my-agent]

# Force a controller up or down, or let the election decide
sensorkit agent override my-sensor up   [-e my-agent]
sensorkit agent override my-sensor down [-e my-agent]
sensorkit agent override my-sensor none [-e my-agent]

# Enable or disable the scheduler
sensorkit agent scheduling on  [-e my-agent]
sensorkit agent scheduling off [-e my-agent]

# Add or remove a program from the schedule
sensorkit agent include my-program [-e my-agent]
sensorkit agent exclude my-program [-e my-agent]

# View current agent status
sensorkit agent status [-e my-agent]
```

The `-e` flag defaults to `agent` if omitted.

---

## Typical startup sequence

```bash
# 1. Load agent config
sensorkit kv load agent-config.yaml

# 2. Start the agent
sensorkit service run sensorkit.auto.agent my-agent

# 3. Enable global control (the agent will now manage controllers)
sensorkit agent global-control on -e my-agent

# 4. Enable the scheduler (agent will start/stop based on programs and modes)
sensorkit agent scheduling on -e my-agent

# 5. Check status
sensorkit agent status -e my-agent
```
