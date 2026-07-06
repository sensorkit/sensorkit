# Slack Module

Real-time Slack notifications and daily summaries for SensorKit observatory operations.

## Features

- **Real-time alerts** for device failures, weather changes, and operational events
- **Severity-based routing** (critical, warning, info) to different Slack channels
- **Alert deduplication** to prevent notification floods
- **Alert lifecycle** with resolved notifications (thread replies + checkmark reactions)
- **Nightly summaries** with observation counts, operational state durations, and health events
- **Block Kit formatting** with color-coded severity indicators

## Setup

### 1. Create a Slack Bot

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and create a new app
2. Under **OAuth & Permissions**, add these scopes:
   - `chat:write` — post messages
   - `reactions:write` — add reaction emojis
3. Install the app to your workspace
4. Copy the **Bot User OAuth Token** (`xoxb-...`)
5. Add the token to your `.env` file:
   ```
   SLACK_BOT_TOKEN=xoxb-your-token-here
   ```

### 2. Invite the Bot

Invite the bot to each channel it will post to:
```
/invite @YourBotName
```

### 3. Configure

Either configure the module in your deployment config — add
`sensorkit.slack.service` to `sensorkit.imports` and a top-level `slack:`
section with the fields below — or load a standalone config (see example
below) into the KV store:

```sh
sensorkit kv load slack.yaml
```

### 4. Run

`sensorkit go` runs the service along with the rest of the deployment config.
To run just this service:

```sh
sensorkit service run slack_service sensorkit.slack.service
```

## Example Config

```yaml
entity: slack_service
key: SlackConfig
value:
  env_file: .env
  channels:
    alerts:
      channel: "#observatory-alerts"
      severity: [critical, warning]
    log:
      channel: "#observatory-log"
      severity: [info]
    summary:
      channel: "#daily-summary"
      post_at: "06:00"
      timezone: "US/Hawaii"
  rules:
    - name: device_disconnect
      severity: critical
      state_watches:
        - keyword: Connected
          field: is_connected
          condition: {kind: becomes, threshold: false}
      deduplicate: 300

    - name: dome_close_failed
      severity: critical
      events: [CommandDone]
      entities: [dome1]
      filters:
        success: false
        command_id: CloseEnclosure

    - name: constraint_set
      severity: warning
      state_watches:
        - keyword: ConstraintStatus
          field: active
          condition: {kind: becomes, threshold: true}
      message_template: "Constraint set on `{entity}`: {reason} ({constraint_kind} from {provider})"

    - name: observation_complete
      severity: info
      events: [TaskExecutionState]
```

## Config Reference

### Channels

Each channel defines where messages are routed:

| Field | Type | Description |
|-------|------|-------------|
| `channel` | string | Slack channel name (e.g. `#observatory-alerts`) |
| `severity` | list | Severity levels routed here: `critical`, `warning`, `info` |
| `post_at` | string | Time to post daily summary (e.g. `"06:00"`). Makes this a summary channel. |
| `timezone` | string | Timezone for summary scheduling (e.g. `"US/Hawaii"`) |

### Rules

Each rule defines what triggers a notification:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Rule name (used for dedup keys and logging) |
| `severity` | string | `critical`, `warning`, or `info` (default: `info`) |
| `entities` | list | Entity names to watch (omit for all entities) |
| `events` | list | Event model names to match (e.g. `CommandDone`, `TaskExecutionState`) |
| `filters` | dict | Event fields that must match for the rule to fire (e.g. `success: false`) |
| `state_watches` | list | State keyword watches with conditions |
| `message_template` | string | Custom message template with `{variable}` interpolation |
| `deduplicate` | int | Seconds to suppress repeated triggers (omit for no dedup) |

### State Watch Conditions

Available condition types (set via `kind`):

| Condition | Description | Parameters |
|-----------|-------------|------------|
| `changes` | Any value change (default) | — |
| `becomes` | Value transitions to threshold | `threshold` |
| `equals` | Value equals threshold | `threshold` |
| `above` | Value exceeds threshold | `threshold`, `deadband` |
| `below` | Value below threshold | `threshold`, `deadband` |
| `crosses_above` | Upward crossing (fires once) | `threshold`, `deadband` |
| `crosses_below` | Downward crossing (fires once) | `threshold`, `deadband` |

### Message Templates

Templates support variable interpolation:

**Event templates:** `{event_model}`, `{entity}`, `{rule}`, plus flattened event fields

**State templates:** `{keyword}`, `{field}`, `{entity}`, `{previous}`, `{value}`, `{rule}`

## Severity Levels

- **Critical** — Red sidebar. Device failures, dome stuck, hardware errors. Things a technician needs to address.
- **Warning** — Yellow sidebar. Weather unsafe, minor anomalies. Informational but noteworthy.
- **Info** — Plain text. Observation completions, tracking acquired. Normal operational events.

## Deduplication

When `deduplicate` is set on a rule (in seconds), repeated triggers within that window are suppressed. After the window expires, a consolidated message reports how many additional occurrences were suppressed. This prevents notification floods from flapping devices.

## Daily Summary

Channels with `post_at` configured receive a daily summary at the specified local time. The summary includes:

- **Operational state durations** — time spent executing, idle, weather-closed, etc.
- **Observation counts** — attempted, completed, failed
- **Health events** — device connect/disconnect, safety state changes

## TODO

- **Per-program notifications** — program-scoped rules and routing (e.g. burr /
  otto / udl activity to their own channels, and per-program lines in the
  daily summary).
- **SENPAI results** — rules on published `SenpaiResult` telemetry, e.g.
  plate-solve failures or a dropping zero point / limiting magnitude.
